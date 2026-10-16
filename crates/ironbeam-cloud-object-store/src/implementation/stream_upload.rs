//! Bounded upload and conditional publication of one immutable object.

use std::io::{self, Read};
use std::sync::{Arc, mpsc};

use bytes::Bytes;
use ironbeam_cloud_objects::{CloudIOError, CloudResult, ErrorKind};
use object_store::{ObjectStore, ObjectStoreExt, buffered::BufWriter, path::Path};
use tokio::io::AsyncWriteExt;

use super::remote_objects::store_error;

pub(super) fn conditional_create_from(
    store: Arc<dyn ObjectStore>,
    runtime: &tokio::runtime::Runtime,
    key: &str,
    reader: &mut dyn Read,
) -> CloudResult<()> {
    let destination = Path::parse(key)
        .map_err(|error| CloudIOError::new(ErrorKind::InvalidInput, error.to_string()))?;
    let temporary = Path::from(format!("__ironbeam_uploads/{}", uuid::Uuid::new_v4()));
    let (send, receive) = mpsc::sync_channel::<io::Result<Bytes>>(2);
    let (finished, result) = mpsc::sync_channel(1);
    runtime.spawn(async move {
        let (forward, mut chunks) = tokio::sync::mpsc::channel(2);
        tokio::task::spawn_blocking(move || {
            for chunk in receive {
                if forward.blocking_send(chunk).is_err() {
                    break;
                }
            }
        });
        let mut upload =
            BufWriter::with_capacity(Arc::clone(&store), temporary.clone(), 5 * 1024 * 1024)
                .with_max_concurrency(2);
        let uploaded = loop {
            match chunks.recv().await {
                Some(Ok(chunk)) => {
                    if let Err(error) = upload.put(chunk).await {
                        let _ = upload.abort().await;
                        break Err(store_error(error));
                    }
                }
                Some(Err(error)) => {
                    let _ = upload.abort().await;
                    break Err(CloudIOError::new(ErrorKind::Other, error.to_string()));
                }
                None => {
                    break upload
                        .shutdown()
                        .await
                        .map_err(|error| CloudIOError::new(ErrorKind::Other, error.to_string()));
                }
            }
        };
        let outcome = if let Err(error) = uploaded {
            let _ = store.delete(&temporary).await;
            Err(error)
        } else {
            let copied = store
                .copy_if_not_exists(&temporary, &destination)
                .await
                .map_err(store_error);
            let _ = store.delete(&temporary).await;
            copied
        };
        let _ = finished.send(outcome);
    });
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                if send
                    .send(Ok(Bytes::copy_from_slice(&buffer[..count])))
                    .is_err()
                {
                    break;
                }
            }
            Err(error) => {
                let _ = send.send(Err(error));
                break;
            }
        }
    }
    drop(send);
    result.recv().map_err(|_| {
        CloudIOError::new(
            ErrorKind::InternalError,
            "object upload task ended unexpectedly",
        )
    })?
}
