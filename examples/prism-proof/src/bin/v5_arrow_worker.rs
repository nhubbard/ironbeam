//! Small, independent Arrow IPC source and finalized-output Prism worker.

use ironbeam_core::{Element, Error};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Row {
    id: i32,
    label: String,
}

impl Element for Row {
    const TYPE_ID: &'static str = "tests/prism-arrow-row";
    const CODER_ID: &'static str = "tests/prism-arrow-row-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = self.id.to_le_bytes().to_vec();
        bytes.extend_from_slice(self.label.as_bytes());
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let id = bytes
            .get(..4)
            .ok_or_else(|| Error::incompatible("short Arrow row"))?
            .try_into()
            .map_err(|_| Error::incompatible("short Arrow row"))?;
        let label = String::from_utf8(bytes[4..].to_vec())
            .map_err(|_| Error::incompatible("invalid Arrow row label"))?;
        Ok(Self {
            id: i32::from_le_bytes(id),
            label,
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    ironbeam_dataflow::run_v5_from_env(
        |pipeline| {
            let input = std::env::var("IRONBEAM_V5_ARROW_INPUT")?;
            let staging = std::env::var("IRONBEAM_V5_ARROW_STAGING")?;
            let batch_staging = std::env::var("IRONBEAM_V5_ARROW_BATCH_STAGING")?;
            let rows = ironbeam_io_arrow::arrow_ipc_source::<Row>(pipeline, "arrow-source", input)?;
            ironbeam_io_arrow::arrow_ipc_row_fragment_sink(
                &rows.reshuffle_named("arrow-boundary")?,
                "arrow-final",
                &staging,
                &staging,
                "prism-arrow-fixture",
            )?;
            let batches = ironbeam_io_arrow::arrow_ipc_batches_source(
                pipeline,
                "arrow-batch-source",
                std::env::var("IRONBEAM_V5_ARROW_INPUT")?,
            )?;
            ironbeam_io_arrow::arrow_ipc_batch_fragment_sink(
                &batches.reshuffle_named("arrow-batch-boundary")?,
                "arrow-batch-final",
                &batch_staging,
                &batch_staging,
                "prism-arrow-fixture",
            )?;
            Ok(())
        },
        "prism-arrow-fixture",
    )
    .await?;
    Ok(())
}
