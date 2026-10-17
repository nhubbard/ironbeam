use super::*;

// ============================================================================
// FakeQueueIO
// ============================================================================

#[derive(Clone)]
/// In-memory queue service for tests.
pub struct FakeQueueIO {
    queues: Arc<Mutex<HashMap<String, Vec<QueueMessage>>>>,
    message_counter: Arc<Mutex<u64>>,
}

impl FakeQueueIO {
    #[must_use]
    /// Create an empty in-memory queue service for tests.
    pub fn new() -> Self {
        Self {
            queues: Arc::new(Mutex::new(HashMap::new())),
            message_counter: Arc::new(Mutex::new(0)),
        }
    }

    fn next_id(&self) -> String {
        let mut counter = self
            .message_counter
            .lock()
            .expect("message_counter mutex poisoned");
        *counter += 1;
        let id = *counter;
        drop(counter);
        format!("msg-{id}")
    }
}

impl Default for FakeQueueIO {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueIO for FakeQueueIO {
    fn send(
        &self,
        queue: &str,
        body: &str,
        attributes: HashMap<String, String>,
    ) -> CloudResult<String> {
        let msg_id = self.next_id();
        let message = QueueMessage {
            id: msg_id.clone(),
            receipt_handle: format!("receipt-{msg_id}"),
            body: body.to_string(),
            attributes,
            receive_count: 0,
        };

        self.queues
            .lock()
            .expect("queues mutex poisoned")
            .entry(queue.to_string())
            .or_default()
            .push(message);

        Ok(msg_id)
    }

    fn send_batch(
        &self,
        queue: &str,
        messages: Vec<(String, HashMap<String, String>)>,
    ) -> CloudResult<Vec<String>> {
        messages
            .into_iter()
            .map(|(body, attrs)| self.send(queue, &body, attrs))
            .collect()
    }

    fn receive(
        &self,
        queue: &str,
        max_messages: u32,
        _visibility_timeout_secs: u32,
    ) -> CloudResult<Vec<QueueMessage>> {
        let mut queues = self.queues.lock().expect("queues mutex poisoned");
        let q = queues.entry(queue.to_string()).or_default();

        let count = std::cmp::min(max_messages as usize, q.len());
        let messages = q.drain(0..count).collect();
        drop(queues);
        Ok(messages)
    }

    fn delete(&self, _queue: &str, _receipt_handle: &str) -> CloudResult<()> {
        Ok(())
    }

    fn delete_batch(&self, _queue: &str, _receipt_handles: Vec<String>) -> CloudResult<()> {
        Ok(())
    }

    fn queue_size(&self, queue: &str) -> CloudResult<u64> {
        let queues = self.queues.lock().expect("queues mutex poisoned");
        Ok(queues.get(queue).map_or(0, |q| q.len() as u64))
    }

    fn purge(&self, queue: &str) -> CloudResult<()> {
        if let Some(q) = self
            .queues
            .lock()
            .expect("queues mutex poisoned")
            .get_mut(queue)
        {
            q.clear();
        }
        Ok(())
    }
}
