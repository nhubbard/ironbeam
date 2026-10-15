impl V5BundleExecutor {
    fn apply_stateful_cell(
        &self,
        node: &Node,
        parent: &str,
        wire: &WireValue,
        prior: Option<&[u8]>,
    ) -> Result<(Option<Vec<u8>>, WireValue)> {
        let envelope = Envelope {
            value: self.registry.decode_element(parent, &wire.bytes)?,
            timestamp_ns: wire.timestamp_ns,
            window: node.time.window.clone(),
            assigned_window: wire.window,
            pane: wire.pane,
        };
        let (next, result) = self.registry.apply_stateful_value(&node.key, envelope, prior)?;
        Ok((next, WireValue {
            bytes: self.registry.encode_element(&node.key, result.value.as_ref())?,
            timestamp_ns: result.timestamp_ns,
            window: result.assigned_window,
            pane: result.pane,
            element_metadata: wire.element_metadata.clone(),
        }))
    }
}
