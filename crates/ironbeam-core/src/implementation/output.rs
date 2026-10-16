/// Named terminal for a runner to collect or publish typed values.
#[derive(Clone)]
pub struct Output<T> {
    pipeline: Pipeline,
    key: String,
    marker: PhantomData<T>,
}
/// Named terminal that delegates writes to a registered sink.
#[derive(Clone)]
pub struct Sink<T> {
    pipeline: Pipeline,
    key: String,
    marker: PhantomData<T>,
}
impl<T> Sink<T> {
    /// Return the sink terminal's stable graph key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
    /// Snapshot the graph and local implementations for runner submission.
    pub fn snapshot(&self) -> Result<(Graph, Registry), Error> {
        self.pipeline.snapshot()
    }
    /// Whether two sinks were built from the same logical graph.
    #[must_use]
    pub fn shares_pipeline_with<U>(&self, other: &Sink<U>) -> bool {
        Arc::ptr_eq(&self.pipeline.inner, &other.pipeline.inner)
    }
}
impl<T> Output<T> {
    /// Return the output terminal's stable graph key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
    /// Snapshot the graph and local implementations for runner execution.
    pub fn snapshot(&self) -> Result<(Graph, Registry), Error> {
        self.pipeline.snapshot()
    }

    /// Whether two terminals belong to the same in-memory graph builder.
    #[must_use]
    pub fn shares_pipeline_with<U>(&self, other: &Output<U>) -> bool {
        Arc::ptr_eq(&self.pipeline.inner, &other.pipeline.inner)
    }
}
