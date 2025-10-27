use crate::{PCollection, RFBound, TimestampMs, Timestamped};

impl<T: RFBound> PCollection<T> {
    /// Attach event timestamps using a user function.
    /// Example: .attach_timestamps(|x| x.ts_field)
    pub fn attach_timestamps<F>(self, ts_fn: F) -> PCollection<Timestamped<T>>
    where
        F: 'static + Send + Sync + Fn(&T) -> TimestampMs,
    {
        self.map(move |t| Timestamped::new(ts_fn(t), t.clone()))
    }
}

impl<T: RFBound> PCollection<(TimestampMs, T)> {
    /// Convenience: if your input already is (ts, value), convert to Timestamped<T>.
    pub fn to_timestamped(self) -> PCollection<Timestamped<T>> {
        self.map(|p| Timestamped::new(p.0, p.1.clone()))
    }
}