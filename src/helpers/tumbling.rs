use std::hash::Hash;
use crate::{PCollection, RFBound, Timestamped, Window};

impl<T: RFBound> PCollection<Timestamped<T>> {
    /// Key by a Window computed via tumbling of the attached timestamp.
    /// Returns (Window, T)
    pub fn key_by_window(self, size_ms: u64, offset_ms: u64) -> PCollection<(Window, T)> {
        self.map(move |ev: &Timestamped<T>| {
            let w = Window::tumble(ev.ts, size_ms, offset_ms);
            (w, ev.value.clone())
        })
    }

    /// Group by window → (Window, Vec<T>)
    pub fn group_by_window(self, size_ms: u64, offset_ms: u64) -> PCollection<(Window, Vec<T>)> {
        self.key_by_window(size_ms, offset_ms).group_by_key()
    }
}

// -------------- Tumbling windows: keyed --------------
// Input: (K, Timestamped<V>)  → output: ((K, Window), V)
impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, Timestamped<V>)> {
    pub fn key_by_window(self, size_ms: u64, offset_ms: u64) -> PCollection<((K, Window), V)> {
        self.map(move |kv: &(K, Timestamped<V>)| {
            let w = Window::tumble(kv.1.ts, size_ms, offset_ms);
            ((kv.0.clone(), w), kv.1.value.clone())
        })
    }

    /// Group by (K, Window) → ((K, Window), Vec<V>)
    pub fn group_by_key_and_window(
        self,
        size_ms: u64,
        offset_ms: u64,
    ) -> PCollection<((K, Window), Vec<V>)> {
        self.key_by_window(size_ms, offset_ms).group_by_key()
    }
}