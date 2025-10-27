use crate::{PCollection, RFBound};

impl<T: RFBound + Ord> PCollection<T> {
    /// Collect (seq) and sort to a total order.
    pub fn collect_seq_sorted(self) -> anyhow::Result<Vec<T>> {
        let mut v = self.collect_seq()?;
        v.sort();
        Ok(v)
    }
}

impl<T: RFBound + Ord> PCollection<T> {
    /// Collect (par) and sort to a total order.
    pub fn collect_par_sorted(self, parts: Option<usize>, chunk: Option<usize>) -> anyhow::Result<Vec<T>> {
        let mut v = self.collect_par(parts, chunk)?;
        v.sort();
        Ok(v)
    }
}

// Keyed convenience: sort by key only
impl<K: RFBound + Ord, V: RFBound> PCollection<(K, V)> {
    pub fn collect_par_sorted_by_key(
        self,
        parts: Option<usize>,
        chunk: Option<usize>,
    ) -> anyhow::Result<Vec<(K, V)>> {
        let mut v = self.collect_par(parts, chunk)?;
        v.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(v)
    }
}