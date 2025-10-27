use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use crate::{PCollection, Partition, RFBound};
use crate::node::Node;

impl<T: RFBound> PCollection<T> {
    /// Derive a key and produce (K, T)
    pub fn key_by<K, F>(self, key_fn: F) -> PCollection<(K, T)>
    where
        K: RFBound + Eq + Hash,
        F: 'static + Send + Sync + Fn(&T) -> K,
    {
        self.map(move |t| (key_fn(t), t.clone()))
    }
}

impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, V)> {
    /// Group values by key: (K, V) -> (K, Vec<V>)
    pub fn group_by_key(self) -> PCollection<(K, Vec<V>)> {
        // typed closures, so no runtime type checks needed
        let local = Arc::new(|p: Partition| -> Partition {
            let kv = *p.downcast::<Vec<(K, V)>>().expect("GBK local: bad input");
            let mut m: HashMap<K, Vec<V>> = HashMap::new();
            for (k, v) in kv {
                m.entry(k).or_default().push(v);
            }
            Box::new(m) as Partition
        });

        let merge = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut acc: HashMap<K, Vec<V>> = HashMap::new();
            for p in parts {
                let m = *p
                    .downcast::<HashMap<K, Vec<V>>>()
                    .expect("GBK merge: bad part");
                for (k, vs) in m {
                    acc.entry(k).or_default().extend(vs);
                }
            }
            Box::new(acc.into_iter().collect::<Vec<(K, Vec<V>)>>()) as Partition
        });

        let id = self.pipeline.insert_node(Node::GroupByKey { local, merge });
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}