use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use crate::{CombineFn, PCollection, Partition, RFBound};
use crate::collection::LiftableCombiner;
use crate::node::Node;

impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, V)> {
    /// Generic combine-by-key using a user-supplied `CombineFn`
    pub fn combine_values<C, A, O>(self, comb: C) -> PCollection<(K, O)>
    where
        C: CombineFn<V, A, O> + 'static,
        A: Send + 'static + Sync,
        O: RFBound,
    {
        let comb = Arc::new(comb);

        let local = {
            let comb = Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let kv = *p
                    .downcast::<Vec<(K, V)>>()
                    .expect("combine local: bad input");
                let mut map: HashMap<K, A> = HashMap::new();
                for (k, v) in kv {
                    comb.add_input(map.entry(k).or_insert_with(|| comb.create()), v);
                }
                Box::new(map) as Partition
            })
        };

        let merge = {
            let comb = Arc::clone(&comb);
            Arc::new(move |parts: Vec<Partition>| -> Partition {
                let mut accs: HashMap<K, A> = HashMap::new();
                for p in parts {
                    let m = *p
                        .downcast::<HashMap<K, A>>()
                        .expect("combine merge: bad part");
                    for (k, a) in m {
                        comb.merge(accs.entry(k).or_insert_with(|| comb.create()), a);
                    }
                }
                let out: Vec<(K, O)> = accs.into_iter().map(|(k, a)| (k, comb.finish(a))).collect();
                Box::new(out) as Partition
            })
        };

        let id = self.pipeline.insert_node(Node::CombineValues {
            local_pairs: local, // existing closure from your current code
            local_groups: None, // not liftable from grouped input by default
            merge,              // existing
        });
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}

impl<K, V> PCollection<(K, Vec<V>)>
where
    K: RFBound + Eq + Hash,
    V: RFBound,
{
    /// Lifted combine: to be used right after `group_by_key()`.
    /// Skips the GBK barrier by creating accumulators from each group's full value slice.
    pub fn combine_values_lifted<C, A, O>(self, comb: C) -> PCollection<(K, O)>
    where
        C: CombineFn<V, A, O> + LiftableCombiner<V, A, O> + 'static,
        A: Send + Sync + 'static,
        O: RFBound,
    {
        use crate::node::Node;
        use crate::type_token::Partition;
        use std::collections::HashMap;
        use std::marker::PhantomData;
        use std::sync::Arc;

        let comb = Arc::new(comb);

        // regular local_pairs: Vec<(K,V)> → HashMap<K, A>
        let local_pairs = {
            let comb = Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let kv = *p
                    .downcast::<Vec<(K, V)>>()
                    .expect("combine local_pairs: expected Vec<(K, V)>");
                let mut map: HashMap<K, A> = HashMap::new();
                for (k, v) in kv {
                    comb.add_input(map.entry(k).or_insert_with(|| comb.create()), v);
                }
                Box::new(map) as Partition
            })
        };

        // lifted local_groups: Vec<(K, Vec<V>)> → HashMap<K, A>
        let local_groups = {
            let comb = Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let kvv = *p
                    .downcast::<Vec<(K, Vec<V>)>>()
                    .expect("lifted combine local: expected Vec<(K, Vec<V>)>");
                let mut map: HashMap<K, A> = HashMap::new();
                for (k, vs) in kvv {
                    let acc = comb.build_from_group(&vs);
                    map.insert(k, acc);
                }
                Box::new(map) as Partition
            })
        };

        // merge: Vec<HashMap<K,A>> → Vec<(K,O)>
        let merge = {
            let comb = Arc::clone(&comb);
            Arc::new(move |parts: Vec<Partition>| -> Partition {
                let mut accs: HashMap<K, A> = HashMap::new();
                for p in parts {
                    let m = *p
                        .downcast::<HashMap<K, A>>()
                        .expect("lifted combine merge: expected HashMap<K, A>");
                    for (k, a) in m {
                        let entry = accs.entry(k).or_insert_with(|| comb.create());
                        comb.merge(entry, a);
                    }
                }
                let out: Vec<(K, O)> = accs.into_iter().map(|(k, a)| (k, comb.finish(a))).collect();
                Box::new(out) as Partition
            })
        };

        let id = self.pipeline.insert_node(Node::CombineValues {
            local_pairs,
            local_groups: Some(local_groups),
            merge,
        });
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}