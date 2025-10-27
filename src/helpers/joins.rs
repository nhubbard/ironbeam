// ===== Joins (CoGroup-based) ==================================================

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use crate::{NodeId, PCollection, Partition, Pipeline, RFBound};
use crate::node::Node;
use crate::type_token::{vec_ops_for, TypeTag};

// Build a linear chain for a terminal node by snapshotting and back-walking.
fn chain_from(p: &Pipeline, terminal: NodeId) -> anyhow::Result<Vec<Node>> {
    let (mut nodes, edges) = p.snapshot();
    let mut chain = Vec::<Node>::new();
    let mut cur = terminal;
    loop {
        let n = nodes
            .remove(&cur)
            .ok_or_else(|| anyhow::anyhow!("missing node {cur:?}"))?;
        chain.push(n);
        if let Some((from, _)) = edges.iter().find(|(_, to)| *to == cur).cloned() {
            cur = from;
        } else {
            break;
        }
    }
    chain.reverse();
    Ok(chain)
}

// Insert a tiny dummy source (Vec<u8> with a single element) so the outer plan starts with Source.
fn insert_dummy_source(p: &Pipeline) -> NodeId {
    p.insert_node(Node::Source {
        payload: Arc::new(vec![0u8]),
        vec_ops: vec_ops_for::<u8>(),
        elem_tag: TypeTag::of::<u8>(),
    })
}

impl<K, V> PCollection<(K, V)>
where
    K: RFBound + Eq + Hash,
    V: RFBound,
{
    /// Inner join on key with another (K, W).
    pub fn join_inner<W>(&self, right: &PCollection<(K, W)>) -> PCollection<(K, (V, W))>
    where
        W: RFBound,
    {
        // Build subchains
        let left_chain = chain_from(&self.pipeline, self.id).expect("left chain build");
        let right_chain = chain_from(&right.pipeline, right.id).expect("right chain build");

        // Exec closure: Vec<(K,V)>, Vec<(K,W)> -> Vec<(K,(V,W))>
        let exec = Arc::new(|left_part: Partition, right_part: Partition| {
            let left_rows = *left_part
                .downcast::<Vec<(K, V)>>()
                .expect("cogroup exec: left type Vec<(K,V)>");
            let right_rows = *right_part
                .downcast::<Vec<(K, W)>>()
                .expect("cogroup exec: right type Vec<(K,W)>");

            let mut lm: HashMap<K, Vec<V>> = HashMap::new();
            for (k, v) in left_rows {
                lm.entry(k).or_default().push(v);
            }
            let mut rm: HashMap<K, Vec<W>> = HashMap::new();
            for (k, w) in right_rows {
                rm.entry(k).or_default().push(w);
            }

            let mut out: Vec<(K, (V, W))> = Vec::new();
            for (k, vs) in lm.into_iter() {
                if let Some(ws) = rm.get(&k) {
                    for v in &vs {
                        for w in ws {
                            out.push((k.clone(), (v.clone(), w.clone())));
                        }
                    }
                }
            }
            Box::new(out) as Partition
        });

        // Insert dummy source + CoGroup
        let source_id = insert_dummy_source(&self.pipeline);
        let coalesce_left = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, V)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, V)>>()
                    .expect("coalesce_left: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let coalesce_right = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, W)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, W)>>()
                    .expect("coalesce_right: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let id = self.pipeline.insert_node(Node::CoGroup {
            left_chain: left_chain.into(),
            right_chain: right_chain.into(),
            coalesce_left,
            coalesce_right,
            exec,
        });
        self.pipeline.connect(source_id, id);
        PCollection {
            pipeline: self.pipeline.clone(),
            id,
            _t: PhantomData,
        }
    }

    /// Left outer join on key with another (K, W) → (K, (V, Option<W>))
    pub fn join_left<W>(&self, right: &PCollection<(K, W)>) -> PCollection<(K, (V, Option<W>))>
    where
        W: RFBound,
    {
        let left_chain = chain_from(&self.pipeline, self.id).expect("left chain build");
        let right_chain = chain_from(&right.pipeline, right.id).expect("right chain build");

        let exec = Arc::new(|left_part: Partition, right_part: Partition| {
            let left_rows = *left_part
                .downcast::<Vec<(K, V)>>()
                .expect("cogroup exec: left type Vec<(K,V)>");
            let right_rows = *right_part
                .downcast::<Vec<(K, W)>>()
                .expect("cogroup exec: right type Vec<(K,W)>");

            let mut lm: HashMap<K, Vec<V>> = HashMap::new();
            for (k, v) in left_rows {
                lm.entry(k).or_default().push(v);
            }
            let mut rm: HashMap<K, Vec<W>> = HashMap::new();
            for (k, w) in right_rows {
                rm.entry(k).or_default().push(w);
            }

            let mut out: Vec<(K, (V, Option<W>))> = Vec::new();
            for (k, vs) in lm.into_iter() {
                match rm.get(&k) {
                    Some(ws) => {
                        for v in &vs {
                            for w in ws {
                                out.push((k.clone(), (v.clone(), Some(w.clone()))));
                            }
                        }
                    }
                    None => {
                        for v in vs {
                            out.push((k.clone(), (v, None)));
                        }
                    }
                }
            }
            Box::new(out) as Partition
        });

        let source_id = insert_dummy_source(&self.pipeline);
        let coalesce_left = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, V)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, V)>>()
                    .expect("coalesce_left: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let coalesce_right = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, W)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, W)>>()
                    .expect("coalesce_right: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let id = self.pipeline.insert_node(Node::CoGroup {
            left_chain: left_chain.into(),
            right_chain: right_chain.into(),
            coalesce_left,
            coalesce_right,
            exec,
        });
        self.pipeline.connect(source_id, id);
        PCollection {
            pipeline: self.pipeline.clone(),
            id,
            _t: PhantomData,
        }
    }

    /// Right outer join on key with another (K, W) → (K, (Option<V>, W))
    pub fn join_right<W>(&self, right: &PCollection<(K, W)>) -> PCollection<(K, (Option<V>, W))>
    where
        W: RFBound,
    {
        let left_chain = chain_from(&self.pipeline, self.id).expect("left chain build");
        let right_chain = chain_from(&right.pipeline, right.id).expect("right chain build");

        let exec = Arc::new(|left_part: Partition, right_part: Partition| {
            let left_rows = *left_part
                .downcast::<Vec<(K, V)>>()
                .expect("cogroup exec: left type Vec<(K,V)>");
            let right_rows = *right_part
                .downcast::<Vec<(K, W)>>()
                .expect("cogroup exec: right type Vec<(K,W)>");

            let mut lm: HashMap<K, Vec<V>> = HashMap::new();
            for (k, v) in left_rows {
                lm.entry(k).or_default().push(v);
            }
            let mut rm: HashMap<K, Vec<W>> = HashMap::new();
            for (k, w) in right_rows {
                rm.entry(k).or_default().push(w);
            }

            let mut out: Vec<(K, (Option<V>, W))> = Vec::new();

            for (k, ws) in rm.into_iter() {
                match lm.get(&k) {
                    Some(vs) => {
                        for w in &ws {
                            for v in vs {
                                out.push((k.clone(), (Some(v.clone()), w.clone())));
                            }
                        }
                    }
                    None => {
                        for w in ws {
                            out.push((k.clone(), (None, w)));
                        }
                    }
                }
            }
            Box::new(out) as Partition
        });

        let source_id = insert_dummy_source(&self.pipeline);
        let coalesce_left = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, V)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, V)>>()
                    .expect("coalesce_left: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let coalesce_right = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, W)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, W)>>()
                    .expect("coalesce_right: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let id = self.pipeline.insert_node(Node::CoGroup {
            left_chain: left_chain.into(),
            right_chain: right_chain.into(),
            coalesce_left,
            coalesce_right,
            exec,
        });
        self.pipeline.connect(source_id, id);
        PCollection {
            pipeline: self.pipeline.clone(),
            id,
            _t: PhantomData,
        }
    }

    /// Full outer join on key with another (K, W) → (K, (Option<V>, Option<W>))
    #[allow(clippy::type_complexity)]
    pub fn join_full<W>(
        &self,
        right: &PCollection<(K, W)>,
    ) -> PCollection<(K, (Option<V>, Option<W>))>
    where
        W: RFBound,
    {
        let left_chain = chain_from(&self.pipeline, self.id).expect("left chain build");
        let right_chain = chain_from(&right.pipeline, right.id).expect("right chain build");

        let exec = Arc::new(|left_part: Partition, right_part: Partition| {
            let left_rows = *left_part
                .downcast::<Vec<(K, V)>>()
                .expect("cogroup exec: left type Vec<(K,V)>");
            let right_rows = *right_part
                .downcast::<Vec<(K, W)>>()
                .expect("cogroup exec: right type Vec<(K,W)>");

            let mut lm: HashMap<K, Vec<V>> = HashMap::new();
            for (k, v) in left_rows {
                lm.entry(k).or_default().push(v);
            }
            let mut rm: HashMap<K, Vec<W>> = HashMap::new();
            for (k, w) in right_rows {
                rm.entry(k).or_default().push(w);
            }

            let mut out: Vec<(K, (Option<V>, Option<W>))> = Vec::new();
            use std::collections::HashSet;
            let mut keys: HashSet<K> = HashSet::new();
            keys.extend(lm.keys().cloned());
            keys.extend(rm.keys().cloned());

            for k in keys {
                match (lm.get(&k), rm.get(&k)) {
                    (Some(vs), Some(ws)) => {
                        for v in vs {
                            for w in ws {
                                out.push((k.clone(), (Some(v.clone()), Some(w.clone()))));
                            }
                        }
                    }
                    (Some(vs), None) => {
                        for v in vs {
                            out.push((k.clone(), (Some(v.clone()), None)));
                        }
                    }
                    (None, Some(ws)) => {
                        for w in ws {
                            out.push((k.clone(), (None, Some(w.clone()))));
                        }
                    }
                    (None, None) => {} // unreachable
                }
            }
            Box::new(out) as Partition
        });

        let source_id = insert_dummy_source(&self.pipeline);
        let coalesce_left = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, V)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, V)>>()
                    .expect("coalesce_left: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let coalesce_right = Arc::new(|parts: Vec<Partition>| -> Partition {
            let mut out: Vec<(K, W)> = Vec::new();
            for p in parts {
                let mut v = *p
                    .downcast::<Vec<(K, W)>>()
                    .expect("coalesce_right: wrong type");
                out.append(&mut v);
            }
            Box::new(out) as Partition
        });

        let id = self.pipeline.insert_node(Node::CoGroup {
            left_chain: left_chain.into(),
            right_chain: right_chain.into(),
            coalesce_left,
            coalesce_right,
            exec,
        });
        self.pipeline.connect(source_id, id);
        PCollection {
            pipeline: self.pipeline.clone(),
            id,
            _t: PhantomData,
        }
    }
}