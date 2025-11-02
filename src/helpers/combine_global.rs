//! rustflow – global (non-keyed) combiners
//!
//! Adds Beam-style `CombineGlobally` with optional fanout and a lifted
//! fast-path that can build accumulators from whole partitions.
//!
//! - `combine_globally`: folds all elements `T` into a single `O` via a
//!   user-provided `CombineFn<V, A, O>`.
//! - `combine_globally_lifted`: same, but uses `LiftableCombiner::build_from_group`
//!   to construct `A` from each partition's full slice for better locality.
//!
//! Both APIs accept an optional `fanout`: during parallel execution we reduce
//! accumulators in rounds, merging at most `fanout` accumulators per round to
//! shorten critical paths for very large datasets.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::collection::{CombineFn, LiftableCombiner};
use crate::node::Node;
use crate::{PCollection, Partition, RFBound};

impl<T: RFBound> PCollection<T> {
    /// Combine all elements (no key) into a single output using a [`CombineFn`].
    ///
    /// # Parameters
    /// - `comb`: the combiner (`create/add_input/merge/finish`)
    /// - `fanout`: if set, merges accumulators in rounds of at most this size.
    ///   Use small values (e.g., 8 or 16) to limit merge breadth on huge inputs.
    ///
    /// # Semantics
    /// Produces exactly **one** element even for empty inputs (by calling
    /// `finish(create())`).
    ///
    /// # Examples
    /// ```no_run
    /// use rustflow::*;
    /// use rustflow::combiners::Sum;
    ///
    /// let p = Pipeline::default();
    /// let coll = from_vec(&p, vec![1u64, 2, 3, 4]);
    /// let out = coll.combine_globally(Sum::<u64>::default(), Some(8))
    ///               .collect_seq()?;
    /// assert_eq!(out, vec![10u64]);
    /// # Ok::<_, anyhow::Error>(())
    /// ```
    pub fn combine_globally<C, A, O>(self, comb: C, fanout: Option<usize>) -> PCollection<O>
    where
        C: CombineFn<T, A, O> + 'static,
        A: Send + Sync + 'static,
        O: RFBound,
    {
        let comb = Arc::new(comb);

        // local: Vec<T> -> A (via the creation and add_input steps)
        let local = {
            let comb = Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let rows = *p
                    .downcast::<Vec<T>>()
                    .expect("CombineGlobally local: expected Vec<T>");
                let mut acc = comb.create();
                for v in rows {
                    comb.add_input(&mut acc, v);
                }
                Box::new(acc) as Partition
            })
        };

        // merge: Vec<A> -> A
        let merge = {
            let comb = Arc::clone(&comb);
            Arc::new(move |parts: Vec<Partition>| -> Partition {
                let mut it = parts.into_iter();
                let mut acc = if let Some(first) = it.next() {
                    *first
                        .downcast::<A>()
                        .expect("CombineGlobally merge: bad part")
                } else {
                    comb.create()
                };
                for p in it {
                    let a = *p.downcast::<A>().expect("CombineGlobally merge: bad part");
                    comb.merge(&mut acc, a);
                }
                Box::new(acc) as Partition
            })
        };

        // finish: A -> Vec<O> (singleton)
        let finish = {
            let comb = Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let acc = *p
                    .downcast::<A>()
                    .expect("CombineGlobally finish: bad acc type");
                let out = vec![comb.finish(acc)];
                Box::new(out) as Partition
            })
        };

        let id = self.pipeline.insert_node(Node::CombineGlobal {
            local,
            merge,
            finish,
            fanout,
        });
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }

    /// Combine all elements (no key) with a **lifted** local path.
    ///
    /// Uses [`LiftableCombiner::build_from_group`] to build each partition
    /// accumulator directly from `&[T]`, skipping per-element calls when
    /// profitable (e.g., `Count`, `TopK`, etc.).
    ///
    /// See [`combine_globally`] for fanout semantics and example usage.
    pub fn combine_globally_lifted<C, A, O>(self, comb: C, fanout: Option<usize>) -> PCollection<O>
    where
        C: CombineFn<T, A, O> + LiftableCombiner<T, A, O> + 'static,
        A: Send + Sync + 'static,
        O: RFBound,
    {
        let comb = Arc::new(comb);

        // local (lifted): Vec<T> -> A via build_from_group(&[T])
        let local = {
            let comb = Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let rows = *p
                    .downcast::<Vec<T>>()
                    .expect("CombineGlobally(lifted) local: expected Vec<T>");
                let acc = comb.build_from_group(&rows);
                Box::new(acc) as Partition
            })
        };

        // merge: Vec<A> -> A
        let merge = {
            let comb = Arc::clone(&comb);
            Arc::new(move |parts: Vec<Partition>| -> Partition {
                let mut it = parts.into_iter();
                let mut acc = if let Some(first) = it.next() {
                    *first
                        .downcast::<A>()
                        .expect("CombineGlobally(lifted) merge: bad part")
                } else {
                    comb.create()
                };
                for p in it {
                    let a = *p
                        .downcast::<A>()
                        .expect("CombineGlobally(lifted) merge: bad part");
                    comb.merge(&mut acc, a);
                }
                Box::new(acc) as Partition
            })
        };

        // finish: A -> Vec<O>
        let finish = {
            let comb = Arc::clone(&comb);
            Arc::new(move |p: Partition| -> Partition {
                let acc = *p
                    .downcast::<A>()
                    .expect("CombineGlobally(lifted) finish: bad acc type");
                let out = vec![comb.finish(acc)];
                Box::new(out) as Partition
            })
        };

        let id = self.pipeline.insert_node(Node::CombineGlobal {
            local,
            merge,
            finish,
            fanout,
        });
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}
