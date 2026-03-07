//! Flatten transform for merging multiple `PCollection`s into one.
//!
//! This module provides the [`flatten`] function that merges multiple
//! `PCollection<T>` of the same type into a single `PCollection<T>`.
//!
//! This is analogous to Apache Beam's `Flatten.pCollections()` transform.
//!
//! ## Example
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let pc1 = from_vec(&p, vec![1, 2, 3]);
//! let pc2 = from_vec(&p, vec![4, 5, 6]);
//! let pc3 = from_vec(&p, vec![7, 8, 9]);
//!
//! let merged = flatten(&[&pc1, &pc2, &pc3]);
//! let result = merged.collect_seq_sorted()?;
//! assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
//! # Ok(()) }
//! ```

use crate::node::Node;
use crate::type_token::{TypeTag, vec_ops_for};
use crate::{NodeId, PCollection, Partition, Pipeline, RFBound};
use anyhow::{Result, anyhow};
use std::marker::PhantomData;
use std::sync::Arc;

/// Build a linear execution chain ending at `terminal` by snapshotting the pipeline
/// and walking backwards through single-input edges.
///
/// This is an internal helper used to capture each collection's subplan so that
/// the runner can execute them as independent "subplans" before the flatten step.
///
/// # Errors
/// Returns an error if the pipeline snapshot is missing a referenced node.
fn chain_from(p: &Pipeline, terminal: NodeId) -> Result<Vec<Node>> {
    let (mut nodes, edges) = p.snapshot();
    let mut chain = Vec::<Node>::new();
    let mut cur = terminal;
    loop {
        let n = nodes
            .remove(&cur)
            .ok_or_else(|| anyhow!("missing node {cur:?}"))?;
        chain.push(n);
        if let Some((from, _)) = edges.iter().find(|(_, to)| *to == cur).copied() {
            cur = from;
        } else {
            break;
        }
    }
    chain.reverse();
    Ok(chain)
}

/// Insert a tiny dummy `Source` so the outer flatten plan always starts with a
/// `Source` node (as expected by the runner).
///
/// The dummy payload is a `Vec<u8>` of length 1. It does not participate in the
/// flatten semantics--it's only a structural anchor for the execution plan.
fn insert_dummy_source(p: &Pipeline) -> NodeId {
    p.insert_node(Node::Source {
        payload: Arc::new(vec![0u8]),
        vec_ops: vec_ops_for::<u8>(),
        elem_tag: TypeTag::of::<u8>(),
    })
}

/// Flatten multiple `PCollection<T>` into a single `PCollection<T>`.
///
/// Takes a slice of references to `PCollection<T>` and merges them into one
/// collection containing all elements from all inputs. The order of elements
/// within each input collection is preserved, but the relative order between
/// different input collections is not guaranteed.
///
/// This is useful for:
/// - Combining results from multiple data sources
/// - Merging branches of processing logic
/// - Union operations in batch ETL pipelines
///
/// # Arguments
/// - `collections`: A slice of references to `PCollection<T>` to merge
///
/// # Returns
/// A new `PCollection<T>` containing all elements from all input collections
///
/// # Panics
/// Panics if the chain building operation fails or if types are mismatched.
///
/// # Example
/// ```no_run
/// use ironbeam::*;
/// use anyhow::Result;
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
/// let numbers1 = from_vec(&p, vec![1, 2, 3]);
/// let numbers2 = from_vec(&p, vec![4, 5]);
/// let numbers3 = from_vec(&p, vec![6, 7, 8, 9]);
///
/// let all_numbers = flatten(&[&numbers1, &numbers2, &numbers3]);
/// let result = all_numbers.collect_seq_sorted()?;
/// assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
/// # Ok(()) }
/// ```
#[must_use]
pub fn flatten<T>(collections: &[&PCollection<T>]) -> PCollection<T>
where
    T: RFBound,
{
    assert!(!collections.is_empty(), "flatten requires at least one input collection");

    // Use the first collection's pipeline
    let pipeline = &collections[0].pipeline;

    // Build subchains for each input collection
    let chains: Vec<Vec<Node>> = collections
        .iter()
        .map(|pc| chain_from(&pc.pipeline, pc.id).expect("chain build"))
        .collect();

    // Coalesce function: merges per-partition outputs from a single subplan
    let coalesce = Arc::new(|parts: Vec<Partition>| -> Partition {
        let mut out: Vec<T> = Vec::new();
        for p in parts {
            let mut v = *p
                .downcast::<Vec<T>>()
                .expect("coalesce: wrong type");
            out.append(&mut v);
        }
        Box::new(out) as Partition
    });

    // Merge function: combines coalesced outputs from all subplans
    let merge = Arc::new(|coalesced_inputs: Vec<Partition>| -> Partition {
        let mut result: Vec<T> = Vec::new();
        for p in coalesced_inputs {
            let mut v = *p
                .downcast::<Vec<T>>()
                .expect("merge: wrong type");
            result.append(&mut v);
        }
        Box::new(result) as Partition
    });

    // Insert dummy source + Flatten node
    let source_id = insert_dummy_source(pipeline);
    let id = pipeline.insert_node(Node::Flatten {
        chains: Arc::new(chains),
        coalesce,
        merge,
    });
    pipeline.connect(source_id, id);

    PCollection {
        pipeline: pipeline.clone(),
        id,
        _t: PhantomData,
    }
}
