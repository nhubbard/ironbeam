//! Dedicated tests for the per-PCollection element coders (`coders` feature).
//!
//! Covers, per the adoption plan's Phase 7:
//! 1. `PostcardCoder` round-trips (primitives, strings, tuples, a struct, an
//!    enum), multi-element partitions, the downcast-mismatch error path, and
//!    `type_name`.
//! 2. `PostcardKvCoder`: independent half-encoding, `is_kv`, in-bundle bytes
//!    matching the plain tuple coder, and the plain coder rejecting a KV split.
//! 3. Registration completeness: every node-creating combinator attaches a
//!    coder for its output, the pre-`GroupByKey` edge is upgraded to a KV
//!    coder, and join inputs are too.
//! 4. The `set_coder_override` escape hatch.
//! 5. End-to-end: the build-time coder round-trips the actual runner output
//!    partition, proving it matches the runtime partition shape.

#![cfg(feature = "coders")]

use std::sync::Arc;

use ironbeam::testing::PCollectionDebugExt;
use ironbeam::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Point {
    x: i64,
    y: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Shape {
    Dot,
    Line(u32),
    Named(String),
}

// ───────────────────────── 1. Coder round-trips ─────────────────────────────

/// Encode a partition of `elems`, then decode each one-element blob back and
/// assert the recovered elements equal the originals in order.
fn assert_roundtrip<T>(elems: &[T])
where
    T: Serialize
        + serde::de::DeserializeOwned
        + Clone
        + Send
        + Sync
        + 'static
        + PartialEq
        + std::fmt::Debug,
{
    let coder = PostcardCoder::<T>::new();
    let part: Partition = Box::new(elems.to_vec());
    let encoded = coder.encode_all(part).expect("encode_all");
    assert_eq!(encoded.len(), elems.len(), "one byte-vec per element");

    let mut decoded = Vec::with_capacity(elems.len());
    for bytes in &encoded {
        let p = coder.decode_one(bytes).expect("decode_one");
        let v = *p.downcast::<Vec<T>>().expect("decode_one yields Vec<T>");
        assert_eq!(v.len(), 1, "decode_one yields a one-element partition");
        decoded.push(v.into_iter().next().unwrap());
    }
    assert_eq!(decoded.as_slice(), elems);
}

#[test]
fn roundtrip_primitives_strings_tuples_structs_enums() {
    assert_roundtrip(&[1u64, 2, 3, u64::MAX]);
    assert_roundtrip(&[-1i32, 0, 7]);
    assert_roundtrip(&["a".to_string(), String::new(), "héllo".to_string()]);
    assert_roundtrip(&[(1u64, "x".to_string()), (2, "y".to_string())]);
    assert_roundtrip(&[Point { x: 1, y: -2 }, Point { x: 0, y: 0 }]);
    assert_roundtrip(&[Shape::Dot, Shape::Line(3), Shape::Named("s".into())]);
}

#[test]
fn encode_multi_element_partition_yields_one_blob_each() {
    let coder = PostcardCoder::<u64>::new();
    let part: Partition = Box::new(vec![10u64, 20, 30, 40]);
    assert_eq!(coder.encode_all(part).unwrap().len(), 4);
}

#[test]
fn downcast_mismatch_is_error() {
    let coder = PostcardCoder::<u64>::new();
    let wrong: Partition = Box::new(vec!["not a u64".to_string()]);
    assert!(coder.encode_all(wrong).is_err());
}

#[test]
fn type_name_reports_concrete_type_and_plain_is_not_kv() {
    let coder = PostcardCoder::<u64>::new();
    assert!(coder.type_name().contains("u64"));
    assert!(!coder.is_kv());
}

// ───────────────────────────── 2. KV coder ──────────────────────────────────

#[test]
fn kv_coder_splits_into_independently_encoded_halves() {
    let coder = PostcardKvCoder::<String, u64>::new();
    assert!(coder.is_kv());

    let part: Partition = Box::new(vec![("alice".to_string(), 1u64), ("bob".to_string(), 2u64)]);
    let pairs = coder.encode_kv_pairs(part).expect("encode_kv_pairs");
    assert_eq!(pairs.len(), 2);

    // Each half must decode independently with the matching plain coder.
    let kc = PostcardCoder::<String>::new();
    let vc = PostcardCoder::<u64>::new();
    let k0 = *kc
        .decode_one(&pairs[0].0)
        .unwrap()
        .downcast::<Vec<String>>()
        .unwrap();
    let v0 = *vc
        .decode_one(&pairs[0].1)
        .unwrap()
        .downcast::<Vec<u64>>()
        .unwrap();
    assert_eq!(k0, vec!["alice".to_string()]);
    assert_eq!(v0, vec![1u64]);
}

#[test]
fn kv_coder_in_bundle_bytes_match_plain_tuple_coder() {
    let kv = PostcardKvCoder::<String, u64>::new();
    let plain = PostcardCoder::<(String, u64)>::new();
    let data = vec![("x".to_string(), 42u64), ("y".to_string(), 7)];

    let a: Partition = Box::new(data.clone());
    let b: Partition = Box::new(data);
    assert_eq!(
        kv.encode_all(a).unwrap(),
        plain.encode_all(b).unwrap(),
        "in-bundle KV encoding must match the plain tuple coder"
    );
}

#[test]
fn plain_coder_rejects_kv_split_with_clear_message() {
    let coder = PostcardCoder::<(String, u64)>::new();
    let part: Partition = Box::new(vec![("k".to_string(), 1u64)]);
    let err = coder.encode_kv_pairs(part).unwrap_err();
    assert!(format!("{err}").contains("not KV-aware"), "got: {err}");
}

// ─────────────────────── 3. Registration completeness ───────────────────────

#[test]
fn every_combinator_registers_a_coder_for_its_output() {
    let p = Pipeline::default();
    let mut coded: Vec<NodeId> = Vec::new();

    // Source.
    let src = from_vec(&p, vec![1u64, 2, 3, 4]);
    coded.push(src.node_id());

    // Stateless / batch / passthrough combinators.
    coded.push(src.clone().map(|x| x + 1).node_id());
    coded.push(src.clone().filter(|x| x % 2 == 0).node_id());
    coded.push(src.clone().flat_map(|x| vec![*x, *x]).node_id());
    coded.push(src.clone().take(2).node_id());
    coded.push(
        src.clone()
            .map_batches(2, |c| c.iter().map(|x| x * 2).collect())
            .node_id(),
    );
    coded.push(src.clone().batch_elements(2).node_id());
    coded.push(src.clone().batch_by_size(8, |_| 4).node_id());
    coded.push(src.clone().log_elements_with(|x| format!("{x}")).node_id());
    coded.push(src.debug_inspect_with("inspect", |_| {}).node_id());
    coded.push(src.debug_count("count").node_id());
    coded.push(src.debug_sample(2, "sample").node_id());
    coded.push(src.clone().reshuffle().node_id());

    // wait_on (signal-only barrier).
    let signal = from_vec(&p, vec![0u64]);
    coded.push(src.clone().wait_on(&signal).node_id());

    // flatten.
    let a = from_vec(&p, vec![1u64]);
    let b = from_vec(&p, vec![2u64]);
    coded.push(flatten(&[&a, &b]).node_id());

    // Keyed combinators.
    let kv = src.clone().key_by(|x| x % 2); // PCollection<(u64, u64)>
    coded.push(kv.node_id());
    coded.push(kv.clone().map_values(|v| v + 1).node_id());
    coded.push(kv.clone().filter_values(|v| *v > 0).node_id());
    coded.push(kv.clone().map_values_batches(2, <[u64]>::to_vec).node_id());
    coded.push(kv.clone().combine_values(Sum::<u64>::default()).node_id());

    // group_by_key (upgrades its predecessor to KV) + lifted combine.
    let gbk_pred = kv.node_id();
    let grouped = kv.group_by_key(); // PCollection<(u64, Vec<u64>)>
    coded.push(grouped.node_id());
    coded.push(
        grouped
            .combine_values_lifted(Sum::<u64>::default())
            .node_id(),
    );

    // Global combine.
    coded.push(
        src.clone()
            .combine_globally(Sum::<u64>::default(), None)
            .node_id(),
    );
    coded.push(
        src.combine_globally_lifted(Sum::<u64>::default(), Some(4))
            .node_id(),
    );

    // Joins (each upgrades both input predecessors to KV coders).
    let left = from_vec(&p, vec![(1u64, "l".to_string())]);
    let right = from_vec(&p, vec![(1u64, 9u64)]);
    coded.push(left.join_inner(&right).node_id());
    coded.push(left.join_left(&right).node_id());
    coded.push(left.join_right(&right).node_id());
    coded.push(left.join_full(&right).node_id());

    let coders = p.snapshot_coders();
    for id in &coded {
        assert!(
            coders.contains_key(id),
            "no coder registered for node {id:?}"
        );
    }

    // The pre-GroupByKey edge must carry a KV-aware coder.
    assert!(
        coders[&gbk_pred].is_kv(),
        "pre-GBK predecessor must carry a KV coder"
    );
    // Join inputs are read as kv<lp, lp>, so both predecessors are KV-aware too.
    assert!(
        coders[&left.node_id()].is_kv(),
        "join left input must be KV-aware"
    );
    assert!(
        coders[&right.node_id()].is_kv(),
        "join right input must be KV-aware"
    );
}

// ───────────────────────────── 4. Override hook ─────────────────────────────

#[test]
fn set_coder_override_replaces_registered_coder() {
    let p = Pipeline::default();
    let pc = from_vec(&p, vec![1u64, 2, 3]);
    let id = pc.node_id();

    // Default registration is a plain (non-KV) coder.
    assert!(!p.snapshot_coders()[&id].is_kv());

    // Override it with a hand-built KV coder.
    p.set_coder_override(id, Arc::new(PostcardKvCoder::<u64, u64>::new()));
    assert!(
        p.snapshot_coders()[&id].is_kv(),
        "override should take effect"
    );
}

// ───────────────────────────── 5. End-to-end ────────────────────────────────

#[test]
fn registered_coder_roundtrips_runtime_partition() {
    let p = Pipeline::default();
    let mapped = from_vec(&p, vec![1u64, 2, 3]).map(|x| x * 10);
    let id = mapped.node_id();
    let coder = p
        .snapshot_coders()
        .get(&id)
        .cloned()
        .expect("coder for mapped node");

    let out = mapped.collect_seq().expect("run pipeline"); // [10, 20, 30]

    // Encode the actual runner output partition, then decode it back.
    let part: Partition = Box::new(out.clone());
    let encoded = coder.encode_all(part).expect("encode runner output");
    let mut decoded: Vec<u64> = Vec::new();
    for bytes in &encoded {
        let pp = coder.decode_one(bytes).expect("decode");
        decoded.extend(*pp.downcast::<Vec<u64>>().expect("downcast Vec<u64>"));
    }
    assert_eq!(decoded, out);
}

#[test]
fn registered_coder_roundtrips_keyed_combine_output() {
    let p = Pipeline::default();
    let summed = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u64),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ],
    )
    .combine_values(Sum::<u64>::default());
    let id = summed.node_id();
    let coder = p
        .snapshot_coders()
        .get(&id)
        .cloned()
        .expect("coder for combine node");

    let mut out = summed.collect_seq().expect("run pipeline");
    out.sort();

    let part: Partition = Box::new(out.clone());
    let encoded = coder.encode_all(part).expect("encode runner output");
    let mut decoded: Vec<(String, u64)> = Vec::new();
    for bytes in &encoded {
        let pp = coder.decode_one(bytes).expect("decode");
        decoded.extend(
            *pp.downcast::<Vec<(String, u64)>>()
                .expect("downcast Vec<(String, u64)>"),
        );
    }
    decoded.sort();
    assert_eq!(decoded, out);
}
