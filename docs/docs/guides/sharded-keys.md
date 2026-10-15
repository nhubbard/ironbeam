# Sharded keys

A sharded key pairs a user key with an opaque shard identifier. Beam uses the
pair when it distributes work for one user key among several workers. Two
values with the same user key and different shard identifiers are different
keys until a later transform removes the shard identifier.

Ironbeam's `BeamShardedKey<K>` is an element type in `ironbeam-core`. Its
portable coder is `beam:coder:sharded_key:v1`. The current key components are
`String`, encoded with Beam's UTF-8 string coder, and `Vec<u8>`, encoded with
Beam's bytes coder. The shard identifier is always a byte vector. This type
does not choose shards automatically.

```rust
use ironbeam_core::{BeamShardedKey, Pipeline};

let pipeline = Pipeline::default();
let keys = pipeline.from_vec(
    "input",
    vec![1_u8, 2],
)?;
let sharded = keys.map_named("assign-shard", |number| {
    BeamShardedKey::new("account".to_owned(), vec![0, number])
})?;
let restored = sharded
    .reshuffle_named("redistribute")?
    .map_named("read-shard", |value| (value.key, value.shard_id))?;
```

The wire value begins with a length-prefixed shard identifier, followed by a
length-prefixed key. Both lengths use Beam's unsigned VarInt encoding. For
example, the string key `key` with shard identifier `shard_id` has the bytes
`08 73 68 61 72 64 5f 69 64 03 6b 65 79`. The key coder is a component of
the sharded-key coder in the portable pipeline; its identity contributes to
the graph fingerprint.

A test sends this value through a reshuffle on Direct and the pinned local
Prism runner. It verifies that both runners preserve the user key and shard
identifier. The [Beam semantics inventory](../concepts/beam-semantics.md)
tracks the separate work needed for automatic sharding and stateful,
byte-limited streaming batches.
