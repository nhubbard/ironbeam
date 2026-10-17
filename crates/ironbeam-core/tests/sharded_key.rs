use ironbeam_core::{BeamShardedKey, Element, ErrorKind, Pipeline};

#[test]
fn matches_pinned_beam_standard_coder_examples() {
    // beam/model/fn-execution/.../standard_coders.yaml, sharded_key:v1.
    for (key, shard, expected) in [
        ("", "", b"\x00\x00".as_slice()),
        ("", "shard_id", b"\x08shard_id\x00".as_slice()),
        ("key", "shard_id", b"\x08shard_id\x03key".as_slice()),
        ("key", "", b"\x00\x03key".as_slice()),
    ] {
        let value = BeamShardedKey::new(key.to_owned(), shard.as_bytes().to_vec());
        assert_eq!(value.encode().unwrap(), expected);
        assert_eq!(value.encoded_len().unwrap(), expected.len());
        assert_eq!(BeamShardedKey::<String>::decode(expected).unwrap(), value);
    }
}

#[test]
fn binary_keys_and_multibyte_lengths_round_trip() {
    let value = BeamShardedKey::new(vec![0, 255, 3], vec![9; 130]);
    let bytes = value.encode().unwrap();
    assert_eq!(&bytes[..2], &[0x82, 0x01]);
    assert_eq!(bytes[132], 3);
    assert_eq!(BeamShardedKey::<Vec<u8>>::decode(&bytes).unwrap(), value);
    assert_eq!(value.encoded_len().unwrap(), bytes.len());
    assert_eq!(
        <BeamShardedKey<Vec<u8>> as Element>::coder_payload(),
        b"beam:coder:bytes:v1"
    );
}

#[test]
fn rejects_malformed_lengths_utf8_and_trailing_bytes() {
    for bytes in [
        b"\x80".as_slice(),
        b"\x02x".as_slice(),
        b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x02".as_slice(),
        b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80".as_slice(),
        b"\x00\x02k".as_slice(),
        b"\x00\x01\xff".as_slice(),
        b"\x00\x01k\x00".as_slice(),
    ] {
        assert_eq!(
            BeamShardedKey::<String>::decode(bytes).unwrap_err().kind,
            ErrorKind::Incompatible,
            "{bytes:?}"
        );
    }
}

#[test]
fn typed_shard_values_bind_their_component_coder_into_graph_identity() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec(
            "shards",
            vec![BeamShardedKey::new("key".to_owned(), vec![1])],
        )
        .unwrap();
    let (mut graph, _) = pipeline.snapshot().unwrap();
    graph.validate().unwrap();
    let fingerprint = graph.fingerprint().unwrap();
    graph.nodes[0].coder_payload = b"beam:coder:bytes:v1".to_vec();
    graph.validate().unwrap();
    assert_ne!(graph.fingerprint().unwrap(), fingerprint);
    graph.nodes[0].coder_payload = b"beam:coder:unknown:v1".to_vec();
    assert_eq!(graph.validate().unwrap_err().kind, ErrorKind::Graph);
}
