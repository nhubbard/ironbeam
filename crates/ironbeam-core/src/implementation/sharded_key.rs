//! Beam's portable sharded-key value and wire coder.

use super::{Element, Error};

const MAX_PREFIX_BYTES: usize = 10;

/// A key whose Beam nested representation is a length-prefixed byte string.
///
/// The supported implementations are `String` (`string_utf8`) and `Vec<u8>`
/// (`bytes`). Other key coders need an explicit implementation and graph
/// lowering support before they can cross a runner boundary.
pub trait BeamPortableKey: Clone + Send + Sync + 'static {
    const CODER_URN: &'static str;
    const TYPE_ID: &'static str;

    fn key_bytes(&self) -> &[u8];
    fn from_key_bytes(bytes: &[u8]) -> Result<Self, Error>
    where
        Self: Sized;
}

impl BeamPortableKey for String {
    const CODER_URN: &'static str = "beam:coder:string_utf8:v1";
    const TYPE_ID: &'static str = "std/string";

    fn key_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    fn from_key_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Self::from_utf8(bytes.to_vec())
            .map_err(|_| Error::incompatible("sharded key contains invalid UTF-8"))
    }
}

impl BeamPortableKey for Vec<u8> {
    const CODER_URN: &'static str = "beam:coder:bytes:v1";
    const TYPE_ID: &'static str = "std/bytes";

    fn key_bytes(&self) -> &[u8] {
        self
    }

    fn from_key_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(bytes.to_vec())
    }
}

/// A user key with an opaque shard ID. Beam encodes the shard ID first, then
/// the nested user key. Equal user keys on different shards remain distinct
/// until a transform explicitly removes the shard ID.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BeamShardedKey<K> {
    pub key: K,
    pub shard_id: Vec<u8>,
}

/// One batch associated with a user key and an opaque shard identifier.
pub type BeamShardedBatch<K, V> = (BeamShardedKey<K>, Vec<V>);

impl<K> BeamShardedKey<K> {
    /// Pair a user key with an opaque, stable shard identifier.
    #[must_use]
    pub const fn new(key: K, shard_id: Vec<u8>) -> Self {
        Self { key, shard_id }
    }
}

impl<K: BeamPortableKey> Element for BeamShardedKey<K> {
    const TYPE_ID: &'static str = "beam/sharded-key";
    const CODER_ID: &'static str = "beam:coder:sharded_key:v1";

    fn type_identity() -> String {
        format!("beam/sharded-key<{}>", K::TYPE_ID)
    }

    fn coder_payload() -> Vec<u8> {
        K::CODER_URN.as_bytes().to_vec()
    }

    fn supports_coder() -> bool {
        true
    }

    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::with_capacity(self.encoded_len()?);
        self.encode_into(&mut bytes)?;
        Ok(bytes)
    }

    fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), Error> {
        encode_part(&self.shard_id, output)?;
        encode_part(self.key.key_bytes(), output)
    }

    fn encoded_len(&self) -> Result<usize, Error> {
        let shard = part_len(self.shard_id.len())?;
        shard
            .checked_add(part_len(self.key.key_bytes().len())?)
            .ok_or_else(|| Error::resource("sharded-key", "encoded length overflow"))
    }

    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut remaining = bytes;
        let shard_id = decode_part(&mut remaining)?.to_vec();
        let key = K::from_key_bytes(decode_part(&mut remaining)?)?;
        if !remaining.is_empty() {
            return Err(Error::incompatible("sharded key has trailing bytes"));
        }
        Ok(Self { key, shard_id })
    }
}

fn part_len(len: usize) -> Result<usize, Error> {
    let mut value =
        u64::try_from(len).map_err(|_| Error::resource("sharded-key", "part length overflow"))?;
    let mut prefix = 1_usize;
    while value >= 0x80 {
        value >>= 7;
        prefix += 1;
    }
    prefix
        .checked_add(len)
        .ok_or_else(|| Error::resource("sharded-key", "encoded length overflow"))
}

fn encode_part(part: &[u8], output: &mut Vec<u8>) -> Result<(), Error> {
    let mut len = u64::try_from(part.len())
        .map_err(|_| Error::resource("sharded-key", "part length overflow"))?;
    while len >= 0x80 {
        output.push(u8::try_from(len & 0x7f).expect("seven bits fit") | 0x80);
        len >>= 7;
    }
    output.push(u8::try_from(len).expect("final VarInt byte fits"));
    output.extend_from_slice(part);
    Ok(())
}

fn decode_part<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], Error> {
    let mut value = 0_u64;
    for index in 0..MAX_PREFIX_BYTES {
        let Some((&byte, rest)) = input.split_first() else {
            return Err(Error::incompatible("truncated sharded key length"));
        };
        if index == 9 && byte > 1 {
            return Err(Error::incompatible("sharded key length exceeds u64"));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        *input = rest;
        if byte & 0x80 == 0 {
            let len = usize::try_from(value)
                .map_err(|_| Error::incompatible("sharded key length exceeds platform range"))?;
            let Some((part, tail)) = input.split_at_checked(len) else {
                return Err(Error::incompatible("truncated sharded key part"));
            };
            *input = tail;
            return Ok(part);
        }
    }
    Err(Error::incompatible("sharded key length VarInt is too long"))
}
