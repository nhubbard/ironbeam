//! Runner-neutral Ironbeam 5 graph descriptors and typed construction.
//! Compiled Rust functions live in a separate local registry, never in the descriptor.
#![allow(
    clippy::missing_errors_doc,
    clippy::too_many_lines,
    clippy::struct_excessive_bools
)]

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::any::{Any, TypeId};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::fmt::Write as _;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, OnceLock};

mod ordered_f64;
pub use ordered_f64::OrdF64;

pub const GRAPH_VERSION: Version = Version { major: 3, minor: 0 };
pub const CODER_VERSION: Version = Version { major: 1, minor: 0 };
pub const CHECKPOINT_VERSION: Version = Version { major: 1, minor: 2 };
pub const BUILTIN_OPERATION_VERSION: Version = Version { major: 1, minor: 0 };

/// Stable semantic type and wire coder identities supplied by the element owner.
/// A coder ID is a declaration; a remote backend must register its implementation.
pub trait Element: Send + 'static {
    const TYPE_ID: &'static str;
    const CODER_ID: &'static str;
    const CODER_VERSION: Version = crate::CODER_VERSION;
    #[must_use]
    fn type_identity() -> String {
        Self::TYPE_ID.into()
    }
    #[must_use]
    fn coder_identity() -> String {
        Self::CODER_ID.into()
    }
    #[must_use]
    fn supports_coder() -> bool {
        false
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"))
    }
    /// Append wire bytes without replacing the caller's existing buffer.
    fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), Error> {
        output.extend_from_slice(&self.encode()?);
        Ok(())
    }
    /// Replace `buffer` with this value's wire bytes, reusing its allocation when possible.
    fn encode_reusing(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        *buffer = self.encode()?;
        Ok(())
    }
    /// Exact encoded byte length. Override when it can be computed without allocating.
    fn encoded_len(&self) -> Result<usize, Error> {
        Ok(self.encode()?.len())
    }
    /// Exact wire length when every value of this element type has the same size.
    #[must_use]
    fn fixed_encoded_len() -> Option<usize> {
        None
    }
    fn decode(_bytes: &[u8]) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"))
    }
}

/// An owned partition passed to a reusable dynamic operator.
pub type Partition = Box<dyn Any + Send + Sync>;

/// A v4-style reusable operator over one partition. The v5 graph binds each
/// registration to an application semantic ID and version; the runner does not
/// infer those from the compiled operator. Planner hints are retained for API
/// migration, but the v5 planner does not yet reorder operations using them.
pub trait DynOp: Send + Sync {
    fn apply(&self, input: Partition) -> Partition;
    fn key_preserving(&self) -> bool {
        false
    }
    fn value_only(&self) -> bool {
        false
    }
    fn reorder_safe_with_value_only(&self) -> bool {
        false
    }
    fn cost_hint(&self) -> u8 {
        10
    }
    fn cardinality_reducing(&self) -> bool {
        false
    }
    fn limit_n(&self) -> Option<usize> {
        None
    }
    fn cardinality_multiplier_hint(&self) -> f64 {
        1.0
    }
}
macro_rules! integer_element {
    ($type:ty, $id:literal, $coder:literal, $size:expr) => {
        impl Element for $type {
            const TYPE_ID: &'static str = $id;
            const CODER_ID: &'static str = $coder;
            fn supports_coder() -> bool {
                true
            }
            fn encode(&self) -> Result<Vec<u8>, Error> {
                Ok(self.to_le_bytes().to_vec())
            }
            fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), Error> {
                output.extend_from_slice(&self.to_le_bytes());
                Ok(())
            }
            fn encode_reusing(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
                buffer.clear();
                buffer.extend_from_slice(&self.to_le_bytes());
                Ok(())
            }
            fn encoded_len(&self) -> Result<usize, Error> {
                Ok($size)
            }
            fn fixed_encoded_len() -> Option<usize> {
                Some($size)
            }
            fn decode(bytes: &[u8]) -> Result<Self, Error> {
                let array: [u8; $size] = bytes
                    .try_into()
                    .map_err(|_| Error::incompatible("invalid integer coder length"))?;
                Ok(Self::from_le_bytes(array))
            }
        }
    };
}
integer_element!(i32, "std/i32", "ironbeam/i32-le", 4);
integer_element!(i64, "std/i64", "ironbeam/i64-le", 8);
integer_element!(i8, "std/i8", "ironbeam/i8-le", 1);
integer_element!(i16, "std/i16", "ironbeam/i16-le", 2);
integer_element!(u8, "std/u8", "ironbeam/u8-le", 1);
integer_element!(u16, "std/u16", "ironbeam/u16-le", 2);
integer_element!(u32, "std/u32", "ironbeam/u32-le", 4);
integer_element!(u64, "std/u64", "ironbeam/u64-le", 8);
integer_element!(f32, "std/f32", "ironbeam/f32-ieee754-le", 4);
integer_element!(f64, "std/f64", "ironbeam/f64-ieee754-le", 8);
impl Element for bool {
    const TYPE_ID: &'static str = "std/bool";
    const CODER_ID: &'static str = "ironbeam/bool-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        Ok(vec![u8::from(*self)])
    }
    fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), Error> {
        output.push(u8::from(*self));
        Ok(())
    }
    fn encode_reusing(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        buffer.clear();
        buffer.push(u8::from(*self));
        Ok(())
    }
    fn encoded_len(&self) -> Result<usize, Error> {
        Ok(1)
    }
    fn fixed_encoded_len() -> Option<usize> {
        Some(1)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        match bytes {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(Error::incompatible("invalid bool coder value")),
        }
    }
}
impl Element for String {
    const TYPE_ID: &'static str = "std/string";
    const CODER_ID: &'static str = "ironbeam/utf8-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        Ok(self.as_bytes().to_vec())
    }
    fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), Error> {
        output.extend_from_slice(self.as_bytes());
        Ok(())
    }
    fn encode_reusing(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        buffer.clear();
        buffer.extend_from_slice(self.as_bytes());
        Ok(())
    }
    fn encoded_len(&self) -> Result<usize, Error> {
        Ok(self.len())
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::from_utf8(bytes.to_vec())
            .map_err(|_| Error::incompatible("invalid UTF-8 coder value"))
    }
}

fn encode_part(bytes: &[u8], output: &mut Vec<u8>) {
    let length = bytes.len() as u64;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(bytes);
}
fn encode_part_into<T: Element>(value: &T, output: &mut Vec<u8>) -> Result<(), Error> {
    let prefix = output.len();
    output.extend_from_slice(&[0; 8]);
    let start = output.len();
    value.encode_into(output)?;
    let length = u64::try_from(output.len() - start)
        .map_err(|_| Error::resource("coder", "encoded length overflow"))?;
    output[prefix..start].copy_from_slice(&length.to_le_bytes());
    Ok(())
}
fn checked_encoded_add(total: usize, part: usize) -> Result<usize, Error> {
    total
        .checked_add(part)
        .ok_or_else(|| Error::resource("coder", "encoded length overflow"))
}
fn decode_part<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8], Error> {
    let prefix = bytes
        .get(..8)
        .ok_or_else(|| Error::incompatible("truncated length prefix"))?;
    let mut prefix_array = [0_u8; 8];
    prefix_array.copy_from_slice(prefix);
    let length = usize_from_u64(u64::from_le_bytes(prefix_array))?;
    let part = bytes
        .get(
            8..8_usize
                .checked_add(length)
                .ok_or_else(|| Error::incompatible("encoded part length overflow"))?,
        )
        .ok_or_else(|| Error::incompatible("truncated encoded part"))?;
    *bytes = &bytes[8 + length..];
    Ok(part)
}
#[allow(
    clippy::unnecessary_wraps,
    clippy::cast_possible_truncation,
    clippy::missing_const_for_fn
)]
fn usize_from_u64(value: u64) -> Result<usize, Error> {
    #[cfg(target_pointer_width = "64")]
    {
        Ok(value as usize)
    }
    #[cfg(not(target_pointer_width = "64"))]
    {
        usize::try_from(value).map_err(|_| Error::incompatible("encoded part exceeds usize"))
    }
}
impl<T: Element> Element for Vec<T> {
    const TYPE_ID: &'static str = "std/vector";
    const CODER_ID: &'static str = "ironbeam/vector-length-prefixed";
    fn type_identity() -> String {
        format!("std/vector<{}>", T::type_identity())
    }
    fn coder_identity() -> String {
        format!("ironbeam/vector-v1<{}>", T::coder_identity())
    }
    fn supports_coder() -> bool {
        T::supports_coder()
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        let mut output = Vec::new();
        self.encode_into(&mut output)?;
        Ok(output)
    }
    fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        output.extend_from_slice(&(self.len() as u64).to_le_bytes());
        for value in self {
            encode_part_into(value, output)?;
        }
        Ok(())
    }
    fn encode_reusing(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        buffer.clear();
        self.encode_into(buffer)
    }
    fn encoded_len(&self) -> Result<usize, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        self.iter().try_fold(8, |total, value| {
            checked_encoded_add(total, checked_encoded_add(8, value.encoded_len()?)?)
        })
    }
    fn decode(mut bytes: &[u8]) -> Result<Self, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        let prefix = bytes
            .get(..8)
            .ok_or_else(|| Error::incompatible("truncated vector count"))?;
        let mut count = [0_u8; 8];
        count.copy_from_slice(prefix);
        bytes = &bytes[8..];
        let count = usize_from_u64(u64::from_le_bytes(count))?;
        let mut values = Self::new();
        for _ in 0..count {
            values.push(T::decode(decode_part(&mut bytes)?)?);
        }
        if !bytes.is_empty() {
            return Err(Error::incompatible("trailing vector bytes"));
        }
        Ok(values)
    }
}
impl<T: Element> Element for Option<T> {
    const TYPE_ID: &'static str = "std/option";
    const CODER_ID: &'static str = "ironbeam/option-tagged";
    fn type_identity() -> String {
        format!("std/option<{}>", T::type_identity())
    }
    fn coder_identity() -> String {
        format!("ironbeam/option-v1<{}>", T::coder_identity())
    }
    fn supports_coder() -> bool {
        T::supports_coder()
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        let mut output = Vec::with_capacity(1);
        self.encode_into(&mut output)?;
        Ok(output)
    }
    fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        match self {
            None => output.push(0),
            Some(value) => {
                output.push(1);
                value.encode_into(output)?;
            }
        }
        Ok(())
    }
    fn encode_reusing(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        buffer.clear();
        self.encode_into(buffer)
    }
    fn encoded_len(&self) -> Result<usize, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        match self {
            None => Ok(1),
            Some(value) => checked_encoded_add(1, value.encoded_len()?),
        }
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        match bytes.split_first() {
            Some((&0, [])) => Ok(None),
            Some((&1, value)) => T::decode(value).map(Some),
            _ => Err(Error::incompatible("invalid option tag or trailing bytes")),
        }
    }
}
impl<A: Element, B: Element> Element for (A, B) {
    const TYPE_ID: &'static str = "std/tuple2";
    const CODER_ID: &'static str = "ironbeam/tuple2-length-prefixed";
    fn type_identity() -> String {
        format!("std/tuple2<{},{}>", A::type_identity(), B::type_identity())
    }
    fn coder_identity() -> String {
        format!(
            "ironbeam/tuple2-v1<{},{}>",
            A::coder_identity(),
            B::coder_identity()
        )
    }
    fn supports_coder() -> bool {
        A::supports_coder() && B::supports_coder()
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        let mut output = Vec::with_capacity(self.encoded_len()?);
        self.encode_into(&mut output)?;
        Ok(output)
    }
    fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        output.extend_from_slice(&(self.0.encoded_len()? as u64).to_le_bytes());
        self.0.encode_into(output)?;
        output.extend_from_slice(&(self.1.encoded_len()? as u64).to_le_bytes());
        self.1.encode_into(output)
    }
    fn encode_reusing(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        buffer.clear();
        self.encode_into(buffer)
    }
    fn encoded_len(&self) -> Result<usize, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        let first = checked_encoded_add(8, self.0.encoded_len()?)?;
        let second = checked_encoded_add(8, self.1.encoded_len()?)?;
        checked_encoded_add(first, second)
    }
    fn fixed_encoded_len() -> Option<usize> {
        16_usize
            .checked_add(A::fixed_encoded_len()?)?
            .checked_add(B::fixed_encoded_len()?)
    }
    fn decode(mut bytes: &[u8]) -> Result<Self, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        let first = A::decode(decode_part(&mut bytes)?)?;
        let second = B::decode(decode_part(&mut bytes)?)?;
        if !bytes.is_empty() {
            return Err(Error::incompatible("trailing tuple bytes"));
        }
        Ok((first, second))
    }
}

macro_rules! impl_tuple_element {
    ($count:literal; $($ty:ident:$index:tt),+) => {
        impl<$($ty: Element),+> Element for ($($ty,)+) {
            const TYPE_ID: &'static str = concat!("std/tuple", $count);
            const CODER_ID: &'static str = concat!("ironbeam/tuple", $count, "-length-prefixed");
            fn type_identity() -> String {
                format!("{}<{}>", Self::TYPE_ID, [$($ty::type_identity()),+].join(","))
            }
            fn coder_identity() -> String {
                format!("ironbeam/tuple{}-v1<{}>", $count, [$($ty::coder_identity()),+].join(","))
            }
            fn supports_coder() -> bool {
                true $(&& $ty::supports_coder())+
            }
            fn encode(&self) -> Result<Vec<u8>, Error> {
                if !Self::supports_coder() {
                    return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
                }
                let mut bytes = Vec::new();
                self.encode_into(&mut bytes)?;
                Ok(bytes)
            }
            fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), Error> {
                if !Self::supports_coder() {
                    return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
                }
                $(encode_part_into(&self.$index, output)?;)+
                Ok(())
            }
            fn encode_reusing(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
                buffer.clear();
                self.encode_into(buffer)
            }
            fn encoded_len(&self) -> Result<usize, Error> {
                if !Self::supports_coder() {
                    return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
                }
                let mut total = 0;
                $(total = checked_encoded_add(total, checked_encoded_add(8, self.$index.encoded_len()?)?)?;)+
                Ok(total)
            }
            fn fixed_encoded_len() -> Option<usize> {
                let mut total = 0_usize;
                $(total = total.checked_add(8)?.checked_add($ty::fixed_encoded_len()?)?;)+
                Some(total)
            }
            fn decode(mut bytes: &[u8]) -> Result<Self, Error> {
                if !Self::supports_coder() {
                    return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
                }
                let value = ($($ty::decode(decode_part(&mut bytes)?)?,)+);
                if !bytes.is_empty() {
                    return Err(Error::incompatible("trailing tuple bytes"));
                }
                Ok(value)
            }
        }
    };
}
impl_tuple_element!(3; A:0, B:1, C:2);
impl_tuple_element!(4; A:0, B:1, C:2, D:3);
impl_tuple_element!(5; A:0, B:1, C:2, D:3, E:4);
impl_tuple_element!(6; A:0, B:1, C:2, D:3, E:4, F:5);
impl_tuple_element!(7; A:0, B:1, C:2, D:3, E:4, F:5, G:6);
impl_tuple_element!(8; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7);
impl_tuple_element!(9; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8);
impl_tuple_element!(10; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9);

// The built-in set coder deliberately uses Rust's default hasher on decode;
// its wire order is the sorted encoded value order, independent of hashing.
#[allow(clippy::implicit_hasher)]
impl<T: Element + Eq + Hash> Element for HashSet<T> {
    const TYPE_ID: &'static str = "std/hash-set";
    const CODER_ID: &'static str = "ironbeam/hash-set-sorted";
    fn type_identity() -> String {
        format!("std/hash-set<{}>", T::type_identity())
    }
    fn coder_identity() -> String {
        format!("ironbeam/hash-set-sorted-v1<{}>", T::coder_identity())
    }
    fn supports_coder() -> bool {
        T::supports_coder()
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        let mut parts = self
            .iter()
            .map(Element::encode)
            .collect::<Result<Vec<_>, _>>()?;
        parts.sort();
        if parts.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(Error::incompatible(
                "distinct set values have the same encoded bytes",
            ));
        }
        let mut bytes = (parts.len() as u64).to_le_bytes().to_vec();
        for part in parts {
            encode_part(&part, &mut bytes);
        }
        Ok(bytes)
    }
    fn decode(mut bytes: &[u8]) -> Result<Self, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        let header = bytes
            .get(..8)
            .ok_or_else(|| Error::incompatible("truncated set count"))?;
        let count = usize_from_u64(u64::from_le_bytes(
            header
                .try_into()
                .map_err(|_| Error::incompatible("invalid set count"))?,
        ))?;
        bytes = &bytes[8..];
        let mut values = Self::with_capacity(count.min(1024));
        for _ in 0..count {
            if !values.insert(T::decode(decode_part(&mut bytes)?)?) {
                return Err(Error::incompatible("duplicate value in encoded set"));
            }
        }
        if !bytes.is_empty() {
            return Err(Error::incompatible("trailing set bytes"));
        }
        Ok(values)
    }
}

// The built-in map coder deliberately uses Rust's default hasher on decode;
// its wire order is the sorted encoded key order, independent of hashing.
#[allow(clippy::implicit_hasher)]
impl<K: Element + Eq + Hash, V: Element> Element for HashMap<K, V> {
    const TYPE_ID: &'static str = "std/hash-map";
    const CODER_ID: &'static str = "ironbeam/hash-map-sorted";
    fn type_identity() -> String {
        format!(
            "std/hash-map<{},{}>",
            K::type_identity(),
            V::type_identity()
        )
    }
    fn coder_identity() -> String {
        format!(
            "ironbeam/hash-map-sorted-v1<{},{}>",
            K::coder_identity(),
            V::coder_identity()
        )
    }
    fn supports_coder() -> bool {
        K::supports_coder() && V::supports_coder()
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        let mut parts = self
            .iter()
            .map(|(key, value)| Ok((key.encode()?, value.encode()?)))
            .collect::<Result<Vec<_>, Error>>()?;
        parts.sort_by(|left, right| left.0.cmp(&right.0));
        if parts.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(Error::incompatible(
                "distinct map keys have the same encoded bytes",
            ));
        }
        let mut bytes = (parts.len() as u64).to_le_bytes().to_vec();
        for (key, value) in parts {
            encode_part(&key, &mut bytes);
            encode_part(&value, &mut bytes);
        }
        Ok(bytes)
    }
    fn decode(mut bytes: &[u8]) -> Result<Self, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        let header = bytes
            .get(..8)
            .ok_or_else(|| Error::incompatible("truncated map count"))?;
        let count = usize_from_u64(u64::from_le_bytes(
            header
                .try_into()
                .map_err(|_| Error::incompatible("invalid map count"))?,
        ))?;
        bytes = &bytes[8..];
        let mut values = Self::with_capacity(count.min(1024));
        for _ in 0..count {
            let key = K::decode(decode_part(&mut bytes)?)?;
            let value = V::decode(decode_part(&mut bytes)?)?;
            if values.insert(key, value).is_some() {
                return Err(Error::incompatible("duplicate key in encoded map"));
            }
        }
        if !bytes.is_empty() {
            return Err(Error::incompatible("trailing map bytes"));
        }
        Ok(values)
    }
}

impl<T: Element, E: Element> Element for Result<T, E> {
    const TYPE_ID: &'static str = "std/result";
    const CODER_ID: &'static str = "ironbeam/result-tagged";
    fn type_identity() -> String {
        format!("std/result<{},{}>", T::type_identity(), E::type_identity())
    }
    fn coder_identity() -> String {
        format!(
            "ironbeam/result-tagged-v1<{},{}>",
            T::coder_identity(),
            E::coder_identity()
        )
    }
    fn supports_coder() -> bool {
        T::supports_coder() && E::supports_coder()
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        let mut bytes = Vec::new();
        match self {
            Ok(value) => {
                bytes.push(0);
                encode_part(&value.encode()?, &mut bytes);
            }
            Err(error) => {
                bytes.push(1);
                encode_part(&error.encode()?, &mut bytes);
            }
        }
        Ok(bytes)
    }
    fn decode(mut bytes: &[u8]) -> Result<Self, Error> {
        if !Self::supports_coder() {
            return Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"));
        }
        let tag = *bytes
            .first()
            .ok_or_else(|| Error::incompatible("missing result tag"))?;
        bytes = &bytes[1..];
        let value = match tag {
            0 => Ok(T::decode(decode_part(&mut bytes)?)?),
            1 => Err(E::decode(decode_part(&mut bytes)?)?),
            _ => return Err(Error::incompatible("invalid result tag")),
        };
        if !bytes.is_empty() {
            return Err(Error::incompatible("trailing result bytes"));
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Version {
    pub major: u16,
    pub minor: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Boundedness {
    Bounded,
    Unbounded,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Window {
    Global,
    Fixed { size_ns: i64 },
    FixedOffset { size_ns: i64, offset_ns: i64 },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct WindowInterval {
    pub start_ns: i64,
    pub end_ns: i64,
}

impl Window {
    pub fn assign(&self, timestamp_ns: i64) -> Result<Option<WindowInterval>, Error> {
        match *self {
            Self::Global => Ok(None),
            Self::Fixed { size_ns } if size_ns > 0 => {
                let start_ns = timestamp_ns
                    .div_euclid(size_ns)
                    .checked_mul(size_ns)
                    .ok_or_else(|| {
                        Error::graph("time", "window start overflows i64 nanoseconds")
                    })?;
                let end_ns = start_ns
                    .checked_add(size_ns)
                    .ok_or_else(|| Error::graph("time", "window end overflows i64 nanoseconds"))?;
                Ok(Some(WindowInterval { start_ns, end_ns }))
            }
            Self::FixedOffset { size_ns, offset_ns } if size_ns > 0 => {
                let size = i128::from(size_ns);
                let offset = i128::from(offset_ns);
                let start = (i128::from(timestamp_ns) - offset).div_euclid(size) * size + offset;
                let end = start + size;
                let start_ns = i64::try_from(start)
                    .map_err(|_| Error::graph("time", "window start overflows i64 nanoseconds"))?;
                let end_ns = i64::try_from(end)
                    .map_err(|_| Error::graph("time", "window end overflows i64 nanoseconds"))?;
                Ok(Some(WindowInterval { start_ns, end_ns }))
            }
            Self::Fixed { .. } => Err(Error::graph("time", "fixed window size must be positive")),
            Self::FixedOffset { .. } => {
                Err(Error::graph("time", "fixed window size must be positive"))
            }
        }
    }
}

impl Element for WindowInterval {
    const TYPE_ID: &'static str = "ironbeam/window-interval";
    const CODER_ID: &'static str = "ironbeam/window-interval-i64-le";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = self.start_ns.to_le_bytes().to_vec();
        bytes.extend_from_slice(&self.end_ns.to_le_bytes());
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != 16 {
            return Err(Error::incompatible("window interval requires 16 bytes"));
        }
        let start_ns = i64::from_le_bytes(
            bytes[..8]
                .try_into()
                .map_err(|_| Error::incompatible("invalid window start"))?,
        );
        let end_ns = i64::from_le_bytes(
            bytes[8..]
                .try_into()
                .map_err(|_| Error::incompatible("invalid window end"))?,
        );
        if end_ns <= start_ns {
            return Err(Error::incompatible("window end must follow start"));
        }
        Ok(Self { start_ns, end_ns })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Time {
    pub initial_timestamp_ns: i64,
    pub window: Window,
}

impl Default for Time {
    fn default() -> Self {
        Self {
            initial_timestamp_ns: 0,
            window: Window::Global,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Replay {
    None,
    AtLeastOnce,
    Idempotent,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceProgress {
    pub stable_split_id: bool,
    pub replay: Replay,
    pub reports_event_time: bool,
}

/// A control signal from one stable source split, separate from data envelopes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProgressSignal {
    pub source_key: String,
    pub split_id: String,
    pub checkpoint_position: Option<Vec<u8>>,
    pub event_time_ns: Option<i64>,
    pub completed: bool,
}

/// A deterministic, finite script for testing an unbounded source. Time is in
/// nanoseconds, like the rest of the logical graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestStreamEvent<T> {
    Elements(Vec<(T, i64)>),
    AdvanceWatermark(i64),
    AdvanceProcessingTime(i64),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EncodedTestStreamEvent {
    Elements(Vec<(Vec<u8>, i64)>),
    AdvanceWatermark(i64),
    AdvanceProcessingTime(i64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SinkMode {
    Append,
    Update,
    FinalResult,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SinkSpec {
    pub mode: SinkMode,
    pub replay: Replay,
    pub commit: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Operation {
    Source {
        progress: SourceProgress,
    },
    Map,
    Filter,
    FlatMap,
    AssignTimestamp,
    Flatten,
    Reshuffle,
    WaitOn,
    GroupByKey,
    CoGroupByKey,
    JoinInner,
    JoinLeft,
    JoinRight,
    JoinFull,
    SideInputMap,
    FixedWindow,
    GlobalCombine,
    FoldGlobal,
    FoldPerKey,
    FoldOrderedGlobal,
    FoldOrderedPerKey,
    BatchMapValues {
        batch_size: usize,
    },
    Custom {
        semantic_id: String,
        version: Version,
    },
    Output,
    Sink {
        spec: SinkSpec,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ElementDescriptor {
    pub type_id: String,
    pub coder_id: String,
    pub coder_version: Version,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InputEdge {
    pub key: String,
    pub element: ElementDescriptor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub key: String,
    pub input: Option<String>,
    pub input_element: Option<ElementDescriptor>,
    #[serde(default)]
    pub additional_inputs: Vec<InputEdge>,
    pub operation: Operation,
    pub semantic_id: String,
    pub semantic_version: Version,
    pub type_id: String,
    pub coder_id: String,
    pub coder_version: Version,
    pub boundedness: Boundedness,
    pub time: Time,
    /// Build-local index; deliberately excluded from the portable descriptor.
    #[serde(skip)]
    pub local_id: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Graph {
    pub version: Version,
    pub checkpoint_version: Version,
    pub nodes: Vec<Node>,
}

/// Identity checked by a remote worker before any bundle is processed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionIdentity {
    pub graph_fingerprint: String,
    pub binary_build_id: String,
    pub registry_manifest: String,
    pub coder_manifest: String,
    pub source_config_digest: String,
    pub sink_config_digest: String,
}
impl ExecutionIdentity {
    pub fn verify(&self, worker: &Self) -> Result<(), Error> {
        if self != worker {
            return Err(Error::incompatible(
                "driver/worker execution identity mismatch",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryClass {
    Permanent,
    Transient,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Graph,
    UnsupportedCapability { capability: String },
    Incompatible,
    Resource,
    Execution,
    Io,
    Recovery,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    pub kind: ErrorKind,
    pub node: Option<String>,
    pub backend: Option<String>,
    pub retry: RetryClass,
    pub message: String,
}

impl Error {
    pub fn graph(node: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Graph,
            node: Some(node.into()),
            backend: None,
            retry: RetryClass::Permanent,
            message: message.into(),
        }
    }
    pub fn unsupported(
        node: impl Into<String>,
        backend: impl Into<String>,
        capability: impl Into<String>,
    ) -> Self {
        let capability = capability.into();
        Self {
            kind: ErrorKind::UnsupportedCapability {
                capability: capability.clone(),
            },
            node: Some(node.into()),
            backend: Some(backend.into()),
            retry: RetryClass::Permanent,
            message: format!("missing capability {capability}"),
        }
    }
    pub fn incompatible(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Incompatible,
            node: None,
            backend: None,
            retry: RetryClass::Permanent,
            message: message.into(),
        }
    }
    pub fn execution(
        node: impl Into<String>,
        backend: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: ErrorKind::Execution,
            node: Some(node.into()),
            backend: Some(backend.into()),
            retry: RetryClass::Unknown,
            message: message.into(),
        }
    }
    pub fn io(
        node: impl Into<String>,
        backend: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: ErrorKind::Io,
            node: Some(node.into()),
            backend: Some(backend.into()),
            retry: RetryClass::Unknown,
            message: message.into(),
        }
    }
    pub fn recovery(node: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Recovery,
            node: Some(node.into()),
            backend: Some("direct".into()),
            retry: RetryClass::Permanent,
            message: message.into(),
        }
    }
    pub fn resource(node: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Resource,
            node: Some(node.into()),
            backend: Some("direct".into()),
            retry: RetryClass::Permanent,
            message: message.into(),
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} at {:?} on {:?}: {}",
            self.kind, self.node, self.backend, self.message
        )
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Debug, Default)]
pub struct Capabilities {
    pub external_bounded_source: bool,
    pub unbounded_source: bool,
    pub flatten: bool,
    pub reshuffle: bool,
    pub wait_on: bool,
    pub group_by_key: bool,
    pub co_group_by_key: bool,
    pub join_inner: bool,
    pub join_outer: bool,
    pub side_input_map: bool,
    pub global_combine: bool,
    pub incremental_combine: bool,
    pub ordered_fold: bool,
    pub batch_map_values: bool,
    pub fixed_window: bool,
    pub assign_timestamp: bool,
    pub append_sink: bool,
    pub final_result_sink: bool,
    pub update_sink: bool,
    pub custom_operations: BTreeSet<String>,
}

impl Graph {
    pub fn canonical_json(&self) -> Result<String, Error> {
        let mut copy = self.clone();
        copy.nodes.sort_by(|a, b| a.key.cmp(&b.key));
        serde_json::to_string(&copy).map_err(|e| Error::incompatible(e.to_string()))
    }
    pub fn fingerprint(&self) -> Result<String, Error> {
        let bytes = self.canonical_json()?;
        let digest = Sha256::digest(bytes.as_bytes());
        let mut result = String::from("sha256-v1:");
        for byte in digest {
            write!(result, "{byte:02x}").map_err(|e| Error::incompatible(e.to_string()))?;
        }
        Ok(result)
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.version.major != GRAPH_VERSION.major || self.version.minor > GRAPH_VERSION.minor {
            return Err(Error::incompatible("unsupported graph descriptor version"));
        }
        if self.checkpoint_version.major != CHECKPOINT_VERSION.major
            || self.checkpoint_version.minor > CHECKPOINT_VERSION.minor
        {
            return Err(Error::incompatible("unsupported checkpoint schema version"));
        }
        let mut by_key = BTreeMap::new();
        for node in &self.nodes {
            if node.key.is_empty() || by_key.insert(node.key.as_str(), node).is_some() {
                return Err(Error::graph(
                    &node.key,
                    "empty or duplicate stable node key",
                ));
            }
            if node.type_id.is_empty() || node.coder_id.is_empty() {
                return Err(Error::graph(&node.key, "type and coder IDs are required"));
            }
            if node.semantic_id.is_empty() || node.semantic_version.major == 0 {
                return Err(Error::graph(
                    &node.key,
                    "semantic operation ID and major version are required",
                ));
            }
            if matches!(node.operation, Operation::Source { .. }) && !node.semantic_id.contains('/')
            {
                return Err(Error::graph(
                    &node.key,
                    "source semantic ID must be namespaced",
                ));
            }
            if node.coder_version.major != CODER_VERSION.major
                || node.coder_version.minor > CODER_VERSION.minor
            {
                return Err(Error::incompatible(format!(
                    "unsupported coder version at {}",
                    node.key
                )));
            }
            if matches!(node.time.window, Window::Fixed { size_ns } | Window::FixedOffset { size_ns, .. } if size_ns <= 0)
            {
                return Err(Error::graph(
                    &node.key,
                    "fixed window size must be positive",
                ));
            }
            if matches!(node.operation, Operation::FixedWindow)
                && !matches!(
                    node.time.window,
                    Window::Fixed { .. } | Window::FixedOffset { .. }
                )
            {
                return Err(Error::graph(
                    &node.key,
                    "fixed-window operation requires a fixed window descriptor",
                ));
            }
            if matches!(node.operation, Operation::BatchMapValues { batch_size: 0 }) {
                return Err(Error::graph(&node.key, "batch size must be positive"));
            }
            if matches!(node.operation, Operation::BatchMapValues { .. })
                && node.boundedness == Boundedness::Unbounded
            {
                return Err(Error::graph(
                    &node.key,
                    "batch mapping requires a bounded input",
                ));
            }
            if let Operation::Custom {
                semantic_id,
                version,
            } = &node.operation
                && (!semantic_id.contains('/') || version.major == 0)
            {
                return Err(Error::graph(
                    &node.key,
                    "custom operation requires a namespaced ID and major version",
                ));
            }
            if matches!(
                node.operation,
                Operation::Flatten
                    | Operation::WaitOn
                    | Operation::CoGroupByKey
                    | Operation::JoinInner
                    | Operation::JoinLeft
                    | Operation::JoinRight
                    | Operation::JoinFull
                    | Operation::SideInputMap
            ) == node.additional_inputs.is_empty()
            {
                return Err(Error::graph(
                    &node.key,
                    "multi-input operation requires additional inputs and other operations forbid them",
                ));
            }
            if matches!(
                node.operation,
                Operation::JoinInner
                    | Operation::JoinLeft
                    | Operation::JoinRight
                    | Operation::JoinFull
                    | Operation::WaitOn
                    | Operation::SideInputMap
            ) && node.additional_inputs.len() != 1
            {
                return Err(Error::graph(
                    &node.key,
                    "operation requires exactly two inputs",
                ));
            }
            if let Operation::Custom {
                semantic_id,
                version,
            } = &node.operation
                && (semantic_id != &node.semantic_id || version != &node.semantic_version)
            {
                return Err(Error::graph(
                    &node.key,
                    "custom operation identity differs from descriptor",
                ));
            }
        }
        for node in &self.nodes {
            match (&node.operation, &node.input, &node.input_element) {
                (Operation::Source { .. }, None, None) => {}
                (Operation::Source { .. }, _, _) | (_, None, _) | (_, _, None) => {
                    return Err(Error::graph(&node.key, "invalid source/input relationship"));
                }
                (_, Some(parent), Some(expected)) => {
                    let upstream = by_key
                        .get(parent.as_str())
                        .ok_or_else(|| Error::graph(&node.key, "missing input node"))?;
                    if expected.type_id != upstream.type_id
                        || expected.coder_id != upstream.coder_id
                        || expected.coder_version != upstream.coder_version
                    {
                        return Err(Error::graph(&node.key, "input edge type/coder mismatch"));
                    }
                    if matches!(
                        node.operation,
                        Operation::Filter
                            | Operation::AssignTimestamp
                            | Operation::Flatten
                            | Operation::Reshuffle
                            | Operation::WaitOn
                            | Operation::FixedWindow
                            | Operation::Output
                            | Operation::Sink { .. }
                    ) && (node.type_id != upstream.type_id
                        || node.coder_id != upstream.coder_id
                        || node.coder_version != upstream.coder_version)
                    {
                        return Err(Error::graph(&node.key, "input type/coder mismatch"));
                    }
                    if !matches!(
                        node.operation,
                        Operation::Flatten
                            | Operation::CoGroupByKey
                            | Operation::JoinInner
                            | Operation::JoinLeft
                            | Operation::JoinRight
                            | Operation::JoinFull
                            | Operation::WaitOn
                            | Operation::SideInputMap
                    ) && node.boundedness != upstream.boundedness
                    {
                        return Err(Error::graph(&node.key, "boundedness must propagate"));
                    }
                    if matches!(
                        node.operation,
                        Operation::Map
                            | Operation::AssignTimestamp
                            | Operation::FlatMap
                            | Operation::Filter
                            | Operation::Flatten
                            | Operation::Reshuffle
                            | Operation::WaitOn
                            | Operation::GroupByKey
                            | Operation::CoGroupByKey
                            | Operation::JoinInner
                            | Operation::JoinLeft
                            | Operation::JoinRight
                            | Operation::JoinFull
                            | Operation::SideInputMap
                            | Operation::GlobalCombine
                            | Operation::FoldGlobal
                            | Operation::FoldPerKey
                            | Operation::FoldOrderedGlobal
                            | Operation::FoldOrderedPerKey
                            | Operation::BatchMapValues { .. }
                            | Operation::Output
                            | Operation::Sink { .. }
                    ) && node.time != upstream.time
                    {
                        return Err(Error::graph(&node.key, "event time/window must propagate"));
                    }
                    if matches!(
                        node.operation,
                        Operation::GlobalCombine
                            | Operation::FoldGlobal
                            | Operation::FoldPerKey
                            | Operation::FoldOrderedGlobal
                            | Operation::FoldOrderedPerKey
                            | Operation::GroupByKey
                            | Operation::CoGroupByKey
                    ) && node.boundedness == Boundedness::Unbounded
                        && node.time.window == Window::Global
                    {
                        return Err(Error::graph(
                            &node.key,
                            "unbounded global aggregation requires a finite window",
                        ));
                    }
                    if matches!(
                        node.operation,
                        Operation::Flatten
                            | Operation::WaitOn
                            | Operation::CoGroupByKey
                            | Operation::JoinInner
                            | Operation::JoinLeft
                            | Operation::JoinRight
                            | Operation::JoinFull
                            | Operation::SideInputMap
                    ) {
                        let mut unbounded = upstream.boundedness == Boundedness::Unbounded;
                        for edge in &node.additional_inputs {
                            let other = by_key.get(edge.key.as_str()).ok_or_else(|| {
                                Error::graph(&node.key, "missing additional input node")
                            })?;
                            if edge.element.type_id != other.type_id
                                || edge.element.coder_id != other.coder_id
                                || edge.element.coder_version != other.coder_version
                            {
                                return Err(Error::graph(
                                    &node.key,
                                    "additional input type/coder mismatch",
                                ));
                            }
                            if matches!(node.operation, Operation::Flatten)
                                && (node.type_id != other.type_id
                                    || node.coder_id != other.coder_id
                                    || node.coder_version != other.coder_version)
                            {
                                return Err(Error::graph(
                                    &node.key,
                                    "flatten input type/coder mismatch",
                                ));
                            }
                            if other.time != node.time
                                && !matches!(node.operation, Operation::WaitOn)
                            {
                                return Err(Error::graph(
                                    &node.key,
                                    "flatten time/window mismatch",
                                ));
                            }
                            unbounded |= other.boundedness == Boundedness::Unbounded;
                        }
                        let expected = if matches!(node.operation, Operation::WaitOn) {
                            upstream.boundedness
                        } else if unbounded {
                            Boundedness::Unbounded
                        } else {
                            Boundedness::Bounded
                        };
                        if node.boundedness != expected {
                            return Err(Error::graph(
                                &node.key,
                                "multi-input boundedness mismatch",
                            ));
                        }
                        if matches!(
                            node.operation,
                            Operation::JoinInner
                                | Operation::CoGroupByKey
                                | Operation::JoinLeft
                                | Operation::JoinRight
                                | Operation::JoinFull
                        ) && expected == Boundedness::Unbounded
                            && node.time.window == Window::Global
                        {
                            return Err(Error::graph(
                                &node.key,
                                "unbounded global join requires a finite window",
                            ));
                        }
                        if matches!(node.operation, Operation::SideInputMap)
                            && node.additional_inputs.iter().any(|edge| {
                                by_key[edge.key.as_str()].boundedness == Boundedness::Unbounded
                            })
                        {
                            return Err(Error::graph(&node.key, "side input must be bounded"));
                        }
                        if matches!(node.operation, Operation::WaitOn)
                            && node.additional_inputs.iter().any(|edge| {
                                by_key[edge.key.as_str()].boundedness == Boundedness::Unbounded
                            })
                        {
                            return Err(Error::graph(&node.key, "wait signal must be bounded"));
                        }
                    }
                }
            }
        }
        for node in &self.nodes {
            let mut active = BTreeSet::new();
            let mut complete = BTreeSet::new();
            let mut stack = vec![(node.key.as_str(), false)];
            while let Some((key, exiting)) = stack.pop() {
                if exiting {
                    active.remove(key);
                    complete.insert(key);
                    continue;
                }
                if complete.contains(key) {
                    continue;
                }
                if !active.insert(key) {
                    return Err(Error::graph(&node.key, "cycle in graph"));
                }
                stack.push((key, true));
                let current = by_key[key];
                for edge in &current.additional_inputs {
                    stack.push((edge.key.as_str(), false));
                }
                if let Some(parent) = &current.input {
                    stack.push((parent.as_str(), false));
                }
            }
        }
        Ok(())
    }
    pub fn validate_for(&self, backend: &str, capabilities: &Capabilities) -> Result<(), Error> {
        self.validate()?;
        for node in &self.nodes {
            let missing = match &node.operation {
                Operation::Source { .. }
                    if node.boundedness == Boundedness::Unbounded
                        && !capabilities.unbounded_source =>
                {
                    Some("unbounded-source".to_string())
                }
                Operation::Source { .. }
                    if node.semantic_id != "ironbeam/source"
                        && !capabilities.external_bounded_source =>
                {
                    Some(node.semantic_id.clone())
                }
                Operation::FixedWindow if !capabilities.fixed_window => {
                    Some("fixed-window".to_string())
                }
                Operation::AssignTimestamp if !capabilities.assign_timestamp => {
                    Some("assign-timestamp".to_string())
                }
                Operation::Flatten if !capabilities.flatten => Some("flatten".to_string()),
                Operation::Reshuffle if !capabilities.reshuffle => Some("reshuffle".to_string()),
                Operation::WaitOn if !capabilities.wait_on => Some("wait-on".to_string()),
                Operation::GroupByKey if !capabilities.group_by_key => {
                    Some("group-by-key".to_string())
                }
                Operation::CoGroupByKey if !capabilities.co_group_by_key => {
                    Some("co-group-by-key".to_string())
                }
                Operation::JoinInner if !capabilities.join_inner => Some("join-inner".to_string()),
                Operation::JoinLeft | Operation::JoinRight | Operation::JoinFull
                    if !capabilities.join_outer =>
                {
                    Some("join-outer".to_string())
                }
                Operation::SideInputMap if !capabilities.side_input_map => {
                    Some("side-input-map".to_string())
                }
                Operation::GlobalCombine if !capabilities.global_combine => {
                    Some("global-combine".to_string())
                }
                Operation::FoldGlobal | Operation::FoldPerKey
                    if !capabilities.incremental_combine =>
                {
                    Some("incremental-combine".to_string())
                }
                Operation::FoldOrderedGlobal | Operation::FoldOrderedPerKey
                    if !capabilities.ordered_fold =>
                {
                    Some("ordered-fold".to_string())
                }
                Operation::BatchMapValues { .. } if !capabilities.batch_map_values => {
                    Some("batch-map-values".to_string())
                }
                Operation::Sink { spec }
                    if spec.mode == SinkMode::Append && !capabilities.append_sink =>
                {
                    Some("append-sink".to_string())
                }
                Operation::Sink { spec }
                    if spec.mode == SinkMode::Update && !capabilities.update_sink =>
                {
                    Some("update-sink".to_string())
                }
                Operation::Sink { spec }
                    if spec.mode == SinkMode::FinalResult && !capabilities.final_result_sink =>
                {
                    Some("final-result-sink".to_string())
                }
                Operation::Custom { semantic_id, .. }
                    if !capabilities.custom_operations.contains(semantic_id) =>
                {
                    Some(semantic_id.clone())
                }
                _ => None,
            };
            if let Some(missing) = missing {
                return Err(Error::unsupported(&node.key, backend, missing));
            }
        }
        Ok(())
    }
    #[must_use]
    pub fn node(&self, key: &str) -> Option<&Node> {
        self.nodes.iter().find(|n| n.key == key)
    }
}

pub type Payload = Box<dyn Any + Send>;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ElementMetadata {
    pub timestamp_ns: i64,
    pub window: Window,
    pub assigned_window: Option<WindowInterval>,
}
pub struct Envelope {
    pub value: Payload,
    pub timestamp_ns: i64,
    pub window: Window,
    pub assigned_window: Option<WindowInterval>,
}
pub type SourceIterator = Box<dyn Iterator<Item = Result<Envelope, Error>> + Send>;
pub type TypedSourceIterator<T> = Box<dyn Iterator<Item = Result<T, Error>> + Send>;
/// One nonblocking poll result from an unbounded source split.
#[derive(Clone, Debug)]
pub enum UnboundedPoll<T> {
    Pending,
    Ready(UnboundedBatch<T>),
}

/// One timestamped value emitted by a source poll.
#[derive(Clone, Debug)]
pub struct Timestamped<T> {
    pub value: T,
    pub timestamp_ns: i64,
}

/// Data and the durable position after those elements from one source split.
#[derive(Clone, Debug)]
pub struct UnboundedBatch<T> {
    pub elements: Vec<T>,
    pub progress: ProgressSignal,
}

/// A resumable, nonblocking reader. `Pending` lets a runner check cancellation
/// and schedule another poll without trapping a worker thread in the source.
pub trait UnboundedSource<T>: Send {
    fn poll(&mut self, max_elements: usize) -> Result<UnboundedPoll<T>, Error>;
}

pub type UnboundedSourceReader = Box<dyn UnboundedSource<Envelope>>;
pub type TypedUnboundedSourceReader<T> = Box<dyn UnboundedSource<Timestamped<T>>>;
type UnboundedSourceFactory =
    Arc<dyn Fn(Option<Vec<u8>>) -> Result<UnboundedSourceReader, Error> + Send + Sync>;
type SourceFactory = Arc<dyn Fn() -> Result<SourceIterator, Error> + Send + Sync>;
type TypedSourceFactory<T> = Arc<dyn Fn() -> Result<TypedSourceIterator<T>, Error> + Send + Sync>;
type TypedVecBatchFactory<T> = Arc<dyn Fn(usize, usize) -> Result<Vec<T>, Error> + Send + Sync>;
/// Local typed join callback used by a runner with an in-memory vector source.
pub type TypedJoin<T> =
    dyn Fn(usize, &dyn Fn() -> Result<(), Error>) -> Result<Vec<T>, Error> + Send + Sync;
type TypedJoinFactory<T> = Arc<TypedJoin<T>>;
type ErasedVecBatchFactory = Arc<dyn Fn(usize, usize) -> Result<Payload, Error> + Send + Sync>;
type VecEnvelopeBatchFactory =
    Arc<dyn Fn(usize, usize) -> Result<Vec<Envelope>, Error> + Send + Sync>;
/// A typed bounded-slice reader retained by an in-memory vector source.
pub type VecBatchReader<'a, T> = &'a (dyn Fn(usize, usize) -> Result<Vec<T>, Error> + Send + Sync);
/// A bounded vector slice whose type is checked by a fold before execution.
pub type ErasedVecBatchReader<'a> =
    &'a (dyn Fn(usize, usize) -> Result<Payload, Error> + Send + Sync);
/// Read an in-memory vector slice as envelopes for a checkpointed transform path.
pub type VecEnvelopeBatchReader<'a> =
    &'a (dyn Fn(usize, usize) -> Result<Vec<Envelope>, Error> + Send + Sync);
type SingleTransform = Arc<dyn Fn(Envelope) -> Result<Option<Envelope>, Error> + Send + Sync>;
type MultiTransform = Arc<dyn Fn(Envelope) -> Result<Vec<Envelope>, Error> + Send + Sync>;
type TypedMap<T, U> = Arc<dyn Fn(T) -> U + Send + Sync>;
type TypedFilter<T> = Arc<dyn Fn(&T) -> bool + Send + Sync>;
type TypedTimestamp<T> = Arc<dyn Fn(&T) -> i64 + Send + Sync>;
#[derive(Clone)]
enum TypedTransformRegistration {
    Map(Arc<dyn Any + Send + Sync>),
    Filter(Arc<dyn Any + Send + Sync>),
    Timestamp(Arc<dyn Any + Send + Sync>),
}

/// A same-type value operation for a Direct route with no envelope metadata changes.
pub enum TypedValueTransform<'a, T> {
    Map(&'a (dyn Fn(T) -> T + Send + Sync)),
    Filter(&'a (dyn Fn(&T) -> bool + Send + Sync)),
}
impl<T> TypedValueTransform<'_, T> {
    pub fn apply(&self, value: T) -> Option<T> {
        match self {
            Self::Map(map) => Some(map(value)),
            Self::Filter(predicate) => predicate(&value).then_some(value),
        }
    }
}
#[derive(Clone)]
enum Transform {
    Single(SingleTransform),
    Multi(MultiTransform),
}
type Aggregate = Arc<dyn Fn(Vec<Vec<Envelope>>) -> Result<Vec<Envelope>, Error> + Send + Sync>;
type OrderedGlobalGroups<Q, T> =
    BTreeMap<Option<i64>, (BTreeMap<Q, (T, i64)>, Option<WindowInterval>)>;
type LatestTieGlobalGroups<Q> = BTreeMap<Option<i64>, (BTreeSet<(i64, Q)>, Envelope)>;
pub trait CombineFn<V, A, O>: Send + Sync + 'static {
    fn create(&self) -> A;
    fn add_input(&self, accumulator: &mut A, value: V);
    fn merge(&self, accumulator: &mut A, other: A);
    fn finish(&self, accumulator: A) -> O;
}
pub trait OrderedFoldFn<V, A, O>: Send + Sync + 'static {
    fn create(&self) -> A;
    fn add_input(&self, accumulator: &mut A, value: V);
    fn finish(&self, accumulator: A) -> O;
}
trait FoldLogic<V, A, O>: Send + Sync + 'static {
    fn create(&self) -> A;
    fn add_input(&self, accumulator: &mut A, value: V);
    fn finish(&self, accumulator: A) -> O;
}
struct MergeableAdapter<C>(C);
impl<V, A, O, C: CombineFn<V, A, O>> FoldLogic<V, A, O> for MergeableAdapter<C> {
    fn create(&self) -> A {
        self.0.create()
    }
    fn add_input(&self, accumulator: &mut A, value: V) {
        self.0.add_input(accumulator, value);
    }
    fn finish(&self, accumulator: A) -> O {
        self.0.finish(accumulator)
    }
}
struct OrderedAdapter<C>(C);
impl<V, A, O, C: OrderedFoldFn<V, A, O>> FoldLogic<V, A, O> for OrderedAdapter<C> {
    fn create(&self) -> A {
        self.0.create()
    }
    fn add_input(&self, accumulator: &mut A, value: V) {
        self.0.add_input(accumulator, value);
    }
    fn finish(&self, accumulator: A) -> O {
        self.0.finish(accumulator)
    }
}
pub trait FoldSession: Send {
    fn add(&mut self, event: Envelope) -> Result<(), Error>;
    /// Return a versioned snapshot when this fold supports in-progress recovery.
    fn checkpoint_state(&self) -> Result<Option<Vec<u8>>, Error> {
        Ok(None)
    }
    /// Restore a snapshot into a newly opened fold before feeding more input.
    fn restore_checkpoint_state(&mut self, _state: &[u8]) -> Result<(), Error> {
        Err(Error::unsupported(
            "fold",
            "runner",
            "fold-checkpoint-state",
        ))
    }
    /// Whether a built-in vector source can feed a whole globally windowed batch.
    fn accepts_vec_batch(&self, _batch_type: TypeId) -> bool {
        false
    }
    /// Feed a checked built-in vector batch without per-element envelopes.
    fn add_vec_batch(&mut self, _values: Payload) -> Result<(), Error> {
        Err(Error::unsupported("fold", "direct", "typed-vector-batch"))
    }
    /// Whether this fold can consume a typed vector batch after timestamp
    /// assignment and private keyed timestamp capture.
    fn accepts_timed_vec_batch(&self, _batch_type: TypeId) -> bool {
        false
    }
    fn add_timed_vec_batch(
        &mut self,
        _values: Payload,
        _timestamp_fn: &(dyn Any + Send + Sync),
        _timestamp_key: &str,
    ) -> Result<(), Error> {
        Err(Error::unsupported(
            "fold",
            "direct",
            "typed-timestamp-batch",
        ))
    }
    fn finish(self: Box<Self>) -> Result<Vec<Envelope>, Error>;
    /// Drain completed output without requiring a second whole-result vector.
    fn finish_into(
        self: Box<Self>,
        emit: &mut dyn FnMut(Envelope) -> Result<(), Error>,
    ) -> Result<(), Error> {
        for event in self.finish()? {
            emit(event)?;
        }
        Ok(())
    }
}
type FoldFactory = Arc<dyn Fn() -> Box<dyn FoldSession> + Send + Sync>;
type FoldEntry<A> = (A, i64, Window, Option<WindowInterval>);

struct GlobalFoldState<T, A, O, C> {
    key: String,
    combiner: Arc<C>,
    window: Window,
    entries: BTreeMap<Option<i64>, FoldEntry<A>>,
    marker: PhantomData<(T, O)>,
}
impl<T: Element, A: Send + 'static, O: Element, C: FoldLogic<T, A, O>> FoldSession
    for GlobalFoldState<T, A, O, C>
{
    fn add(&mut self, event: Envelope) -> Result<(), Error> {
        let Envelope {
            value,
            timestamp_ns,
            window,
            assigned_window,
        } = event;
        let value = *value
            .downcast::<T>()
            .map_err(|_| Error::execution(&self.key, "direct", "fold input type mismatch"))?;
        let scope = assigned_window.map(|interval| interval.start_ns);
        let entry = self.entries.entry(scope).or_insert_with(|| {
            (
                self.combiner.create(),
                timestamp_ns,
                window,
                assigned_window,
            )
        });
        self.combiner.add_input(&mut entry.0, value);
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<Vec<Envelope>, Error> {
        if self.entries.is_empty() && self.window == Window::Global {
            self.entries
                .insert(None, (self.combiner.create(), 0, Window::Global, None));
        }
        Ok(std::mem::take(&mut self.entries)
            .into_values()
            .map(
                |(accumulator, timestamp_ns, window, assigned_window)| Envelope {
                    value: Box::new(self.combiner.finish(accumulator)),
                    timestamp_ns,
                    window,
                    assigned_window,
                },
            )
            .collect())
    }
    fn finish_into(
        mut self: Box<Self>,
        emit: &mut dyn FnMut(Envelope) -> Result<(), Error>,
    ) -> Result<(), Error> {
        if self.entries.is_empty() && self.window == Window::Global {
            self.entries
                .insert(None, (self.combiner.create(), 0, Window::Global, None));
        }
        for (accumulator, timestamp_ns, window, assigned_window) in
            std::mem::take(&mut self.entries).into_values()
        {
            emit(Envelope {
                value: Box::new(self.combiner.finish(accumulator)),
                timestamp_ns,
                window,
                assigned_window,
            })?;
        }
        Ok(())
    }
}

struct KeyedFoldState<K, V, A, O, C> {
    key: String,
    combiner: Arc<C>,
    indices: HashMap<(K, Option<WindowInterval>), usize>,
    rows: Vec<(K, FoldEntry<A>)>,
    marker: PhantomData<(V, O)>,
}
impl<
    K: Element + Eq + Hash + Clone,
    V: Element,
    A: Send + 'static,
    O: Element,
    C: FoldLogic<V, A, O>,
> FoldSession for KeyedFoldState<K, V, A, O, C>
{
    fn add(&mut self, event: Envelope) -> Result<(), Error> {
        let Envelope {
            value,
            timestamp_ns,
            window,
            assigned_window,
        } = event;
        let (row_key, value) = *value
            .downcast::<(K, V)>()
            .map_err(|_| Error::execution(&self.key, "direct", "keyed fold input type mismatch"))?;
        let scope = (row_key.clone(), assigned_window);
        let position = if let Some(&existing) = self.indices.get(&scope) {
            existing
        } else {
            let next = self.rows.len();
            self.indices.insert(scope, next);
            self.rows.push((
                row_key,
                (
                    self.combiner.create(),
                    timestamp_ns,
                    window,
                    assigned_window,
                ),
            ));
            next
        };
        self.combiner.add_input(&mut self.rows[position].1.0, value);
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<Vec<Envelope>, Error> {
        Ok(std::mem::take(&mut self.rows)
            .into_iter()
            .map(
                |(key, (accumulator, timestamp_ns, window, assigned_window))| Envelope {
                    value: Box::new((key, self.combiner.finish(accumulator))),
                    timestamp_ns,
                    window,
                    assigned_window,
                },
            )
            .collect())
    }
    fn finish_into(
        mut self: Box<Self>,
        emit: &mut dyn FnMut(Envelope) -> Result<(), Error>,
    ) -> Result<(), Error> {
        for (key, (accumulator, timestamp_ns, window, assigned_window)) in
            std::mem::take(&mut self.rows)
        {
            emit(Envelope {
                value: Box::new((key, self.combiner.finish(accumulator))),
                timestamp_ns,
                window,
                assigned_window,
            })?;
        }
        Ok(())
    }
}
pub trait SinkSession: Send {
    /// Bind a portable runner's stable bundle-attempt ID before the first write.
    /// Direct streaming binds this to the source cursor; bounded Direct jobs
    /// do not call this method.
    fn begin_bundle(&mut self, _instruction_id: &str) -> Result<(), Error> {
        Ok(())
    }
    fn write(&mut self, event: Envelope) -> Result<(), Error>;
    fn commit(&mut self) -> Result<(), Error>;
    fn abort(&mut self) -> Result<(), Error>;
    /// Whether this committed bundle needs a runner callback before its side
    /// effect is considered accepted.
    fn requires_bundle_finalization(&self) -> bool {
        false
    }
    /// Complete a previously committed bundle after the runner accepts it.
    /// Implementations must make retries idempotent.
    fn finalize_bundle(&mut self) -> Result<(), Error> {
        Err(Error::unsupported("sink", "runner", "bundle-finalization"))
    }
    /// Recover a committed attempt after worker state was lost. Implementations
    /// must verify durable state for this exact ID and fail if it is missing.
    fn recover_bundle_finalization(&mut self, _instruction_id: &str) -> Result<(), Error> {
        Err(Error::unsupported(
            "sink",
            "runner",
            "bundle-finalization-recovery",
        ))
    }
}
pub trait TypedSink<T: Element>: Send {
    fn begin_bundle(&mut self, _instruction_id: &str) -> Result<(), Error> {
        Ok(())
    }
    fn write(&mut self, value: T) -> Result<(), Error>;
    /// Write a value with its event-time and window context. Existing sinks
    /// can continue implementing `write`; metadata-aware sinks override this.
    fn write_with_metadata(&mut self, value: T, _metadata: ElementMetadata) -> Result<(), Error> {
        self.write(value)
    }
    fn commit(&mut self) -> Result<(), Error>;
    fn abort(&mut self) -> Result<(), Error>;
    fn requires_bundle_finalization(&self) -> bool {
        false
    }
    fn finalize_bundle(&mut self) -> Result<(), Error> {
        Err(Error::unsupported("sink", "runner", "bundle-finalization"))
    }
    fn recover_bundle_finalization(&mut self, _instruction_id: &str) -> Result<(), Error> {
        Err(Error::unsupported(
            "sink",
            "runner",
            "bundle-finalization-recovery",
        ))
    }
}
type SinkFactory = Arc<dyn Fn() -> Result<Box<dyn SinkSession>, Error> + Send + Sync>;
#[derive(Clone)]
struct SinkRegistration {
    factory: SinkFactory,
    config_digest: Option<String>,
    bundle_finalization: bool,
}
struct SinkAdapter<T: Element> {
    key: String,
    inner: Box<dyn TypedSink<T>>,
}
impl<T: Element> SinkSession for SinkAdapter<T> {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.inner.begin_bundle(instruction_id)
    }
    fn write(&mut self, event: Envelope) -> Result<(), Error> {
        let value = event
            .value
            .downcast::<T>()
            .map_err(|_| Error::execution(&self.key, "direct", "sink input type mismatch"))?;
        self.inner.write_with_metadata(
            *value,
            ElementMetadata {
                timestamp_ns: event.timestamp_ns,
                window: event.window,
                assigned_window: event.assigned_window,
            },
        )
    }
    fn commit(&mut self) -> Result<(), Error> {
        self.inner.commit()
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.inner.abort()
    }
    fn requires_bundle_finalization(&self) -> bool {
        self.inner.requires_bundle_finalization()
    }
    fn finalize_bundle(&mut self) -> Result<(), Error> {
        self.inner.finalize_bundle()
    }
    fn recover_bundle_finalization(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.inner.recover_bundle_finalization(instruction_id)
    }
}

#[derive(Clone)]
struct SourceRegistration {
    factory: SourceFactory,
    unbounded_factory: Option<UnboundedSourceFactory>,
    typed_factory: Option<Arc<dyn Any + Send + Sync>>,
    vec_batch_factory: Option<Arc<dyn Any + Send + Sync>>,
    erased_vec_batch_factory: Option<(TypeId, ErasedVecBatchFactory)>,
    vec_envelope_batch_factory: Option<VecEnvelopeBatchFactory>,
    len_hint: Option<usize>,
    config_digest: Option<SourceConfigDigest>,
    test_stream: Option<Vec<EncodedTestStreamEvent>>,
}

struct TypedUnboundedAdapter<T> {
    inner: TypedUnboundedSourceReader<T>,
}

impl<T: Element> UnboundedSource<Envelope> for TypedUnboundedAdapter<T> {
    fn poll(&mut self, max_elements: usize) -> Result<UnboundedPoll<Envelope>, Error> {
        match self.inner.poll(max_elements)? {
            UnboundedPoll::Pending => Ok(UnboundedPoll::Pending),
            UnboundedPoll::Ready(batch) => Ok(UnboundedPoll::Ready(UnboundedBatch {
                elements: batch
                    .elements
                    .into_iter()
                    .map(|item| Envelope {
                        value: Box::new(item.value),
                        timestamp_ns: item.timestamp_ns,
                        window: Window::Global,
                        assigned_window: None,
                    })
                    .collect(),
                progress: batch.progress,
            })),
        }
    }
}

struct ValidatingUnboundedSource {
    inner: UnboundedSourceReader,
    key: String,
    split_id: Option<String>,
    last_watermark: Option<i64>,
    completed: bool,
}

impl UnboundedSource<Envelope> for ValidatingUnboundedSource {
    fn poll(&mut self, max_elements: usize) -> Result<UnboundedPoll<Envelope>, Error> {
        if max_elements == 0 || self.completed {
            return Err(Error::execution(
                &self.key,
                "source",
                "invalid poll after completion or with zero capacity",
            ));
        }
        let result = self.inner.poll(max_elements)?;
        if let UnboundedPoll::Ready(batch) = &result {
            let signal = &batch.progress;
            if batch.elements.len() > max_elements {
                return Err(Error::execution(
                    &self.key,
                    "source",
                    "source exceeded poll capacity",
                ));
            }
            if signal.source_key != self.key || signal.split_id.is_empty() {
                return Err(Error::execution(
                    &self.key,
                    "source",
                    "source progress identity differs",
                ));
            }
            if self
                .split_id
                .as_ref()
                .is_some_and(|id| id != &signal.split_id)
            {
                return Err(Error::execution(
                    &self.key,
                    "source",
                    "source split changed during a session",
                ));
            }
            if !batch.elements.is_empty() && signal.checkpoint_position.is_none() {
                return Err(Error::execution(
                    &self.key,
                    "source",
                    "data batch has no checkpoint position",
                ));
            }
            if batch.elements.is_empty()
                && signal.checkpoint_position.is_none()
                && signal.event_time_ns.is_none()
                && !signal.completed
            {
                return Err(Error::execution(
                    &self.key,
                    "source",
                    "empty poll made no progress",
                ));
            }
            if signal
                .checkpoint_position
                .as_ref()
                .is_some_and(|position| position.len() > 1024 * 1024)
            {
                return Err(Error::execution(
                    &self.key,
                    "source",
                    "source checkpoint position exceeds 1 MiB",
                ));
            }
            if let Some(watermark) = signal.event_time_ns {
                if self.last_watermark.is_some_and(|last| watermark < last) {
                    return Err(Error::execution(
                        &self.key,
                        "source",
                        "source watermark regressed",
                    ));
                }
                self.last_watermark = Some(watermark);
            }
            self.split_id.get_or_insert_with(|| signal.split_id.clone());
            self.completed = signal.completed;
        }
        Ok(result)
    }
}

#[derive(Clone)]
enum SourceConfigDigest {
    Ready(String),
    Deferred(Arc<DeferredSourceDigest>),
}

struct DeferredSourceDigest {
    value: OnceLock<String>,
    compute: Box<dyn Fn() -> String + Send + Sync>,
}

impl SourceConfigDigest {
    fn as_str(&self) -> &str {
        match self {
            Self::Ready(value) => value,
            Self::Deferred(deferred) => deferred.value.get_or_init(|| (deferred.compute)()),
        }
    }
}

fn infallible_builtin_coder<T: Element>() -> bool {
    let type_id = TypeId::of::<T>();
    [
        TypeId::of::<i32>(),
        TypeId::of::<i64>(),
        TypeId::of::<u32>(),
        TypeId::of::<u64>(),
        TypeId::of::<f32>(),
        TypeId::of::<f64>(),
        TypeId::of::<bool>(),
        TypeId::of::<String>(),
        TypeId::of::<(u32, u32)>(),
    ]
    .contains(&type_id)
}

fn vector_source_digest<T: Element>(values: &[T]) -> Result<String, Error> {
    let mut hash = Sha256::new();
    hash.update(T::coder_identity().as_bytes());
    hash.update(T::CODER_VERSION.major.to_le_bytes());
    hash.update(T::CODER_VERSION.minor.to_le_bytes());
    let mut encoded = Vec::new();
    for value in values {
        value.encode_reusing(&mut encoded)?;
        // Supported Rust targets have at most 64-bit `usize`.
        #[allow(clippy::cast_possible_truncation)]
        let length = encoded.len() as u64;
        hash.update(length.to_le_bytes());
        hash.update(&encoded);
    }
    let digest = hash.finalize();
    let mut result = String::from("sha256-v1:");
    for byte in digest {
        write!(result, "{byte:02x}").map_err(|e| Error::incompatible(e.to_string()))?;
    }
    Ok(result)
}

#[derive(Clone, Default)]
pub struct Registry {
    sources: BTreeMap<String, SourceRegistration>,
    transforms: BTreeMap<String, Transform>,
    typed_transforms: BTreeMap<String, TypedTransformRegistration>,
    keyed_timestamp_reifications: BTreeSet<String>,
    aggregates: BTreeMap<String, Aggregate>,
    typed_joins: BTreeMap<String, Arc<dyn Any + Send + Sync>>,
    folds: BTreeMap<String, FoldFactory>,
    sinks: BTreeMap<String, SinkRegistration>,
    coders: BTreeMap<String, CoderRegistration>,
    group_coders: BTreeMap<String, GroupCoderRegistration>,
    cogroup_coders: BTreeMap<String, CoGroupCoderRegistration>,
    join_coders: BTreeMap<String, JoinCoderRegistration>,
    side_inputs: BTreeMap<String, SideInputRegistration>,
}
#[derive(Clone, Copy)]
struct CoderRegistration {
    encode: fn(&(dyn Any + Send)) -> Result<Vec<u8>, Error>,
    decode: CoderDecoder,
}
type CoderDecoder = fn(&[u8]) -> Result<Box<dyn Any + Send>, Error>;
#[derive(Clone, Copy)]
struct GroupCoderRegistration {
    split_input: GroupInputSplitter,
    assemble_input: GroupInputAssembler,
    assemble_output: GroupOutputAssembler,
}
type GroupInputSplitter = fn(&[u8]) -> Result<(Vec<u8>, Vec<u8>), Error>;
type GroupInputAssembler = fn(&[u8], &[u8]) -> Result<Vec<u8>, Error>;
type GroupOutputAssembler = fn(&[u8], &[Vec<u8>]) -> Result<Vec<u8>, Error>;
#[derive(Clone)]
struct CoGroupCoderRegistration {
    split_inputs: Vec<GroupInputSplitter>,
    assemble_output: GroupOutputAssembler,
}
#[derive(Clone, Copy)]
struct JoinCoderRegistration {
    split_inputs: [GroupInputSplitter; 2],
    assemble_inputs: [JoinInputAssembler; 2],
}
type JoinInputAssembler = fn(&[u8], &[u8]) -> Result<Payload, Error>;
#[derive(Clone)]
struct SideInputRegistration {
    materialize: SideMaterializer,
    apply: SideMapper,
}
type SideMaterializer = Arc<dyn Fn(Vec<Payload>) -> Result<Payload, Error> + Send + Sync>;
type SideMapper = Arc<dyn Fn(Envelope, &dyn Any) -> Result<Envelope, Error> + Send + Sync>;
impl GroupCoderRegistration {
    fn for_global<T: Element>() -> Self {
        Self {
            split_input: |bytes| {
                T::decode(bytes)?;
                Ok((Vec::new(), bytes.to_vec()))
            },
            assemble_input: |key, value| {
                if !key.is_empty() {
                    return Err(Error::incompatible("global fold has a nonempty runner key"));
                }
                T::decode(value)?;
                Ok(value.to_vec())
            },
            assemble_output: |_, _| {
                Err(Error::incompatible(
                    "global fold has no grouped output coder",
                ))
            },
        }
    }
    fn for_elements<K: Element, V: Element>() -> Self {
        Self {
            split_input: |bytes| {
                let (key, value) = <(K, V)>::decode(bytes)?;
                Ok((key.encode()?, value.encode()?))
            },
            assemble_input: |key, value| (K::decode(key)?, V::decode(value)?).encode(),
            assemble_output: |key, values| {
                let key = K::decode(key)?;
                let values = values
                    .iter()
                    .map(|value| V::decode(value))
                    .collect::<Result<Vec<_>, _>>()?;
                (key, values).encode()
            },
        }
    }
}
impl CoderRegistration {
    fn for_element<T: Element>() -> Self {
        Self {
            encode: |value| {
                value
                    .downcast_ref::<T>()
                    .ok_or_else(|| {
                        Error::incompatible("element type differs from registered coder")
                    })?
                    .encode()
            },
            decode: |bytes| T::decode(bytes).map(|value| Box::new(value) as Box<dyn Any + Send>),
        }
    }
}
impl Registry {
    #[must_use]
    pub fn has_side_input(&self, key: &str) -> bool {
        self.side_inputs.contains_key(key)
    }

    /// Materialize a decoded bounded side input once per execution scope.
    pub fn materialize_side_input(
        &self,
        key: &str,
        values: Vec<Payload>,
    ) -> Result<Payload, Error> {
        let side = self.side_inputs.get(key).ok_or_else(|| {
            Error::incompatible(format!("no side input registered for node {key}"))
        })?;
        (side.materialize)(values)
    }

    /// Apply a main element using a view made by `materialize_side_input`.
    pub fn apply_side_input(
        &self,
        key: &str,
        event: Envelope,
        view: &dyn Any,
    ) -> Result<Envelope, Error> {
        let side = self.side_inputs.get(key).ok_or_else(|| {
            Error::incompatible(format!("no side input registered for node {key}"))
        })?;
        (side.apply)(event, view)
    }

    #[must_use]
    pub fn has_join_coder(&self, key: &str) -> bool {
        self.join_coders.contains_key(key)
    }

    /// Preserve each input event timestamp inside the tagged shuffle value.
    pub fn split_join_input(
        &self,
        key: &str,
        input_index: usize,
        bytes: &[u8],
        timestamp_ns: i64,
    ) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let coder = self.join_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no join coder registered for node {key}"))
        })?;
        let splitter = coder.split_inputs.get(input_index).ok_or_else(|| {
            Error::incompatible(format!("join input index {input_index} is out of range"))
        })?;
        let (encoded_key, encoded_value) = splitter(bytes)?;
        let mut tagged = Vec::with_capacity(9 + encoded_value.len());
        tagged.push(
            u8::try_from(input_index)
                .map_err(|_| Error::incompatible("join input tag exceeds one byte"))?,
        );
        tagged.extend_from_slice(&timestamp_ns.to_be_bytes());
        tagged.extend_from_slice(&encoded_value);
        Ok((encoded_key, tagged))
    }

    /// Reuse the Direct join aggregate for each Beam grouped key.
    pub fn execute_join_group(
        &self,
        key: &str,
        encoded_key: &[u8],
        tagged_values: &[Vec<u8>],
        window: &Window,
        assigned_window: Option<WindowInterval>,
    ) -> Result<Vec<(Vec<u8>, i64)>, Error> {
        let coder = self.join_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no join coder registered for node {key}"))
        })?;
        let mut inputs = [Vec::new(), Vec::new()];
        for tagged in tagged_values {
            let (&tag, payload) = tagged
                .split_first()
                .ok_or_else(|| Error::incompatible("join tagged value is empty"))?;
            let assembler = coder
                .assemble_inputs
                .get(usize::from(tag))
                .ok_or_else(|| Error::incompatible("join input tag is out of range"))?;
            let timestamp_bytes = payload
                .get(..8)
                .ok_or_else(|| Error::incompatible("join tagged value lacks timestamp"))?;
            let mut timestamp_array = [0_u8; 8];
            timestamp_array.copy_from_slice(timestamp_bytes);
            let timestamp_ns = i64::from_be_bytes(timestamp_array);
            inputs[usize::from(tag)].push(Envelope {
                value: assembler(encoded_key, &payload[8..])?,
                timestamp_ns,
                window: window.clone(),
                assigned_window,
            });
        }
        self.apply_aggregate(key, inputs.into_iter().collect())?
            .into_iter()
            .map(|event| {
                Ok((
                    self.encode_element(key, event.value.as_ref())?,
                    event.timestamp_ns,
                ))
            })
            .collect()
    }

    /// Whether a multi-input co-group has typed tagged-union coders.
    #[must_use]
    pub fn has_cogroup_coder(&self, key: &str) -> bool {
        self.cogroup_coders.contains_key(key)
    }

    /// Encode an input pair as a common key and a tagged value for Beam GBK.
    pub fn split_cogroup_input(
        &self,
        key: &str,
        input_index: usize,
        bytes: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let coder = self.cogroup_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no co-group coder registered for node {key}"))
        })?;
        let splitter = coder.split_inputs.get(input_index).ok_or_else(|| {
            Error::incompatible(format!(
                "co-group input index {input_index} is out of range"
            ))
        })?;
        splitter(bytes)
    }

    /// Reassemble a Beam GBK of tagged values into the typed co-group result.
    pub fn assemble_cogroup_output(
        &self,
        key: &str,
        encoded_key: &[u8],
        encoded_values: &[Vec<u8>],
    ) -> Result<Vec<u8>, Error> {
        let coder = self.cogroup_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no co-group coder registered for node {key}"))
        })?;
        (coder.assemble_output)(encoded_key, encoded_values)
    }

    /// Whether this node has a registered typed key/value split coder for a
    /// runner grouping boundary. Global folds use the empty key.
    #[must_use]
    pub fn has_group_coder(&self, key: &str) -> bool {
        self.group_coders.contains_key(key)
    }
    /// Split a registered grouping input into independently encoded key and value.
    pub fn split_group_input(&self, key: &str, bytes: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let coder = self.group_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no group coder registered for node {key}"))
        })?;
        (coder.split_input)(bytes)
    }
    /// Reassemble a runner KV record into the registered input tuple coder.
    pub fn assemble_group_input(
        &self,
        key: &str,
        encoded_key: &[u8],
        encoded_value: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let coder = self.group_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no group coder registered for node {key}"))
        })?;
        (coder.assemble_input)(encoded_key, encoded_value)
    }
    /// Reassemble runner grouped key/value bytes into the v5 output element coder.
    pub fn assemble_group_output(
        &self,
        key: &str,
        encoded_key: &[u8],
        encoded_values: &[Vec<u8>],
    ) -> Result<Vec<u8>, Error> {
        let coder = self.group_coders.get(key).ok_or_else(|| {
            Error::incompatible(format!("no group coder registered for node {key}"))
        })?;
        (coder.assemble_output)(encoded_key, encoded_values)
    }
    #[must_use]
    pub fn has_coder(&self, key: &str) -> bool {
        self.coders.contains_key(key)
    }
    pub fn encode_element(&self, key: &str, value: &(dyn Any + Send)) -> Result<Vec<u8>, Error> {
        let coder = self
            .coders
            .get(key)
            .ok_or_else(|| Error::incompatible(format!("no coder registered for node {key}")))?;
        (coder.encode)(value)
    }
    pub fn decode_element(&self, key: &str, bytes: &[u8]) -> Result<Box<dyn Any + Send>, Error> {
        let coder = self
            .coders
            .get(key)
            .ok_or_else(|| Error::incompatible(format!("no coder registered for node {key}")))?;
        (coder.decode)(bytes)
    }
    pub fn open_fold(&self, key: &str) -> Result<Box<dyn FoldSession>, Error> {
        self.folds
            .get(key)
            .map(|factory| factory())
            .ok_or_else(|| Error::execution(key, "direct", "fold factory missing"))
    }
    #[must_use]
    pub fn has_fold(&self, key: &str) -> bool {
        self.folds.contains_key(key)
    }
    pub fn open_source(&self, key: &str) -> Result<SourceIterator, Error> {
        self.sources
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "source factory missing"))
            .and_then(|source| (source.factory)())
    }
    /// Resume a registered unbounded split from its last committed progress.
    /// The returned reader validates source identity, poll limits, and watermark
    /// order before a runner can consume its events.
    #[allow(clippy::needless_pass_by_value)] // Keep the public resume contract owned.
    pub fn open_unbounded_source(
        &self,
        key: &str,
        resume: Option<ProgressSignal>,
    ) -> Result<UnboundedSourceReader, Error> {
        if let Some(signal) = &resume
            && (signal.source_key != key
                || signal.split_id.is_empty()
                || signal.completed
                || signal.checkpoint_position.is_none()
                || signal
                    .checkpoint_position
                    .as_ref()
                    .is_some_and(|position| position.len() > 1024 * 1024))
        {
            return Err(Error::execution(
                key,
                "source",
                "invalid unbounded resume progress",
            ));
        }
        let factory = self
            .sources
            .get(key)
            .and_then(|source| source.unbounded_factory.as_ref())
            .ok_or_else(|| Error::execution(key, "source", "unbounded source factory missing"))?;
        let inner = factory(
            resume
                .as_ref()
                .and_then(|signal| signal.checkpoint_position.clone()),
        )?;
        Ok(Box::new(ValidatingUnboundedSource {
            inner,
            key: key.to_owned(),
            split_id: resume.as_ref().map(|signal| signal.split_id.clone()),
            last_watermark: resume.as_ref().and_then(|signal| signal.event_time_ns),
            completed: false,
        }))
    }
    #[must_use]
    pub fn has_unbounded_source(&self, key: &str) -> bool {
        self.sources
            .get(key)
            .is_some_and(|source| source.unbounded_factory.is_some())
    }
    /// Open a source directly as `T` when its registration retains a typed factory.
    ///
    /// `None` means that the backend should use the regular envelope source.
    pub fn open_typed_source<T: Element>(
        &self,
        key: &str,
    ) -> Result<Option<TypedSourceIterator<T>>, Error> {
        let source = self
            .sources
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "source factory missing"))?;
        source
            .typed_factory
            .as_ref()
            .and_then(|factory| factory.downcast_ref::<TypedSourceFactory<T>>())
            .map(|factory| factory())
            .transpose()
    }
    /// Read bounded slices directly from an immutable in-memory vector source.
    /// Other source kinds retain their ordinary factory contract.
    #[must_use]
    pub fn vec_source_batches<T: Element>(
        &self,
        key: &str,
    ) -> Option<(usize, VecBatchReader<'_, T>)> {
        let source = self.sources.get(key)?;
        let batch = source
            .vec_batch_factory
            .as_ref()?
            .downcast_ref::<TypedVecBatchFactory<T>>()?;
        Some((source.len_hint?, batch.as_ref()))
    }
    /// Return type-erased vector batches for folds that accept the exact source type.
    #[must_use]
    pub fn erased_vec_source_batches(
        &self,
        key: &str,
    ) -> Option<(usize, TypeId, ErasedVecBatchReader<'_>)> {
        let source = self.sources.get(key)?;
        let (batch_type, batch) = source.erased_vec_batch_factory.as_ref()?;
        Some((source.len_hint?, *batch_type, batch.as_ref()))
    }
    /// Return envelopes for a bounded vector slice without cloning earlier rows.
    #[must_use]
    pub fn vec_source_envelope_batches(
        &self,
        key: &str,
    ) -> Option<(usize, VecEnvelopeBatchReader<'_>)> {
        let source = self.sources.get(key)?;
        Some((
            source.len_hint?,
            source.vec_envelope_batch_factory.as_ref()?.as_ref(),
        ))
    }
    #[must_use]
    pub fn source_len_hint(&self, key: &str) -> Option<usize> {
        self.sources.get(key).and_then(|source| source.len_hint)
    }
    #[must_use]
    pub fn source_config_digest(&self, key: &str) -> Option<&str> {
        self.sources.get(key).and_then(|source| {
            source
                .config_digest
                .as_ref()
                .map(SourceConfigDigest::as_str)
        })
    }
    /// Return a portable test stream script without opening a source.
    #[must_use]
    pub fn test_stream(&self, key: &str) -> Option<&[EncodedTestStreamEvent]> {
        self.sources.get(key)?.test_stream.as_deref()
    }
    pub fn apply(&self, key: &str, input: Envelope) -> Result<Option<Envelope>, Error> {
        match self
            .transforms
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "transform missing"))?
        {
            Transform::Single(transform) => transform(input),
            Transform::Multi(transform) => {
                let mut values = transform(input)?;
                if values.len() > 1 {
                    return Err(Error::execution(
                        key,
                        "direct",
                        "operation emits multiple elements",
                    ));
                }
                Ok(values.pop())
            }
        }
    }
    pub fn apply_many(&self, key: &str, input: Envelope) -> Result<Vec<Envelope>, Error> {
        match self
            .transforms
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "transform missing"))?
        {
            Transform::Single(transform) => Ok(transform(input)?.into_iter().collect()),
            Transform::Multi(transform) => transform(input),
        }
    }
    #[must_use]
    pub fn has_single_transform(&self, key: &str) -> bool {
        matches!(self.transforms.get(key), Some(Transform::Single(_)))
    }
    #[must_use]
    pub fn has_transform(&self, key: &str) -> bool {
        self.transforms.contains_key(key)
    }
    #[must_use]
    pub fn single_transform(
        &self,
        key: &str,
    ) -> Option<&(dyn Fn(Envelope) -> Result<Option<Envelope>, Error> + Send + Sync)> {
        match self.transforms.get(key)? {
            Transform::Single(transform) => Some(transform.as_ref()),
            Transform::Multi(_) => None,
        }
    }
    /// Return a typed same-type map or filter when a route can avoid envelopes.
    #[must_use]
    pub fn typed_value_transform<T: Element>(
        &self,
        key: &str,
    ) -> Option<TypedValueTransform<'_, T>> {
        match self.typed_transforms.get(key)? {
            TypedTransformRegistration::Map(map) => map
                .downcast_ref::<TypedMap<T, T>>()
                .map(|map| TypedValueTransform::Map(map.as_ref())),
            TypedTransformRegistration::Filter(predicate) => predicate
                .downcast_ref::<TypedFilter<T>>()
                .map(|predicate| TypedValueTransform::Filter(predicate.as_ref())),
            TypedTransformRegistration::Timestamp(_) => None,
        }
    }
    #[must_use]
    pub fn typed_timestamp_transform(&self, key: &str) -> Option<&(dyn Any + Send + Sync)> {
        match self.typed_transforms.get(key)? {
            TypedTransformRegistration::Timestamp(timestamp) => Some(timestamp.as_ref()),
            _ => None,
        }
    }
    #[must_use]
    pub fn is_keyed_timestamp_reification(&self, key: &str) -> bool {
        self.keyed_timestamp_reifications.contains(key)
    }
    pub fn apply_aggregate(
        &self,
        key: &str,
        inputs: Vec<Vec<Envelope>>,
    ) -> Result<Vec<Envelope>, Error> {
        self.aggregates
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "aggregate missing"))?(inputs)
    }
    /// Return a typed in-memory inner join registered for two vector sources.
    #[must_use]
    pub fn typed_join<T: Element>(&self, key: &str) -> Option<&TypedJoin<T>> {
        self.typed_joins
            .get(key)?
            .downcast_ref::<TypedJoinFactory<T>>()
            .map(Arc::as_ref)
    }
    #[must_use]
    pub fn has_aggregate(&self, key: &str) -> bool {
        self.aggregates.contains_key(key)
    }
    #[must_use]
    pub fn has_sink(&self, key: &str) -> bool {
        self.sinks.contains_key(key)
    }
    #[must_use]
    pub fn sink_config_digest(&self, key: &str) -> Option<&str> {
        self.sinks
            .get(key)
            .and_then(|sink| sink.config_digest.as_deref())
    }
    #[must_use]
    pub fn sink_requires_bundle_finalization(&self, key: &str) -> bool {
        self.sinks
            .get(key)
            .is_some_and(|sink| sink.bundle_finalization)
    }
    pub fn open_sink(&self, key: &str) -> Result<Box<dyn SinkSession>, Error> {
        let sink = self
            .sinks
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "sink factory missing"))?;
        (sink.factory)()
    }
}

#[derive(Default)]
struct Builder {
    graph: Graph,
    registry: Registry,
}
impl Default for Graph {
    fn default() -> Self {
        Self {
            version: GRAPH_VERSION,
            checkpoint_version: CHECKPOINT_VERSION,
            nodes: Vec::new(),
        }
    }
}
#[derive(Clone, Default)]
pub struct Pipeline {
    inner: Arc<Mutex<Builder>>,
}

pub struct PCollection<T> {
    pipeline: Pipeline,
    key: String,
    marker: PhantomData<T>,
}
impl<T> Clone for PCollection<T> {
    fn clone(&self) -> Self {
        Self {
            pipeline: self.pipeline.clone(),
            key: self.key.clone(),
            marker: PhantomData,
        }
    }
}

impl Pipeline {
    fn semantic_identity(operation: &Operation) -> (String, Version) {
        match operation {
            Operation::Source { .. } => ("ironbeam/source".into(), BUILTIN_OPERATION_VERSION),
            Operation::Map => ("ironbeam/map".into(), BUILTIN_OPERATION_VERSION),
            Operation::Filter => ("ironbeam/filter".into(), BUILTIN_OPERATION_VERSION),
            Operation::FlatMap => ("ironbeam/flat-map".into(), BUILTIN_OPERATION_VERSION),
            Operation::AssignTimestamp => (
                "ironbeam/assign-timestamp".into(),
                BUILTIN_OPERATION_VERSION,
            ),
            Operation::Flatten => ("ironbeam/flatten".into(), BUILTIN_OPERATION_VERSION),
            Operation::Reshuffle => ("ironbeam/reshuffle".into(), BUILTIN_OPERATION_VERSION),
            Operation::WaitOn => ("ironbeam/wait-on".into(), BUILTIN_OPERATION_VERSION),
            Operation::GroupByKey => ("ironbeam/group-by-key".into(), BUILTIN_OPERATION_VERSION),
            Operation::CoGroupByKey => {
                ("ironbeam/co-group-by-key".into(), BUILTIN_OPERATION_VERSION)
            }
            Operation::JoinInner => ("ironbeam/join-inner".into(), BUILTIN_OPERATION_VERSION),
            Operation::JoinLeft => ("ironbeam/join-left".into(), BUILTIN_OPERATION_VERSION),
            Operation::JoinRight => ("ironbeam/join-right".into(), BUILTIN_OPERATION_VERSION),
            Operation::JoinFull => ("ironbeam/join-full".into(), BUILTIN_OPERATION_VERSION),
            Operation::SideInputMap => {
                ("ironbeam/side-input-map".into(), BUILTIN_OPERATION_VERSION)
            }
            Operation::FixedWindow => ("ironbeam/fixed-window".into(), BUILTIN_OPERATION_VERSION),
            Operation::GlobalCombine => {
                ("ironbeam/global-combine".into(), BUILTIN_OPERATION_VERSION)
            }
            Operation::FoldGlobal => ("ironbeam/fold-global".into(), BUILTIN_OPERATION_VERSION),
            Operation::FoldPerKey => ("ironbeam/fold-per-key".into(), BUILTIN_OPERATION_VERSION),
            Operation::FoldOrderedGlobal => (
                "ironbeam/fold-ordered-global".into(),
                BUILTIN_OPERATION_VERSION,
            ),
            Operation::FoldOrderedPerKey => (
                "ironbeam/fold-ordered-per-key".into(),
                BUILTIN_OPERATION_VERSION,
            ),
            Operation::BatchMapValues { .. } => (
                "ironbeam/batch-map-values".into(),
                BUILTIN_OPERATION_VERSION,
            ),
            Operation::Custom {
                semantic_id,
                version,
            } => (semantic_id.clone(), *version),
            Operation::Output => ("ironbeam/output".into(), BUILTIN_OPERATION_VERSION),
            Operation::Sink { .. } => ("ironbeam/sink".into(), BUILTIN_OPERATION_VERSION),
        }
    }
    fn add<T: Element>(
        &self,
        mut node: Node,
        source: Option<SourceRegistration>,
        transform: Option<Transform>,
        aggregate: Option<Aggregate>,
        sink: Option<SinkRegistration>,
        group_coder: Option<GroupCoderRegistration>,
    ) -> Result<(), Error> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| Error::graph(&node.key, "builder lock poisoned"))?;
        if inner.graph.node(&node.key).is_some() {
            return Err(Error::graph(&node.key, "duplicate stable node key"));
        }
        if node.type_id != T::type_identity()
            || node.coder_id != T::coder_identity()
            || node.coder_version != T::CODER_VERSION
        {
            return Err(Error::graph(
                &node.key,
                "node element descriptor differs from coder",
            ));
        }
        node.local_id = inner.graph.nodes.len();
        if T::supports_coder() {
            inner
                .registry
                .coders
                .insert(node.key.clone(), CoderRegistration::for_element::<T>());
        }
        if let Some(source) = source {
            inner.registry.sources.insert(node.key.clone(), source);
        }
        if let Some(transform) = transform {
            inner
                .registry
                .transforms
                .insert(node.key.clone(), transform);
        }
        if let Some(aggregate) = aggregate {
            inner
                .registry
                .aggregates
                .insert(node.key.clone(), aggregate);
        }
        if let Some(sink) = sink {
            inner.registry.sinks.insert(node.key.clone(), sink);
        }
        if let Some(group_coder) = group_coder {
            inner
                .registry
                .group_coders
                .insert(node.key.clone(), group_coder);
        }
        inner.graph.nodes.push(node);
        drop(inner);
        Ok(())
    }
    fn add_fold<T: Element>(
        &self,
        mut node: Node,
        factory: FoldFactory,
        group_coder: Option<GroupCoderRegistration>,
    ) -> Result<(), Error> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| Error::graph(&node.key, "builder lock poisoned"))?;
        if inner.graph.node(&node.key).is_some() {
            return Err(Error::graph(&node.key, "duplicate stable node key"));
        }
        if node.type_id != T::type_identity()
            || node.coder_id != T::coder_identity()
            || node.coder_version != T::CODER_VERSION
        {
            return Err(Error::graph(
                &node.key,
                "node element descriptor differs from coder",
            ));
        }
        node.local_id = inner.graph.nodes.len();
        if T::supports_coder() {
            inner
                .registry
                .coders
                .insert(node.key.clone(), CoderRegistration::for_element::<T>());
        }
        inner.registry.folds.insert(node.key.clone(), factory);
        if let Some(group_coder) = group_coder {
            inner
                .registry
                .group_coders
                .insert(node.key.clone(), group_coder);
        }
        inner.graph.nodes.push(node);
        drop(inner);
        Ok(())
    }
    fn add_typed_transform(
        &self,
        key: &str,
        transform: TypedTransformRegistration,
    ) -> Result<(), Error> {
        self.inner
            .lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?
            .registry
            .typed_transforms
            .insert(key.to_owned(), transform);
        Ok(())
    }
    /// Build a bounded collection from an owned vector.
    ///
    /// # Panics
    /// Panics only if an internal built-in coder violates its infallible contract
    /// while computing the deferred source identity.
    pub fn from_vec<T: Element + Clone + Sync>(
        &self,
        key: &str,
        values: Vec<T>,
    ) -> Result<PCollection<T>, Error> {
        let values = Arc::new(values);
        let config_digest = if !T::supports_coder() {
            None
        } else if infallible_builtin_coder::<T>() {
            let digest_values = Arc::clone(&values);
            Some(SourceConfigDigest::Deferred(Arc::new(
                DeferredSourceDigest {
                    value: OnceLock::new(),
                    compute: Box::new(move || {
                        vector_source_digest(&digest_values)
                            .expect("built-in vector coder is infallible")
                    }),
                },
            )))
        } else {
            Some(SourceConfigDigest::Ready(vector_source_digest(&values)?))
        };
        let len_hint = values.len();
        let batch_values = Arc::clone(&values);
        let batch_key = key.to_owned();
        let vec_batch_factory: TypedVecBatchFactory<T> = Arc::new(move |start, len| {
            let end = start
                .checked_add(len)
                .ok_or_else(|| Error::graph(&batch_key, "vector source batch range overflow"))?;
            let slice = batch_values.get(start..end).ok_or_else(|| {
                Error::graph(&batch_key, "vector source batch range exceeds length")
            })?;
            Ok(slice.to_vec())
        });
        let erased_batch = Arc::clone(&vec_batch_factory);
        let erased_vec_batch_factory: ErasedVecBatchFactory = Arc::new(move |start, len| {
            erased_batch(start, len).map(|values| Box::new(values) as Payload)
        });
        let envelope_batch = Arc::clone(&vec_batch_factory);
        let vec_envelope_batch_factory: VecEnvelopeBatchFactory = Arc::new(move |start, len| {
            envelope_batch(start, len).map(|values| {
                values
                    .into_iter()
                    .map(|value| Envelope {
                        value: Box::new(value),
                        timestamp_ns: 0,
                        window: Window::Global,
                        assigned_window: None,
                    })
                    .collect()
            })
        });
        let typed_factory: TypedSourceFactory<T> = Arc::new(move || {
            let values = Arc::clone(&values);
            Ok(Box::new(
                (0..values.len()).map(move |i| Ok(values[i].clone())),
            ))
        });
        let envelope_factory = Arc::clone(&typed_factory);
        let factory: SourceFactory = Arc::new(move || {
            let source = envelope_factory()?;
            Ok(Box::new(source.map(|result| {
                result.map(|value| Envelope {
                    value: Box::new(value),
                    timestamp_ns: 0,
                    window: Window::Global,
                    assigned_window: None,
                })
            })) as SourceIterator)
        });
        self.source_node::<T>(
            key,
            "ironbeam/source",
            Boundedness::Bounded,
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            Some(SourceRegistration {
                factory,
                unbounded_factory: None,
                typed_factory: Some(Arc::new(typed_factory)),
                vec_batch_factory: Some(Arc::new(vec_batch_factory)),
                erased_vec_batch_factory: Some((TypeId::of::<Vec<T>>(), erased_vec_batch_factory)),
                vec_envelope_batch_factory: Some(vec_envelope_batch_factory),
                len_hint: Some(len_hint),
                config_digest,
                test_stream: None,
            }),
            0,
        )
    }
    /// Create a bounded in-memory source from an iterator.
    pub fn from_iter<T: Element + Clone + Sync>(
        &self,
        key: &str,
        values: impl IntoIterator<Item = T>,
    ) -> Result<PCollection<T>, Error> {
        self.from_vec(key, values.into_iter().collect())
    }
    pub fn bounded_source<T: Element>(
        &self,
        key: &str,
        progress: SourceProgress,
        config_digest: String,
        len_hint: Option<usize>,
        open: impl Fn() -> Result<TypedSourceIterator<T>, Error> + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error> {
        self.bounded_source_with_semantic(
            key,
            "ironbeam/source",
            progress,
            config_digest,
            len_hint,
            open,
        )
    }
    pub fn bounded_source_with_semantic<T: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        progress: SourceProgress,
        config_digest: String,
        len_hint: Option<usize>,
        open: impl Fn() -> Result<TypedSourceIterator<T>, Error> + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error> {
        if config_digest.is_empty() {
            return Err(Error::graph(
                key,
                "bounded source configuration digest is required",
            ));
        }
        let typed_factory: TypedSourceFactory<T> = Arc::new(open);
        let envelope_factory = Arc::clone(&typed_factory);
        let factory: SourceFactory = Arc::new(move || {
            let source = envelope_factory()?;
            Ok(Box::new(source.map(|result| {
                result.map(|value| Envelope {
                    value: Box::new(value),
                    timestamp_ns: 0,
                    window: Window::Global,
                    assigned_window: None,
                })
            })) as SourceIterator)
        });
        self.source_node(
            key,
            semantic_id,
            Boundedness::Bounded,
            progress,
            Some(SourceRegistration {
                factory,
                unbounded_factory: None,
                typed_factory: Some(Arc::new(typed_factory)),
                vec_batch_factory: None,
                erased_vec_batch_factory: None,
                vec_envelope_batch_factory: None,
                len_hint,
                config_digest: Some(SourceConfigDigest::Ready(config_digest)),
                test_stream: None,
            }),
            0,
        )
    }
    pub fn unbounded_source<T: Element>(
        &self,
        key: &str,
        progress: SourceProgress,
        initial_timestamp_ns: i64,
    ) -> Result<PCollection<T>, Error> {
        self.source_node::<T>(
            key,
            "ironbeam/source",
            Boundedness::Unbounded,
            progress,
            None,
            initial_timestamp_ns,
        )
    }
    /// Register a resumable, nonblocking unbounded source implementation.
    /// `open` receives the last committed checkpoint position, if any.
    pub fn unbounded_source_with_factory<T: Element>(
        &self,
        key: &str,
        progress: SourceProgress,
        config_digest: String,
        initial_timestamp_ns: i64,
        open: impl Fn(Option<Vec<u8>>) -> Result<TypedUnboundedSourceReader<T>, Error>
        + Send
        + Sync
        + 'static,
    ) -> Result<PCollection<T>, Error> {
        if config_digest.is_empty() {
            return Err(Error::graph(
                key,
                "unbounded source configuration digest is required",
            ));
        }
        let typed_factory = Arc::new(open);
        let unbounded_factory: UnboundedSourceFactory = Arc::new(move |position| {
            Ok(Box::new(TypedUnboundedAdapter {
                inner: typed_factory(position)?,
            }))
        });
        self.source_node::<T>(
            key,
            "ironbeam/source",
            Boundedness::Unbounded,
            progress,
            Some(SourceRegistration {
                factory: Arc::new(|| Err(Error::incompatible("unbounded source requires polling"))),
                unbounded_factory: Some(unbounded_factory),
                typed_factory: None,
                vec_batch_factory: None,
                erased_vec_batch_factory: None,
                vec_envelope_batch_factory: None,
                len_hint: None,
                config_digest: Some(SourceConfigDigest::Ready(config_digest)),
                test_stream: None,
            }),
            initial_timestamp_ns,
        )
    }
    /// Register a deterministic unbounded fixture with event-time and
    /// processing-time controls for portable runner conformance tests.
    pub fn test_stream_source<T: Element>(
        &self,
        key: &str,
        events: Vec<TestStreamEvent<T>>,
    ) -> Result<PCollection<T>, Error> {
        if !T::supports_coder() || events.is_empty() {
            return Err(Error::graph(key, "test stream needs a coder and events"));
        }
        let mut encoded = Vec::with_capacity(events.len());
        let mut watermark = i64::MIN;
        for event in events {
            encoded.push(match event {
                TestStreamEvent::Elements(values) => EncodedTestStreamEvent::Elements(
                    values
                        .into_iter()
                        .map(|(value, timestamp)| value.encode().map(|bytes| (bytes, timestamp)))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                TestStreamEvent::AdvanceWatermark(next) => {
                    if next <= watermark {
                        return Err(Error::graph(key, "test stream watermark must advance"));
                    }
                    watermark = next;
                    EncodedTestStreamEvent::AdvanceWatermark(next)
                }
                TestStreamEvent::AdvanceProcessingTime(duration) => {
                    if duration <= 0 {
                        return Err(Error::graph(
                            key,
                            "test stream processing time must advance",
                        ));
                    }
                    EncodedTestStreamEvent::AdvanceProcessingTime(duration)
                }
            });
        }
        let bytes =
            serde_json::to_vec(&encoded).map_err(|error| Error::incompatible(error.to_string()))?;
        let mut digest = String::from("sha256-v1:");
        for byte in Sha256::digest(bytes) {
            write!(digest, "{byte:02x}").map_err(|error| Error::incompatible(error.to_string()))?;
        }
        self.source_node::<T>(
            key,
            "ironbeam/test-stream",
            Boundedness::Unbounded,
            SourceProgress {
                stable_split_id: true,
                replay: Replay::Idempotent,
                reports_event_time: true,
            },
            Some(SourceRegistration {
                factory: Arc::new(|| {
                    Err(Error::incompatible(
                        "test stream requires a portable runner",
                    ))
                }),
                unbounded_factory: None,
                typed_factory: None,
                vec_batch_factory: None,
                erased_vec_batch_factory: None,
                vec_envelope_batch_factory: None,
                len_hint: None,
                config_digest: Some(SourceConfigDigest::Ready(digest)),
                test_stream: Some(encoded),
            }),
            0,
        )
    }
    fn source_node<T: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        boundedness: Boundedness,
        progress: SourceProgress,
        factory: Option<SourceRegistration>,
        initial_timestamp_ns: i64,
    ) -> Result<PCollection<T>, Error> {
        let operation = Operation::Source { progress };
        if !semantic_id.contains('/') {
            return Err(Error::graph(key, "source semantic ID must be namespaced"));
        }
        let (_, semantic_version) = Self::semantic_identity(&operation);
        self.add::<T>(
            Node {
                key: key.into(),
                input: None,
                input_element: None,
                additional_inputs: Vec::new(),
                operation,
                semantic_id: semantic_id.into(),
                semantic_version,
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
                boundedness,
                time: Time {
                    initial_timestamp_ns,
                    window: Window::Global,
                },
                local_id: 0,
            },
            factory,
            None,
            None,
            None,
            None,
        )?;
        Ok(PCollection {
            pipeline: self.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    pub fn snapshot(&self) -> Result<(Graph, Registry), Error> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| Error::graph("pipeline", "builder lock poisoned"))?;
        inner.graph.validate()?;
        Ok((inner.graph.clone(), inner.registry.clone()))
    }
}

impl<T: Element> PCollection<T> {
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
    fn parent(&self) -> Result<Node, Error> {
        let inner = self
            .pipeline
            .inner
            .lock()
            .map_err(|_| Error::graph(&self.key, "builder lock poisoned"))?;
        inner
            .graph
            .node(&self.key)
            .cloned()
            .ok_or_else(|| Error::graph(&self.key, "collection missing"))
    }
    fn next<U: Element>(
        &self,
        key: &str,
        operation: Operation,
        transform: Option<Transform>,
        window: Option<Window>,
    ) -> Result<PCollection<U>, Error> {
        self.next_with_semantic(key, operation, transform, window, None)
    }
    fn next_with_semantic<U: Element>(
        &self,
        key: &str,
        operation: Operation,
        transform: Option<Transform>,
        window: Option<Window>,
        semantic: Option<(&str, Version)>,
    ) -> Result<PCollection<U>, Error> {
        let parent = self.parent()?;
        let (semantic_id, semantic_version) = semantic.map_or_else(
            || Pipeline::semantic_identity(&operation),
            |(id, version)| (id.to_owned(), version),
        );
        let node = Node {
            key: key.into(),
            input: Some(self.key.clone()),
            input_element: Some(ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs: Vec::new(),
            operation,
            semantic_id,
            semantic_version,
            type_id: U::type_identity(),
            coder_id: U::coder_identity(),
            coder_version: U::CODER_VERSION,
            boundedness: parent.boundedness,
            time: Time {
                initial_timestamp_ns: parent.time.initial_timestamp_ns,
                window: window.unwrap_or(parent.time.window),
            },
            local_id: 0,
        };
        self.pipeline
            .add::<U>(node, None, transform, None, None, None)?;
        Ok(PCollection {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    fn next_aggregate<U: Element>(
        &self,
        key: &str,
        operation: Operation,
        additional_inputs: Vec<InputEdge>,
        boundedness: Boundedness,
        aggregate: Aggregate,
        group_coder: Option<GroupCoderRegistration>,
    ) -> Result<PCollection<U>, Error> {
        let parent = self.parent()?;
        let (semantic_id, semantic_version) = Pipeline::semantic_identity(&operation);
        let node = Node {
            key: key.into(),
            input: Some(self.key.clone()),
            input_element: Some(ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs,
            operation,
            semantic_id,
            semantic_version,
            type_id: U::type_identity(),
            coder_id: U::coder_identity(),
            coder_version: U::CODER_VERSION,
            boundedness,
            time: parent.time,
            local_id: 0,
        };
        self.pipeline
            .add::<U>(node, None, None, Some(aggregate), None, group_coder)?;
        Ok(PCollection {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    fn next_fold<U: Element>(
        &self,
        key: &str,
        operation: Operation,
        factory: FoldFactory,
        group_coder: Option<GroupCoderRegistration>,
    ) -> Result<PCollection<U>, Error> {
        let parent = self.parent()?;
        let (semantic_id, semantic_version) = Pipeline::semantic_identity(&operation);
        let node = Node {
            key: key.into(),
            input: Some(self.key.clone()),
            input_element: Some(ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs: Vec::new(),
            operation,
            semantic_id,
            semantic_version,
            type_id: U::type_identity(),
            coder_id: U::coder_identity(),
            coder_version: U::CODER_VERSION,
            boundedness: parent.boundedness,
            time: parent.time,
            local_id: 0,
        };
        self.pipeline.add_fold::<U>(node, factory, group_coder)?;
        Ok(PCollection {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    pub fn map_named<U: Element>(
        &self,
        key: &str,
        f: impl Fn(T) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.map_registered(key, None, f)
    }
    /// Register a versioned unary map implementation under an application
    /// semantic ID. The portable worker must construct the same handler, and
    /// callers must change the version when its behavior changes: closure
    /// bytes cannot be fingerprinted by the graph descriptor.
    pub fn map_with_semantic_named<U: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        f: impl Fn(T) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        self.map_registered(key, Some((semantic_id, version)), f)
    }
    fn validate_semantic_handler(
        key: &str,
        semantic_id: &str,
        version: Version,
    ) -> Result<(), Error> {
        if !semantic_id.contains('/') || semantic_id.starts_with("ironbeam/") || version.major == 0
        {
            return Err(Error::graph(
                key,
                "semantic handler requires an application namespace and major version",
            ));
        }
        Ok(())
    }
    fn map_registered<U: Element>(
        &self,
        key: &str,
        semantic: Option<(&str, Version)>,
        f: impl Fn(T) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let key_owned = key.to_owned();
        let f = Arc::new(f);
        let envelope_f = Arc::clone(&f);
        let typed_f: TypedMap<T, U> = f;
        let transform = Transform::Single(Arc::new(move |mut e| {
            let value = e
                .value
                .downcast::<T>()
                .map_err(|_| Error::execution(&key_owned, "direct", "input type mismatch"))?;
            e.value = Box::new(envelope_f(*value));
            Ok(Some(e))
        }));
        let output =
            self.next_with_semantic(key, Operation::Map, Some(transform), None, semantic)?;
        self.pipeline
            .add_typed_transform(key, TypedTransformRegistration::Map(Arc::new(typed_f)))?;
        Ok(output)
    }
    pub fn filter_named(
        &self,
        key: &str,
        f: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        self.filter_registered(key, None, f)
    }
    /// Register a versioned predicate under an application semantic ID.
    /// The portable worker must construct the same predicate and version.
    pub fn filter_with_semantic_named(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        f: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        self.filter_registered(key, Some((semantic_id, version)), f)
    }
    fn filter_registered(
        &self,
        key: &str,
        semantic: Option<(&str, Version)>,
        f: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        let key_owned = key.to_owned();
        let f = Arc::new(f);
        let envelope_f = Arc::clone(&f);
        let typed_f: TypedFilter<T> = f;
        let transform = Transform::Single(Arc::new(move |e| {
            let value = e
                .value
                .downcast_ref::<T>()
                .ok_or_else(|| Error::execution(&key_owned, "direct", "input type mismatch"))?;
            Ok(if envelope_f(value) { Some(e) } else { None })
        }));
        let output =
            self.next_with_semantic(key, Operation::Filter, Some(transform), None, semantic)?;
        self.pipeline
            .add_typed_transform(key, TypedTransformRegistration::Filter(Arc::new(typed_f)))?;
        Ok(output)
    }
    pub fn flat_map_named<U: Element>(
        &self,
        key: &str,
        f: impl Fn(T) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.flat_map_registered(key, None, f)
    }
    /// Register a versioned zero-or-many-output handler under an application
    /// semantic ID. The portable worker must construct the same handler.
    pub fn flat_map_with_semantic_named<U: Element>(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
        f: impl Fn(T) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        Self::validate_semantic_handler(key, semantic_id, version)?;
        self.flat_map_registered(key, Some((semantic_id, version)), f)
    }
    fn flat_map_registered<U: Element>(
        &self,
        key: &str,
        semantic: Option<(&str, Version)>,
        f: impl Fn(T) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let key_owned = key.to_owned();
        let transform = Transform::Multi(Arc::new(move |e| {
            let Envelope {
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = e;
            let value = value
                .downcast::<T>()
                .map_err(|_| Error::execution(&key_owned, "direct", "input type mismatch"))?;
            Ok(f(*value)
                .into_iter()
                .map(|value| Envelope {
                    value: Box::new(value),
                    timestamp_ns,
                    window: window.clone(),
                    assigned_window,
                })
                .collect())
        }));
        self.next_with_semantic(key, Operation::FlatMap, Some(transform), None, semantic)
    }
    pub fn flatten_named(&self, key: &str, others: &[Self]) -> Result<Self, Error> {
        if others.is_empty() {
            return Err(Error::graph(key, "flatten requires at least two inputs"));
        }
        let parent = self.parent()?;
        let mut additional_inputs = Vec::with_capacity(others.len());
        let mut unbounded = parent.boundedness == Boundedness::Unbounded;
        for other in others {
            if !Arc::ptr_eq(&self.pipeline.inner, &other.pipeline.inner) {
                return Err(Error::graph(
                    key,
                    "flatten inputs belong to different pipelines",
                ));
            }
            let upstream = other.parent()?;
            if upstream.time != parent.time {
                return Err(Error::graph(
                    key,
                    "flatten inputs must have equal time/window declarations",
                ));
            }
            unbounded |= upstream.boundedness == Boundedness::Unbounded;
            additional_inputs.push(InputEdge {
                key: other.key.clone(),
                element: ElementDescriptor {
                    type_id: T::type_identity(),
                    coder_id: T::coder_identity(),
                    coder_version: T::CODER_VERSION,
                },
            });
        }
        let operation = Operation::Flatten;
        let (semantic_id, semantic_version) = Pipeline::semantic_identity(&operation);
        let node = Node {
            key: key.into(),
            input: Some(self.key.clone()),
            input_element: Some(ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs,
            operation,
            semantic_id,
            semantic_version,
            type_id: T::type_identity(),
            coder_id: T::coder_identity(),
            coder_version: T::CODER_VERSION,
            boundedness: if unbounded {
                Boundedness::Unbounded
            } else {
                Boundedness::Bounded
            },
            time: parent.time,
            local_id: 0,
        };
        self.pipeline.add::<T>(node, None, None, None, None, None)?;
        Ok(Self {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    pub fn reshuffle_named(&self, key: &str) -> Result<Self, Error> {
        self.next(key, Operation::Reshuffle, None, None)
    }
    pub fn wait_on_named<S: Element>(
        &self,
        key: &str,
        signal: &PCollection<S>,
    ) -> Result<Self, Error> {
        if !Arc::ptr_eq(&self.pipeline.inner, &signal.pipeline.inner) {
            return Err(Error::graph(
                key,
                "wait inputs belong to different pipelines",
            ));
        }
        let parent = self.parent()?;
        if signal.parent()?.boundedness == Boundedness::Unbounded {
            return Err(Error::graph(key, "wait signal must be bounded"));
        }
        let operation = Operation::WaitOn;
        let (semantic_id, semantic_version) = Pipeline::semantic_identity(&operation);
        let node = Node {
            key: key.into(),
            input: Some(self.key.clone()),
            input_element: Some(ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs: vec![InputEdge {
                key: signal.key.clone(),
                element: ElementDescriptor {
                    type_id: S::type_identity(),
                    coder_id: S::coder_identity(),
                    coder_version: S::CODER_VERSION,
                },
            }],
            operation,
            semantic_id,
            semantic_version,
            type_id: T::type_identity(),
            coder_id: T::coder_identity(),
            coder_version: T::CODER_VERSION,
            boundedness: parent.boundedness,
            time: parent.time,
            local_id: 0,
        };
        self.pipeline.add::<T>(node, None, None, None, None, None)?;
        Ok(Self {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    pub fn fixed_windows(&self, key: &str, size_ns: i64) -> Result<Self, Error> {
        if size_ns <= 0 {
            return Err(Error::graph(key, "fixed window size must be positive"));
        }
        let window = Window::Fixed { size_ns };
        let runtime_window = window.clone();
        let transform = Transform::Single(Arc::new(move |mut e| {
            e.assigned_window = runtime_window.assign(e.timestamp_ns)?;
            e.window = runtime_window.clone();
            Ok(Some(e))
        }));
        self.next(key, Operation::FixedWindow, Some(transform), Some(window))
    }
    pub fn fixed_windows_with_offset(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<Self, Error> {
        if size_ns <= 0 {
            return Err(Error::graph(key, "fixed window size must be positive"));
        }
        let window = Window::FixedOffset { size_ns, offset_ns };
        let runtime_window = window.clone();
        let transform = Transform::Single(Arc::new(move |mut event| {
            event.assigned_window = runtime_window.assign(event.timestamp_ns)?;
            event.window = runtime_window.clone();
            Ok(Some(event))
        }));
        self.next(key, Operation::FixedWindow, Some(transform), Some(window))
    }
    pub fn with_window_named(&self, key: &str) -> Result<PCollection<(WindowInterval, T)>, Error> {
        if self.parent()?.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "window metadata requires a finite window",
            ));
        }
        let node_key = key.to_owned();
        let transform = Transform::Single(Arc::new(move |event| {
            let Envelope {
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let interval = assigned_window
                .ok_or_else(|| Error::execution(&node_key, "direct", "window interval missing"))?;
            let value = *value
                .downcast::<T>()
                .map_err(|_| Error::execution(&node_key, "direct", "window input type mismatch"))?;
            Ok(Some(Envelope {
                value: Box::new((interval, value)),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        self.next(key, Operation::Map, Some(transform), None)
    }
    pub fn assign_timestamps_named(
        &self,
        key: &str,
        f: impl Fn(&T) -> i64 + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        if self.parent()?.time.window != Window::Global {
            return Err(Error::graph(key, "assign timestamps before windowing"));
        }
        let key_owned = key.to_owned();
        let timestamp: TypedTimestamp<T> = Arc::new(f);
        let envelope_timestamp = Arc::clone(&timestamp);
        let transform = Transform::Single(Arc::new(move |mut event| {
            let value = event.value.downcast_ref::<T>().ok_or_else(|| {
                Error::execution(&key_owned, "direct", "timestamp input type mismatch")
            })?;
            event.timestamp_ns = envelope_timestamp(value);
            Ok(Some(event))
        }));
        let output = self.next(key, Operation::AssignTimestamp, Some(transform), None)?;
        self.pipeline.add_typed_transform(
            key,
            TypedTransformRegistration::Timestamp(Arc::new(timestamp)),
        )?;
        Ok(output)
    }
    /// Expose each element's event timestamp as `(nanoseconds, value)` while
    /// retaining the timestamp and window in the output envelope.
    pub fn reify_timestamps_named(&self, key: &str) -> Result<PCollection<(i64, T)>, Error> {
        let key_owned = key.to_owned();
        let transform = Transform::Single(Arc::new(move |event| {
            let Envelope {
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let value = *value.downcast::<T>().map_err(|_| {
                Error::execution(&key_owned, "direct", "timestamp input type mismatch")
            })?;
            Ok(Some(Envelope {
                value: Box::new((timestamp_ns, value)),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        self.next(key, Operation::Map, Some(transform), None)
    }
    pub fn output(&self, key: &str) -> Result<Output<T>, Error> {
        self.next::<T>(key, Operation::Output, None, None)?;
        Ok(Output {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    /// Create an output terminal on an isolated graph snapshot.
    /// Repeated local collections can use the same stable terminal key without
    /// registering duplicate nodes on the source pipeline.
    pub fn detached_output(&self, key: &str) -> Result<Output<T>, Error> {
        let (graph, registry) = self.pipeline.snapshot()?;
        let detached = Self {
            pipeline: Pipeline {
                inner: Arc::new(Mutex::new(Builder { graph, registry })),
            },
            key: self.key.clone(),
            marker: PhantomData,
        };
        detached.output(key)
    }
    pub fn append_sink(&self, key: &str, replay: Replay) -> Result<(), Error> {
        self.next::<T>(
            key,
            Operation::Sink {
                spec: SinkSpec {
                    mode: SinkMode::Append,
                    replay,
                    commit: true,
                },
            },
            None,
            None,
        )?;
        Ok(())
    }
    pub fn sink_named(
        &self,
        key: &str,
        spec: SinkSpec,
        open: impl Fn() -> Result<Box<dyn TypedSink<T>>, Error> + Send + Sync + 'static,
    ) -> Result<Sink<T>, Error> {
        self.sink_named_inner(key, spec, None, false, open)
    }

    /// Register a sink with a stable digest of its destination and options.
    /// Portable runners require this digest to reject a differently configured
    /// worker before opening either the source or the sink.
    pub fn sink_named_with_config(
        &self,
        key: &str,
        spec: SinkSpec,
        config_digest: String,
        open: impl Fn() -> Result<Box<dyn TypedSink<T>>, Error> + Send + Sync + 'static,
    ) -> Result<Sink<T>, Error> {
        if config_digest.is_empty() {
            return Err(Error::graph(key, "sink configuration digest is required"));
        }
        self.sink_named_inner(key, spec, Some(config_digest), false, open)
    }

    /// Register a sink whose bundle commit must be confirmed by the runner.
    /// The session returned by `open` must also declare finalization support.
    pub fn sink_named_with_config_and_finalization(
        &self,
        key: &str,
        spec: SinkSpec,
        config_digest: String,
        open: impl Fn() -> Result<Box<dyn TypedSink<T>>, Error> + Send + Sync + 'static,
    ) -> Result<Sink<T>, Error> {
        if config_digest.is_empty() || !spec.commit {
            return Err(Error::graph(
                key,
                "finalized sink needs a digest and commit",
            ));
        }
        self.sink_named_inner(key, spec, Some(config_digest), true, open)
    }

    fn sink_named_inner(
        &self,
        key: &str,
        spec: SinkSpec,
        config_digest: Option<String>,
        bundle_finalization: bool,
        open: impl Fn() -> Result<Box<dyn TypedSink<T>>, Error> + Send + Sync + 'static,
    ) -> Result<Sink<T>, Error> {
        let parent = self.parent()?;
        let operation = Operation::Sink { spec };
        let (semantic_id, semantic_version) = Pipeline::semantic_identity(&operation);
        let node = Node {
            key: key.into(),
            input: Some(self.key.clone()),
            input_element: Some(ElementDescriptor {
                type_id: T::type_identity(),
                coder_id: T::coder_identity(),
                coder_version: T::CODER_VERSION,
            }),
            additional_inputs: Vec::new(),
            operation,
            semantic_id,
            semantic_version,
            type_id: T::type_identity(),
            coder_id: T::coder_identity(),
            coder_version: T::CODER_VERSION,
            boundedness: parent.boundedness,
            time: parent.time,
            local_id: 0,
        };
        let key_owned = key.to_owned();
        let factory: SinkFactory = Arc::new(move || {
            Ok(Box::new(SinkAdapter {
                key: key_owned.clone(),
                inner: open()?,
            }) as Box<dyn SinkSession>)
        });
        self.pipeline.add::<T>(
            node,
            None,
            None,
            None,
            Some(SinkRegistration {
                factory,
                config_digest,
                bundle_finalization,
            }),
            None,
        )?;
        Ok(Sink {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
    }
    pub fn custom_named(
        &self,
        key: &str,
        semantic_id: &str,
        version: Version,
    ) -> Result<Self, Error> {
        if !semantic_id.contains('/') {
            return Err(Error::graph(key, "custom operation ID must be namespaced"));
        }
        self.next(
            key,
            Operation::Custom {
                semantic_id: semantic_id.into(),
                version,
            },
            None,
            None,
        )
    }
    pub fn global_combine_marker(&self, key: &str) -> Result<Self, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        self.next(key, Operation::GlobalCombine, None, None)
    }
    pub fn combine_globally_named<U: Element>(
        &self,
        key: &str,
        f: impl Fn(Vec<T>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let key_owned = key.to_owned();
        let output_window = parent.time.window.clone();
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [values]: [Vec<Envelope>; 1] = inputs.try_into().map_err(|_| {
                Error::execution(&key_owned, "direct", "combine requires exactly one input")
            })?;
            let mut groups: BTreeMap<Option<i64>, (Vec<T>, i64, Option<WindowInterval>)> =
                BTreeMap::new();
            for event in values {
                let typed = *event.value.downcast::<T>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "combine input type mismatch")
                })?;
                let scope = event.assigned_window.map(|interval| interval.start_ns);
                groups
                    .entry(scope)
                    .or_insert_with(|| (Vec::new(), event.timestamp_ns, event.assigned_window))
                    .0
                    .push(typed);
            }
            if groups.is_empty() && output_window == Window::Global {
                groups.insert(None, (Vec::new(), 0, None));
            }
            Ok(groups
                .into_values()
                .map(|(values, timestamp_ns, assigned_window)| Envelope {
                    value: Box::new(f(values)),
                    timestamp_ns,
                    window: output_window.clone(),
                    assigned_window,
                })
                .collect())
        });
        self.next_aggregate(
            key,
            Operation::GlobalCombine,
            Vec::new(),
            parent.boundedness,
            aggregate,
            Some(GroupCoderRegistration::for_global::<T>()),
        )
    }
    pub fn combine_globally_lifted_named<C, A, O>(
        &self,
        key: &str,
        combiner: C,
    ) -> Result<PCollection<O>, Error>
    where
        C: CombineFn<T, A, O>,
        A: Send + 'static,
        O: Element,
    {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let window = parent.time.window;
        let node_key = key.to_owned();
        let combiner = Arc::new(MergeableAdapter(combiner));
        let factory: FoldFactory = Arc::new(move || {
            Box::new(GlobalFoldState::<T, A, O, MergeableAdapter<C>> {
                key: node_key.clone(),
                combiner: Arc::clone(&combiner),
                window: window.clone(),
                entries: BTreeMap::new(),
                marker: PhantomData,
            })
        });
        self.next_fold(
            key,
            Operation::FoldGlobal,
            factory,
            Some(GroupCoderRegistration::for_global::<T>()),
        )
    }
    pub fn fold_globally_ordered_named<C, A, O>(
        &self,
        key: &str,
        fold: C,
    ) -> Result<PCollection<O>, Error>
    where
        C: OrderedFoldFn<T, A, O>,
        A: Send + 'static,
        O: Element,
    {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let window = parent.time.window;
        let node_key = key.to_owned();
        let fold = Arc::new(OrderedAdapter(fold));
        let factory: FoldFactory = Arc::new(move || {
            Box::new(GlobalFoldState::<T, A, O, OrderedAdapter<C>> {
                key: node_key.clone(),
                combiner: Arc::clone(&fold),
                window: window.clone(),
                entries: BTreeMap::new(),
                marker: PhantomData,
            })
        });
        self.next_fold(key, Operation::FoldOrderedGlobal, factory, None)
    }
    /// Fold each window in an explicit order on Direct and portable runners.
    /// Order keys must be unique within a window. An empty global input emits
    /// `fold.finish(fold.create())` at timestamp zero.
    pub fn fold_globally_ordered_by_named<Q, C, A, O>(
        &self,
        key: &str,
        order_key: impl Fn(&T) -> Q + Send + Sync + 'static,
        fold: C,
    ) -> Result<PCollection<O>, Error>
    where
        Q: Ord + Send + 'static,
        C: OrderedFoldFn<T, A, O>,
        A: Send + 'static,
        O: Element,
    {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let node_key = key.to_owned();
        let output_window = parent.time.window.clone();
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [events]: [Vec<Envelope>; 1] = inputs.try_into().map_err(|_| {
                Error::execution(
                    &node_key,
                    "runner",
                    "ordered global fold requires one input",
                )
            })?;
            let mut groups: OrderedGlobalGroups<Q, T> = BTreeMap::new();
            for event in events {
                let value = *event.value.downcast::<T>().map_err(|_| {
                    Error::execution(
                        &node_key,
                        "runner",
                        "ordered global fold input type mismatch",
                    )
                })?;
                let scope = event.assigned_window.map(|interval| interval.start_ns);
                let entry = groups
                    .entry(scope)
                    .or_insert_with(|| (BTreeMap::new(), event.assigned_window));
                if entry
                    .0
                    .insert(order_key(&value), (value, event.timestamp_ns))
                    .is_some()
                {
                    return Err(Error::execution(
                        &node_key,
                        "runner",
                        "duplicate ordered fold key in key/window",
                    ));
                }
            }
            if groups.is_empty() && output_window == Window::Global {
                groups.insert(None, (BTreeMap::new(), None));
            }
            let mut result = Vec::with_capacity(groups.len());
            for (ordered, assigned_window) in groups.into_values() {
                let timestamp_ns = ordered
                    .first_key_value()
                    .map_or(0, |(_, (_, timestamp))| *timestamp);
                let mut accumulator = fold.create();
                for (_, (value, _)) in ordered {
                    fold.add_input(&mut accumulator, value);
                }
                result.push(Envelope {
                    value: Box::new(fold.finish(accumulator)),
                    timestamp_ns,
                    window: output_window.clone(),
                    assigned_window,
                });
            }
            Ok(result)
        });
        self.next_aggregate(
            key,
            Operation::GlobalCombine,
            Vec::new(),
            parent.boundedness,
            aggregate,
            Some(GroupCoderRegistration::for_global::<T>()),
        )
    }
    pub fn latest_globally_named(&self, key: &str) -> Result<Self, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let node_key = key.to_owned();
        let aggregate: Aggregate = Arc::new(move |mut inputs| {
            let values = inputs
                .pop()
                .ok_or_else(|| Error::execution(&node_key, "direct", "latest input missing"))?;
            let mut latest: BTreeMap<Option<i64>, Envelope> = BTreeMap::new();
            for event in values {
                let scope = event.assigned_window.map(|interval| interval.start_ns);
                match latest.entry(scope) {
                    std::collections::btree_map::Entry::Occupied(mut slot) => {
                        if event.timestamp_ns > slot.get().timestamp_ns {
                            slot.insert(event);
                        }
                    }
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert(event);
                    }
                }
            }
            if latest.is_empty() {
                return Err(Error::execution(
                    &node_key,
                    "direct",
                    "latest on empty input",
                ));
            }
            Ok(latest.into_values().collect())
        });
        self.next_aggregate(
            key,
            Operation::GlobalCombine,
            Vec::new(),
            parent.boundedness,
            aggregate,
            None,
        )
    }
    /// Select the latest event in each window with a deterministic tie key.
    /// Equal event times choose the greatest tie key; duplicate pairs fail.
    pub fn latest_globally_by_named<Q: Ord + Send + 'static>(
        &self,
        key: &str,
        tie_key: impl Fn(&T) -> Q + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let node_key = key.to_owned();
        let aggregate: Aggregate = Arc::new(move |mut inputs| {
            let events = inputs
                .pop()
                .ok_or_else(|| Error::execution(&node_key, "runner", "latest tie input missing"))?;
            let mut groups: LatestTieGlobalGroups<Q> = BTreeMap::new();
            for event in events {
                let value = event.value.downcast_ref::<T>().ok_or_else(|| {
                    Error::execution(&node_key, "runner", "latest tie input type mismatch")
                })?;
                let scope = event.assigned_window.map(|interval| interval.start_ns);
                let order = (event.timestamp_ns, tie_key(value));
                match groups.entry(scope) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert((BTreeSet::from([order]), event));
                    }
                    std::collections::btree_map::Entry::Occupied(mut slot) => {
                        let (seen, latest) = slot.get_mut();
                        let is_later = seen.last().is_none_or(|current| order.cmp(current).is_gt());
                        if !seen.insert(order) {
                            return Err(Error::execution(
                                &node_key,
                                "runner",
                                "duplicate latest tie key in key/window/event time",
                            ));
                        }
                        if is_later {
                            *latest = event;
                        }
                    }
                }
            }
            if groups.is_empty() {
                return Err(Error::execution(
                    &node_key,
                    "runner",
                    "latest on empty input",
                ));
            }
            Ok(groups.into_values().map(|(_, latest)| latest).collect())
        });
        self.next_aggregate(
            key,
            Operation::GlobalCombine,
            Vec::new(),
            parent.boundedness,
            aggregate,
            Some(GroupCoderRegistration::for_global::<T>()),
        )
    }
    pub fn combine_per_window_named<U: Element>(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
        f: impl Fn(Vec<T>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<(WindowInterval, U)>, Error> {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .combine_globally_named(&format!("{key}/combine"), f)?
            .with_window_named(key)
    }
    pub fn batch_elements_named(
        &self,
        key: &str,
        batch_size: usize,
    ) -> Result<PCollection<Vec<T>>, Error> {
        let limit = batch_size.max(1);
        self.combine_globally_named(&format!("{key}/collect"), |values| values)?
            .flat_map_named(key, move |values| {
                let mut batches = Vec::new();
                let mut current = Vec::with_capacity(limit);
                for value in values {
                    current.push(value);
                    if current.len() == limit {
                        batches.push(std::mem::replace(&mut current, Vec::with_capacity(limit)));
                    }
                }
                if !current.is_empty() {
                    batches.push(current);
                }
                batches
            })
    }
    pub fn map_batches_named<U: Element>(
        &self,
        key: &str,
        batch_size: usize,
        f: impl Fn(&[T]) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.batch_elements_named(&format!("{key}/batches"), batch_size)?
            .flat_map_named(key, move |batch| f(&batch))
    }
    /// Apply a versioned custom handler to explicitly sized batches.
    /// Batch boundaries follow `batch_elements_named`; input encounter order
    /// within a batch is not portable across runners after a shuffle.
    pub fn map_batches_with_semantic_named<U: Element>(
        &self,
        key: &str,
        batch_size: usize,
        semantic_id: &str,
        version: Version,
        f: impl Fn(&[T]) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.apply_transform_with_semantic_named(
            key,
            batch_size,
            semantic_id,
            version,
            move |batch| f(&batch),
        )
    }
    /// Apply a versioned partition transform to an owned, explicitly sized
    /// batch. This is the typed v5 replacement for the executable part of
    /// v4 `apply_transform`; the runner chooses no implicit partition size.
    /// A portable shuffle can change encounter order within each batch.
    pub fn apply_transform_with_semantic_named<U: Element>(
        &self,
        key: &str,
        batch_size: usize,
        semantic_id: &str,
        version: Version,
        f: impl Fn(Vec<T>) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        if batch_size == 0 {
            return Err(Error::graph(key, "semantic batch size must be positive"));
        }
        Self::validate_semantic_handler(key, semantic_id, version)?;
        self.batch_elements_named(&format!("{key}/batches"), batch_size)?
            .flat_map_with_semantic_named(key, semantic_id, version, f)
    }
    /// Register a reusable v4-style partition operator with a portable
    /// application identity. The same operator must be registered by the
    /// worker. Batch size is explicit because physical partitions differ
    /// between runners.
    pub fn apply_dyn_transform_with_semantic_named<U: Element + Sync>(
        &self,
        key: &str,
        batch_size: usize,
        semantic_id: &str,
        version: Version,
        op: Arc<dyn DynOp>,
    ) -> Result<PCollection<U>, Error>
    where
        T: Sync,
    {
        if batch_size == 0 {
            return Err(Error::graph(key, "semantic batch size must be positive"));
        }
        Self::validate_semantic_handler(key, semantic_id, version)?;
        let key_owned = key.to_owned();
        let transform = Transform::Multi(Arc::new(move |event| {
            let Envelope {
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let input = value.downcast::<Vec<T>>().map_err(|_| {
                Error::execution(&key_owned, "direct", "partition input type mismatch")
            })?;
            let output = op
                .apply(Box::new(*input))
                .downcast::<Vec<U>>()
                .map_err(|_| {
                    Error::execution(&key_owned, "direct", "partition output type mismatch")
                })?;
            Ok((*output)
                .into_iter()
                .map(|value| Envelope {
                    value: Box::new(value) as Payload,
                    timestamp_ns,
                    window: window.clone(),
                    assigned_window,
                })
                .collect())
        }));
        self.batch_elements_named(&format!("{key}/batches"), batch_size)?
            .next_with_semantic(
                key,
                Operation::FlatMap,
                Some(transform),
                None,
                Some((semantic_id, version)),
            )
    }
    pub fn batch_by_size_named(
        &self,
        key: &str,
        max_bytes: usize,
        size_fn: impl Fn(&T) -> usize + Send + Sync + 'static,
    ) -> Result<PCollection<Vec<T>>, Error> {
        let limit = max_bytes.max(1);
        self.combine_globally_named(&format!("{key}/collect"), move |values| {
            let mut batches = Vec::new();
            let mut current = Vec::new();
            let mut bytes = 0_usize;
            for value in values {
                let size = size_fn(&value);
                if !current.is_empty() && bytes.saturating_add(size) > limit {
                    batches.push(std::mem::take(&mut current));
                    bytes = 0;
                }
                bytes = bytes.saturating_add(size);
                current.push(value);
            }
            if !current.is_empty() {
                batches.push(current);
            }
            batches
        })?
        .flat_map_named(key, |batches| batches)
    }
    pub fn map_with_side_named<S: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        f: impl Fn(T, &[S]) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.map_with_materialized_side_named(key, side, Ok, move |value, values: &Vec<S>| {
            Ok(f(value, values))
        })
    }
    /// Apply a bounded side collection separately within each matching window.
    /// This composes a keyed co-group, so the runner may shuffle and buffer all
    /// values in a window. Main element timestamps are restored after the group.
    /// Use `map_with_side_named` for one global side view across all windows.
    pub fn map_with_windowed_side_named<S: Element, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        f: impl Fn(T, &[S]) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        if !Arc::ptr_eq(&self.pipeline.inner, &side.pipeline.inner) {
            return Err(Error::graph(
                key,
                "side input belongs to a different pipeline",
            ));
        }
        let main = self.parent()?;
        let other = side.parent()?;
        if other.boundedness != Boundedness::Bounded {
            return Err(Error::graph(key, "side input must be bounded"));
        }
        if main.time != other.time {
            return Err(Error::graph(
                key,
                "side input time/window declaration mismatch",
            ));
        }
        let main = self
            .reify_timestamps_named(&format!("{key}/main-time"))?
            .map_named(&format!("{key}/main-key"), |value| (0_i32, value))?;
        let side = side.map_named(&format!("{key}/side-key"), |value| (0_i32, value))?;
        crate::cogroup_by_key_2_named(&format!("{key}/group"), &main, &side)?
            .flat_map_named(&format!("{key}/apply"), move |(_, (main, side))| {
                main.into_iter()
                    .map(|(timestamp_ns, value)| (timestamp_ns, f(value, &side)))
                    .collect::<Vec<_>>()
            })?
            .timestamps_from_pairs_in_window_named(key)
    }
    /// Build a bounded side view once per execution, then apply it to each main value.
    /// Materialization failures are returned as typed execution errors. The
    /// view must be transferable because portable workers can resume a bundle
    /// on another executor thread.
    pub fn map_with_materialized_side_named<S: Element, M: Send + 'static, U: Element>(
        &self,
        key: &str,
        side: &PCollection<S>,
        materialize: impl Fn(Vec<S>) -> Result<M, Error> + Send + Sync + 'static,
        f: impl Fn(T, &M) -> Result<U, Error> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        if !Arc::ptr_eq(&self.pipeline.inner, &side.pipeline.inner) {
            return Err(Error::graph(
                key,
                "side input belongs to a different pipeline",
            ));
        }
        let main = self.parent()?;
        let other = side.parent()?;
        if other.boundedness != Boundedness::Bounded {
            return Err(Error::graph(key, "side input must be bounded"));
        }
        if main.time != other.time {
            return Err(Error::graph(
                key,
                "side input time/window declaration mismatch",
            ));
        }
        let key_owned = key.to_owned();
        let materialize = Arc::new(materialize);
        let f = Arc::new(f);
        let side_key = key_owned.clone();
        let main_key = key_owned.clone();
        let registration = SideInputRegistration {
            materialize: Arc::new(move |values| {
                let mut typed = Vec::with_capacity(values.len());
                for value in values {
                    typed.push(*value.downcast::<S>().map_err(|_| {
                        Error::execution(&side_key, "direct", "side input type mismatch")
                    })?);
                }
                Ok(Box::new(materialize(typed)?) as Payload)
            }),
            apply: Arc::new(move |event, view| {
                let Envelope {
                    value,
                    timestamp_ns,
                    window,
                    assigned_window,
                } = event;
                let value = *value.downcast::<T>().map_err(|_| {
                    Error::execution(&main_key, "direct", "main input type mismatch")
                })?;
                let view = view.downcast_ref::<M>().ok_or_else(|| {
                    Error::execution(&main_key, "direct", "side input view type mismatch")
                })?;
                Ok(Envelope {
                    value: Box::new(f(value, view)?),
                    timestamp_ns,
                    window,
                    assigned_window,
                })
            }),
        };
        let direct_registration = registration.clone();
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [main_values, side_values]: [Vec<Envelope>; 2] =
                inputs.try_into().map_err(|_| {
                    Error::execution(&key_owned, "direct", "side input requires two inputs")
                })?;
            let side_view = (direct_registration.materialize)(
                side_values.into_iter().map(|event| event.value).collect(),
            )?;
            let mut output = Vec::with_capacity(main_values.len());
            for event in main_values {
                output.push((direct_registration.apply)(event, side_view.as_ref())?);
            }
            Ok(output)
        });
        let collection = self.next_aggregate(
            key,
            Operation::SideInputMap,
            vec![InputEdge {
                key: side.key.clone(),
                element: ElementDescriptor {
                    type_id: S::type_identity(),
                    coder_id: S::coder_identity(),
                    coder_version: S::CODER_VERSION,
                },
            }],
            main.boundedness,
            aggregate,
            None,
        )?;
        collection
            .pipeline
            .inner
            .lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?
            .registry
            .side_inputs
            .insert(key.to_owned(), registration);
        Ok(collection)
    }
}

impl<T: Element> PCollection<(i64, T)> {
    /// Move timestamps from tuple values into event-time metadata. Timestamp
    /// assignment must happen before windowing; the timestamp field is removed
    /// from the value without cloning the payload.
    pub fn timestamps_from_pairs_named(&self, key: &str) -> Result<PCollection<T>, Error> {
        let assigned = self.assign_timestamps_named(&format!("{key}/assign"), |pair| pair.0)?;
        assigned.map_named(key, |(_, value)| value)
    }

    /// Restore a reified timestamp after grouping, provided it still belongs
    /// to the element's current window. This cannot move a row between windows.
    pub fn timestamps_from_pairs_in_window_named(
        &self,
        key: &str,
    ) -> Result<PCollection<T>, Error> {
        let window = self.parent()?.time.window;
        if window == Window::Global {
            return self.timestamps_from_pairs_named(key);
        }
        let assign_key = format!("{key}/assign");
        let error_key = assign_key.clone();
        let transform = Transform::Single(Arc::new(move |mut event| {
            let pair = event.value.downcast_ref::<(i64, T)>().ok_or_else(|| {
                Error::execution(&error_key, "runner", "timestamp pair type mismatch")
            })?;
            if window.assign(pair.0)? != event.assigned_window {
                return Err(Error::execution(
                    &error_key,
                    "runner",
                    "restored timestamp falls outside its window",
                ));
            }
            event.timestamp_ns = pair.0;
            Ok(Some(event))
        }));
        self.next::<(i64, T)>(
            &assign_key,
            Operation::AssignTimestamp,
            Some(transform),
            None,
        )?
        .map_named(key, |(_, value)| value)
    }
}

type GroupBuffer<K, V> = Vec<(K, Vec<V>, i64, Window, Option<WindowInterval>)>;
type ReifiedKeyedValue<K, V> = (K, (i64, V));
type LatestGroupBuffer<K, V> = Vec<(K, (i64, V), i64, Window, Option<WindowInterval>)>;
enum JoinPositions {
    One(usize),
    Many(Vec<usize>),
}
impl JoinPositions {
    fn record<K: Eq + Hash>(
        index: &mut HashMap<(K, Option<WindowInterval>), Self>,
        key: K,
        window: Option<WindowInterval>,
        position: usize,
    ) {
        match index.entry((key, window)) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().push(position);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(Self::One(position));
            }
        }
    }

    fn push(&mut self, position: usize) {
        match self {
            Self::One(first) => {
                let first = *first;
                *self = Self::Many(vec![first, position]);
            }
            Self::Many(positions) => positions.push(position),
        }
    }

    const fn as_slice(&self) -> &[usize] {
        match self {
            Self::One(position) => std::slice::from_ref(position),
            Self::Many(positions) => positions.as_slice(),
        }
    }
}

// Keep the specialized inner paths out of the outer-join dispatcher.
#[inline(never)]
fn inner_join_reusing_right<K, V, W, O>(
    left: Vec<Envelope>,
    right: Vec<Envelope>,
    key: &str,
    emit: &impl Fn(Option<V>, Option<W>) -> Option<O>,
) -> Result<Vec<Envelope>, Error>
where
    K: Element + Eq + Hash + Clone,
    V: Element + Clone,
    W: Element + Clone,
    O: Element,
{
    let mut index: HashMap<(K, Option<WindowInterval>), JoinPositions> = HashMap::new();
    let mut left_values = Vec::with_capacity(left.len());
    for event in left {
        let (row_key, value) = *event
            .value
            .downcast::<(K, V)>()
            .map_err(|_| Error::execution(key, "direct", "left join type mismatch"))?;
        let position = left_values.len();
        JoinPositions::record(&mut index, row_key, event.assigned_window, position);
        left_values.push(value);
    }
    // Consume each right input slot before appending its matches. This reuses
    // the input allocation while output grows.
    let mut output = VecDeque::from(right);
    let input_count = output.len();
    for _ in 0..input_count {
        let event = output.pop_front().expect("right input count is fixed");
        let Envelope {
            value,
            timestamp_ns,
            window,
            assigned_window,
        } = event;
        let (row_key, value) = *value
            .downcast::<(K, W)>()
            .map_err(|_| Error::execution(key, "direct", "right join type mismatch"))?;
        if let Some(matches) = index.get(&(row_key.clone(), assigned_window)) {
            for &position in matches.as_slice() {
                if let Some(pair) = emit(Some(left_values[position].clone()), Some(value.clone())) {
                    output.push_back(Envelope {
                        value: Box::new((row_key.clone(), pair)),
                        timestamp_ns,
                        window: window.clone(),
                        assigned_window,
                    });
                }
            }
        }
    }
    Ok(output.into())
}

#[inline(never)]
fn inner_join_indexed_right<K, V, W, O>(
    left: Vec<Envelope>,
    right: Vec<Envelope>,
    key: &str,
    emit: &impl Fn(Option<V>, Option<W>) -> Option<O>,
) -> Result<Vec<Envelope>, Error>
where
    K: Element + Eq + Hash + Clone,
    V: Element + Clone,
    W: Element + Clone,
    O: Element,
{
    let mut index: HashMap<(K, Option<WindowInterval>), JoinPositions> = HashMap::new();
    let mut right_rows = Vec::with_capacity(right.len());
    for event in right {
        let (row_key, value) = *event
            .value
            .downcast::<(K, W)>()
            .map_err(|_| Error::execution(key, "direct", "right join type mismatch"))?;
        JoinPositions::record(&mut index, row_key, event.assigned_window, right_rows.len());
        right_rows.push((
            value,
            event.timestamp_ns,
            event.window,
            event.assigned_window,
        ));
    }
    let mut output = VecDeque::from(left);
    let input_count = output.len();
    for _ in 0..input_count {
        let event = output.pop_front().expect("left input count is fixed");
        let (row_key, value) = *event
            .value
            .downcast::<(K, V)>()
            .map_err(|_| Error::execution(key, "direct", "left join type mismatch"))?;
        if let Some(matches) = index.get(&(row_key.clone(), event.assigned_window)) {
            for &position in matches.as_slice() {
                let (right_value, timestamp_ns, window, assigned_window) = &right_rows[position];
                if let Some(pair) = emit(Some(value.clone()), Some(right_value.clone())) {
                    output.push_back(Envelope {
                        value: Box::new((row_key.clone(), pair)),
                        timestamp_ns: *timestamp_ns,
                        window: window.clone(),
                        assigned_window: *assigned_window,
                    });
                }
            }
        }
    }
    Ok(output.into())
}

fn typed_inner_join<K, V, W, O>(
    left: Vec<(K, V)>,
    right: Vec<(K, W)>,
    memory_budget: usize,
    check_cancelled: &dyn Fn() -> Result<(), Error>,
    emit: &impl Fn(Option<V>, Option<W>) -> Option<O>,
) -> Result<Vec<(K, O)>, Error>
where
    K: Element + Eq + Hash + Clone,
    V: Element + Clone,
    W: Element + Clone,
    O: Element,
{
    let mut index: HashMap<(K, Option<WindowInterval>), JoinPositions> = HashMap::new();
    let mut output = Vec::new();
    let max_rows = memory_budget / std::mem::size_of::<(K, O)>().max(1);
    if left.len() <= right.len() {
        let mut left_values = Vec::with_capacity(left.len());
        for (key, value) in left {
            JoinPositions::record(&mut index, key, None, left_values.len());
            left_values.push(value);
        }
        for (position, (key, value)) in right.into_iter().enumerate() {
            if position % 4096 == 0 {
                check_cancelled()?;
            }
            if let Some(matches) = index.get(&(key.clone(), None)) {
                for &slot in matches.as_slice() {
                    if let Some(pair) = emit(Some(left_values[slot].clone()), Some(value.clone())) {
                        if output.len() % 4096 == 0 {
                            check_cancelled()?;
                        }
                        if output.len() >= max_rows {
                            return Err(Error::resource(
                                "join",
                                "output exceeds job memory budget",
                            ));
                        }
                        output.push((key.clone(), pair));
                    }
                }
            }
        }
    } else {
        let mut right_values = Vec::with_capacity(right.len());
        for (key, value) in right {
            JoinPositions::record(&mut index, key, None, right_values.len());
            right_values.push(value);
        }
        for (position, (key, value)) in left.into_iter().enumerate() {
            if position % 4096 == 0 {
                check_cancelled()?;
            }
            if let Some(matches) = index.get(&(key.clone(), None)) {
                for &slot in matches.as_slice() {
                    if let Some(pair) = emit(Some(value.clone()), Some(right_values[slot].clone()))
                    {
                        if output.len() % 4096 == 0 {
                            check_cancelled()?;
                        }
                        if output.len() >= max_rows {
                            return Err(Error::resource(
                                "join",
                                "output exceeds job memory budget",
                            ));
                        }
                        output.push((key.clone(), pair));
                    }
                }
            }
        }
    }
    check_cancelled()?;
    Ok(output)
}
struct GroupByKeyFoldState<K, V> {
    key: String,
    global_indices: HashMap<K, usize>,
    window_indices: HashMap<(K, WindowInterval), usize>,
    groups: GroupBuffer<K, V>,
}

#[derive(Serialize, Deserialize)]
struct GroupFoldCheckpoint {
    version: Version,
    key_coder: String,
    value_coder: String,
    groups: Vec<GroupFoldCheckpointRow>,
}

#[derive(Serialize, Deserialize)]
struct GroupFoldCheckpointRow {
    key: Vec<u8>,
    values: Vec<u8>,
    timestamp_ns: i64,
    window: Window,
    assigned_window: Option<WindowInterval>,
}

const GROUP_FOLD_CHECKPOINT_VERSION: Version = Version { major: 2, minor: 0 };

impl<K: Element + Eq + Hash + Clone, V: Element> GroupByKeyFoldState<K, V> {
    fn add_row(
        &mut self,
        group_key: K,
        value: V,
        timestamp_ns: i64,
        window: Window,
        assigned_window: Option<WindowInterval>,
    ) {
        if let Some(interval) = assigned_window {
            let scope = (group_key.clone(), interval);
            if let Some(&index) = self.window_indices.get(&scope) {
                self.groups[index].1.push(value);
                self.groups[index].2 = self.groups[index].2.min(timestamp_ns);
                return;
            }
            self.window_indices.insert(scope, self.groups.len());
        } else {
            if let Some(&index) = self.global_indices.get(&group_key) {
                self.groups[index].1.push(value);
                self.groups[index].2 = self.groups[index].2.min(timestamp_ns);
                return;
            }
            self.global_indices
                .insert(group_key.clone(), self.groups.len());
        }
        self.groups.push((
            group_key,
            vec![value],
            timestamp_ns,
            window,
            assigned_window,
        ));
    }
}

impl<K: Element + Eq + Hash + Clone, V: Element> FoldSession for GroupByKeyFoldState<K, V> {
    fn add(&mut self, event: Envelope) -> Result<(), Error> {
        let Envelope {
            value,
            timestamp_ns,
            window,
            assigned_window,
        } = event;
        let (group_key, value) = *value
            .downcast::<(K, V)>()
            .map_err(|_| Error::execution(&self.key, "direct", "group input type mismatch"))?;
        self.add_row(group_key, value, timestamp_ns, window, assigned_window);
        Ok(())
    }

    fn accepts_vec_batch(&self, batch_type: TypeId) -> bool {
        batch_type == TypeId::of::<Vec<(K, V)>>()
    }

    fn checkpoint_state(&self) -> Result<Option<Vec<u8>>, Error> {
        if !<(K, Vec<V>) as Element>::supports_coder() {
            return Ok(None);
        }
        let groups = self
            .groups
            .iter()
            .map(|(key, values, timestamp_ns, window, assigned_window)| {
                Ok(GroupFoldCheckpointRow {
                    key: key.encode()?,
                    values: values.encode()?,
                    timestamp_ns: *timestamp_ns,
                    window: window.clone(),
                    assigned_window: *assigned_window,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        serde_json::to_vec(&GroupFoldCheckpoint {
            version: GROUP_FOLD_CHECKPOINT_VERSION,
            key_coder: K::coder_identity(),
            value_coder: V::coder_identity(),
            groups,
        })
        .map(Some)
        .map_err(|error| Error::recovery(&self.key, error.to_string()))
    }

    fn restore_checkpoint_state(&mut self, state: &[u8]) -> Result<(), Error> {
        if !self.groups.is_empty() {
            return Err(Error::recovery(&self.key, "fold already contains input"));
        }
        let checkpoint: GroupFoldCheckpoint = serde_json::from_slice(state)
            .map_err(|error| Error::recovery(&self.key, error.to_string()))?;
        if checkpoint.version != GROUP_FOLD_CHECKPOINT_VERSION {
            return Err(Error::incompatible(
                "group fold checkpoint version mismatch",
            ));
        }
        if checkpoint.key_coder != K::coder_identity()
            || checkpoint.value_coder != V::coder_identity()
        {
            return Err(Error::incompatible("group fold checkpoint coder mismatch"));
        }
        let mut restored = Self {
            key: self.key.clone(),
            global_indices: HashMap::new(),
            window_indices: HashMap::new(),
            groups: Vec::with_capacity(checkpoint.groups.len()),
        };
        for row in checkpoint.groups {
            let key = K::decode(&row.key)?;
            let values = Vec::<V>::decode(&row.values)?;
            if values.is_empty() {
                return Err(Error::recovery(&self.key, "empty group in fold checkpoint"));
            }
            let position = restored.groups.len();
            if let Some(interval) = row.assigned_window {
                if restored
                    .window_indices
                    .insert((key.clone(), interval), position)
                    .is_some()
                {
                    return Err(Error::recovery(
                        &self.key,
                        "duplicate group in fold checkpoint",
                    ));
                }
            } else if restored
                .global_indices
                .insert(key.clone(), position)
                .is_some()
            {
                return Err(Error::recovery(
                    &self.key,
                    "duplicate group in fold checkpoint",
                ));
            }
            restored.groups.push((
                key,
                values,
                row.timestamp_ns,
                row.window,
                row.assigned_window,
            ));
        }
        *self = restored;
        Ok(())
    }

    fn add_vec_batch(&mut self, values: Payload) -> Result<(), Error> {
        let values = values.downcast::<Vec<(K, V)>>().map_err(|_| {
            Error::execution(&self.key, "direct", "group vector batch type mismatch")
        })?;
        for (key, value) in *values {
            self.add_row(key, value, 0, Window::Global, None);
        }
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<Vec<Envelope>, Error> {
        Ok(self
            .groups
            .into_iter()
            .map(
                |(key, values, timestamp_ns, window, assigned_window)| Envelope {
                    value: Box::new((key, values)),
                    timestamp_ns,
                    window,
                    assigned_window,
                },
            )
            .collect())
    }

    fn finish_into(
        self: Box<Self>,
        emit: &mut dyn FnMut(Envelope) -> Result<(), Error>,
    ) -> Result<(), Error> {
        for (key, values, timestamp_ns, window, assigned_window) in self.groups {
            emit(Envelope {
                value: Box::new((key, values)),
                timestamp_ns,
                window,
                assigned_window,
            })?;
        }
        Ok(())
    }
}

// This fold is used only by latest_per_key's private GroupByKey node. Beam
// performs the ordinary shuffle; Direct can discard older values as they
// arrive instead of retaining every member of each group.
struct LatestPerKeyFoldState<K, V> {
    key: String,
    indices: HashMap<(K, Option<WindowInterval>), usize>,
    groups: LatestGroupBuffer<K, V>,
}

const LATEST_GROUP_FOLD_CHECKPOINT_VERSION: Version = Version { major: 2, minor: 0 };

impl<K: Element + Eq + Hash + Clone, V: Element> LatestPerKeyFoldState<K, V> {
    fn add_row(
        &mut self,
        group_key: K,
        candidate: (i64, V),
        timestamp_ns: i64,
        window: Window,
        assigned_window: Option<WindowInterval>,
    ) {
        let scope = (group_key.clone(), assigned_window);
        if let Some(&index) = self.indices.get(&scope) {
            if candidate.0 > self.groups[index].1.0 {
                self.groups[index].1 = candidate;
            }
        } else {
            self.indices.insert(scope, self.groups.len());
            self.groups
                .push((group_key, candidate, timestamp_ns, window, assigned_window));
        }
    }
}

impl<K: Element + Eq + Hash + Clone, V: Element> FoldSession for LatestPerKeyFoldState<K, V> {
    fn add(&mut self, event: Envelope) -> Result<(), Error> {
        let Envelope {
            value,
            timestamp_ns,
            window,
            assigned_window,
        } = event;
        let (group_key, candidate) = *value.downcast::<(K, (i64, V))>().map_err(|_| {
            Error::execution(&self.key, "direct", "latest group input type mismatch")
        })?;
        self.add_row(group_key, candidate, timestamp_ns, window, assigned_window);
        Ok(())
    }

    fn accepts_timed_vec_batch(&self, batch_type: TypeId) -> bool {
        batch_type == TypeId::of::<Vec<(K, V)>>()
    }

    fn add_timed_vec_batch(
        &mut self,
        values: Payload,
        timestamp_fn: &(dyn Any + Send + Sync),
        timestamp_key: &str,
    ) -> Result<(), Error> {
        let values = values.downcast::<Vec<(K, V)>>().map_err(|_| {
            Error::execution(&self.key, "direct", "latest vector batch type mismatch")
        })?;
        let timestamp = timestamp_fn
            .downcast_ref::<TypedTimestamp<(K, V)>>()
            .ok_or_else(|| Error::execution(timestamp_key, "direct", "timestamp type mismatch"))?;
        for row in *values {
            let timestamp_ns =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| timestamp(&row)))
                    .map_err(|_| {
                        Error::execution(timestamp_key, "direct", "user transform panicked")
                    })?;
            let (key, value) = row;
            self.add_row(
                key,
                (timestamp_ns, value),
                timestamp_ns,
                Window::Global,
                None,
            );
        }
        Ok(())
    }

    fn checkpoint_state(&self) -> Result<Option<Vec<u8>>, Error> {
        if !<(K, Vec<(i64, V)>) as Element>::supports_coder() {
            return Ok(None);
        }
        let groups = self
            .groups
            .iter()
            .map(|(key, value, timestamp_ns, window, assigned_window)| {
                Ok(GroupFoldCheckpointRow {
                    key: key.encode()?,
                    values: value.encode()?,
                    timestamp_ns: *timestamp_ns,
                    window: window.clone(),
                    assigned_window: *assigned_window,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        serde_json::to_vec(&GroupFoldCheckpoint {
            version: LATEST_GROUP_FOLD_CHECKPOINT_VERSION,
            key_coder: K::coder_identity(),
            value_coder: <(i64, V) as Element>::coder_identity(),
            groups,
        })
        .map(Some)
        .map_err(|error| Error::recovery(&self.key, error.to_string()))
    }

    fn restore_checkpoint_state(&mut self, state: &[u8]) -> Result<(), Error> {
        if !self.groups.is_empty() {
            return Err(Error::recovery(&self.key, "fold already contains input"));
        }
        let checkpoint: GroupFoldCheckpoint = serde_json::from_slice(state)
            .map_err(|error| Error::recovery(&self.key, error.to_string()))?;
        if checkpoint.version != LATEST_GROUP_FOLD_CHECKPOINT_VERSION {
            return Err(Error::incompatible(
                "latest group checkpoint version mismatch",
            ));
        }
        if checkpoint.key_coder != K::coder_identity()
            || checkpoint.value_coder != <(i64, V) as Element>::coder_identity()
        {
            return Err(Error::incompatible(
                "latest group checkpoint coder mismatch",
            ));
        }
        for row in checkpoint.groups {
            let key = K::decode(&row.key)?;
            let value = <(i64, V)>::decode(&row.values)?;
            let scope = (key.clone(), row.assigned_window);
            if self.indices.insert(scope, self.groups.len()).is_some() {
                return Err(Error::recovery(
                    &self.key,
                    "duplicate latest group in checkpoint",
                ));
            }
            self.groups.push((
                key,
                value,
                row.timestamp_ns,
                row.window,
                row.assigned_window,
            ));
        }
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<Vec<Envelope>, Error> {
        Ok(self
            .groups
            .into_iter()
            .map(
                |(key, value, timestamp_ns, window, assigned_window)| Envelope {
                    value: Box::new((key, vec![value])),
                    timestamp_ns,
                    window,
                    assigned_window,
                },
            )
            .collect())
    }

    fn finish_into(
        self: Box<Self>,
        emit: &mut dyn FnMut(Envelope) -> Result<(), Error>,
    ) -> Result<(), Error> {
        for (key, value, timestamp_ns, window, assigned_window) in self.groups {
            emit(Envelope {
                value: Box::new((key, vec![value])),
                timestamp_ns,
                window,
                assigned_window,
            })?;
        }
        Ok(())
    }
}
pub type Joined<K, V, W> = (K, (V, W));
pub type LeftJoined<K, V, W> = (K, (V, Option<W>));
pub type RightJoined<K, V, W> = (K, (Option<V>, W));
pub type FullJoined<K, V, W> = (K, (Option<V>, Option<W>));
pub type KeyedWindowed<K, V> = ((K, WindowInterval), V);

macro_rules! collect_cogroup_input {
    ($inputs:ident, $rows:ident, $indices:ident, $ty:ident, $position:tt, $empty:expr, $node_key:ident) => {
        for event in $inputs
            .next()
            .ok_or_else(|| Error::execution(&$node_key, "direct", "co-group input missing"))?
        {
            let Envelope {
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let (row_key, value) = *value.downcast::<(K, $ty)>().map_err(|_| {
                Error::execution(&$node_key, "direct", "co-group input type mismatch")
            })?;
            let scope = (row_key.clone(), assigned_window);
            let group_index = if let Some(&existing) = $indices.get(&scope) {
                existing
            } else {
                let next = $rows.len();
                $indices.insert(scope, next);
                $rows.push((row_key, $empty, timestamp_ns, window, assigned_window));
                next
            };
            $rows[group_index].2 = $rows[group_index].2.min(timestamp_ns);
            $rows[group_index].1.$position.push(value);
        }
    };
}

macro_rules! define_cogroup {
    ($name:ident; $first:ident:$first_ty:ident:0, $($other:ident:$other_ty:ident:$position:tt),+) => {
        #[allow(clippy::too_many_arguments, clippy::type_complexity)]
        pub fn $name<K, $first_ty, $($other_ty),+>(
            key: &str,
            $first: &PCollection<(K, $first_ty)>,
            $($other: &PCollection<(K, $other_ty)>),+
        ) -> Result<PCollection<(K, (Vec<$first_ty>, $(Vec<$other_ty>),+))>, Error>
        where
            K: Element + Eq + Hash + Clone,
            $first_ty: Element,
            $($other_ty: Element),+
        {
            let first_node = $first.parent()?;
            let mut unbounded = first_node.boundedness == Boundedness::Unbounded;
            $(
                if !Arc::ptr_eq(&$first.pipeline.inner, &$other.pipeline.inner) {
                    return Err(Error::graph(key, "co-group inputs belong to different pipelines"));
                }
                let other_node = $other.parent()?;
                if other_node.time != first_node.time {
                    return Err(Error::graph(key, "co-group inputs must have equal time/window declarations"));
                }
                unbounded |= other_node.boundedness == Boundedness::Unbounded;
            )+
            if unbounded && first_node.time.window == Window::Global {
                return Err(Error::graph(key, "unbounded global co-group requires a finite window"));
            }
            let node_key = key.to_owned();
            let aggregate: Aggregate = Arc::new(move |inputs| {
                let expected = 1 $(+ { let _ = stringify!($other); 1 })+;
                if inputs.len() != expected {
                    return Err(Error::execution(&node_key, "direct", "co-group input count mismatch"));
                }
                let mut inputs = inputs.into_iter();
                let mut indices: HashMap<(K, Option<WindowInterval>), usize> = HashMap::new();
                let mut rows: Vec<(K, (Vec<$first_ty>, $(Vec<$other_ty>),+), i64, Window, Option<WindowInterval>)> = Vec::new();
                macro_rules! empty_values {
                    () => { (Vec::<$first_ty>::new(), $(Vec::<$other_ty>::new()),+) };
                }
                collect_cogroup_input!(inputs, rows, indices, $first_ty, 0,
                    empty_values!(), node_key);
                $(collect_cogroup_input!(inputs, rows, indices, $other_ty, $position,
                    empty_values!(), node_key);)+
                Ok(rows.into_iter().map(|(key, values, timestamp_ns, window, assigned_window)| Envelope {
                    value: Box::new((key, values)), timestamp_ns, window, assigned_window
                }).collect())
            });
            let collection = $first.next_aggregate(
                key,
                Operation::CoGroupByKey,
                vec![$(InputEdge {
                    key: $other.key.clone(),
                    element: ElementDescriptor {
                        type_id: <(K, $other_ty)>::type_identity(),
                        coder_id: <(K, $other_ty)>::coder_identity(),
                        coder_version: <(K, $other_ty)>::CODER_VERSION,
                    },
                }),+],
                if unbounded { Boundedness::Unbounded } else { Boundedness::Bounded },
                aggregate,
                None,
            )?;
            let split_inputs: Vec<GroupInputSplitter> = vec![
                |bytes| {
                    let (row_key, value) = <(K, $first_ty)>::decode(bytes)?;
                    let mut tagged = vec![0];
                    tagged.extend(value.encode()?);
                    Ok((row_key.encode()?, tagged))
                },
                $(|bytes| {
                    let (row_key, value) = <(K, $other_ty)>::decode(bytes)?;
                    let mut tagged = vec![$position];
                    tagged.extend(value.encode()?);
                    Ok((row_key.encode()?, tagged))
                }),+
            ];
            let registration = CoGroupCoderRegistration {
                split_inputs,
                assemble_output: |encoded_key, encoded_values| {
                    let mut grouped = (Vec::<$first_ty>::new(), $(Vec::<$other_ty>::new()),+);
                    for tagged in encoded_values {
                        let (&tag, payload) = tagged.split_first().ok_or_else(||
                            Error::incompatible("co-group tagged value is empty"))?;
                        match tag {
                            0 => grouped.0.push(<$first_ty>::decode(payload)?),
                            $($position => grouped.$position.push(<$other_ty>::decode(payload)?),)+
                            _ => return Err(Error::incompatible("co-group input tag is out of range")),
                        }
                    }
                    (K::decode(encoded_key)?, grouped).encode()
                },
            };
            {
                let mut inner = collection.pipeline.inner.lock()
                    .map_err(|_| Error::graph(key, "builder lock poisoned"))?;
                inner.registry.cogroup_coders.insert(key.to_owned(), registration);
            }
            Ok(collection)
        }
    };
}

define_cogroup!(cogroup_by_key_2_named; c1:V1:0, c2:V2:1);
define_cogroup!(cogroup_by_key_3_named; c1:V1:0, c2:V2:1, c3:V3:2);
define_cogroup!(cogroup_by_key_4_named; c1:V1:0, c2:V2:1, c3:V3:2, c4:V4:3);
define_cogroup!(cogroup_by_key_5_named; c1:V1:0, c2:V2:1, c3:V3:2, c4:V4:3, c5:V5:4);
define_cogroup!(cogroup_by_key_6_named; c1:V1:0, c2:V2:1, c3:V3:2, c4:V4:3, c5:V5:4, c6:V6:5);
define_cogroup!(cogroup_by_key_7_named; c1:V1:0, c2:V2:1, c3:V3:2, c4:V4:3, c5:V5:4, c6:V6:5, c7:V7:6);
define_cogroup!(cogroup_by_key_8_named; c1:V1:0, c2:V2:1, c3:V3:2, c4:V4:3, c5:V5:4, c6:V6:5, c7:V7:6, c8:V8:7);
define_cogroup!(cogroup_by_key_9_named; c1:V1:0, c2:V2:1, c3:V3:2, c4:V4:3, c5:V5:4, c6:V6:5, c7:V7:6, c8:V8:7, c9:V9:8);
define_cogroup!(cogroup_by_key_10_named; c1:V1:0, c2:V2:1, c3:V3:2, c4:V4:3, c5:V5:4, c6:V6:5, c7:V7:6, c8:V8:7, c9:V9:8, c10:V10:9);

#[macro_export]
macro_rules! cogroup_by_key_named {
    ($key:expr, $a:expr, $b:expr) => {
        $crate::cogroup_by_key_2_named($key, &$a, &$b)
    };
    ($key:expr, $a:expr, $b:expr, $c:expr) => {
        $crate::cogroup_by_key_3_named($key, &$a, &$b, &$c)
    };
    ($key:expr, $a:expr, $b:expr, $c:expr, $d:expr) => {
        $crate::cogroup_by_key_4_named($key, &$a, &$b, &$c, &$d)
    };
    ($key:expr, $a:expr, $b:expr, $c:expr, $d:expr, $e:expr) => {
        $crate::cogroup_by_key_5_named($key, &$a, &$b, &$c, &$d, &$e)
    };
    ($key:expr, $a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr) => {
        $crate::cogroup_by_key_6_named($key, &$a, &$b, &$c, &$d, &$e, &$f)
    };
    ($key:expr, $a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr) => {
        $crate::cogroup_by_key_7_named($key, &$a, &$b, &$c, &$d, &$e, &$f, &$g)
    };
    ($key:expr, $a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr) => {
        $crate::cogroup_by_key_8_named($key, &$a, &$b, &$c, &$d, &$e, &$f, &$g, &$h)
    };
    ($key:expr, $a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr, $i:expr) => {
        $crate::cogroup_by_key_9_named($key, &$a, &$b, &$c, &$d, &$e, &$f, &$g, &$h, &$i)
    };
    ($key:expr, $a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr, $i:expr, $j:expr) => {
        $crate::cogroup_by_key_10_named($key, &$a, &$b, &$c, &$d, &$e, &$f, &$g, &$h, &$i, &$j)
    };
}

impl<K: Element + Eq + Hash + Clone, V: Element> PCollection<(K, V)> {
    pub fn combine_values_lifted_named<C, A, O>(
        &self,
        key: &str,
        combiner: C,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        C: CombineFn<V, A, O>,
        A: Send + 'static,
        O: Element,
    {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let node_key = key.to_owned();
        let combiner = Arc::new(MergeableAdapter(combiner));
        let factory: FoldFactory = Arc::new(move || {
            Box::new(KeyedFoldState::<K, V, A, O, MergeableAdapter<C>> {
                key: node_key.clone(),
                combiner: Arc::clone(&combiner),
                indices: HashMap::new(),
                rows: Vec::new(),
                marker: PhantomData,
            })
        });
        self.next_fold(
            key,
            Operation::FoldPerKey,
            factory,
            Some(GroupCoderRegistration::for_elements::<K, V>()),
        )
    }
    pub fn fold_values_ordered_named<C, A, O>(
        &self,
        key: &str,
        fold: C,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        C: OrderedFoldFn<V, A, O>,
        A: Send + 'static,
        O: Element,
    {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let node_key = key.to_owned();
        let fold = Arc::new(OrderedAdapter(fold));
        let factory: FoldFactory = Arc::new(move || {
            Box::new(KeyedFoldState::<K, V, A, O, OrderedAdapter<C>> {
                key: node_key.clone(),
                combiner: Arc::clone(&fold),
                indices: HashMap::new(),
                rows: Vec::new(),
                marker: PhantomData,
            })
        });
        self.next_fold(key, Operation::FoldOrderedPerKey, factory, None)
    }
    /// Fold each key/window in an explicit order on Direct and portable runners.
    /// The order key must be unique within its key/window. The result timestamp
    /// is the timestamp of the first value in that order.
    pub fn fold_values_ordered_by_named<Q, C, A, O>(
        &self,
        key: &str,
        order_key: impl Fn(&V) -> Q + Send + Sync + 'static,
        fold: C,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        Q: Element + Ord,
        C: OrderedFoldFn<V, A, O>,
        A: Send + 'static,
        O: Element,
    {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let reify_key = format!("{key}/order");
        let order_transform = Transform::Single(Arc::new(move |event| {
            let Envelope {
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let (group_key, value) = *value.downcast::<(K, V)>().map_err(|_| {
                Error::execution(&reify_key, "runner", "ordered fold input type mismatch")
            })?;
            let order = order_key(&value);
            Ok(Some(Envelope {
                value: Box::new((group_key, (order, (timestamp_ns, value)))),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        let keyed = self.next::<(K, (Q, (i64, V)))>(
            &format!("{key}/order"),
            Operation::Map,
            Some(order_transform),
            None,
        )?;
        let grouped = keyed.group_by_key_named(&format!("{key}/group"))?;
        let node_key = key.to_owned();
        let select = Transform::Single(Arc::new(move |event| {
            let Envelope {
                value,
                window,
                assigned_window,
                ..
            } = event;
            let (group_key, values) =
                *value.downcast::<(K, Vec<(Q, (i64, V))>)>().map_err(|_| {
                    Error::execution(&node_key, "runner", "ordered fold group type mismatch")
                })?;
            let mut ordered = BTreeMap::new();
            for (order, row) in values {
                if ordered.insert(order, row).is_some() {
                    return Err(Error::execution(
                        &node_key,
                        "runner",
                        "duplicate ordered fold key in key/window",
                    ));
                }
            }
            let timestamp_ns = ordered
                .first_key_value()
                .ok_or_else(|| {
                    Error::execution(&node_key, "runner", "ordered fold group is empty")
                })?
                .1
                .0;
            let mut accumulator = fold.create();
            for (_, (_, value)) in ordered {
                fold.add_input(&mut accumulator, value);
            }
            Ok(Some(Envelope {
                value: Box::new((group_key, fold.finish(accumulator))),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        grouped.next(key, Operation::Map, Some(select), None)
    }
    pub fn map_values_batches_named<O: Element>(
        &self,
        key: &str,
        batch_size: usize,
        f: impl Fn(&[V]) -> Vec<O> + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded {
            return Err(Error::graph(key, "batch mapping requires a bounded input"));
        }
        let limit = batch_size.max(1);
        let node_key = key.to_owned();
        let aggregate: Aggregate = Arc::new(move |mut inputs| {
            let events = inputs
                .pop()
                .ok_or_else(|| Error::execution(&node_key, "direct", "batch input missing"))?;
            let mut output = Vec::new();
            let mut pending: Vec<(K, V, i64, Window, Option<WindowInterval>)> = Vec::new();
            let mut flush = |pending: &mut Vec<(K, V, i64, Window, Option<WindowInterval>)>| -> Result<(), Error> {
                if pending.is_empty() { return Ok(()); }
                let rows = std::mem::take(pending);
                let mut keys = Vec::with_capacity(rows.len());
                let mut values = Vec::with_capacity(rows.len());
                let mut metadata = Vec::with_capacity(rows.len());
                for (key, value, timestamp_ns, window, assigned_window) in rows {
                    keys.push(key);
                    values.push(value);
                    metadata.push((timestamp_ns, window, assigned_window));
                }
                let results = f(&values);
                if results.len() != values.len() {
                    return Err(Error::execution(&node_key, "direct", "batch value map changed length"));
                }
                for ((key, value), (timestamp_ns, window, assigned_window)) in
                    keys.into_iter().zip(results).zip(metadata) {
                    output.push(Envelope {
                        value: Box::new((key, value)), timestamp_ns, window, assigned_window
                    });
                }
                Ok(())
            };
            for event in events {
                let Envelope {
                    value,
                    timestamp_ns,
                    window,
                    assigned_window,
                } = event;
                if pending.len() == limit
                    || pending.first().is_some_and(|row| row.4 != assigned_window)
                {
                    flush(&mut pending)?;
                }
                let (row_key, row_value) = *value.downcast::<(K, V)>().map_err(|_| {
                    Error::execution(&node_key, "direct", "batch keyed input type mismatch")
                })?;
                pending.push((row_key, row_value, timestamp_ns, window, assigned_window));
            }
            flush(&mut pending)?;
            Ok(output)
        });
        self.next_aggregate(
            key,
            Operation::BatchMapValues { batch_size: limit },
            Vec::new(),
            parent.boundedness,
            aggregate,
            None,
        )
    }
    pub fn group_into_batches_named(
        &self,
        key: &str,
        batch_size: usize,
    ) -> Result<PCollection<(K, Vec<V>)>, Error> {
        let limit = batch_size.max(1);
        self.group_by_key_named(&format!("{key}/group"))?
            .flat_map_named(key, move |(group_key, values)| {
                let mut batches = Vec::new();
                let mut current = Vec::with_capacity(limit);
                for value in values {
                    current.push(value);
                    if current.len() == limit {
                        batches.push((
                            group_key.clone(),
                            std::mem::replace(&mut current, Vec::with_capacity(limit)),
                        ));
                    }
                }
                if !current.is_empty() {
                    batches.push((group_key, current));
                }
                batches
            })
    }
    pub fn combine_per_key_and_window_named<U: Element>(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
        f: impl Fn(Vec<V>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<KeyedWindowed<K, U>>, Error> {
        let grouped = self
            .fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .group_by_key_named(&format!("{key}/group"))?;
        grouped
            .map_named(&format!("{key}/combine"), move |(key, values)| {
                (key, f(values))
            })?
            .with_window_named(&format!("{key}/tag"))?
            .map_named(key, |(window, (key, value))| ((key, window), value))
    }
    pub fn latest_per_key_named(&self, key: &str) -> Result<Self, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let group_key = format!("{key}/group");
        let grouped = self
            .reify_keyed_timestamp_named(&format!("{key}/timestamp"))?
            .group_by_key_named(&group_key)?;
        let fold_key = group_key.clone();
        let fold_factory: FoldFactory = Arc::new(move || {
            Box::new(LatestPerKeyFoldState::<K, V> {
                key: fold_key.clone(),
                indices: HashMap::new(),
                groups: Vec::new(),
            })
        });
        let aggregate_factory = Arc::clone(&fold_factory);
        let aggregate_key = group_key.clone();
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [events]: [Vec<Envelope>; 1] = inputs.try_into().map_err(|_| {
                Error::execution(&aggregate_key, "direct", "latest group requires one input")
            })?;
            let mut fold = aggregate_factory();
            for event in events {
                fold.add(event)?;
            }
            fold.finish()
        });
        {
            let mut builder = grouped
                .pipeline
                .inner
                .lock()
                .map_err(|_| Error::graph(&group_key, "builder lock poisoned"))?;
            builder
                .registry
                .aggregates
                .insert(group_key.clone(), aggregate);
            builder.registry.folds.insert(group_key, fold_factory);
        }
        let select_key = key.to_owned();
        let select = Transform::Single(Arc::new(move |event| {
            let Envelope {
                value,
                window,
                assigned_window,
                ..
            } = event;
            let (group_key, values) = *value.downcast::<(K, Vec<(i64, V)>)>().map_err(|_| {
                Error::execution(&select_key, "direct", "latest group type mismatch")
            })?;
            let mut values = values.into_iter();
            let (mut latest_timestamp, mut latest_value) = values
                .next()
                .ok_or_else(|| Error::execution(&select_key, "direct", "latest group is empty"))?;
            for (timestamp, value) in values {
                if timestamp > latest_timestamp {
                    latest_timestamp = timestamp;
                    latest_value = value;
                }
            }
            Ok(Some(Envelope {
                value: Box::new((group_key, latest_value)),
                timestamp_ns: latest_timestamp,
                window,
                assigned_window,
            }))
        }));
        grouped.next(key, Operation::Map, Some(select), None)
    }
    /// Select the latest event per key/window with a deterministic tie key.
    /// Equal event times choose the greatest tie key. A repeated event time
    /// and tie key in one key/window is an error on every runner.
    pub fn latest_per_key_by_named<Q: Element + Ord>(
        &self,
        key: &str,
        tie_key: impl Fn(&V) -> Q + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let reify_key = format!("{key}/tie");
        let transform = Transform::Single(Arc::new(move |event| {
            let Envelope {
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let (group_key, value) = *value.downcast::<(K, V)>().map_err(|_| {
                Error::execution(&reify_key, "runner", "latest tie input type mismatch")
            })?;
            let tie = tie_key(&value);
            Ok(Some(Envelope {
                value: Box::new((group_key, (timestamp_ns, (tie, value)))),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        let reified = self.next::<(K, (i64, (Q, V)))>(
            &format!("{key}/tie"),
            Operation::Map,
            Some(transform),
            None,
        )?;
        let grouped = reified.group_by_key_named(&format!("{key}/group"))?;
        let select_key = key.to_owned();
        let select = Transform::Single(Arc::new(move |event| {
            let Envelope {
                value,
                window,
                assigned_window,
                ..
            } = event;
            let (group_key, values) =
                *value.downcast::<(K, Vec<(i64, (Q, V))>)>().map_err(|_| {
                    Error::execution(&select_key, "runner", "latest tie group type mismatch")
                })?;
            let mut seen = BTreeSet::new();
            let mut latest = None;
            for (timestamp, (tie, value)) in values {
                let order = (timestamp, tie);
                let is_later = seen.last().is_none_or(|current| order.cmp(current).is_gt());
                if !seen.insert(order) {
                    return Err(Error::execution(
                        &select_key,
                        "runner",
                        "duplicate latest tie key in key/window/event time",
                    ));
                }
                if is_later {
                    latest = Some((timestamp, value));
                }
            }
            let (timestamp_ns, latest) = latest.ok_or_else(|| {
                Error::execution(&select_key, "runner", "latest tie group is empty")
            })?;
            Ok(Some(Envelope {
                value: Box::new((group_key, latest)),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        grouped.next(key, Operation::Map, Some(select), None)
    }
    fn reify_keyed_timestamp_named(
        &self,
        key: &str,
    ) -> Result<PCollection<ReifiedKeyedValue<K, V>>, Error> {
        let reify_key = key.to_owned();
        let transform = Transform::Single(Arc::new(move |event| {
            let Envelope {
                value,
                timestamp_ns,
                window,
                assigned_window,
            } = event;
            let (group_key, value) = *value.downcast::<(K, V)>().map_err(|_| {
                Error::execution(&reify_key, "direct", "latest input type mismatch")
            })?;
            Ok(Some(Envelope {
                value: Box::new((group_key, (timestamp_ns, value))),
                timestamp_ns,
                window,
                assigned_window,
            }))
        }));
        let output = self.next(key, Operation::Map, Some(transform), None)?;
        self.pipeline
            .inner
            .lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?
            .registry
            .keyed_timestamp_reifications
            .insert(key.to_owned());
        Ok(output)
    }
    pub fn group_by_key_named(&self, key: &str) -> Result<PCollection<(K, Vec<V>)>, Error> {
        let parent = self.parent()?;
        if parent.boundedness == Boundedness::Unbounded && parent.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global aggregation requires a finite window",
            ));
        }
        let key_owned = key.to_owned();
        let fold_key = key_owned.clone();
        let fold_factory: FoldFactory = Arc::new(move || {
            Box::new(GroupByKeyFoldState::<K, V> {
                key: fold_key.clone(),
                global_indices: HashMap::new(),
                window_indices: HashMap::new(),
                groups: Vec::new(),
            })
        });
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [events]: [Vec<Envelope>; 1] = inputs.try_into().map_err(|_| {
                Error::execution(&key_owned, "direct", "group requires exactly one input")
            })?;
            let mut fold: Box<dyn FoldSession> = Box::new(GroupByKeyFoldState::<K, V> {
                key: key_owned.clone(),
                global_indices: HashMap::new(),
                window_indices: HashMap::new(),
                groups: Vec::new(),
            });
            for event in events {
                fold.add(event)?;
            }
            fold.finish()
        });
        let result = self.next_aggregate(
            key,
            Operation::GroupByKey,
            Vec::new(),
            parent.boundedness,
            aggregate,
            Some(GroupCoderRegistration::for_elements::<K, V>()),
        )?;
        self.pipeline
            .inner
            .lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?
            .registry
            .folds
            .insert(key.to_owned(), fold_factory);
        Ok(result)
    }

    pub fn join_inner_named<W: Element + Clone>(
        &self,
        key: &str,
        right: &PCollection<(K, W)>,
    ) -> Result<PCollection<Joined<K, V, W>>, Error>
    where
        V: Clone,
    {
        self.join_named(key, right, Operation::JoinInner, |left, right| {
            left.zip(right)
        })
    }

    pub fn join_left_named<W: Element + Clone>(
        &self,
        key: &str,
        right: &PCollection<(K, W)>,
    ) -> Result<PCollection<LeftJoined<K, V, W>>, Error>
    where
        V: Clone,
    {
        self.join_named(key, right, Operation::JoinLeft, |left, right| {
            left.map(|value| (value, right))
        })
    }

    pub fn join_right_named<W: Element + Clone>(
        &self,
        key: &str,
        right: &PCollection<(K, W)>,
    ) -> Result<PCollection<RightJoined<K, V, W>>, Error>
    where
        V: Clone,
    {
        self.join_named(key, right, Operation::JoinRight, |left, right| {
            right.map(|value| (left, value))
        })
    }

    pub fn join_full_named<W: Element + Clone>(
        &self,
        key: &str,
        right: &PCollection<(K, W)>,
    ) -> Result<PCollection<FullJoined<K, V, W>>, Error>
    where
        V: Clone,
    {
        self.join_named(key, right, Operation::JoinFull, |left, right| {
            Some((left, right))
        })
    }

    fn join_named<W: Element + Clone, O: Element>(
        &self,
        key: &str,
        right: &PCollection<(K, W)>,
        operation: Operation,
        emit: impl Fn(Option<V>, Option<W>) -> Option<O> + Send + Sync + 'static,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        V: Clone,
    {
        if !Arc::ptr_eq(&self.pipeline.inner, &right.pipeline.inner) {
            return Err(Error::graph(
                key,
                "join inputs belong to different pipelines",
            ));
        }
        let left = self.parent()?;
        let other = right.parent()?;
        if left.time != other.time {
            return Err(Error::graph(
                key,
                "join inputs must have equal time/window declarations",
            ));
        }
        let unbounded = left.boundedness == Boundedness::Unbounded
            || other.boundedness == Boundedness::Unbounded;
        if unbounded && left.time.window == Window::Global {
            return Err(Error::graph(
                key,
                "unbounded global join requires a finite window",
            ));
        }
        let inner_join = matches!(operation, Operation::JoinInner);
        let key_owned = key.to_owned();
        let emit = Arc::new(emit);
        let typed_emit = Arc::clone(&emit);
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [left, right]: [Vec<Envelope>; 2] = inputs
                .try_into()
                .map_err(|_| Error::execution(&key_owned, "direct", "join requires two inputs"))?;
            if inner_join && left.len() <= right.len() {
                return inner_join_reusing_right::<K, V, W, O>(
                    left,
                    right,
                    &key_owned,
                    emit.as_ref(),
                );
            }
            if inner_join && right.len() < left.len() {
                return inner_join_indexed_right::<K, V, W, O>(
                    left,
                    right,
                    &key_owned,
                    emit.as_ref(),
                );
            }
            // Join output order is unspecified; indexing the smaller side
            // avoids retaining one position per row of a highly skewed input.
            if right.len() < left.len() {
                let mut index: HashMap<(K, Option<WindowInterval>), JoinPositions> = HashMap::new();
                let mut right_rows = Vec::with_capacity(right.len());
                for event in right {
                    let (row_key, value) = *event.value.downcast::<(K, W)>().map_err(|_| {
                        Error::execution(&key_owned, "direct", "right join type mismatch")
                    })?;
                    let position = right_rows.len();
                    match index.entry((row_key.clone(), event.assigned_window)) {
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            entry.get_mut().push(position);
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(JoinPositions::One(position));
                        }
                    }
                    right_rows.push((
                        row_key,
                        value,
                        event.timestamp_ns,
                        event.window,
                        event.assigned_window,
                        false,
                    ));
                }
                let mut output = Vec::new();
                for event in left {
                    let Envelope {
                        value,
                        timestamp_ns,
                        window,
                        assigned_window,
                    } = event;
                    let (row_key, value) = *value.downcast::<(K, V)>().map_err(|_| {
                        Error::execution(&key_owned, "direct", "left join type mismatch")
                    })?;
                    if let Some(matches) = index.get(&(row_key.clone(), assigned_window)) {
                        for &position in matches.as_slice() {
                            let (_, right_value, right_time, right_window, right_assigned, matched) =
                                &mut right_rows[position];
                            *matched = true;
                            if let Some(pair) = emit(Some(value.clone()), Some(right_value.clone()))
                            {
                                output.push(Envelope {
                                    value: Box::new((row_key.clone(), pair)),
                                    timestamp_ns: *right_time,
                                    window: right_window.clone(),
                                    assigned_window: *right_assigned,
                                });
                            }
                        }
                    } else if let Some(pair) = emit(Some(value), None) {
                        output.push(Envelope {
                            value: Box::new((row_key, pair)),
                            timestamp_ns,
                            window,
                            assigned_window,
                        });
                    }
                }
                for (row_key, value, timestamp_ns, window, assigned_window, matched) in right_rows {
                    if !matched && let Some(pair) = emit(None, Some(value)) {
                        output.push(Envelope {
                            value: Box::new((row_key, pair)),
                            timestamp_ns,
                            window,
                            assigned_window,
                        });
                    }
                }
                return Ok(output);
            }
            let mut index: HashMap<(K, Option<WindowInterval>), JoinPositions> = HashMap::new();
            let mut left_rows = Vec::with_capacity(left.len());
            for event in left {
                let (row_key, value) = *event.value.downcast::<(K, V)>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "left join type mismatch")
                })?;
                let position = left_rows.len();
                match index.entry((row_key.clone(), event.assigned_window)) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().push(position);
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(JoinPositions::One(position));
                    }
                }
                left_rows.push((
                    row_key,
                    value,
                    event.timestamp_ns,
                    event.window,
                    event.assigned_window,
                    false,
                ));
            }
            let mut output = Vec::new();
            for event in right {
                let Envelope {
                    value,
                    timestamp_ns,
                    window,
                    assigned_window,
                } = event;
                let (row_key, value) = *value.downcast::<(K, W)>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "right join type mismatch")
                })?;
                if let Some(matches) = index.get(&(row_key.clone(), assigned_window)) {
                    for &position in matches.as_slice() {
                        let (_, left_value, _, _, _, matched) = &mut left_rows[position];
                        *matched = true;
                        if let Some(pair) = emit(Some(left_value.clone()), Some(value.clone())) {
                            output.push(Envelope {
                                value: Box::new((row_key.clone(), pair)),
                                timestamp_ns,
                                window: window.clone(),
                                assigned_window,
                            });
                        }
                    }
                } else if let Some(pair) = emit(None, Some(value)) {
                    output.push(Envelope {
                        value: Box::new((row_key, pair)),
                        timestamp_ns,
                        window,
                        assigned_window,
                    });
                }
            }
            for (row_key, value, timestamp_ns, window, assigned_window, matched) in left_rows {
                if !matched && let Some(pair) = emit(Some(value), None) {
                    output.push(Envelope {
                        value: Box::new((row_key, pair)),
                        timestamp_ns,
                        window,
                        assigned_window,
                    });
                }
            }
            Ok(output)
        });
        let collection = self.next_aggregate(
            key,
            operation,
            vec![InputEdge {
                key: right.key.clone(),
                element: ElementDescriptor {
                    type_id: <(K, W)>::type_identity(),
                    coder_id: <(K, W)>::coder_identity(),
                    coder_version: <(K, W)>::CODER_VERSION,
                },
            }],
            if unbounded {
                Boundedness::Unbounded
            } else {
                Boundedness::Bounded
            },
            aggregate,
            None,
        )?;
        let registration = JoinCoderRegistration {
            split_inputs: [
                |bytes| {
                    let (row_key, value) = <(K, V)>::decode(bytes)?;
                    Ok((row_key.encode()?, value.encode()?))
                },
                |bytes| {
                    let (row_key, value) = <(K, W)>::decode(bytes)?;
                    Ok((row_key.encode()?, value.encode()?))
                },
            ],
            assemble_inputs: [
                |encoded_key, encoded_value| {
                    Ok(Box::new((
                        K::decode(encoded_key)?,
                        V::decode(encoded_value)?,
                    )))
                },
                |encoded_key, encoded_value| {
                    Ok(Box::new((
                        K::decode(encoded_key)?,
                        W::decode(encoded_value)?,
                    )))
                },
            ],
        };
        let mut inner = collection
            .pipeline
            .inner
            .lock()
            .map_err(|_| Error::graph(key, "builder lock poisoned"))?;
        inner
            .registry
            .join_coders
            .insert(key.to_owned(), registration);
        if inner_join && left.time.window == Window::Global && left.time.initial_timestamp_ns == 0 {
            let left_source = inner.registry.sources.get(&self.key).and_then(|source| {
                Some((
                    source.len_hint?,
                    source
                        .vec_batch_factory
                        .as_ref()?
                        .downcast_ref::<TypedVecBatchFactory<(K, V)>>()?
                        .clone(),
                ))
            });
            let right_source = inner.registry.sources.get(&right.key).and_then(|source| {
                Some((
                    source.len_hint?,
                    source
                        .vec_batch_factory
                        .as_ref()?
                        .downcast_ref::<TypedVecBatchFactory<(K, W)>>()?
                        .clone(),
                ))
            });
            if let (Some((left_len, left_factory)), Some((right_len, right_factory))) =
                (left_source, right_source)
            {
                let left_key = self.key.clone();
                let right_key = right.key.clone();
                let typed_join: TypedJoinFactory<(K, O)> = Arc::new(move |budget, check| {
                    check()?;
                    for (source_key, len) in [(&left_key, left_len), (&right_key, right_len)] {
                        if len.saturating_mul(std::mem::size_of::<Envelope>()) > budget {
                            return Err(Error::resource(
                                source_key,
                                "materialized source exceeds job memory budget",
                            ));
                        }
                    }
                    let left = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        left_factory(0, left_len)
                    }))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(
                            &left_key,
                            "direct",
                            "source value clone panicked",
                        ))
                    })?;
                    check()?;
                    let right = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        right_factory(0, right_len)
                    }))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(
                            &right_key,
                            "direct",
                            "source value clone panicked",
                        ))
                    })?;
                    typed_inner_join::<K, V, W, O>(left, right, budget, check, typed_emit.as_ref())
                });
                inner
                    .registry
                    .typed_joins
                    .insert(key.to_owned(), Arc::new(typed_join));
            }
        }
        drop(inner);
        Ok(collection)
    }
}

#[derive(Clone)]
pub struct Output<T> {
    pipeline: Pipeline,
    key: String,
    marker: PhantomData<T>,
}
#[derive(Clone)]
pub struct Sink<T> {
    pipeline: Pipeline,
    key: String,
    marker: PhantomData<T>,
}
impl<T> Sink<T> {
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn snapshot(&self) -> Result<(Graph, Registry), Error> {
        self.pipeline.snapshot()
    }
    /// Whether two sinks were built from the same logical graph.
    #[must_use]
    pub fn shares_pipeline_with<U>(&self, other: &Sink<U>) -> bool {
        Arc::ptr_eq(&self.pipeline.inner, &other.pipeline.inner)
    }
}
impl<T> Output<T> {
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn snapshot(&self) -> Result<(Graph, Registry), Error> {
        self.pipeline.snapshot()
    }

    /// Whether two terminals belong to the same in-memory graph builder.
    #[must_use]
    pub fn shares_pipeline_with<U>(&self, other: &Output<U>) -> bool {
        Arc::ptr_eq(&self.pipeline.inner, &other.pipeline.inner)
    }
}

#[cfg(test)]
mod internal_tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn semantic_map_records_handler_identity_and_rejects_invalid_versions() {
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![2_i32]).unwrap();
        let version = Version { major: 2, minor: 1 };
        source
            .map_with_semantic_named("twice", "example/twice", version, |value| value * 2)
            .unwrap();
        let (graph, registry) = pipeline.snapshot().unwrap();
        let node = graph.node("twice").unwrap();
        assert_eq!(node.operation, Operation::Map);
        assert_eq!(node.semantic_id, "example/twice");
        assert_eq!(node.semantic_version, version);
        assert!(registry.has_single_transform("twice"));
        assert!(
            source
                .map_with_semantic_named("bad-id", "twice", version, |value| value)
                .is_err()
        );
        assert!(
            source
                .map_with_semantic_named("builtin-id", "ironbeam/map", version, |value| value)
                .is_err()
        );
        assert!(
            source
                .map_with_semantic_named(
                    "bad-version",
                    "example/twice",
                    Version { major: 0, minor: 1 },
                    |value| value,
                )
                .is_err()
        );
        assert_eq!(pipeline.snapshot().unwrap().0.nodes.len(), 2);
        let changed = Pipeline::default();
        changed
            .from_vec("source", vec![2_i32])
            .unwrap()
            .map_with_semantic_named(
                "twice",
                "example/twice",
                Version { major: 2, minor: 2 },
                |value| value * 2,
            )
            .unwrap();
        assert_ne!(
            graph.fingerprint().unwrap(),
            changed.snapshot().unwrap().0.fingerprint().unwrap()
        );
    }

    #[test]
    fn semantic_filter_and_flat_map_record_versions_in_graph_identity() {
        let build = |minor| {
            let pipeline = Pipeline::default();
            let source = pipeline.from_vec("source", vec![1_i32, 2, 3]).unwrap();
            let version = Version { major: 1, minor };
            let filtered = source
                .filter_with_semantic_named("filter", "example/odd", version, |value| {
                    value % 2 != 0
                })
                .unwrap();
            filtered
                .flat_map_with_semantic_named("expand", "example/duplicate", version, |value| {
                    vec![value, value]
                })
                .unwrap();
            pipeline.snapshot().unwrap().0
        };
        let graph = build(0);
        assert_eq!(graph.node("filter").unwrap().operation, Operation::Filter);
        assert_eq!(graph.node("expand").unwrap().operation, Operation::FlatMap);
        assert_eq!(graph.node("filter").unwrap().semantic_id, "example/odd");
        assert_eq!(
            graph.node("expand").unwrap().semantic_id,
            "example/duplicate"
        );
        assert_ne!(
            graph.fingerprint().unwrap(),
            build(1).fingerprint().unwrap()
        );
        let source = Pipeline::default().from_vec("source", vec![1_i32]).unwrap();
        assert!(
            source
                .filter_with_semantic_named(
                    "bad-filter",
                    "ironbeam/odd",
                    Version { major: 1, minor: 0 },
                    |_| true
                )
                .is_err()
        );
        assert!(
            source
                .flat_map_with_semantic_named(
                    "bad-expand",
                    "example/duplicate",
                    Version { major: 0, minor: 1 },
                    |value| vec![value]
                )
                .is_err()
        );
    }

    #[test]
    fn semantic_batch_handler_records_application_version() {
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
        let version = Version { major: 2, minor: 3 };
        source
            .map_batches_with_semantic_named("batch", 2, "example/partition", version, |values| {
                values.iter().rev().copied().collect::<Vec<_>>()
            })
            .unwrap();
        let (graph, registry) = pipeline.snapshot().unwrap();
        let node = graph.node("batch").unwrap();
        assert_eq!(node.operation, Operation::FlatMap);
        assert_eq!(node.semantic_id, "example/partition");
        assert_eq!(node.semantic_version, version);
        let result = registry
            .apply_many(
                "batch",
                Envelope {
                    value: Box::new(vec![1_i32, 2]),
                    timestamp_ns: 0,
                    window: Window::Global,
                    assigned_window: None,
                },
            )
            .unwrap();
        assert_eq!(
            result
                .into_iter()
                .map(|event| *event.value.downcast::<i32>().unwrap())
                .collect::<Vec<_>>(),
            [2, 1]
        );
        assert!(
            source
                .map_batches_with_semantic_named(
                    "invalid-batch",
                    2,
                    "example/partition",
                    Version { major: 0, minor: 1 },
                    |values| values.to_vec(),
                )
                .is_err()
        );
        assert!(
            source
                .apply_transform_with_semantic_named(
                    "zero-batch",
                    0,
                    "example/partition",
                    version,
                    |values| values,
                )
                .is_err()
        );
        assert!(
            pipeline
                .snapshot()
                .unwrap()
                .0
                .node("zero-batch/batches")
                .is_none()
        );
    }

    #[test]
    fn bundle_recovery_is_explicit_and_typed_adapter_forwards_the_exact_id() {
        struct RecoveringSink;
        impl TypedSink<i32> for RecoveringSink {
            fn write(&mut self, _: i32) -> Result<(), Error> {
                Ok(())
            }
            fn commit(&mut self) -> Result<(), Error> {
                Ok(())
            }
            fn abort(&mut self) -> Result<(), Error> {
                Ok(())
            }
            fn recover_bundle_finalization(&mut self, instruction_id: &str) -> Result<(), Error> {
                if instruction_id == "bundle-7" {
                    Ok(())
                } else {
                    Err(Error::incompatible("wrong bundle ID"))
                }
            }
        }
        struct OrdinarySession;
        impl SinkSession for OrdinarySession {
            fn write(&mut self, _: Envelope) -> Result<(), Error> {
                Ok(())
            }
            fn commit(&mut self) -> Result<(), Error> {
                Ok(())
            }
            fn abort(&mut self) -> Result<(), Error> {
                Ok(())
            }
        }

        let mut direct = OrdinarySession;
        assert!(direct.recover_bundle_finalization("bundle-7").is_err());
        let mut typed = SinkAdapter {
            key: "sink".into(),
            inner: Box::new(RecoveringSink),
        };
        assert!(typed.recover_bundle_finalization("bundle-7").is_ok());
        assert!(typed.recover_bundle_finalization("other").is_err());
    }

    #[test]
    fn poisoned_builder_reports_typed_errors() {
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
        let inner = pipeline.inner.clone();
        assert!(
            catch_unwind(AssertUnwindSafe(move || {
                let _guard = inner.lock().unwrap();
                panic!("poison builder");
            }))
            .is_err()
        );
        assert_eq!(pipeline.snapshot().err().unwrap().kind, ErrorKind::Graph);
        assert_eq!(
            pipeline
                .from_vec("another", vec![2_i32])
                .err()
                .unwrap()
                .kind,
            ErrorKind::Graph
        );
        assert_eq!(
            input.map_named("map", |x| x + 1).err().unwrap().kind,
            ErrorKind::Graph
        );
    }
}
