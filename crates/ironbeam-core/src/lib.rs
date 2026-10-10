//! Runner-neutral Ironbeam 5 graph descriptors and typed construction.
//! Compiled Rust functions live in a separate local registry, never in the descriptor.
#![allow(
    clippy::missing_errors_doc,
    clippy::too_many_lines,
    clippy::struct_excessive_bools
)]

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::fmt::Write as _;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

pub const GRAPH_VERSION: Version = Version { major: 3, minor: 0 };
pub const CODER_VERSION: Version = Version { major: 1, minor: 0 };
pub const CHECKPOINT_VERSION: Version = Version { major: 1, minor: 0 };
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
    fn decode(_bytes: &[u8]) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Err(Error::unsupported(Self::TYPE_ID, "direct", "element-coder"))
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
        output.extend_from_slice(&(self.len() as u64).to_le_bytes());
        for value in self {
            encode_part(&value.encode()?, &mut output);
        }
        Ok(output)
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
        match self {
            None => Ok(vec![0]),
            Some(value) => {
                let mut bytes = vec![1];
                bytes.extend(value.encode()?);
                Ok(bytes)
            }
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
        let mut output = Vec::new();
        encode_part(&self.0.encode()?, &mut output);
        encode_part(&self.1.encode()?, &mut output);
        Ok(output)
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
                $(encode_part(&self.$index.encode()?, &mut bytes);)+
                Ok(bytes)
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
        let mut values = HashSet::with_capacity(count.min(1024));
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
        let mut values = HashMap::with_capacity(count.min(1024));
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
pub struct Envelope {
    pub value: Payload,
    pub timestamp_ns: i64,
    pub window: Window,
    pub assigned_window: Option<WindowInterval>,
}
pub type SourceIterator = Box<dyn Iterator<Item = Result<Envelope, Error>> + Send>;
pub type TypedSourceIterator<T> = Box<dyn Iterator<Item = Result<T, Error>> + Send>;
type SourceFactory = Arc<dyn Fn() -> Result<SourceIterator, Error> + Send + Sync>;
type Transform = Arc<dyn Fn(Envelope) -> Result<Vec<Envelope>, Error> + Send + Sync>;
type Aggregate = Arc<dyn Fn(Vec<Vec<Envelope>>) -> Result<Vec<Envelope>, Error> + Send + Sync>;
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
    fn finish(self: Box<Self>) -> Result<Vec<Envelope>, Error>;
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
}
pub trait SinkSession: Send {
    fn write(&mut self, event: Envelope) -> Result<(), Error>;
    fn commit(&mut self) -> Result<(), Error>;
    fn abort(&mut self) -> Result<(), Error>;
}
pub trait TypedSink<T: Element>: Send {
    fn write(&mut self, value: T) -> Result<(), Error>;
    fn commit(&mut self) -> Result<(), Error>;
    fn abort(&mut self) -> Result<(), Error>;
}
type SinkFactory = Arc<dyn Fn() -> Result<Box<dyn SinkSession>, Error> + Send + Sync>;
struct SinkAdapter<T: Element> {
    key: String,
    inner: Box<dyn TypedSink<T>>,
}
impl<T: Element> SinkSession for SinkAdapter<T> {
    fn write(&mut self, event: Envelope) -> Result<(), Error> {
        let value = event
            .value
            .downcast::<T>()
            .map_err(|_| Error::execution(&self.key, "direct", "sink input type mismatch"))?;
        self.inner.write(*value)
    }
    fn commit(&mut self) -> Result<(), Error> {
        self.inner.commit()
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.inner.abort()
    }
}

#[derive(Clone)]
struct SourceRegistration {
    factory: SourceFactory,
    len_hint: Option<usize>,
    config_digest: Option<String>,
}

#[derive(Clone, Default)]
pub struct Registry {
    sources: BTreeMap<String, SourceRegistration>,
    transforms: BTreeMap<String, Transform>,
    aggregates: BTreeMap<String, Aggregate>,
    folds: BTreeMap<String, FoldFactory>,
    sinks: BTreeMap<String, SinkFactory>,
}
impl Registry {
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
    #[must_use]
    pub fn source_len_hint(&self, key: &str) -> Option<usize> {
        self.sources.get(key).and_then(|source| source.len_hint)
    }
    #[must_use]
    pub fn source_config_digest(&self, key: &str) -> Option<&str> {
        self.sources
            .get(key)
            .and_then(|source| source.config_digest.as_deref())
    }
    pub fn apply(&self, key: &str, input: Envelope) -> Result<Option<Envelope>, Error> {
        let mut values = self.apply_many(key, input)?;
        if values.len() > 1 {
            return Err(Error::execution(
                key,
                "direct",
                "operation emits multiple elements",
            ));
        }
        Ok(values.pop())
    }
    pub fn apply_many(&self, key: &str, input: Envelope) -> Result<Vec<Envelope>, Error> {
        self.transforms
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "transform missing"))?(input)
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
    #[must_use]
    pub fn has_aggregate(&self, key: &str) -> bool {
        self.aggregates.contains_key(key)
    }
    #[must_use]
    pub fn has_sink(&self, key: &str) -> bool {
        self.sinks.contains_key(key)
    }
    pub fn open_sink(&self, key: &str) -> Result<Box<dyn SinkSession>, Error> {
        self.sinks
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "sink factory missing"))?()
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
    fn add(
        &self,
        mut node: Node,
        source: Option<SourceRegistration>,
        transform: Option<Transform>,
        aggregate: Option<Aggregate>,
        sink: Option<SinkFactory>,
    ) -> Result<(), Error> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| Error::graph(&node.key, "builder lock poisoned"))?;
        if inner.graph.node(&node.key).is_some() {
            return Err(Error::graph(&node.key, "duplicate stable node key"));
        }
        node.local_id = inner.graph.nodes.len();
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
        inner.graph.nodes.push(node);
        drop(inner);
        Ok(())
    }
    fn add_fold(&self, mut node: Node, factory: FoldFactory) -> Result<(), Error> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| Error::graph(&node.key, "builder lock poisoned"))?;
        if inner.graph.node(&node.key).is_some() {
            return Err(Error::graph(&node.key, "duplicate stable node key"));
        }
        node.local_id = inner.graph.nodes.len();
        inner.registry.folds.insert(node.key.clone(), factory);
        inner.graph.nodes.push(node);
        Ok(())
    }
    pub fn from_vec<T: Element + Clone + Sync>(
        &self,
        key: &str,
        values: Vec<T>,
    ) -> Result<PCollection<T>, Error> {
        let config_digest = if T::supports_coder() {
            let mut hash = Sha256::new();
            hash.update(T::coder_identity().as_bytes());
            hash.update(T::CODER_VERSION.major.to_le_bytes());
            hash.update(T::CODER_VERSION.minor.to_le_bytes());
            for value in &values {
                let encoded = value.encode()?;
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
            Some(result)
        } else {
            None
        };
        let values = Arc::new(values);
        let len_hint = values.len();
        let factory: SourceFactory = Arc::new(move || {
            let values = Arc::clone(&values);
            Ok(Box::new((0..values.len()).map(move |i| {
                Ok(Envelope {
                    value: Box::new(values[i].clone()),
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
                len_hint: Some(len_hint),
                config_digest,
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
        let factory: SourceFactory = Arc::new(move || {
            let source = open()?;
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
                len_hint,
                config_digest: Some(config_digest),
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
        self.add(
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
            time: Time {
                initial_timestamp_ns: parent.time.initial_timestamp_ns,
                window: window.unwrap_or(parent.time.window),
            },
            local_id: 0,
        };
        self.pipeline.add(node, None, transform, None, None)?;
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
        self.pipeline.add(node, None, None, Some(aggregate), None)?;
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
        self.pipeline.add_fold(node, factory)?;
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
        let key_owned = key.to_owned();
        let transform: Transform = Arc::new(move |mut e| {
            let value = e
                .value
                .downcast::<T>()
                .map_err(|_| Error::execution(&key_owned, "direct", "input type mismatch"))?;
            e.value = Box::new(f(*value));
            Ok(vec![e])
        });
        self.next(key, Operation::Map, Some(transform), None)
    }
    pub fn filter_named(
        &self,
        key: &str,
        f: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        let key_owned = key.to_owned();
        let transform: Transform = Arc::new(move |e| {
            let value = e
                .value
                .downcast_ref::<T>()
                .ok_or_else(|| Error::execution(&key_owned, "direct", "input type mismatch"))?;
            Ok(if f(value) { vec![e] } else { Vec::new() })
        });
        self.next(key, Operation::Filter, Some(transform), None)
    }
    pub fn flat_map_named<U: Element>(
        &self,
        key: &str,
        f: impl Fn(T) -> Vec<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        let key_owned = key.to_owned();
        let transform: Transform = Arc::new(move |e| {
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
        });
        self.next(key, Operation::FlatMap, Some(transform), None)
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
        self.pipeline.add(node, None, None, None, None)?;
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
        self.pipeline.add(node, None, None, None, None)?;
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
        let transform: Transform = Arc::new(move |mut e| {
            e.assigned_window = runtime_window.assign(e.timestamp_ns)?;
            e.window = runtime_window.clone();
            Ok(vec![e])
        });
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
        let transform: Transform = Arc::new(move |mut event| {
            event.assigned_window = runtime_window.assign(event.timestamp_ns)?;
            event.window = runtime_window.clone();
            Ok(vec![event])
        });
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
        let transform: Transform = Arc::new(move |event| {
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
            Ok(vec![Envelope {
                value: Box::new((interval, value)),
                timestamp_ns,
                window,
                assigned_window,
            }])
        });
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
        let transform: Transform = Arc::new(move |mut event| {
            let value = event.value.downcast_ref::<T>().ok_or_else(|| {
                Error::execution(&key_owned, "direct", "timestamp input type mismatch")
            })?;
            event.timestamp_ns = f(value);
            Ok(vec![event])
        });
        self.next(key, Operation::AssignTimestamp, Some(transform), None)
    }
    pub fn output(&self, key: &str) -> Result<Output<T>, Error> {
        self.next::<T>(key, Operation::Output, None, None)?;
        Ok(Output {
            pipeline: self.pipeline.clone(),
            key: key.into(),
            marker: PhantomData,
        })
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
        self.pipeline.add(node, None, None, None, Some(factory))?;
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
        let aggregate: Aggregate = Arc::new(move |mut inputs| {
            let values = inputs
                .pop()
                .ok_or_else(|| Error::execution(&key_owned, "direct", "combine input missing"))?;
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
        self.next_fold(key, Operation::FoldGlobal, factory)
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
        self.next_fold(key, Operation::FoldOrderedGlobal, factory)
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
    /// Build a bounded side view once per execution, then apply it to each main value.
    /// Materialization failures are returned as typed execution errors.
    pub fn map_with_materialized_side_named<S: Element, M: 'static, U: Element>(
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
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [main_values, side_values]: [Vec<Envelope>; 2] =
                inputs.try_into().map_err(|_| {
                    Error::execution(&key_owned, "direct", "side input requires two inputs")
                })?;
            let mut typed_side = Vec::with_capacity(side_values.len());
            for event in side_values {
                typed_side.push(*event.value.downcast::<S>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "side input type mismatch")
                })?);
            }
            let side_view = materialize(typed_side)?;
            let mut output = Vec::with_capacity(main_values.len());
            for event in main_values {
                let Envelope {
                    value,
                    timestamp_ns,
                    window,
                    assigned_window,
                } = event;
                let value = *value.downcast::<T>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "main input type mismatch")
                })?;
                output.push(Envelope {
                    value: Box::new(f(value, &side_view)?),
                    timestamp_ns,
                    window,
                    assigned_window,
                });
            }
            Ok(output)
        });
        self.next_aggregate(
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
        )
    }
}

type GroupBuffer<K, V> = Vec<(K, Vec<V>, i64, Window, Option<WindowInterval>)>;
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
            $first.next_aggregate(
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
            )
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
        self.next_fold(key, Operation::FoldPerKey, factory)
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
        self.next_fold(key, Operation::FoldOrderedPerKey, factory)
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
        let node_key = key.to_owned();
        let aggregate: Aggregate = Arc::new(move |mut inputs| {
            let events = inputs.pop().ok_or_else(|| {
                Error::execution(&node_key, "direct", "latest keyed input missing")
            })?;
            let mut indices: HashMap<(K, Option<WindowInterval>), usize> = HashMap::new();
            let mut rows: Vec<(K, V, i64, Window, Option<WindowInterval>)> = Vec::new();
            for event in events {
                let Envelope {
                    value,
                    timestamp_ns,
                    window,
                    assigned_window,
                } = event;
                let (row_key, row_value) = *value.downcast::<(K, V)>().map_err(|_| {
                    Error::execution(&node_key, "direct", "latest keyed input type mismatch")
                })?;
                let scope = (row_key.clone(), assigned_window);
                if let Some(&position) = indices.get(&scope) {
                    if timestamp_ns > rows[position].2 {
                        rows[position] =
                            (row_key, row_value, timestamp_ns, window, assigned_window);
                    }
                } else {
                    indices.insert(scope, rows.len());
                    rows.push((row_key, row_value, timestamp_ns, window, assigned_window));
                }
            }
            Ok(rows
                .into_iter()
                .map(
                    |(key, value, timestamp_ns, window, assigned_window)| Envelope {
                        value: Box::new((key, value)),
                        timestamp_ns,
                        window,
                        assigned_window,
                    },
                )
                .collect())
        });
        self.next_aggregate(
            key,
            Operation::GroupByKey,
            Vec::new(),
            parent.boundedness,
            aggregate,
        )
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
        let aggregate: Aggregate = Arc::new(move |mut inputs| {
            let events = inputs
                .pop()
                .ok_or_else(|| Error::execution(&key_owned, "direct", "group input missing"))?;
            let mut indices = HashMap::<(K, Option<WindowInterval>), usize>::new();
            let mut groups: GroupBuffer<K, V> = Vec::new();
            for event in events {
                let Envelope {
                    value,
                    timestamp_ns,
                    window,
                    assigned_window,
                } = event;
                let (group_key, value) = *value.downcast::<(K, V)>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "group input type mismatch")
                })?;
                let scope = (group_key.clone(), assigned_window);
                if let Some(&index) = indices.get(&scope) {
                    groups[index].1.push(value);
                } else {
                    indices.insert(scope, groups.len());
                    groups.push((
                        group_key,
                        vec![value],
                        timestamp_ns,
                        window,
                        assigned_window,
                    ));
                }
            }
            Ok(groups
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
        });
        self.next_aggregate(
            key,
            Operation::GroupByKey,
            Vec::new(),
            parent.boundedness,
            aggregate,
        )
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
        let key_owned = key.to_owned();
        let aggregate: Aggregate = Arc::new(move |inputs| {
            let [left, right]: [Vec<Envelope>; 2] = inputs
                .try_into()
                .map_err(|_| Error::execution(&key_owned, "direct", "join requires two inputs"))?;
            let mut index: HashMap<(K, Option<WindowInterval>), Vec<usize>> = HashMap::new();
            let mut left_rows = Vec::with_capacity(left.len());
            for event in left {
                let (row_key, value) = *event.value.downcast::<(K, V)>().map_err(|_| {
                    Error::execution(&key_owned, "direct", "left join type mismatch")
                })?;
                index
                    .entry((row_key.clone(), event.assigned_window))
                    .or_default()
                    .push(left_rows.len());
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
                    for &position in matches {
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
        self.next_aggregate(
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
        )
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
}
impl<T> Output<T> {
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn snapshot(&self) -> Result<(Graph, Registry), Error> {
        self.pipeline.snapshot()
    }
}

#[cfg(test)]
mod internal_tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};

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
