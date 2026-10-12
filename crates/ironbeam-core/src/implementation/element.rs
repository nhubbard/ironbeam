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

/// A v4-style reusable operator over one partition.
///
/// The v5 graph binds each
/// registration to an application semantic ID and version; the runner does not
/// infer those from the compiled operator. Planner hints are retained for API
/// migration. They do not change v5 execution or graph identity; batch size
/// and semantic version are the execution contract.
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
