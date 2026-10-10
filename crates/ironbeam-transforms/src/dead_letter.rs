//! Fallible transforms with independently addressable success and error branches.
use ironbeam_core::{Element, Error, PCollection};
use std::fmt::Display;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetter<T> {
    pub element: T,
    pub error: String,
}
impl<T> DeadLetter<T> {
    pub fn new(element: T, error: impl Into<String>) -> Self {
        Self {
            element,
            error: error.into(),
        }
    }
}

fn encode_part(bytes: &[u8], output: &mut Vec<u8>) -> Result<(), Error> {
    let length = u64::try_from(bytes.len())
        .map_err(|_| Error::incompatible("dead-letter part length overflow"))?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}
fn decode_part<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8], Error> {
    let prefix = bytes
        .get(..8)
        .ok_or_else(|| Error::incompatible("truncated dead-letter length prefix"))?;
    let length = usize::try_from(u64::from_le_bytes(
        prefix
            .try_into()
            .map_err(|_| Error::incompatible("invalid dead-letter length"))?,
    ))
    .map_err(|_| Error::incompatible("dead-letter part exceeds usize"))?;
    let end = 8_usize
        .checked_add(length)
        .ok_or_else(|| Error::incompatible("dead-letter part length overflow"))?;
    let part = bytes
        .get(8..end)
        .ok_or_else(|| Error::incompatible("truncated dead-letter part"))?;
    *bytes = &bytes[end..];
    Ok(part)
}

impl<T: Element> Element for DeadLetter<T> {
    const TYPE_ID: &'static str = "ironbeam/dead-letter";
    const CODER_ID: &'static str = "ironbeam/dead-letter-v1";
    fn type_identity() -> String {
        format!("ironbeam/dead-letter<{}>", T::type_identity())
    }
    fn coder_identity() -> String {
        format!("ironbeam/dead-letter-v1<{}>", T::coder_identity())
    }
    fn supports_coder() -> bool {
        T::supports_coder()
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut output = Vec::new();
        encode_part(&self.element.encode()?, &mut output)?;
        encode_part(&self.error.encode()?, &mut output)?;
        Ok(output)
    }
    fn decode(mut bytes: &[u8]) -> Result<Self, Error> {
        let element = T::decode(decode_part(&mut bytes)?)?;
        let error = String::decode(decode_part(&mut bytes)?)?;
        if !bytes.is_empty() {
            return Err(Error::incompatible("trailing dead-letter bytes"));
        }
        Ok(Self { element, error })
    }
}

pub trait DeadLetterExt<T: Element> {
    fn map_catching_named<O: Element, E: Display>(
        &self,
        classify_key: &str,
        good_key: &str,
        error_key: &str,
        f: impl Fn(&T) -> Result<O, E> + Send + Sync + 'static,
    ) -> Result<(PCollection<O>, PCollection<DeadLetter<T>>), Error>;
    fn flat_map_catching_named<O: Element, E: Display>(
        &self,
        classify_key: &str,
        good_key: &str,
        error_key: &str,
        f: impl Fn(&T) -> Result<Vec<O>, E> + Send + Sync + 'static,
    ) -> Result<(PCollection<O>, PCollection<DeadLetter<T>>), Error>;
}
impl<T: Element> DeadLetterExt<T> for PCollection<T> {
    fn map_catching_named<O: Element, E: Display>(
        &self,
        classify_key: &str,
        good_key: &str,
        error_key: &str,
        f: impl Fn(&T) -> Result<O, E> + Send + Sync + 'static,
    ) -> Result<(PCollection<O>, PCollection<DeadLetter<T>>), Error> {
        let classified = self.map_named(classify_key, move |value| match f(&value) {
            Ok(output) => Ok(output),
            Err(error) => Err(DeadLetter::new(value, error.to_string())),
        })?;
        let good = classified.flat_map_named(good_key, |item| item.ok().into_iter().collect())?;
        let errors =
            classified.flat_map_named(error_key, |item| item.err().into_iter().collect())?;
        Ok((good, errors))
    }
    fn flat_map_catching_named<O: Element, E: Display>(
        &self,
        classify_key: &str,
        good_key: &str,
        error_key: &str,
        f: impl Fn(&T) -> Result<Vec<O>, E> + Send + Sync + 'static,
    ) -> Result<(PCollection<O>, PCollection<DeadLetter<T>>), Error> {
        let classified = self.map_named(classify_key, move |value| match f(&value) {
            Ok(output) => Ok(output),
            Err(error) => Err(DeadLetter::new(value, error.to_string())),
        })?;
        let good = classified.flat_map_named(good_key, |item| item.unwrap_or_default())?;
        let errors =
            classified.flat_map_named(error_key, |item| item.err().into_iter().collect())?;
        Ok((good, errors))
    }
}
