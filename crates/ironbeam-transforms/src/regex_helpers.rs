//! V4-compatible regex results with construction-time pattern errors.
use ironbeam_core::{Error, PCollection};
use regex::Regex;

fn compile(key: &str, pattern: &str) -> Result<Regex, Error> {
    Regex::new(pattern)
        .map_err(|error| Error::graph(key, format!("invalid regex pattern: {error}")))
}

pub trait RegexExt {
    fn regex_matches_named(&self, key: &str, pattern: &str) -> Result<PCollection<String>, Error>;
    fn regex_extract_named(
        &self,
        key: &str,
        pattern: &str,
        group: usize,
    ) -> Result<PCollection<String>, Error>;
    fn regex_extract_kv_named(
        &self,
        key: &str,
        pattern: &str,
        key_group: usize,
        value_group: usize,
    ) -> Result<PCollection<(String, String)>, Error>;
    fn regex_find_named(&self, key: &str, pattern: &str) -> Result<PCollection<String>, Error>;
    fn regex_replace_all_named(
        &self,
        key: &str,
        pattern: &str,
        replacement: &str,
    ) -> Result<PCollection<String>, Error>;
    fn regex_split_named(
        &self,
        key: &str,
        pattern: &str,
    ) -> Result<PCollection<Vec<String>>, Error>;
}
impl RegexExt for PCollection<String> {
    fn regex_matches_named(&self, key: &str, pattern: &str) -> Result<Self, Error> {
        let re = compile(key, pattern)?;
        self.filter_named(key, move |line| re.is_match(line))
    }
    fn regex_extract_named(&self, key: &str, pattern: &str, group: usize) -> Result<Self, Error> {
        let re = compile(key, pattern)?;
        self.flat_map_named(key, move |line| {
            re.captures(&line)
                .and_then(|caps| caps.get(group).map(|m| m.as_str().to_owned()))
                .into_iter()
                .collect()
        })
    }
    fn regex_extract_kv_named(
        &self,
        key: &str,
        pattern: &str,
        key_group: usize,
        value_group: usize,
    ) -> Result<PCollection<(String, String)>, Error> {
        let re = compile(key, pattern)?;
        self.flat_map_named(key, move |line| {
            re.captures(&line)
                .and_then(|caps| {
                    Some((
                        caps.get(key_group)?.as_str().to_owned(),
                        caps.get(value_group)?.as_str().to_owned(),
                    ))
                })
                .into_iter()
                .collect()
        })
    }
    fn regex_find_named(&self, key: &str, pattern: &str) -> Result<Self, Error> {
        let re = compile(key, pattern)?;
        self.flat_map_named(key, move |line| {
            re.find(&line)
                .map(|m| m.as_str().to_owned())
                .into_iter()
                .collect()
        })
    }
    fn regex_replace_all_named(
        &self,
        key: &str,
        pattern: &str,
        replacement: &str,
    ) -> Result<Self, Error> {
        let re = compile(key, pattern)?;
        let replacement = replacement.to_owned();
        self.map_named(key, move |line| {
            re.replace_all(&line, replacement.as_str()).into_owned()
        })
    }
    fn regex_split_named(
        &self,
        key: &str,
        pattern: &str,
    ) -> Result<PCollection<Vec<String>>, Error> {
        let re = compile(key, pattern)?;
        self.map_named(key, move |line| {
            re.split(&line).map(str::to_owned).collect()
        })
    }
}
