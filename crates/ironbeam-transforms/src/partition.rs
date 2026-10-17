//! Route each element to one of several typed output collections.

use ironbeam_core::{Element, Error, PCollection, Version};

fn outputs<T: Element>(
    classified: &PCollection<(u64, T)>,
    prefix: &str,
    partitions: usize,
) -> Result<Vec<PCollection<T>>, Error> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(partitions)
        .map_err(|error| Error::resource(prefix, error.to_string()))?;
    for index in 0..partitions {
        let branch = u64::try_from(index)
            .map_err(|_| Error::resource(prefix, "partition index exceeds u64"))?;
        result.push(classified.flat_map_named(
            &format!("{prefix}/{index}"),
            move |(selected, value)| {
                if selected == branch {
                    vec![value]
                } else {
                    Vec::new()
                }
            },
        )?);
    }
    Ok(result)
}

fn validate(classify_key: &str, prefix: &str, partitions: usize) -> Result<(), Error> {
    if partitions == 0 {
        return Err(Error::graph(
            classify_key,
            "partition count must be positive",
        ));
    }
    if prefix.is_empty() {
        return Err(Error::graph(
            classify_key,
            "partition output prefix must not be empty",
        ));
    }
    u64::try_from(partitions)
        .map_err(|_| Error::resource(classify_key, "partition count exceeds u64"))?;
    Ok(())
}

/// Split a collection into numbered outputs, as in Beam `Partition`.
///
/// The classifier must return a stable index in `0..partitions` for every
/// element. An out-of-range result fails the job; it is never silently
/// discarded. Runners may evaluate a classifier again for another branch or
/// a retry, so it must be deterministic and free of observable side effects.
/// Each output has the stable graph key `{prefix}/{index}`.
pub trait PartitionExt<T: Element> {
    /// Route each element to one numbered output using a stable classifier.
    fn partition_named(
        &self,
        classify_key: &str,
        prefix: &str,
        partitions: usize,
        classify: impl Fn(&T) -> usize + Send + Sync + 'static,
    ) -> Result<Vec<PCollection<T>>, Error>;

    /// Bind the classifier's application identity and version to the graph.
    /// Change the version when its routing behavior changes.
    fn partition_with_semantic_named(
        &self,
        classify_key: &str,
        prefix: &str,
        partitions: usize,
        semantic_id: &str,
        version: Version,
        classify: impl Fn(&T) -> usize + Send + Sync + 'static,
    ) -> Result<Vec<PCollection<T>>, Error>;
}

impl<T: Element> PartitionExt<T> for PCollection<T> {
    fn partition_named(
        &self,
        classify_key: &str,
        prefix: &str,
        partitions: usize,
        classify: impl Fn(&T) -> usize + Send + Sync + 'static,
    ) -> Result<Vec<Self>, Error> {
        validate(classify_key, prefix, partitions)?;
        let classified = self.map_named(classify_key, move |value| {
            let index = classify(&value);
            assert!(
                index < partitions,
                "partition classifier returned out-of-range index"
            );
            (
                u64::try_from(index).expect("validated partition count fits u64"),
                value,
            )
        })?;
        outputs(&classified, prefix, partitions)
    }

    fn partition_with_semantic_named(
        &self,
        classify_key: &str,
        prefix: &str,
        partitions: usize,
        semantic_id: &str,
        version: Version,
        classify: impl Fn(&T) -> usize + Send + Sync + 'static,
    ) -> Result<Vec<Self>, Error> {
        validate(classify_key, prefix, partitions)?;
        let classified =
            self.map_with_semantic_named(classify_key, semantic_id, version, move |value| {
                let index = classify(&value);
                assert!(
                    index < partitions,
                    "partition classifier returned out-of-range index"
                );
                (
                    u64::try_from(index).expect("validated partition count fits u64"),
                    value,
                )
            })?;
        outputs(&classified, prefix, partitions)
    }
}
