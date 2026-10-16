//! Local portable final-result coordination after a runner reports `DONE`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use crate::finalization_journal::{
    CompletedFinalizationManifest, FileFinalizationJournal, ObjectFinalizationJournal,
};
use anyhow::{bail, Context, Result};
use beam_proto::job_management_v1::{job_state, MetricResults};
use beam_proto::pipeline_v1::ParDoPayload;
use ironbeam_cloud_objects::ObjectStores;
use ironbeam_core::ExecutionIdentity;
use ironbeam_graphx::v5::TranslatedV5;
use prost::Message;

use crate::portable_metrics::committed_sink_input_count;

/// The graph-bound information needed before submitting a portable job.
pub struct PortablePublicationPlan {
    identity: ExecutionIdentity,
    sink_transforms: BTreeMap<String, String>,
}

/// Runner-committed counts and exact finalized attempts for every declared sink.
pub struct ReadyPublication {
    manifest: CompletedFinalizationManifest,
    rows: BTreeMap<String, u64>,
}

/// Compatibility name for publication decisions backed by a local journal.
pub type ReadyLocalPublication = ReadyPublication;

impl PortablePublicationPlan {
    /// Capture final-result sinks from an already validated v5 translation.
    pub fn from_translated(translated: &TranslatedV5) -> Result<Self> {
        let components = translated
            .pipeline
            .components
            .as_ref()
            .context("translated pipeline has no components")?;
        let mut sink_transforms = BTreeMap::new();
        for sink in &translated.finalization_sinks {
            let mut matches = components
                .transforms
                .iter()
                .filter(|(_, transform)| transform.unique_name == *sink);
            let (id, transform) = matches
                .next()
                .with_context(|| format!("final-result sink {sink:?} is absent from Beam graph"))?;
            if matches.next().is_some() {
                bail!("final-result sink {sink:?} has multiple Beam transforms");
            }
            let spec = transform
                .spec
                .as_ref()
                .with_context(|| format!("final-result sink {sink:?} has no Beam spec"))?;
            if spec.urn != "beam:transform:pardo:v1"
                || !ParDoPayload::decode(spec.payload.as_slice())
                    .with_context(|| format!("decode final-result sink {sink:?}"))?
                    .requests_finalization
                || !transform.outputs.is_empty()
            {
                bail!("final-result sink {sink:?} has an invalid Beam finalization contract");
            }
            sink_transforms.insert(sink.clone(), id.clone());
        }
        if sink_transforms.is_empty() {
            bail!("portable publication plan has no final-result sinks");
        }
        for (id, transform) in &components.transforms {
            let Some(spec) = &transform.spec else {
                continue;
            };
            if spec.urn != "beam:transform:pardo:v1" {
                continue;
            }
            let pardo = ParDoPayload::decode(spec.payload.as_slice())
                .with_context(|| format!("decode Beam transform {id:?}"))?;
            if pardo.requests_finalization
                && sink_transforms.get(&transform.unique_name) != Some(id)
            {
                bail!("finalizing Beam transform {id:?} is absent from the publication plan");
            }
        }
        Ok(Self {
            identity: translated.identity.clone(),
            sink_transforms,
        })
    }

    /// Validate one successful local job before any final output is published.
    /// The caller must supply the state and metrics from the same JobService job.
    pub fn complete_local(
        &self,
        state: job_state::Enum,
        metrics: &MetricResults,
        journal_dir: impl AsRef<Path>,
    ) -> Result<ReadyPublication> {
        if state != job_state::Enum::Done {
            bail!("portable job ended in {state:?}; final output cannot publish");
        }
        let journal = FileFinalizationJournal::new(journal_dir, self.identity.clone())?;
        let manifest = journal.completed_manifest(self.sink_transforms.keys())?;
        self.ready_with_metrics(metrics, manifest)
    }

    /// Validate a successful job against an object-backed finalization journal.
    /// The caller supplies committed metrics from the same runner job. The
    /// provider must support consistent prefix listing and conditional create.
    pub fn complete_object(
        &self,
        state: job_state::Enum,
        metrics: &MetricResults,
        stores: Arc<ObjectStores>,
        journal_uri: &str,
    ) -> Result<ReadyPublication> {
        if state != job_state::Enum::Done {
            bail!("portable job ended in {state:?}; final output cannot publish");
        }
        let journal = ObjectFinalizationJournal::new(stores, journal_uri, self.identity.clone())?;
        let manifest = journal.completed_manifest(self.sink_transforms.keys())?;
        self.ready_with_metrics(metrics, manifest)
    }

    fn ready_with_metrics(
        &self,
        metrics: &MetricResults,
        manifest: CompletedFinalizationManifest,
    ) -> Result<ReadyPublication> {
        let mut rows = BTreeMap::new();
        for (sink, transform_id) in &self.sink_transforms {
            rows.insert(
                sink.clone(),
                committed_sink_input_count(metrics, transform_id)?,
            );
        }
        Ok(ReadyPublication { manifest, rows })
    }
}

impl ReadyPublication {
    /// Return accepted bundle attempts for one final-result sink.
    pub fn bundle_attempts(&self, sink: &str) -> Result<&[String]> {
        self.manifest.attempts_for_sink(sink)
    }

    /// Return the runner-confirmed row count for one sink.
    pub fn committed_rows(&self, sink: &str) -> Result<u64> {
        self.rows
            .get(sink)
            .copied()
            .with_context(|| format!("sink {sink:?} is absent from the publication plan"))
    }

    /// Iterate over the sinks authorized for publication.
    pub fn sinks(&self) -> impl Iterator<Item = &str> {
        self.manifest.sinks()
    }
}

#[cfg(test)]
#[path = "../tests/unit/portable_publication__tests.rs"]
mod tests;
