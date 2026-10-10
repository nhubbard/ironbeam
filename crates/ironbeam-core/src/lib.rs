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
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Write as _;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

pub const GRAPH_VERSION: Version = Version { major: 2, minor: 0 };
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
integer_element!(u32, "std/u32", "ironbeam/u32-le", 4);
integer_element!(u64, "std/u64", "ironbeam/u64-le", 8);
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
            Self::Fixed { .. } => Err(Error::graph("time", "fixed window size must be positive")),
        }
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
    FixedWindow,
    GlobalCombine,
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
pub struct Node {
    pub key: String,
    pub input: Option<String>,
    pub input_element: Option<ElementDescriptor>,
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
    pub unbounded_source: bool,
    pub fixed_window: bool,
    pub append_sink: bool,
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
            if node.coder_version.major != CODER_VERSION.major
                || node.coder_version.minor > CODER_VERSION.minor
            {
                return Err(Error::incompatible(format!(
                    "unsupported coder version at {}",
                    node.key
                )));
            }
            if matches!(node.time.window, Window::Fixed { size_ns } if size_ns <= 0) {
                return Err(Error::graph(
                    &node.key,
                    "fixed window size must be positive",
                ));
            }
            if matches!(node.operation, Operation::FixedWindow)
                && !matches!(node.time.window, Window::Fixed { .. })
            {
                return Err(Error::graph(
                    &node.key,
                    "fixed-window operation requires a fixed window descriptor",
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
                            | Operation::FixedWindow
                            | Operation::Output
                            | Operation::Sink { .. }
                    ) && (node.type_id != upstream.type_id
                        || node.coder_id != upstream.coder_id
                        || node.coder_version != upstream.coder_version)
                    {
                        return Err(Error::graph(&node.key, "input type/coder mismatch"));
                    }
                    if node.boundedness != upstream.boundedness {
                        return Err(Error::graph(&node.key, "boundedness must propagate"));
                    }
                    if matches!(
                        node.operation,
                        Operation::Map
                            | Operation::Filter
                            | Operation::Output
                            | Operation::Sink { .. }
                    ) && node.time != upstream.time
                    {
                        return Err(Error::graph(&node.key, "event time/window must propagate"));
                    }
                    if matches!(node.operation, Operation::GlobalCombine)
                        && node.boundedness == Boundedness::Unbounded
                        && node.time.window == Window::Global
                    {
                        return Err(Error::graph(
                            &node.key,
                            "unbounded global aggregation requires a finite window",
                        ));
                    }
                }
            }
        }
        for node in &self.nodes {
            let mut seen = BTreeSet::new();
            let mut current = Some(node);
            while let Some(next) = current {
                if !seen.insert(next.key.as_str()) {
                    return Err(Error::graph(&node.key, "cycle in graph"));
                }
                current = next
                    .input
                    .as_ref()
                    .and_then(|key| by_key.get(key.as_str()).copied());
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
                Operation::FixedWindow if !capabilities.fixed_window => {
                    Some("fixed-window".to_string())
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
pub type SourceIterator = Box<dyn Iterator<Item = Envelope> + Send>;
type SourceFactory = Arc<dyn Fn() -> SourceIterator + Send + Sync>;
type Transform = Arc<dyn Fn(Envelope) -> Result<Option<Envelope>, Error> + Send + Sync>;

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
}
impl Registry {
    pub fn open_source(&self, key: &str) -> Result<SourceIterator, Error> {
        self.sources
            .get(key)
            .map(|source| (source.factory)())
            .ok_or_else(|| Error::execution(key, "direct", "source factory missing"))
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
        self.transforms
            .get(key)
            .ok_or_else(|| Error::execution(key, "direct", "transform missing"))?(input)
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

#[derive(Clone)]
pub struct PCollection<T> {
    pipeline: Pipeline,
    key: String,
    marker: PhantomData<T>,
}

impl Pipeline {
    fn semantic_identity(operation: &Operation) -> (String, Version) {
        match operation {
            Operation::Source { .. } => ("ironbeam/source".into(), BUILTIN_OPERATION_VERSION),
            Operation::Map => ("ironbeam/map".into(), BUILTIN_OPERATION_VERSION),
            Operation::Filter => ("ironbeam/filter".into(), BUILTIN_OPERATION_VERSION),
            Operation::FixedWindow => ("ironbeam/fixed-window".into(), BUILTIN_OPERATION_VERSION),
            Operation::GlobalCombine => {
                ("ironbeam/global-combine".into(), BUILTIN_OPERATION_VERSION)
            }
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
        inner.graph.nodes.push(node);
        drop(inner);
        Ok(())
    }
    pub fn from_vec<T: Element + Clone + Sync>(
        &self,
        key: &str,
        values: Vec<T>,
    ) -> Result<PCollection<T>, Error> {
        let config_digest = if T::supports_coder() {
            let mut hash = Sha256::new();
            hash.update(T::CODER_ID.as_bytes());
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
            Box::new((0..values.len()).map(move |i| Envelope {
                value: Box::new(values[i].clone()),
                timestamp_ns: 0,
                window: Window::Global,
                assigned_window: None,
            }))
        });
        self.source_node::<T>(
            key,
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
    pub fn unbounded_source<T: Element>(
        &self,
        key: &str,
        progress: SourceProgress,
        initial_timestamp_ns: i64,
    ) -> Result<PCollection<T>, Error> {
        self.source_node::<T>(
            key,
            Boundedness::Unbounded,
            progress,
            None,
            initial_timestamp_ns,
        )
    }
    fn source_node<T: Element>(
        &self,
        key: &str,
        boundedness: Boundedness,
        progress: SourceProgress,
        factory: Option<SourceRegistration>,
        initial_timestamp_ns: i64,
    ) -> Result<PCollection<T>, Error> {
        let operation = Operation::Source { progress };
        let (semantic_id, semantic_version) = Self::semantic_identity(&operation);
        self.add(
            Node {
                key: key.into(),
                input: None,
                input_element: None,
                operation,
                semantic_id,
                semantic_version,
                type_id: T::TYPE_ID.into(),
                coder_id: T::CODER_ID.into(),
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
                type_id: T::TYPE_ID.into(),
                coder_id: T::CODER_ID.into(),
                coder_version: T::CODER_VERSION,
            }),
            operation,
            semantic_id,
            semantic_version,
            type_id: U::TYPE_ID.into(),
            coder_id: U::CODER_ID.into(),
            coder_version: U::CODER_VERSION,
            boundedness: parent.boundedness,
            time: Time {
                initial_timestamp_ns: parent.time.initial_timestamp_ns,
                window: window.unwrap_or(parent.time.window),
            },
            local_id: 0,
        };
        self.pipeline.add(node, None, transform)?;
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
            Ok(Some(e))
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
            Ok(f(value).then_some(e))
        });
        self.next(key, Operation::Filter, Some(transform), None)
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
            Ok(Some(e))
        });
        self.next(key, Operation::FixedWindow, Some(transform), Some(window))
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
}

#[derive(Clone)]
pub struct Output<T> {
    pipeline: Pipeline,
    key: String,
    marker: PhantomData<T>,
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
