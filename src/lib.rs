use anyhow::{bail, Context, Result};
use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

/// -------- NodeId (defined) --------
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct NodeId(u64);

impl NodeId {
    fn new(v: u64) -> Self { Self(v) }
    pub fn raw(&self) -> u64 { self.0 }
}

/// -------- Pipeline + nodes --------
/// We keep a tiny graph so the public API doesn't change later.
pub struct Pipeline {
    inner: Arc<Mutex<PipelineInner>>,
}

struct PipelineInner {
    next_id: u64,
    nodes: HashMap<NodeId, Node>,
    edges: Vec<(NodeId, NodeId)>,
}

impl Default for Pipeline {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PipelineInner {
                next_id: 0,
                nodes: HashMap::new(),
                edges: Vec::new(),
            })),
        }
    }
}

enum Node {
    /// Holds a materialized Vec<T> as type-erased payload.
    Materialized(Box<dyn Any + Send + Sync>),
}

impl Pipeline {
    fn insert_node(&self, node: Node) -> NodeId {
        let mut g = self.inner.lock().unwrap();
        let id = NodeId::new(g.next_id);
        g.next_id += 1;
        g.nodes.insert(id, node);
        id
    }

    fn connect(&self, from: NodeId, to: NodeId) {
        let mut g = self.inner.lock().unwrap();
        g.edges.push((from, to));
    }

    //noinspection RsUnwrap
    fn read_vec<T: 'static + Send + Sync + Clone>(&self, id: NodeId) -> Result<Vec<T>> {
        let g = self.inner.lock().unwrap();
        let Some(Node::Materialized(payload)) = g.nodes.get(&id) else {
            bail!("node {id:?} not found");
        };
        // Downcast safely; don't use expect—propagate a nice error instead.
        payload
            .downcast_ref::<Vec<T>>()
            .cloned()
            .with_context(|| format!("type mismatch reading node {:?}", id))
    }

    fn write_vec<T: 'static + Send + Sync>(&self, data: Vec<T>) -> NodeId {
        self.insert_node(Node::Materialized(Box::new(data)))
    }
}

/// A typed handle to a node's output.
pub struct PCollection<T> {
    pipeline: Pipeline,
    id: NodeId,
    _t: PhantomData<T>,
}

impl<T> Clone for PCollection<T> {
    fn clone(&self) -> Self {
        Self {
            pipeline: Pipeline { inner: Arc::clone(&self.pipeline.inner) },
            id: self.id,
            _t: PhantomData,
        }
    }
}

/// -------- Sources --------
pub fn from_vec<T>(p: &Pipeline, data: Vec<T>) -> PCollection<T>
where
    T: 'static + Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned,
{
    let id = p.write_vec(data);
    PCollection { pipeline: p.clone(), id, _t: PhantomData }
}

/// -------- Transforms (eager execution into new node) --------
impl<T> PCollection<T>
where
    T: 'static + Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned,
{
    /// Map T -> O
    pub fn map<O, F>(self, f: F) -> PCollection<O>
    where
        O: 'static + Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned,
        F: 'static + Send + Sync + Fn(&T) -> O,
    {
        let input: Vec<T> = self.pipeline.read_vec(self.id)
            .expect("internal: source vector missing");
        let out: Vec<O> = input.iter().map(f).collect();
        let out_id = self.pipeline.write_vec(out);
        self.pipeline.connect(self.id, out_id);
        PCollection { pipeline: self.pipeline, id: out_id, _t: PhantomData }
    }

    /// Filter T with predicate(&T) -> bool
    pub fn filter<F>(self, pred: F) -> PCollection<T>
    where
        F: 'static + Send + Sync + Fn(&T) -> bool,
    {
        let input: Vec<T> = self.pipeline.read_vec(self.id)
            .expect("internal: source vector missing");
        let out: Vec<T> = input.into_iter().filter(|t| pred(t)).collect();
        let out_id = self.pipeline.write_vec(out);
        self.pipeline.connect(self.id, out_id);
        PCollection { pipeline: self.pipeline, id: out_id, _t: PhantomData }
    }

    /// FlatMap T -> Vec<O>
    pub fn flat_map<O, F>(self, f: F) -> PCollection<O>
    where
        O: 'static + Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned,
        F: 'static + Send + Sync + Fn(&T) -> Vec<O>,
    {
        let input: Vec<T> = self.pipeline.read_vec(self.id)
            .expect("internal: source vector missing");
        let mut out: Vec<O> = Vec::new();
        for t in &input {
            out.extend(f(t));
        }
        let out_id = self.pipeline.write_vec(out);
        self.pipeline.connect(self.id, out_id);
        PCollection { pipeline: self.pipeline, id: out_id, _t: PhantomData }
    }

    /// Materialize result as Vec<T>.
    pub fn collect(self) -> Result<Vec<T>> {
        self.pipeline.read_vec::<T>(self.id)
    }
}

/// Allow `Pipeline` cloning.
impl Clone for Pipeline {
    fn clone(&self) -> Self {
        Pipeline { inner: Arc::clone(&self.inner) }
    }
}

#[test]
fn run_pipeline() -> Result<()> {
    let p = Pipeline::default();

    let lines = from_vec(
        &p,
        vec![
            "The quick brown fox".to_string(),
            "Jumps over the lazy dog".to_string()
        ]
    );

    let words = lines.flat_map(|s: &String| {
        s.split_whitespace()
            .map(|w| w.to_lowercase())
            .collect::<Vec<_>>()
    });

    let filtered = words.filter(|w: &String| w.len() >= 3);

    let out = filtered.collect()?;

    assert_eq!(out.len(), 9);

    let expected = vec!["the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"];
    assert_eq!(expected, out);

    Ok(())
}