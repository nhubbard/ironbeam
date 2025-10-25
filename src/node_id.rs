/// -------- NodeId (defined) --------
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct NodeId(u64);

impl NodeId {
    pub(crate) fn new(v: u64) -> Self { Self(v) }
    pub fn raw(&self) -> u64 { self.0 }
}