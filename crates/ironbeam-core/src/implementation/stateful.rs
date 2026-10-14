/// Beam user-state cell shape. Coder IDs identify the wire representation,
/// rather than a Rust type or a process-local registry entry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum UserStateKind {
    Value { coder_id: String },
    Bag { element_coder_id: String },
    Set { element_coder_id: String },
    OrderedList { element_coder_id: String },
    Combining {
        accumulator_coder_id: String,
        combine_semantic_id: String,
        combine_version: Version,
    },
    Map { key_coder_id: String, value_coder_id: String },
    Multimap { key_coder_id: String, value_coder_id: String },
}

impl UserStateKind {
    /// Fn State protocol used to access this state family.
    #[must_use]
    pub const fn protocol_urn(&self) -> &'static str {
        match self {
            Self::Value { .. } | Self::Bag { .. } | Self::Set { .. } | Self::Combining { .. } => {
                "beam:user_state:bag:v1"
            }
            Self::Map { .. } | Self::Multimap { .. } => "beam:user_state:multimap:v1",
            Self::OrderedList { .. } => "beam:user_state:ordered_list:v1",
        }
    }

    fn validate(&self, id: &str) -> Result<(), Error> {
        let coder_ids = match self {
            Self::Value { coder_id } => vec![coder_id.as_str()],
            Self::Bag { element_coder_id }
            | Self::Set { element_coder_id }
            | Self::OrderedList { element_coder_id } => vec![element_coder_id.as_str()],
            Self::Combining { accumulator_coder_id, combine_semantic_id, combine_version } => {
                if !combine_semantic_id.contains('/') || combine_version.major == 0 {
                    return Err(Error::graph(id, "combining state requires a versioned semantic ID"));
                }
                vec![accumulator_coder_id.as_str()]
            }
            Self::Map { key_coder_id, value_coder_id }
            | Self::Multimap { key_coder_id, value_coder_id } => {
                vec![key_coder_id.as_str(), value_coder_id.as_str()]
            }
        };
        if coder_ids.iter().any(|coder| coder.is_empty()) {
            return Err(Error::graph(id, "user state requires coder IDs"));
        }
        Ok(())
    }
}

/// Beam's two user-facing timer domains.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum UserTimerDomain {
    EventTime,
    ProcessingTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UserTimerFamily {
    pub domain: UserTimerDomain,
    pub coder_id: String,
}

/// Stable declaration of one keyed, windowed stateful function. The map keys
/// become Beam user-state and timer-family IDs in a `ParDo` payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StatefulParDoContract {
    pub semantic_id: String,
    pub version: Version,
    pub key_coder_id: String,
    pub window_coder_id: String,
    pub states: BTreeMap<String, UserStateKind>,
    pub timers: BTreeMap<String, UserTimerFamily>,
    pub on_window_expiration_timer: Option<String>,
}

impl StatefulParDoContract {
    /// Validate identity and all referenced coder and timer IDs before adding
    /// a stateful function to a graph.
    pub fn validate(&self) -> Result<(), Error> {
        if !self.semantic_id.contains('/') || self.version.major == 0 {
            return Err(Error::graph(&self.semantic_id, "stateful function requires a versioned semantic ID"));
        }
        if self.key_coder_id.is_empty() || self.window_coder_id.is_empty() {
            return Err(Error::graph(&self.semantic_id, "stateful function requires key and window coders"));
        }
        if self.states.is_empty() && self.timers.is_empty() {
            return Err(Error::graph(&self.semantic_id, "stateful function requires state or a timer"));
        }
        for (id, kind) in &self.states {
            if id.is_empty() {
                return Err(Error::graph(&self.semantic_id, "user state ID must not be empty"));
            }
            kind.validate(id)?;
        }
        for (id, family) in &self.timers {
            if id.is_empty() || family.coder_id.is_empty() {
                return Err(Error::graph(&self.semantic_id, "timer family requires an ID and coder"));
            }
        }
        if let Some(id) = &self.on_window_expiration_timer
            && !self.timers.contains_key(id)
        {
            return Err(Error::graph(&self.semantic_id, "window-expiration timer family is not declared"));
        }
        Ok(())
    }
}
