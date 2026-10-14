type GroupBuffer<K, V> = Vec<(K, Vec<V>, i64, Window, Option<WindowInterval>)>;
type ReifiedKeyedValue<K, V> = (K, (i64, V));
type LatestGroupBuffer<K, V> = Vec<(K, (i64, V), i64, Window, Option<WindowInterval>)>;
enum JoinPositions {
    One(usize),
    Many(Vec<usize>),
}
impl JoinPositions {
    fn record<K: Eq + Hash>(
        index: &mut HashMap<(K, Option<WindowInterval>), Self>,
        key: K,
        window: Option<WindowInterval>,
        position: usize,
    ) {
        match index.entry((key, window)) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().push(position);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(Self::One(position));
            }
        }
    }

    fn push(&mut self, position: usize) {
        match self {
            Self::One(first) => {
                let first = *first;
                *self = Self::Many(vec![first, position]);
            }
            Self::Many(positions) => positions.push(position),
        }
    }

    const fn as_slice(&self) -> &[usize] {
        match self {
            Self::One(position) => std::slice::from_ref(position),
            Self::Many(positions) => positions.as_slice(),
        }
    }
}

// Keep the specialized inner paths out of the outer-join dispatcher.
#[inline(never)]
fn inner_join_reusing_right<K, V, W, O>(
    left: Vec<Envelope>,
    right: Vec<Envelope>,
    key: &str,
    emit: &impl Fn(Option<V>, Option<W>) -> Option<O>,
) -> Result<Vec<Envelope>, Error>
where
    K: Element + Eq + Hash + Clone,
    V: Element + Clone,
    W: Element + Clone,
    O: Element,
{
    let mut index: HashMap<(K, Option<WindowInterval>), JoinPositions> = HashMap::new();
    let mut left_values = Vec::with_capacity(left.len());
    for event in left {
        let (row_key, value) = *event
            .value
            .downcast::<(K, V)>()
            .map_err(|_| Error::execution(key, "direct", "left join type mismatch"))?;
        let position = left_values.len();
        JoinPositions::record(&mut index, row_key, event.assigned_window, position);
        left_values.push((value, event.timestamp_ns));
    }
    // Consume each right input slot before appending its matches. This reuses
    // the input allocation while output grows.
    let mut output = VecDeque::from(right);
    let input_count = output.len();
    for _ in 0..input_count {
        let event = output.pop_front().expect("right input count is fixed");
        let Envelope {
            value,
            timestamp_ns,
            window,
            assigned_window,
        } = event;
        let (row_key, value) = *value
            .downcast::<(K, W)>()
            .map_err(|_| Error::execution(key, "direct", "right join type mismatch"))?;
        if let Some(matches) = index.get(&(row_key.clone(), assigned_window)) {
            for &position in matches.as_slice() {
                if let Some(pair) = emit(Some(left_values[position].0.clone()), Some(value.clone()))
                {
                    output.push_back(Envelope {
                        value: Box::new((row_key.clone(), pair)),
                        timestamp_ns: timestamp_ns.min(left_values[position].1),
                        window: window.clone(),
                        assigned_window,
                    });
                }
            }
        }
    }
    Ok(output.into())
}

#[inline(never)]
fn inner_join_indexed_right<K, V, W, O>(
    left: Vec<Envelope>,
    right: Vec<Envelope>,
    key: &str,
    emit: &impl Fn(Option<V>, Option<W>) -> Option<O>,
) -> Result<Vec<Envelope>, Error>
where
    K: Element + Eq + Hash + Clone,
    V: Element + Clone,
    W: Element + Clone,
    O: Element,
{
    let mut index: HashMap<(K, Option<WindowInterval>), JoinPositions> = HashMap::new();
    let mut right_rows = Vec::with_capacity(right.len());
    for event in right {
        let (row_key, value) = *event
            .value
            .downcast::<(K, W)>()
            .map_err(|_| Error::execution(key, "direct", "right join type mismatch"))?;
        JoinPositions::record(&mut index, row_key, event.assigned_window, right_rows.len());
        right_rows.push((
            value,
            event.timestamp_ns,
            event.window,
            event.assigned_window,
        ));
    }
    let mut output = VecDeque::from(left);
    let input_count = output.len();
    for _ in 0..input_count {
        let event = output.pop_front().expect("left input count is fixed");
        let left_time = event.timestamp_ns;
        let (row_key, value) = *event
            .value
            .downcast::<(K, V)>()
            .map_err(|_| Error::execution(key, "direct", "left join type mismatch"))?;
        if let Some(matches) = index.get(&(row_key.clone(), event.assigned_window)) {
            for &position in matches.as_slice() {
                let (right_value, timestamp_ns, window, assigned_window) = &right_rows[position];
                if let Some(pair) = emit(Some(value.clone()), Some(right_value.clone())) {
                    output.push_back(Envelope {
                        value: Box::new((row_key.clone(), pair)),
                        timestamp_ns: left_time.min(*timestamp_ns),
                        window: window.clone(),
                        assigned_window: *assigned_window,
                    });
                }
            }
        }
    }
    Ok(output.into())
}

fn typed_inner_join<K, V, W, O>(
    left: Vec<(K, V)>,
    right: Vec<(K, W)>,
    memory_budget: usize,
    check_cancelled: &dyn Fn() -> Result<(), Error>,
    emit: &impl Fn(Option<V>, Option<W>) -> Option<O>,
) -> Result<Vec<(K, O)>, Error>
where
    K: Element + Eq + Hash + Clone,
    V: Element + Clone,
    W: Element + Clone,
    O: Element,
{
    let mut index: HashMap<(K, Option<WindowInterval>), JoinPositions> = HashMap::new();
    let mut output = Vec::new();
    let max_rows = memory_budget / std::mem::size_of::<(K, O)>().max(1);
    if left.len() <= right.len() {
        let mut left_values = Vec::with_capacity(left.len());
        for (key, value) in left {
            JoinPositions::record(&mut index, key, None, left_values.len());
            left_values.push(value);
        }
        for (position, (key, value)) in right.into_iter().enumerate() {
            if position % 4096 == 0 {
                check_cancelled()?;
            }
            if let Some(matches) = index.get(&(key.clone(), None)) {
                for &slot in matches.as_slice() {
                    if let Some(pair) = emit(Some(left_values[slot].clone()), Some(value.clone())) {
                        if output.len() % 4096 == 0 {
                            check_cancelled()?;
                        }
                        if output.len() >= max_rows {
                            return Err(Error::resource(
                                "join",
                                "output exceeds job memory budget",
                            ));
                        }
                        output.push((key.clone(), pair));
                    }
                }
            }
        }
    } else {
        let mut right_values = Vec::with_capacity(right.len());
        for (key, value) in right {
            JoinPositions::record(&mut index, key, None, right_values.len());
            right_values.push(value);
        }
        for (position, (key, value)) in left.into_iter().enumerate() {
            if position % 4096 == 0 {
                check_cancelled()?;
            }
            if let Some(matches) = index.get(&(key.clone(), None)) {
                for &slot in matches.as_slice() {
                    if let Some(pair) = emit(Some(value.clone()), Some(right_values[slot].clone()))
                    {
                        if output.len() % 4096 == 0 {
                            check_cancelled()?;
                        }
                        if output.len() >= max_rows {
                            return Err(Error::resource(
                                "join",
                                "output exceeds job memory budget",
                            ));
                        }
                        output.push((key.clone(), pair));
                    }
                }
            }
        }
    }
    check_cancelled()?;
    Ok(output)
}
struct GroupByKeyFoldState<K, V> {
    key: String,
    global_indices: HashMap<K, usize>,
    window_indices: HashMap<(K, WindowInterval), usize>,
    session_indices: HashMap<K, Vec<usize>>,
    groups: GroupBuffer<K, V>,
}

#[derive(Serialize, Deserialize)]
struct GroupFoldCheckpoint {
    version: Version,
    key_coder: String,
    value_coder: String,
    groups: Vec<GroupFoldCheckpointRow>,
}

#[derive(Serialize, Deserialize)]
struct GroupFoldCheckpointRow {
    key: Vec<u8>,
    values: Vec<u8>,
    timestamp_ns: i64,
    window: Window,
    assigned_window: Option<WindowInterval>,
}

const GROUP_FOLD_CHECKPOINT_VERSION: Version = Version { major: 2, minor: 0 };

impl<K: Element + Eq + Hash + Clone, V: Element> GroupByKeyFoldState<K, V> {
    fn rebuild_indices(&mut self) {
        self.global_indices.clear();
        self.window_indices.clear();
        self.session_indices.clear();
        for (index, (key, _, _, window, interval)) in self.groups.iter().enumerate() {
            if let Some(interval) = interval {
                self.window_indices.insert((key.clone(), *interval), index);
                if matches!(window, Window::Session { .. }) {
                    self.session_indices.entry(key.clone()).or_default().push(index);
                }
            } else {
                self.global_indices.insert(key.clone(), index);
            }
        }
    }

    fn add_row(
        &mut self,
        group_key: K,
        value: V,
        timestamp_ns: i64,
        window: Window,
        assigned_window: Option<WindowInterval>,
    ) {
        if matches!(window, Window::Session { .. }) {
            let Some(mut merged) = assigned_window else {
                return;
            };
            let matches = self
                .session_indices
                .get(&group_key)
                .into_iter()
                .flatten()
                .copied()
                .filter(|&index| {
                    let interval = self.groups[index]
                        .4
                        .expect("session group has an interval");
                    merged.start_ns <= interval.end_ns && interval.start_ns <= merged.end_ns
                })
                .collect::<Vec<_>>();
            if let [index] = matches.as_slice() {
                let row = &mut self.groups[*index];
                let prior = row.4.expect("session group has an interval");
                merged.start_ns = merged.start_ns.min(prior.start_ns);
                merged.end_ns = merged.end_ns.max(prior.end_ns);
                row.1.push(value);
                row.2 = row.2.min(timestamp_ns);
                row.4 = Some(merged);
                self.window_indices.remove(&(group_key.clone(), prior));
                self.window_indices.insert((group_key, merged), *index);
                return;
            }
            if let Some(&primary) = matches.first() {
                let mut timestamp = timestamp_ns;
                let mut values = Vec::new();
                for &index in matches.iter().rev() {
                    let (_, prior_values, prior_timestamp, _, prior_interval) =
                        self.groups.remove(index);
                    let interval = prior_interval.expect("session group has an interval");
                    merged.start_ns = merged.start_ns.min(interval.start_ns);
                    merged.end_ns = merged.end_ns.max(interval.end_ns);
                    timestamp = timestamp.min(prior_timestamp);
                    values.extend(prior_values);
                }
                values.push(value);
                self.groups.insert(
                    primary,
                    (group_key, values, timestamp, window, Some(merged)),
                );
                self.rebuild_indices();
            } else {
                let index = self.groups.len();
                self.window_indices.insert((group_key.clone(), merged), index);
                self.session_indices
                    .entry(group_key.clone())
                    .or_default()
                    .push(index);
                self.groups.push((
                    group_key,
                    vec![value],
                    timestamp_ns,
                    window,
                    Some(merged),
                ));
            }
            return;
        }
        if let Some(interval) = assigned_window {
            let scope = (group_key.clone(), interval);
            if let Some(&index) = self.window_indices.get(&scope) {
                self.groups[index].1.push(value);
                self.groups[index].2 = self.groups[index].2.min(timestamp_ns);
                return;
            }
            self.window_indices.insert(scope, self.groups.len());
        } else {
            if let Some(&index) = self.global_indices.get(&group_key) {
                self.groups[index].1.push(value);
                self.groups[index].2 = self.groups[index].2.min(timestamp_ns);
                return;
            }
            self.global_indices
                .insert(group_key.clone(), self.groups.len());
        }
        self.groups.push((
            group_key,
            vec![value],
            timestamp_ns,
            window,
            assigned_window,
        ));
    }
}

impl<K: Element + Eq + Hash + Clone, V: Element> FoldSession for GroupByKeyFoldState<K, V> {
    fn add(&mut self, event: Envelope) -> Result<(), Error> {
        let Envelope {
            value,
            timestamp_ns,
            window,
            assigned_window,
        } = event;
        let (group_key, value) = *value
            .downcast::<(K, V)>()
            .map_err(|_| Error::execution(&self.key, "direct", "group input type mismatch"))?;
        self.add_row(group_key, value, timestamp_ns, window, assigned_window);
        Ok(())
    }

    fn drain_closed(&mut self, watermark_ns: i64) -> Result<(Vec<Envelope>, usize), Error> {
        let mut remaining = Vec::new();
        let mut closed = Vec::new();
        let mut released = 0_usize;
        for (key, values, timestamp_ns, window, assigned_window) in std::mem::take(&mut self.groups) {
            if assigned_window.is_some_and(|interval| interval.end_ns <= watermark_ns) {
                released = released.saturating_add(values.len());
                closed.push(Envelope {
                    value: Box::new((key, values)),
                    timestamp_ns,
                    window,
                    assigned_window,
                });
            } else {
                remaining.push((key, values, timestamp_ns, window, assigned_window));
            }
        }
        self.groups = remaining;
        self.rebuild_indices();
        Ok((closed, released))
    }

    fn accepts_vec_batch(&self, batch_type: TypeId) -> bool {
        batch_type == TypeId::of::<Vec<(K, V)>>()
    }

    fn checkpoint_state(&self) -> Result<Option<Vec<u8>>, Error> {
        if !<(K, Vec<V>) as Element>::supports_coder() {
            return Ok(None);
        }
        let groups = self
            .groups
            .iter()
            .map(|(key, values, timestamp_ns, window, assigned_window)| {
                Ok(GroupFoldCheckpointRow {
                    key: key.encode()?,
                    values: values.encode()?,
                    timestamp_ns: *timestamp_ns,
                    window: window.clone(),
                    assigned_window: *assigned_window,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        serde_json::to_vec(&GroupFoldCheckpoint {
            version: GROUP_FOLD_CHECKPOINT_VERSION,
            key_coder: K::coder_identity(),
            value_coder: V::coder_identity(),
            groups,
        })
        .map(Some)
        .map_err(|error| Error::recovery(&self.key, error.to_string()))
    }

    fn restore_checkpoint_state(&mut self, state: &[u8]) -> Result<(), Error> {
        if !self.groups.is_empty() {
            return Err(Error::recovery(&self.key, "fold already contains input"));
        }
        let checkpoint: GroupFoldCheckpoint = serde_json::from_slice(state)
            .map_err(|error| Error::recovery(&self.key, error.to_string()))?;
        if checkpoint.version != GROUP_FOLD_CHECKPOINT_VERSION {
            return Err(Error::incompatible(
                "group fold checkpoint version mismatch",
            ));
        }
        if checkpoint.key_coder != K::coder_identity()
            || checkpoint.value_coder != V::coder_identity()
        {
            return Err(Error::incompatible("group fold checkpoint coder mismatch"));
        }
        let mut restored = Self {
            key: self.key.clone(),
            global_indices: HashMap::new(),
            window_indices: HashMap::new(),
            session_indices: HashMap::new(),
            groups: Vec::with_capacity(checkpoint.groups.len()),
        };
        for row in checkpoint.groups {
            let key = K::decode(&row.key)?;
            let values = Vec::<V>::decode(&row.values)?;
            if values.is_empty() {
                return Err(Error::recovery(&self.key, "empty group in fold checkpoint"));
            }
            let position = restored.groups.len();
            if let Some(interval) = row.assigned_window {
                if restored
                    .window_indices
                    .insert((key.clone(), interval), position)
                    .is_some()
                {
                    return Err(Error::recovery(
                        &self.key,
                        "duplicate group in fold checkpoint",
                    ));
                }
                if matches!(row.window, Window::Session { .. }) {
                    restored
                        .session_indices
                        .entry(key.clone())
                        .or_default()
                        .push(position);
                }
            } else if restored
                .global_indices
                .insert(key.clone(), position)
                .is_some()
            {
                return Err(Error::recovery(
                    &self.key,
                    "duplicate group in fold checkpoint",
                ));
            }
            restored.groups.push((
                key,
                values,
                row.timestamp_ns,
                row.window,
                row.assigned_window,
            ));
        }
        *self = restored;
        Ok(())
    }

    fn add_vec_batch(&mut self, values: Payload) -> Result<(), Error> {
        let values = values.downcast::<Vec<(K, V)>>().map_err(|_| {
            Error::execution(&self.key, "direct", "group vector batch type mismatch")
        })?;
        for (key, value) in *values {
            self.add_row(key, value, 0, Window::Global, None);
        }
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<Vec<Envelope>, Error> {
        Ok(self
            .groups
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
    }

    fn finish_into(
        self: Box<Self>,
        emit: &mut dyn FnMut(Envelope) -> Result<(), Error>,
    ) -> Result<(), Error> {
        for (key, values, timestamp_ns, window, assigned_window) in self.groups {
            emit(Envelope {
                value: Box::new((key, values)),
                timestamp_ns,
                window,
                assigned_window,
            })?;
        }
        Ok(())
    }
}

// This fold is used only by latest_per_key's private GroupByKey node. Beam
// performs the ordinary shuffle; Direct can discard older values as they
// arrive instead of retaining every member of each group.
struct LatestPerKeyFoldState<K, V> {
    key: String,
    indices: HashMap<(K, Option<WindowInterval>), usize>,
    groups: LatestGroupBuffer<K, V>,
}

const LATEST_GROUP_FOLD_CHECKPOINT_VERSION: Version = Version { major: 2, minor: 0 };

impl<K: Element + Eq + Hash + Clone, V: Element> LatestPerKeyFoldState<K, V> {
    fn add_row(
        &mut self,
        group_key: K,
        candidate: (i64, V),
        timestamp_ns: i64,
        window: Window,
        assigned_window: Option<WindowInterval>,
    ) {
        let scope = (group_key.clone(), assigned_window);
        if let Some(&index) = self.indices.get(&scope) {
            if candidate.0 > self.groups[index].1.0 {
                self.groups[index].1 = candidate;
            }
        } else {
            self.indices.insert(scope, self.groups.len());
            self.groups
                .push((group_key, candidate, timestamp_ns, window, assigned_window));
        }
    }
}

impl<K: Element + Eq + Hash + Clone, V: Element> FoldSession for LatestPerKeyFoldState<K, V> {
    fn add(&mut self, event: Envelope) -> Result<(), Error> {
        let Envelope {
            value,
            timestamp_ns,
            window,
            assigned_window,
        } = event;
        let (group_key, candidate) = *value.downcast::<(K, (i64, V))>().map_err(|_| {
            Error::execution(&self.key, "direct", "latest group input type mismatch")
        })?;
        self.add_row(group_key, candidate, timestamp_ns, window, assigned_window);
        Ok(())
    }

    fn accepts_timed_vec_batch(&self, batch_type: TypeId) -> bool {
        batch_type == TypeId::of::<Vec<(K, V)>>()
    }

    fn add_timed_vec_batch(
        &mut self,
        values: Payload,
        timestamp_fn: &(dyn Any + Send + Sync),
        timestamp_key: &str,
    ) -> Result<(), Error> {
        let values = values.downcast::<Vec<(K, V)>>().map_err(|_| {
            Error::execution(&self.key, "direct", "latest vector batch type mismatch")
        })?;
        let timestamp = timestamp_fn
            .downcast_ref::<TypedTimestamp<(K, V)>>()
            .ok_or_else(|| Error::execution(timestamp_key, "direct", "timestamp type mismatch"))?;
        for row in *values {
            let timestamp_ns =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| timestamp(&row)))
                    .map_err(|_| {
                        Error::execution(timestamp_key, "direct", "user transform panicked")
                    })?;
            let (key, value) = row;
            self.add_row(
                key,
                (timestamp_ns, value),
                timestamp_ns,
                Window::Global,
                None,
            );
        }
        Ok(())
    }

    fn checkpoint_state(&self) -> Result<Option<Vec<u8>>, Error> {
        if !<(K, Vec<(i64, V)>) as Element>::supports_coder() {
            return Ok(None);
        }
        let groups = self
            .groups
            .iter()
            .map(|(key, value, timestamp_ns, window, assigned_window)| {
                Ok(GroupFoldCheckpointRow {
                    key: key.encode()?,
                    values: value.encode()?,
                    timestamp_ns: *timestamp_ns,
                    window: window.clone(),
                    assigned_window: *assigned_window,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        serde_json::to_vec(&GroupFoldCheckpoint {
            version: LATEST_GROUP_FOLD_CHECKPOINT_VERSION,
            key_coder: K::coder_identity(),
            value_coder: <(i64, V) as Element>::coder_identity(),
            groups,
        })
        .map(Some)
        .map_err(|error| Error::recovery(&self.key, error.to_string()))
    }

    fn restore_checkpoint_state(&mut self, state: &[u8]) -> Result<(), Error> {
        if !self.groups.is_empty() {
            return Err(Error::recovery(&self.key, "fold already contains input"));
        }
        let checkpoint: GroupFoldCheckpoint = serde_json::from_slice(state)
            .map_err(|error| Error::recovery(&self.key, error.to_string()))?;
        if checkpoint.version != LATEST_GROUP_FOLD_CHECKPOINT_VERSION {
            return Err(Error::incompatible(
                "latest group checkpoint version mismatch",
            ));
        }
        if checkpoint.key_coder != K::coder_identity()
            || checkpoint.value_coder != <(i64, V) as Element>::coder_identity()
        {
            return Err(Error::incompatible(
                "latest group checkpoint coder mismatch",
            ));
        }
        for row in checkpoint.groups {
            let key = K::decode(&row.key)?;
            let value = <(i64, V)>::decode(&row.values)?;
            let scope = (key.clone(), row.assigned_window);
            if self.indices.insert(scope, self.groups.len()).is_some() {
                return Err(Error::recovery(
                    &self.key,
                    "duplicate latest group in checkpoint",
                ));
            }
            self.groups.push((
                key,
                value,
                row.timestamp_ns,
                row.window,
                row.assigned_window,
            ));
        }
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<Vec<Envelope>, Error> {
        Ok(self
            .groups
            .into_iter()
            .map(
                |(key, value, timestamp_ns, window, assigned_window)| Envelope {
                    value: Box::new((key, vec![value])),
                    timestamp_ns,
                    window,
                    assigned_window,
                },
            )
            .collect())
    }

    fn finish_into(
        self: Box<Self>,
        emit: &mut dyn FnMut(Envelope) -> Result<(), Error>,
    ) -> Result<(), Error> {
        for (key, value, timestamp_ns, window, assigned_window) in self.groups {
            emit(Envelope {
                value: Box::new((key, vec![value])),
                timestamp_ns,
                window,
                assigned_window,
            })?;
        }
        Ok(())
    }
}
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
            $rows[group_index].2 = $rows[group_index].2.min(timestamp_ns);
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
            let session_cogroup = matches!(first_node.time.window, Window::Session { .. });
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
            let aggregate: Aggregate = Arc::new(move |mut inputs| {
                let expected = 1 $(+ { let _ = stringify!($other); 1 })+;
                if inputs.len() != expected {
                    return Err(Error::execution(&node_key, "direct", "co-group input count mismatch"));
                }
                if session_cogroup {
                    let extractors: &[SessionKeyExtractor<K>] = &[
                        |value| value.downcast_ref::<(K, $first_ty)>()
                            .map(|(key, _)| key.clone())
                            .ok_or_else(|| Error::incompatible("co-group input type mismatch")),
                        $(|value| value.downcast_ref::<(K, $other_ty)>()
                            .map(|(key, _)| key.clone())
                            .ok_or_else(|| Error::incompatible("co-group input type mismatch"))),+
                    ];
                    merge_bounded_cogroup_sessions(&mut inputs, extractors, &node_key)?;
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
            let collection = $first.next_aggregate(
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
                None,
            )?;
            let split_inputs: Vec<GroupInputSplitter> = vec![
                |bytes| {
                    let (row_key, value) = <(K, $first_ty)>::decode(bytes)?;
                    let mut tagged = vec![0];
                    tagged.extend(value.encode()?);
                    Ok((row_key.encode()?, tagged))
                },
                $(|bytes| {
                    let (row_key, value) = <(K, $other_ty)>::decode(bytes)?;
                    let mut tagged = vec![$position];
                    tagged.extend(value.encode()?);
                    Ok((row_key.encode()?, tagged))
                }),+
            ];
            let registration = CoGroupCoderRegistration {
                split_inputs,
                assemble_output: |encoded_key, encoded_values| {
                    let mut grouped = (Vec::<$first_ty>::new(), $(Vec::<$other_ty>::new()),+);
                    for tagged in encoded_values {
                        let (&tag, payload) = tagged.split_first().ok_or_else(||
                            Error::incompatible("co-group tagged value is empty"))?;
                        match tag {
                            0 => grouped.0.push(<$first_ty>::decode(payload)?),
                            $($position => grouped.$position.push(<$other_ty>::decode(payload)?),)+
                            _ => return Err(Error::incompatible("co-group input tag is out of range")),
                        }
                    }
                    (K::decode(encoded_key)?, grouped).encode()
                },
            };
            {
                let mut inner = collection.pipeline.inner.lock()
                    .map_err(|_| Error::graph(key, "builder lock poisoned"))?;
                inner.registry.cogroup_coders.insert(key.to_owned(), registration);
            }
            Ok(collection)
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
