//! Runner-owned keyed timers shared by Direct execution and checkpoint recovery.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{Error, PaneInfo, StatefulParDoContract, UserTimerDomain, WindowInterval};

/// One timer slot. Setting the same slot replaces its former deadline.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct TimerIdentity {
    pub family_id: String,
    pub dynamic_tag: String,
    pub encoded_key: Vec<u8>,
    pub window: Option<WindowInterval>,
}

/// The fire time and output timestamp of a set timer, in nanoseconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TimerSchedule {
    pub fire_timestamp_ns: i64,
    pub hold_timestamp_ns: i64,
    pub pane: PaneInfo,
}

/// Timer identity and schedule delivered to a stateful callback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimerFiring {
    pub identity: TimerIdentity,
    pub schedule: TimerSchedule,
    pub domain: UserTimerDomain,
}

/// A callback's requested timer change. Mutations are applied together after
/// the callback succeeds, in the order returned by the callback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TimerMutation {
    Set(TimerIdentity, TimerSchedule),
    Clear(TimerIdentity),
}

/// Coded timer slots owned by a runner, independent of user state cells.
pub struct TimerQueue {
    contract: StatefulParDoContract,
    global_window: bool,
    timers: HashMap<TimerIdentity, TimerSchedule>,
}

#[derive(Serialize, Deserialize)]
struct TimerSnapshot {
    version: u8,
    contract: StatefulParDoContract,
    global_window: bool,
    entries: Vec<(TimerIdentity, TimerSchedule)>,
}

impl TimerQueue {
    /// Create a queue for the timer families declared by one stateful function.
    pub fn new(contract: StatefulParDoContract, global_window: bool) -> Result<Self, Error> {
        contract.validate()?;
        Ok(Self {
            contract,
            global_window,
            timers: HashMap::new(),
        })
    }

    fn validate_identity(&self, identity: &TimerIdentity) -> Result<UserTimerDomain, Error> {
        let family = self
            .contract
            .timers
            .get(&identity.family_id)
            .ok_or_else(|| {
                Error::recovery(&self.contract.semantic_id, "undeclared timer family")
            })?;
        if identity.window.is_some() == self.global_window
            || identity
                .window
                .is_some_and(|window| window.end_ns <= window.start_ns)
        {
            return Err(Error::recovery(
                &self.contract.semantic_id,
                "invalid timer window",
            ));
        }
        Ok(family.domain)
    }

    fn validate_schedule(
        &self,
        identity: &TimerIdentity,
        schedule: TimerSchedule,
    ) -> Result<(), Error> {
        self.validate_identity(identity)?;
        if schedule.fire_timestamp_ns % 1_000_000 != 0
            || schedule.hold_timestamp_ns % 1_000_000 != 0
        {
            return Err(Error::recovery(
                &self.contract.semantic_id,
                "timer timestamps require whole milliseconds",
            ));
        }
        schedule.pane.validate()?;
        Ok(())
    }

    /// Set or replace one timer. Beam timer wire timestamps have millisecond precision.
    pub fn set(&mut self, identity: TimerIdentity, schedule: TimerSchedule) -> Result<(), Error> {
        self.validate_schedule(&identity, schedule)?;
        self.timers.insert(identity, schedule);
        Ok(())
    }

    /// Clear one timer slot, including a slot that is not currently armed.
    pub fn clear(&mut self, identity: &TimerIdentity) -> Result<(), Error> {
        self.validate_identity(identity)?;
        self.timers.remove(identity);
        Ok(())
    }

    /// Validate a callback's complete timer batch before changing any slot.
    /// Later mutations to one slot take precedence over earlier ones.
    pub fn apply_mutations(
        &mut self,
        mutations: impl IntoIterator<Item = TimerMutation>,
    ) -> Result<(), Error> {
        let mutations = mutations.into_iter().collect::<Vec<_>>();
        for mutation in &mutations {
            match mutation {
                TimerMutation::Set(identity, schedule) => {
                    self.validate_schedule(identity, *schedule)?;
                }
                TimerMutation::Clear(identity) => {
                    self.validate_identity(identity)?;
                }
            }
        }
        for mutation in mutations {
            match mutation {
                TimerMutation::Set(identity, schedule) => {
                    self.timers.insert(identity, schedule);
                }
                TimerMutation::Clear(identity) => {
                    self.timers.remove(&identity);
                }
            }
        }
        Ok(())
    }

    /// Remove ready timers in deterministic deadline order. A missing clock
    /// leaves timers in that domain armed.
    pub fn take_ready(
        &mut self,
        event_watermark_ns: Option<i64>,
        processing_time_ns: Option<i64>,
    ) -> Vec<TimerFiring> {
        let mut ready = self
            .timers
            .iter()
            .filter_map(|(identity, schedule)| {
                let domain = self.contract.timers[&identity.family_id].domain;
                let clock = match domain {
                    UserTimerDomain::EventTime => event_watermark_ns,
                    UserTimerDomain::ProcessingTime => processing_time_ns,
                };
                (clock.is_some_and(|now| {
                    if self.contract.on_window_expiration_timer.as_deref()
                        == Some(identity.family_id.as_str())
                    {
                        schedule.fire_timestamp_ns < now
                    } else {
                        schedule.fire_timestamp_ns <= now
                    }
                }))
                .then(|| TimerFiring {
                    identity: identity.clone(),
                    schedule: *schedule,
                    domain,
                })
            })
            .collect::<Vec<_>>();
        ready.sort_by(|left, right| {
            (
                left.schedule.fire_timestamp_ns,
                &left.identity.family_id,
                &left.identity.encoded_key,
                left.identity
                    .window
                    .map(|window| (window.start_ns, window.end_ns)),
                &left.identity.dynamic_tag,
            )
                .cmp(&(
                    right.schedule.fire_timestamp_ns,
                    &right.identity.family_id,
                    &right.identity.encoded_key,
                    right
                        .identity
                        .window
                        .map(|window| (window.start_ns, window.end_ns)),
                    &right.identity.dynamic_tag,
                ))
        });
        for firing in &ready {
            self.timers.remove(&firing.identity);
        }
        ready
    }

    /// Earliest armed deadline in one clock domain. Direct uses the
    /// processing-time deadline to wake even when its source is idle.
    #[must_use]
    pub fn next_deadline(&self, domain: UserTimerDomain) -> Option<i64> {
        self.timers
            .iter()
            .filter(|(identity, _)| self.contract.timers[&identity.family_id].domain == domain)
            .map(|(_, schedule)| schedule.fire_timestamp_ns)
            .min()
    }

    /// Return the number of armed timer slots.
    #[must_use]
    pub fn len(&self) -> usize {
        self.timers.len()
    }

    /// Return whether no timer slots are armed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.timers.is_empty()
    }

    /// Snapshot timer slots in a stable order for an atomic runner checkpoint.
    pub fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        let mut entries = self
            .timers
            .iter()
            .map(|(identity, schedule)| (identity.clone(), *schedule))
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            (
                &left.0.family_id,
                &left.0.encoded_key,
                left.0.window.map(|window| (window.start_ns, window.end_ns)),
                &left.0.dynamic_tag,
            )
                .cmp(&(
                    &right.0.family_id,
                    &right.0.encoded_key,
                    right
                        .0
                        .window
                        .map(|window| (window.start_ns, window.end_ns)),
                    &right.0.dynamic_tag,
                ))
        });
        serde_json::to_vec(&TimerSnapshot {
            version: 1,
            contract: self.contract.clone(),
            global_window: self.global_window,
            entries,
        })
        .map_err(|error| Error::recovery(&self.contract.semantic_id, error.to_string()))
    }

    /// Restore a complete snapshot, validating each key with the declared
    /// key coder and leaving the current queue intact on error.
    pub fn restore(
        &mut self,
        bytes: &[u8],
        validate_key: impl Fn(&[u8]) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let snapshot: TimerSnapshot = serde_json::from_slice(bytes)
            .map_err(|error| Error::recovery(&self.contract.semantic_id, error.to_string()))?;
        if snapshot.version != 1
            || snapshot.contract != self.contract
            || snapshot.global_window != self.global_window
        {
            return Err(Error::incompatible("timer checkpoint contract differs"));
        }
        let mut restored = Self::new(self.contract.clone(), self.global_window)?;
        for (identity, schedule) in snapshot.entries {
            if restored.timers.contains_key(&identity) {
                return Err(Error::recovery(
                    &self.contract.semantic_id,
                    "duplicate timer slot",
                ));
            }
            validate_key(&identity.encoded_key)?;
            restored.set(identity, schedule)?;
        }
        self.timers = restored.timers;
        Ok(())
    }
}
