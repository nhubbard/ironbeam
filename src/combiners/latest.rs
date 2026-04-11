//! Latest combiner for selecting the most recent timestamped value.

use crate::RFBound;
use crate::collection::{CombineFn, LiftableCombiner};
use crate::window::Timestamped;
use std::marker::PhantomData;

/* ===================== Latest<T> ===================== */

/// Selects the value with the latest timestamp per key.
///
/// This combiner works with `Timestamped<T>` values and returns the element
/// with the largest (most recent) timestamp. When multiple values have the
/// same maximum timestamp, one is arbitrarily chosen.
///
/// - Accumulator: `Option<Timestamped<T>>`
/// - Output: `T`
///
/// # Requirements
///
/// Values must be wrapped in `Timestamped<T>` which carries a timestamp.
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// use ironbeam::*;
/// use ironbeam::combiners::Latest;
/// use ironbeam::window::Timestamped;
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
///
/// // Select latest value per key
/// let events = from_vec(&p, vec![
///     ("user1", Timestamped::new(100, "login")),
///     ("user1", Timestamped::new(200, "click")),
///     ("user2", Timestamped::new(150, "purchase")),
///     ("user1", Timestamped::new(180, "view")),
/// ]);
///
/// let latest = events.combine_values(Latest::new()).collect_seq_sorted()?;
/// assert_eq!(latest, vec![
///     ("user1", "click"),  // timestamp 200 is latest for user1
///     ("user2", "purchase")
/// ]);
///
/// // Select latest globally
/// let all_events = from_vec(&p, vec![
///     Timestamped::new(100, "event1"),
///     Timestamped::new(300, "event2"),
///     Timestamped::new(200, "event3"),
/// ]);
///
/// let global_latest = all_events.combine_globally(Latest::new(), None).collect_seq()?;
/// assert_eq!(global_latest, vec!["event2"]); // timestamp 300 is latest
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct Latest<T>(PhantomData<T>);

impl<T> Latest<T> {
    /// Creates a new `Latest` combiner.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> CombineFn<Timestamped<T>, Option<Timestamped<T>>, T> for Latest<T>
where
    T: RFBound,
{
    fn create(&self) -> Option<Timestamped<T>> {
        None
    }

    fn add_input(&self, acc: &mut Option<Timestamped<T>>, v: Timestamped<T>) {
        match acc {
            Some(current) => {
                if v.ts > current.ts {
                    *current = v;
                }
            }
            None => *acc = Some(v),
        }
    }

    fn merge(&self, acc: &mut Option<Timestamped<T>>, other: Option<Timestamped<T>>) {
        if let Some(other_val) = other {
            match acc {
                Some(current) => {
                    if other_val.ts > current.ts {
                        *current = other_val;
                    }
                }
                None => *acc = Some(other_val),
            }
        }
    }

    fn finish(&self, acc: Option<Timestamped<T>>) -> T {
        acc.expect("Latest::finish called on empty group").value
    }
}

impl<T> LiftableCombiner<Timestamped<T>, Option<Timestamped<T>>, T> for Latest<T>
where
    T: RFBound,
{
    fn build_from_group(&self, values: &[Timestamped<T>]) -> Option<Timestamped<T>> {
        values.iter().max_by_key(|v| v.ts).cloned()
    }
}
