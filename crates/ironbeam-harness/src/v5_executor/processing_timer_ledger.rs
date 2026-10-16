use std::collections::BTreeSet;

use ironbeam_core::{StatefulParDoContract, UserTimerDomain};

const PROCESSING_TIMER_LEDGER_STATE_ID: &str = "v5_processing_timer_ledger";

#[derive(Default, serde::Deserialize, serde::Serialize)]
struct ProcessingTimerLedger {
    version: u8,
    active: BTreeSet<(String, String)>,
}

impl ProcessingTimerLedger {
    fn decode(encoded: &[u8]) -> Result<Self> {
        let Some(bytes) = V5BundleExecutor::decode_single_value_cell(encoded)? else {
            return Ok(Self {
                version: 1,
                ..Self::default()
            });
        };
        let ledger: Self = serde_json::from_slice(bytes)?;
        if ledger.version != 1 {
            bail!("unsupported processing timer ledger version");
        }
        Ok(ledger)
    }

    fn mutation(&self) -> Result<ValueStateMutation> {
        if self.active.is_empty() {
            return Ok(ValueStateMutation::Clear);
        }
        let bytes = serde_json::to_vec(self)?;
        let mut encoded = Vec::new();
        encode_length_prefix(&bytes, &mut encoded)?;
        Ok(ValueStateMutation::Set(encoded))
    }

    fn contains(&self, firing: &TimerFiring) -> bool {
        self.active.contains(&(
            firing.identity.family_id.clone(),
            firing.identity.dynamic_tag.clone(),
        ))
    }

    fn apply(
        &mut self,
        contract: &StatefulParDoContract,
        fired: Option<&TimerFiring>,
        mutations: &[TimerMutation],
    ) -> Result<()> {
        if let Some(firing) = fired.filter(|firing| firing.domain == UserTimerDomain::ProcessingTime) {
            self.active.remove(&(
                firing.identity.family_id.clone(),
                firing.identity.dynamic_tag.clone(),
            ));
        }
        for mutation in mutations {
            let identity = match mutation {
                TimerMutation::Set(identity, _) | TimerMutation::Clear(identity) => identity,
            };
            let domain = contract
                .timers
                .get(&identity.family_id)
                .ok_or_else(|| anyhow!("callback used an undeclared timer family"))?
                .domain;
            if domain != UserTimerDomain::ProcessingTime {
                continue;
            }
            let slot = (identity.family_id.clone(), identity.dynamic_tag.clone());
            match mutation {
                TimerMutation::Set(..) => {
                    self.active.insert(slot);
                }
                TimerMutation::Clear(..) => {
                    self.active.remove(&slot);
                }
            }
        }
        Ok(())
    }
}

impl V5BundleExecutor {
    async fn processing_timer_is_armed(
        access: &UserStateAccess<'_>,
        firing: &TimerFiring,
    ) -> Result<bool> {
        let encoded = access.read_bag(PROCESSING_TIMER_LEDGER_STATE_ID).await?;
        Ok(ProcessingTimerLedger::decode(&encoded)?.contains(firing))
    }

    async fn update_processing_timer_ledger(
        access: &UserStateAccess<'_>,
        contract: &StatefulParDoContract,
        fired: Option<&TimerFiring>,
        mutations: &[TimerMutation],
    ) -> Result<()> {
        if !contract.states.contains_key(PROCESSING_TIMER_LEDGER_STATE_ID) {
            return Ok(());
        }
        access
            .apply_value_state(PROCESSING_TIMER_LEDGER_STATE_ID, |encoded| {
                let mut ledger = ProcessingTimerLedger::decode(encoded)?;
                ledger.apply(contract, fired, mutations)?;
                Ok(((), ledger.mutation()?))
            })
            .await
    }
}
