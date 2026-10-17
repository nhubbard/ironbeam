// Lower a validated keyed user-state declaration into Beam ParDo fields.

use beam_proto::pipeline_v1::{
    state_spec, time_domain, BagStateSpec, CombiningStateSpec, MapStateSpec, MultimapStateSpec,
    OrderedListStateSpec, ReadModifyWriteStateSpec, SetStateSpec, StateSpec, TimerFamilySpec,
};
use ironbeam_core::{StatefulParDoContract, UserStateKind, UserTimerDomain};

/// Add the state and timer declarations of a versioned function to a `ParDo`.
///
/// The caller must provide executable function registration; this function
/// constructs the wire contract and its pipeline requirements, but does not
/// make user state executable on a runner.
///
/// # Errors
///
/// Returns an error if the declaration is invalid or the payload already has
/// user-state or timer fields.
pub fn attach_stateful_contract(
    payload: &mut ParDoPayload,
    requirements: &mut Vec<String>,
    contract: &StatefulParDoContract,
) -> Result<(), V5Error> {
    contract.validate()?;
    if !payload.state_specs.is_empty()
        || !payload.timer_family_specs.is_empty()
        || !payload.on_window_expiration_timer_family_spec.is_empty()
    {
        return Err(V5Error::Wire("ParDo already has user state or timer declarations".into()));
    }
    let mut state_specs = std::collections::HashMap::new();
    for (id, kind) in &contract.states {
        let spec = match kind {
            UserStateKind::Value { coder_id } => {
                state_spec::Spec::ReadModifyWriteSpec(ReadModifyWriteStateSpec {
                    coder_id: coder_id.clone(),
                })
            }
            UserStateKind::Bag { element_coder_id } => {
                state_spec::Spec::BagSpec(BagStateSpec {
                    element_coder_id: element_coder_id.clone(),
                })
            }
            UserStateKind::Set { element_coder_id } => {
                state_spec::Spec::SetSpec(SetStateSpec {
                    element_coder_id: element_coder_id.clone(),
                })
            }
            UserStateKind::OrderedList { element_coder_id } => {
                state_spec::Spec::OrderedListSpec(OrderedListStateSpec {
                    element_coder_id: element_coder_id.clone(),
                })
            }
            UserStateKind::Combining {
                accumulator_coder_id,
                combine_semantic_id,
                combine_version,
            } => state_spec::Spec::CombiningSpec(CombiningStateSpec {
                accumulator_coder_id: accumulator_coder_id.clone(),
                combine_fn: Some(FunctionSpec {
                    urn: combine_semantic_id.clone(),
                    payload: serde_json::to_vec(combine_version)
                        .map_err(|error| V5Error::Wire(error.to_string()))?,
                }),
            }),
            UserStateKind::Map { key_coder_id, value_coder_id } => {
                state_spec::Spec::MapSpec(MapStateSpec {
                    key_coder_id: key_coder_id.clone(),
                    value_coder_id: value_coder_id.clone(),
                })
            }
            UserStateKind::Multimap { key_coder_id, value_coder_id } => {
                state_spec::Spec::MultimapSpec(MultimapStateSpec {
                    key_coder_id: key_coder_id.clone(),
                    value_coder_id: value_coder_id.clone(),
                })
            }
        };
        state_specs.insert(
            id.clone(),
            StateSpec {
                spec: Some(spec),
                protocol: Some(FunctionSpec {
                    urn: kind.protocol_urn().into(),
                    payload: Vec::new(),
                }),
            },
        );
    }
    let mut timer_family_specs = std::collections::HashMap::new();
    for (id, timer) in &contract.timers {
        let time_domain = match timer.domain {
            UserTimerDomain::EventTime => time_domain::Enum::EventTime,
            UserTimerDomain::ProcessingTime => time_domain::Enum::ProcessingTime,
        };
        timer_family_specs.insert(
            id.clone(),
            TimerFamilySpec {
                time_domain: time_domain as i32,
                timer_family_coder_id: timer.coder_id.clone(),
            },
        );
    }
    payload.state_specs = state_specs;
    payload.timer_family_specs = timer_family_specs;
    payload.on_window_expiration_timer_family_spec = contract
        .on_window_expiration_timer
        .clone()
        .unwrap_or_default();
    append_requirements(requirements, contract);
    Ok(())
}

fn append_requirements(requirements: &mut Vec<String>, contract: &StatefulParDoContract) {
    for requirement in [
        Some(urns::REQUIREMENT_STATEFUL),
        contract
            .on_window_expiration_timer
            .as_ref()
            .map(|_| urns::REQUIREMENT_ON_WINDOW_EXPIRATION),
    ]
    .into_iter()
    .flatten()
    {
        if !requirements.iter().any(|existing| existing == requirement) {
            requirements.push(requirement.into());
        }
    }
}
