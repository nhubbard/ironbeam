use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_runner_contract_tests::{
    BackendUnderTest, ContractCase, FixtureCapabilities, Unsupported, assert_bounded_case,
    validate_fixture,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct DirectV5 {
    starts: AtomicUsize,
}

impl BackendUnderTest for DirectV5 {
    fn validate(&self, case: ContractCase) -> Result<(), Unsupported> {
        validate_fixture(
            case,
            FixtureCapabilities {
                unbounded_source: true,
            },
        )
    }

    fn run_bounded(&self) -> Vec<i32> {
        let pipeline = Pipeline::default();
        let output = pipeline
            .from_vec("input", vec![1_i32, 2, 3, 4])
            .unwrap()
            .map_named("double/v1", |value| value * 2)
            .unwrap()
            .filter_named("divisible-by-four/v1", |value| value % 4 == 0)
            .unwrap()
            .flat_map_named("duplicate-plus-one/v1", |value| vec![value, value + 1])
            .unwrap()
            .output("result")
            .unwrap();
        self.starts.fetch_add(1, Ordering::SeqCst);
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0
    }

    fn starts(&self) -> usize {
        self.starts.load(Ordering::SeqCst)
    }
}

#[test]
fn direct_v5_matches_the_published_v4_bounded_contract() {
    let backend = DirectV5::default();
    assert_bounded_case(&backend);
    assert_eq!(backend.starts(), 1);
}

#[test]
fn direct_v5_declares_the_unbounded_source_capability() {
    let backend = DirectV5::default();
    assert_eq!(
        backend.validate(ContractCase::UnboundedWindowedSink),
        Ok(())
    );
    assert_eq!(backend.starts(), 0);
}
