use ironbeam::{Pipeline, from_vec};
use ironbeam_runner_contract_tests::{
    BackendUnderTest, ContractCase, FixtureCapabilities, Unsupported,
    assert_batch_rejects_unbounded, assert_bounded_case, validate_fixture,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct DirectV4 {
    starts: AtomicUsize,
}

impl BackendUnderTest for DirectV4 {
    fn validate(&self, case: ContractCase) -> Result<(), Unsupported> {
        validate_fixture(case, FixtureCapabilities::default())
    }

    fn run_bounded(&self) -> Vec<i32> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let pipeline = Pipeline::default();
        from_vec(&pipeline, vec![1, 2, 3, 4])
            .map(|n| n * 2)
            .filter(|n| n % 2 == 0)
            .collect_seq()
            .unwrap()
    }

    fn starts(&self) -> usize {
        self.starts.load(Ordering::SeqCst)
    }
}

#[test]
fn direct_v4_satisfies_bounded_case() {
    let backend = DirectV4::default();
    assert_bounded_case(&backend);
    assert_eq!(backend.starts(), 1);
}

#[test]
fn batch_adapter_rejects_unbounded_before_start() {
    assert_batch_rejects_unbounded(&DirectV4::default());
}

#[test]
fn fixture_allows_unbounded_only_with_declared_capability() {
    assert_eq!(
        validate_fixture(
            ContractCase::UnboundedWindowedSink,
            FixtureCapabilities {
                unbounded_source: true,
            },
        ),
        Ok(())
    );
}
