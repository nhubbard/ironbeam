use ironbeam_core::{Element, ErrorKind, PaneInfo, PaneTiming};

#[test]
fn pane_metadata_has_a_stable_element_coder() {
    let panes = [
        PaneInfo::NO_FIRING,
        PaneInfo::FIRST_LAST_ON_TIME,
        PaneInfo {
            is_first: false,
            is_last: false,
            timing: PaneTiming::Early,
            index: 2,
            nonspeculative_index: -1,
            contains_element_metadata: true,
        },
        PaneInfo {
            is_first: false,
            is_last: true,
            timing: PaneTiming::Late,
            index: 4,
            nonspeculative_index: 3,
            contains_element_metadata: false,
        },
    ];
    assert_eq!(PaneInfo::fixed_encoded_len(), Some(18));
    for pane in panes {
        let bytes = pane.encode().unwrap();
        assert_eq!(bytes.len(), 18);
        assert_eq!(PaneInfo::decode(&bytes).unwrap(), pane);
    }
}

#[test]
fn pane_metadata_coder_rejects_invalid_indices_and_bytes() {
    let invalid = PaneInfo {
        index: 1,
        ..PaneInfo::NO_FIRING
    };
    assert_eq!(invalid.encode().unwrap_err().kind, ErrorKind::Incompatible);
    assert_eq!(
        PaneInfo::decode(&[0]).unwrap_err().kind,
        ErrorKind::Incompatible
    );
    let mut bytes = PaneInfo::NO_FIRING.encode().unwrap();
    bytes[1] = 4;
    assert_eq!(
        PaneInfo::decode(&bytes).unwrap_err().kind,
        ErrorKind::Incompatible
    );
}
