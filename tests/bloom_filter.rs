//! Tests for [`BloomFilter`]: the no-false-negatives guarantee, a bounded
//! false-positive rate, the empty filter, and hashable non-integer keys.

use ironbeam::bloom_filter::BloomFilter;

#[test]
fn inserted_keys_are_always_found() {
    let keys: Vec<u32> = (0..500).collect();
    let mut f = BloomFilter::new(keys.len());
    for k in &keys {
        f.insert(k);
    }
    for k in &keys {
        assert!(
            f.might_contain(k),
            "false negative for key {k} — Bloom filter must not produce false negatives"
        );
    }
}

#[test]
fn absent_keys_mostly_absent() {
    let mut f = BloomFilter::new(100);
    for i in 0u32..100 {
        f.insert(&i);
    }
    // Keys 1000..2000 were never inserted; FPR ≈ 1 %, so expect < 5 % false positives.
    let false_positives = (1000u32..2000).filter(|k| f.might_contain(k)).count();
    assert!(
        false_positives < 50,
        "FPR too high: {false_positives}/1000 false positives"
    );
}

#[test]
fn empty_filter_contains_nothing() {
    let f = BloomFilter::new(0);
    assert!(!f.might_contain(&42u32));
    assert!(!f.might_contain(&"hello"));
}

#[test]
fn string_keys_work() {
    let mut f = BloomFilter::new(10);
    f.insert(&"apple");
    f.insert(&"banana");
    assert!(f.might_contain(&"apple"));
    assert!(f.might_contain(&"banana"));
    assert!(!f.might_contain(&"cherry"));
}
