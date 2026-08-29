//! Phase timings, recorded as post-processing works through a bundle.

use std::collections::HashMap;
use std::time::Instant;

pub(super) fn record_phase_ms(target: &mut HashMap<String, u64>, phase: &str, started: Instant) {
    target.insert(
        phase.to_string(),
        started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    );
}

pub(super) fn add_elapsed_phase_ms(
    target: &mut HashMap<String, u64>,
    phase: &str,
    started: Instant,
) {
    add_phase_ms(
        target,
        phase,
        started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    );
}

pub(super) fn add_phase_ms(target: &mut HashMap<String, u64>, phase: &str, elapsed_ms: u64) {
    *target.entry(phase.to_string()).or_default() += elapsed_ms;
}

pub(super) fn merge_raw_phase_ms(target: &mut HashMap<String, u64>, source: &HashMap<String, u64>) {
    for (key, value) in source {
        *target.entry(key.clone()).or_default() += *value;
    }
}
