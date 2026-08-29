//! Bounding how many media encodes run at once.
//!
//! Audio and video encoders are the CPU-heaviest work this service does, and
//! each holds its slot for as long as the encode takes.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Instant;

use crate::ExportPipelineError;

use super::super::limits::{acquire_cpu_budget_permit_blocking, CpuBudgetPermit};
use super::record_max_phase_ms;
use super::timing::add_phase_ms;

pub(super) struct MediaEncodeLimiter {
    pub(super) max: usize,
    pub(super) state: Mutex<usize>,
    pub(super) available: Condvar,
}

pub(super) type MediaEncodeLimiterKey = (MediaEncodeKind, usize);

pub(super) type MediaEncodeLimiterMap = HashMap<MediaEncodeLimiterKey, Arc<MediaEncodeLimiter>>;

pub(crate) struct MediaEncodePermit {
    pub(super) limiter: Arc<MediaEncodeLimiter>,
}

impl Drop for MediaEncodePermit {
    fn drop(&mut self) {
        let mut active = self.limiter.state.lock().unwrap();
        *active = active.saturating_sub(1);
        self.limiter.available.notify_one();
    }
}

pub(crate) struct MediaEncodeAcquire {
    pub(crate) permit: MediaEncodePermit,
    pub(crate) cpu_permit: CpuBudgetPermit,
    pub(crate) kind: MediaEncodeKind,
    pub(crate) wait_ms: u64,
    pub(crate) cpu_budget_wait_ms: u64,
    pub(crate) active: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum MediaEncodeKind {
    Audio,
    Video,
}

impl MediaEncodeKind {
    fn as_metric_prefix(self) -> &'static str {
        match self {
            Self::Audio => "audio_encode",
            Self::Video => "video_encode",
        }
    }
}

pub(crate) fn acquire_media_encode_permit(
    kind: MediaEncodeKind,
    concurrency: usize,
    cpu_budget: usize,
) -> Result<MediaEncodeAcquire, ExportPipelineError> {
    let limiter = media_encode_limiter(kind, concurrency);
    let wait_started = Instant::now();
    let mut active = limiter.state.lock().unwrap();
    while *active >= limiter.max {
        active = limiter.available.wait(active).unwrap();
    }
    *active += 1;
    let active_count = *active;
    drop(active);
    let cpu_slot = acquire_cpu_budget_permit_blocking(cpu_budget)?;
    Ok(MediaEncodeAcquire {
        permit: MediaEncodePermit { limiter },
        cpu_permit: cpu_slot.permit,
        kind,
        wait_ms: wait_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        cpu_budget_wait_ms: cpu_slot.wait_ms,
        active: active_count,
    })
}

pub(super) async fn acquire_media_encode_permit_async(
    kind: MediaEncodeKind,
    concurrency: usize,
    cpu_budget: usize,
) -> Result<MediaEncodeAcquire, ExportPipelineError> {
    tokio::task::spawn_blocking(move || acquire_media_encode_permit(kind, concurrency, cpu_budget))
        .await
        .map_err(|source| ExportPipelineError::WorkerPanic {
            worker: format!("{} limiter", kind.as_metric_prefix()),
            message: source.to_string(),
        })?
}

pub(super) fn media_encode_limiter(
    kind: MediaEncodeKind,
    concurrency: usize,
) -> Arc<MediaEncodeLimiter> {
    let concurrency = concurrency.max(1);
    static LIMITERS: OnceLock<Mutex<MediaEncodeLimiterMap>> = OnceLock::new();
    let limiters = LIMITERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut limiters = limiters.lock().unwrap();
    limiters
        .entry((kind, concurrency))
        .or_insert_with(|| {
            Arc::new(MediaEncodeLimiter {
                max: concurrency,
                state: Mutex::new(0),
                available: Condvar::new(),
            })
        })
        .clone()
}

pub(super) fn record_hca_media_encode_acquire(
    phase_ms: &mut HashMap<String, u64>,
    encode_slot: &MediaEncodeAcquire,
) {
    debug_assert_eq!(encode_slot.kind, MediaEncodeKind::Audio);
    add_phase_ms(
        phase_ms,
        "post_process.hca.audio_pool_wait",
        encode_slot.wait_ms,
    );
    add_phase_ms(
        phase_ms,
        "media_scheduler.audio_encode_wait",
        encode_slot.wait_ms,
    );
    record_max_phase_ms(
        phase_ms,
        "media_scheduler.audio_encode_active_peak",
        encode_slot.active as u64,
    );
    add_phase_ms(
        phase_ms,
        // Compatibility metric for older bench readers.
        "media_scheduler.media_encode_wait",
        encode_slot.wait_ms,
    );
    record_max_phase_ms(
        phase_ms,
        "media_scheduler.media_encode_active_peak",
        encode_slot.active as u64,
    );
    add_phase_ms(
        phase_ms,
        "media_scheduler.cpu_budget_wait",
        encode_slot.cpu_budget_wait_ms,
    );
    add_phase_ms(phase_ms, "cpu_budget.wait", encode_slot.cpu_budget_wait_ms);
}

pub(super) fn record_usm_video_encode_acquire(
    phase_ms: &mut HashMap<String, u64>,
    encode_slot: &MediaEncodeAcquire,
) {
    debug_assert_eq!(encode_slot.kind, MediaEncodeKind::Video);
    add_phase_ms(
        phase_ms,
        "post_process.usm.video_pool_wait",
        encode_slot.wait_ms,
    );
    add_phase_ms(
        phase_ms,
        "media_scheduler.video_encode_wait",
        encode_slot.wait_ms,
    );
    record_max_phase_ms(
        phase_ms,
        "media_scheduler.video_encode_active_peak",
        encode_slot.active as u64,
    );
    add_phase_ms(
        phase_ms,
        "media_scheduler.cpu_budget_wait",
        encode_slot.cpu_budget_wait_ms,
    );
    add_phase_ms(phase_ms, "cpu_budget.wait", encode_slot.cpu_budget_wait_ms);
}
