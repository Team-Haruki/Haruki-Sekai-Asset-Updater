//! Sizing the work pools for the host that is actually running the export.

use super::schema::{ConcurrencyConfig, CpuResourceConfig, ResourcesConfig};

impl ConcurrencyConfig {
    pub fn effective(&self) -> Self {
        if !self.auto_tune {
            return self.clone();
        }
        self.effective_for_cpus(available_cpu_count())
    }

    pub fn effective_for_cpus(&self, cpus: usize) -> Self {
        let cpu_budget = ResourcesConfig::default()
            .cpu
            .effective_budget_for_cpus(cpus.max(1));
        self.effective_for_cpus_with_budget(cpus, cpu_budget)
    }

    /// Sizes each work pool for the host actually running the export.
    ///
    /// The configured value is a *floor*, not a target: each CPU-bound pool is
    /// raised to a core-count-derived width and only then clamped by that
    /// pool's cap. Before this scaled upward, the caps were the whole story, so
    /// a config tuned on a 10-core host kept its literal numbers on a 64-core
    /// one — measured on a 64-core EPYC 7B13, `audio_encode: 12` held the music
    /// rule at exactly 12 busy cores for 290 s that widening finished in 86 s.
    ///
    /// The widths below come from sweeps on that host (JP corpus, all other
    /// limits released), each stated as the point past which wall clock stopped
    /// improving:
    ///
    /// - `post_process` — the budget. This one is easy to get wrong: a sweep
    ///   with every *other* pool already at `2 x` the budget put its knee at
    ///   `cpus / 2`, but that only held while post-processing was not the
    ///   narrowest stage. Shipping `cpus / 2` here made it the narrowest, and
    ///   the music rule pinned at exactly 31.5 busy cores against a 32-wide
    ///   pool — `audio_encode` cannot be used above this number, because audio
    ///   work happens inside a bundle's post-process slot. Widening this alone
    ///   took that rule from 112 s to 74 s.
    /// - `audio_encode` — knee at three quarters of the budget; every setting
    ///   from there to `4 x` landed within run-to-run noise, so the budget
    ///   itself is used.
    /// - `video_encode` — kept at `cpus / 4` because each x264 instance is
    ///   itself about six threads. Wider still measured slightly faster, but
    ///   only by heavily oversubscribing the machine. Verified generous: the
    ///   movie rule reached 54 of 64 cores at this width.
    /// - `usm` — `cpus / 2`, not from a sweep of its own; it was never the
    ///   binding pool in any run.
    ///
    /// Network pools (`download`, `upload`) are deliberately left alone: their
    /// ceiling is the remote endpoint, not this host.
    ///
    /// On a narrow host every floor sits at or below the configured value, so
    /// the result is byte-for-byte what the caps alone produced.
    pub fn effective_for_cpus_with_budget(&self, cpus: usize, cpu_budget: usize) -> Self {
        if !self.auto_tune {
            return self.clone();
        }
        let cpus = cpus.max(1);
        let cpu_budget = cpu_budget.max(1);
        let cpu_oversubscribe = cpu_budget.saturating_mul(2).max(cpu_budget);
        let half_cpus = cpus.div_ceil(2);
        let quarter_cpus = cpus.div_ceil(4).max(1);
        Self {
            auto_tune: true,
            download: self.download.min(cpus.saturating_mul(4).max(4)).max(1),
            upload: self.upload.min(cpus.max(2)).max(1),
            post_process: if self.post_process == 0 {
                0
            } else {
                self.post_process
                    .max(cpu_budget)
                    .min(cpus.saturating_mul(2).max(2))
                    .max(1)
            },
            acb: self.acb.max(cpu_budget).min(cpu_oversubscribe).max(1),
            usm: self.usm.max(half_cpus).min(cpus.max(2)).max(1),
            hca: self
                .hca
                .max(cpu_budget)
                .min(cpus.saturating_mul(2).max(2))
                .min(cpu_oversubscribe)
                .max(1),
            media_encode: self
                .media_encode
                .max(cpu_budget)
                .min(cpu_oversubscribe)
                .max(1),
            audio_encode: self
                .audio_encode
                .max(cpu_budget)
                .min(cpu_oversubscribe)
                .max(1),
            // Floor and cap coincide here, so the configured value never
            // survives auto-tuning; leave `auto_tune` off to hand-hold x264 on
            // a memory-constrained host.
            video_encode: quarter_cpus,
            images: self.images.max(cpu_budget).min(cpu_oversubscribe).max(1),
        }
    }
}

impl CpuResourceConfig {
    pub fn effective_budget(&self) -> usize {
        self.effective_budget_for_cpus(available_cpu_count())
    }

    pub fn effective_budget_for_cpus(&self, cpus: usize) -> usize {
        let cpus = cpus.max(1);
        if !self.budget_auto {
            return cpus;
        }
        ((cpus as f64 * self.budget_ratio).floor() as usize)
            .saturating_sub(self.reserved)
            .max(1)
    }
}

pub(super) fn available_cpu_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
        .max(1)
}
