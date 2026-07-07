use super::*;

pub(super) struct CpuBudgetAcquire {
    pub(super) permit: CpuBudgetPermit,
    pub(super) wait_ms: u64,
}

pub(super) async fn acquire_cpu_budget_permit(
    cpu_budget: usize,
) -> Result<CpuBudgetAcquire, ExportPipelineError> {
    tokio::task::spawn_blocking(move || acquire_cpu_budget_permit_blocking(cpu_budget))
        .await
        .map_err(|source| ExportPipelineError::WorkerPanic {
            worker: "CPU budget limiter".to_string(),
            message: source.to_string(),
        })?
}

pub(super) fn acquire_cpu_budget_permit_blocking(
    cpu_budget: usize,
) -> Result<CpuBudgetAcquire, ExportPipelineError> {
    let limiter = cpu_budget_limiter(cpu_budget);
    let wait_started = Instant::now();
    if !cpu_budget_hard_cap_enabled() {
        wait_for_process_cpu_throttle()?;
        return Ok(CpuBudgetAcquire {
            permit: CpuBudgetPermit {
                limiter,
                active: false,
            },
            wait_ms: wait_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        });
    }
    let mut active = limiter.state.lock().unwrap();
    while *active >= limiter.max {
        active = limiter.available.wait(active).unwrap();
    }
    drop(active);
    wait_for_process_cpu_throttle()?;
    active = limiter.state.lock().unwrap();
    while *active >= limiter.max {
        active = limiter.available.wait(active).unwrap();
    }
    *active += 1;
    drop(active);
    Ok(CpuBudgetAcquire {
        permit: CpuBudgetPermit {
            limiter,
            active: true,
        },
        wait_ms: wait_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    })
}

pub(super) struct CpuBudgetLimiter {
    max: usize,
    state: Mutex<usize>,
    available: Condvar,
}

pub(super) struct CpuBudgetPermit {
    limiter: Arc<CpuBudgetLimiter>,
    active: bool,
}

impl Drop for CpuBudgetPermit {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut active = self.limiter.state.lock().unwrap();
        *active = active.saturating_sub(1);
        self.limiter.available.notify_one();
    }
}

pub(super) fn cpu_budget_limiter(cpu_budget: usize) -> Arc<CpuBudgetLimiter> {
    let cpu_budget = cpu_budget.max(1);
    static LIMITERS: OnceLock<Mutex<HashMap<usize, Arc<CpuBudgetLimiter>>>> = OnceLock::new();
    let mut limiters = LIMITERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    limiters
        .entry(cpu_budget)
        .or_insert_with(|| {
            Arc::new(CpuBudgetLimiter {
                max: cpu_budget,
                state: Mutex::new(0),
                available: Condvar::new(),
            })
        })
        .clone()
}

#[derive(Debug, Clone)]
pub(super) struct CpuThrottleSettings {
    enabled: bool,
    target_percent: f64,
    sample_ms: u64,
}

impl Default for CpuThrottleSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            target_percent: f64::INFINITY,
            sample_ms: 250,
        }
    }
}

#[derive(Debug)]
pub(super) struct CpuThrottleState {
    settings: CpuThrottleSettings,
    last_sample: Option<Instant>,
    last_process_cpu_percent: f64,
    #[cfg(target_os = "linux")]
    last_process_cpu_ticks: Option<u64>,
}

impl Default for CpuThrottleState {
    fn default() -> Self {
        Self {
            settings: CpuThrottleSettings::default(),
            last_sample: None,
            last_process_cpu_percent: 0.0,
            #[cfg(target_os = "linux")]
            last_process_cpu_ticks: None,
        }
    }
}

pub(super) fn configure_cpu_budget_throttle(resources: &ResourcesConfig, cpu_budget: usize) {
    let state = cpu_throttle_state();
    let mut state = state.lock().unwrap();
    state.settings = CpuThrottleSettings {
        enabled: resources.cpu.throttle.enabled,
        target_percent: (cpu_budget.max(1) * 100) as f64,
        sample_ms: resources.cpu.throttle.sample_ms.max(1),
    };
}

pub(super) fn cpu_throttle_state() -> &'static Mutex<CpuThrottleState> {
    static STATE: OnceLock<Mutex<CpuThrottleState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(CpuThrottleState::default()))
}

pub(super) fn cpu_budget_hard_cap_enabled() -> bool {
    let state = cpu_throttle_state().lock().unwrap();
    !state.settings.enabled
}

pub(super) fn wait_for_process_cpu_throttle() -> Result<(), ExportPipelineError> {
    loop {
        let settings = {
            let state = cpu_throttle_state().lock().unwrap();
            state.settings.clone()
        };
        if !settings.enabled {
            return Ok(());
        }

        let process_cpu_percent = sample_process_tree_cpu_percent(&settings)?;
        if process_cpu_percent < settings.target_percent.max(1.0) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(settings.sample_ms.max(1)));
    }
}

pub(super) fn sample_process_tree_cpu_percent(
    settings: &CpuThrottleSettings,
) -> Result<f64, ExportPipelineError> {
    let state = cpu_throttle_state();
    let mut state = state.lock().unwrap();
    let now = Instant::now();
    let sample_interval = Duration::from_millis(settings.sample_ms.max(1));
    if state
        .last_sample
        .is_some_and(|last_sample| now.duration_since(last_sample) < sample_interval)
    {
        return Ok(state.last_process_cpu_percent);
    }

    #[cfg(target_os = "linux")]
    {
        if let Ok(ticks) = current_process_tree_cpu_ticks() {
            let sampled = match (state.last_sample, state.last_process_cpu_ticks) {
                (Some(last_sample), Some(last_ticks)) => {
                    let elapsed = now.duration_since(last_sample).as_secs_f64().max(0.001);
                    let delta_ticks = ticks.saturating_sub(last_ticks) as f64;
                    delta_ticks / linux_clock_ticks_per_second() as f64 / elapsed * 100.0
                }
                _ => 0.0,
            };
            state.last_sample = Some(now);
            state.last_process_cpu_ticks = Some(ticks);
            state.last_process_cpu_percent = sampled;
            return Ok(sampled);
        }
    }

    let sampled = current_process_tree_cpu_percent()?;
    state.last_sample = Some(now);
    state.last_process_cpu_percent = sampled;
    Ok(sampled)
}

pub(super) fn current_process_tree_cpu_percent() -> Result<f64, ExportPipelineError> {
    #[cfg(unix)]
    {
        let output = StdCommand::new("ps")
            .args(["-axo", "pid=,ppid=,pcpu="])
            .output()
            .map_err(|source| ExportPipelineError::Spawn {
                program: "ps".to_string(),
                source,
            })?;
        if !output.status.success() {
            return Err(ExportPipelineError::CommandFailed {
                program: "ps".to_string(),
                status: output.status.to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        Ok(sum_process_tree_cpu_percent(
            std::process::id(),
            &String::from_utf8_lossy(&output.stdout),
        ))
    }

    #[cfg(not(unix))]
    {
        Ok(0.0)
    }
}

#[cfg(target_os = "linux")]
pub(super) fn current_process_tree_cpu_ticks() -> Result<u64, ExportPipelineError> {
    let root_pid = std::process::id();
    let mut rows = Vec::new();
    for entry in std::fs::read_dir("/proc").map_err(|source| ExportPipelineError::Io {
        path: PathBuf::from("/proc"),
        source,
    })? {
        let entry = entry.map_err(|source| ExportPipelineError::Io {
            path: PathBuf::from("/proc"),
            source,
        })?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let stat_path = entry.path().join("stat");
        let Ok(stat) = std::fs::read_to_string(&stat_path) else {
            continue;
        };
        if let Some((ppid, ticks)) = parse_linux_proc_stat(&stat) {
            rows.push((pid, ppid, ticks));
        }
    }
    Ok(sum_process_tree_cpu_ticks(root_pid, &rows))
}

#[cfg(target_os = "linux")]
fn parse_linux_proc_stat(stat: &str) -> Option<(u32, u64)> {
    let after_name = stat.rsplit_once(") ")?.1;
    let fields: Vec<&str> = after_name.split_whitespace().collect();
    let ppid = fields.get(1)?.parse::<u32>().ok()?;
    let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
    let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
    Some((ppid, user_ticks.saturating_add(system_ticks)))
}

#[cfg(target_os = "linux")]
fn sum_process_tree_cpu_ticks(root_pid: u32, rows: &[(u32, u32, u64)]) -> u64 {
    let mut stack = vec![root_pid];
    let mut seen = std::collections::HashSet::new();
    let mut total: u64 = 0;
    while let Some(pid) = stack.pop() {
        if !seen.insert(pid) {
            continue;
        }
        for (row_pid, row_ppid, ticks) in rows {
            if *row_pid == pid {
                total = total.saturating_add(*ticks);
            }
            if *row_ppid == pid {
                stack.push(*row_pid);
            }
        }
    }
    total
}

#[cfg(target_os = "linux")]
fn linux_clock_ticks_per_second() -> u64 {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks > 0 {
        ticks as u64
    } else {
        100
    }
}

#[cfg(unix)]
pub(super) fn sum_process_tree_cpu_percent(root_pid: u32, ps_output: &str) -> f64 {
    let mut rows = Vec::new();
    for line in ps_output.lines() {
        let mut fields = line.split_whitespace();
        let Some(pid) = fields.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        let Some(ppid) = fields.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        let Some(cpu_percent) = fields.next().and_then(|value| value.parse::<f64>().ok()) else {
            continue;
        };
        rows.push((pid, ppid, cpu_percent));
    }

    let mut stack = vec![root_pid];
    let mut seen = std::collections::HashSet::new();
    let mut total = 0.0;
    while let Some(pid) = stack.pop() {
        if !seen.insert(pid) {
            continue;
        }
        for (row_pid, row_ppid, cpu_percent) in &rows {
            if *row_pid == pid {
                total += cpu_percent;
            }
            if *row_ppid == pid {
                stack.push(*row_pid);
            }
        }
    }
    total
}

#[cfg(all(test, target_os = "linux"))]
mod limits_tests {
    use super::*;

    #[test]
    fn parses_linux_proc_stat_with_spaces_in_process_name() {
        let stat = "1234 (name with spaces) S 42 1 1 0 -1 4194304 10 0 0 0 25 7 0 0 20 0 1 0 1";
        assert_eq!(parse_linux_proc_stat(stat), Some((42, 32)));
    }

    #[test]
    fn sums_linux_process_tree_ticks() {
        let rows = vec![
            (10, 1, 3),
            (11, 10, 5),
            (12, 10, 7),
            (13, 11, 11),
            (14, 99, 13),
        ];
        assert_eq!(sum_process_tree_cpu_ticks(10, &rows), 26);
    }
}

pub(super) async fn native_process_recovery_lock() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<TokioMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| TokioMutex::new(())).lock().await
}
