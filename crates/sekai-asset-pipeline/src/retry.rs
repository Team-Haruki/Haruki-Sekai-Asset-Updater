use std::fmt::Display;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::sleep;
use tracing::warn;

use crate::RetryOptions;

pub trait RetryPolicy {
    fn attempts(&self) -> usize;
    fn initial_backoff_ms(&self) -> u64;
    fn max_backoff_ms(&self) -> u64;
}

impl RetryPolicy for RetryOptions {
    fn attempts(&self) -> usize {
        self.attempts
    }

    fn initial_backoff_ms(&self) -> u64 {
        self.initial_backoff_ms
    }

    fn max_backoff_ms(&self) -> u64 {
        self.max_backoff_ms
    }
}

pub async fn retry_async<T, E, Op, Fut, ShouldRetry, Policy>(
    config: &Policy,
    operation: &str,
    mut op: Op,
    should_retry: ShouldRetry,
) -> Result<T, E>
where
    E: Display,
    Op: FnMut(usize) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    ShouldRetry: Fn(&E) -> bool,
    Policy: RetryPolicy,
{
    let attempts = config.attempts().max(1);
    for attempt in 1..=attempts {
        match op(attempt).await {
            Ok(value) => return Ok(value),
            Err(err) if attempt < attempts && should_retry(&err) => {
                let delay = backoff_delay(config, attempt);
                warn!(
                    operation,
                    attempt,
                    max_attempts = attempts,
                    delay_ms = delay.as_millis(),
                    error = %err,
                    "operation failed, retrying"
                );
                sleep(delay).await;
            }
            Err(err) => return Err(err),
        }
    }

    unreachable!("retry_async must return from within the attempt loop")
}

pub fn retry_sync<T, E, Op, ShouldRetry, Policy>(
    config: &Policy,
    operation: &str,
    mut op: Op,
    should_retry: ShouldRetry,
) -> Result<T, E>
where
    E: Display,
    Op: FnMut(usize) -> Result<T, E>,
    ShouldRetry: Fn(&E) -> bool,
    Policy: RetryPolicy,
{
    let attempts = config.attempts().max(1);
    for attempt in 1..=attempts {
        match op(attempt) {
            Ok(value) => return Ok(value),
            Err(err) if attempt < attempts && should_retry(&err) => {
                let delay = backoff_delay(config, attempt);
                warn!(
                    operation,
                    attempt,
                    max_attempts = attempts,
                    delay_ms = delay.as_millis(),
                    error = %err,
                    "operation failed, retrying"
                );
                std::thread::sleep(delay);
            }
            Err(err) => return Err(err),
        }
    }

    unreachable!("retry_sync must return from within the attempt loop")
}

fn backoff_delay(config: &impl RetryPolicy, attempt: usize) -> Duration {
    let base = config.initial_backoff_ms().max(1);
    let max = config.max_backoff_ms().max(base);
    let multiplier = 1u64
        .checked_shl((attempt.saturating_sub(1)) as u32)
        .unwrap_or(u64::MAX);
    let capped = base.saturating_mul(multiplier).min(max);
    let half = capped / 2;
    let jitter = if half > 0 {
        jitter_noise() % (half + 1)
    } else {
        0
    };
    Duration::from_millis(half.saturating_add(jitter))
}

fn jitter_noise() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos() as u64)
        .unwrap_or(0);
    let mut value = COUNTER
        .fetch_add(1, Ordering::Relaxed)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ nanos;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{retry_async, retry_sync};
    use crate::RetryOptions;

    #[tokio::test]
    async fn retry_async_retries_until_success() {
        let attempts = AtomicUsize::new(0);
        let config = RetryOptions {
            attempts: 3,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        };

        let result = retry_async(
            &config,
            "test async",
            |_| async {
                let current = attempts.fetch_add(1, Ordering::SeqCst);
                if current < 2 {
                    Err("try again")
                } else {
                    Ok("ok")
                }
            },
            |_| true,
        )
        .await
        .unwrap();

        assert_eq!(result, "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn retry_sync_stops_on_non_retryable_error() {
        let attempts = AtomicUsize::new(0);
        let config = RetryOptions {
            attempts: 4,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        };

        let error = retry_sync(
            &config,
            "test sync",
            |_| {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>("fatal")
            },
            |_| false,
        )
        .unwrap_err();

        assert_eq!(error, "fatal");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
