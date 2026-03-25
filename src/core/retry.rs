use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

use tokio::time::sleep;
use tracing::warn;

use crate::core::config::RetryConfig;

pub async fn retry_async<T, E, Op, Fut, ShouldRetry>(
    config: &RetryConfig,
    operation: &str,
    mut op: Op,
    should_retry: ShouldRetry,
) -> Result<T, E>
where
    E: Display,
    Op: FnMut(usize) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    ShouldRetry: Fn(&E) -> bool,
{
    let attempts = config.attempts.max(1);
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

pub fn retry_sync<T, E, Op, ShouldRetry>(
    config: &RetryConfig,
    operation: &str,
    mut op: Op,
    should_retry: ShouldRetry,
) -> Result<T, E>
where
    E: Display,
    Op: FnMut(usize) -> Result<T, E>,
    ShouldRetry: Fn(&E) -> bool,
{
    let attempts = config.attempts.max(1);
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

fn backoff_delay(config: &RetryConfig, attempt: usize) -> Duration {
    let base = config.initial_backoff_ms.max(1);
    let max = config.max_backoff_ms.max(base);
    let multiplier = 1u64
        .checked_shl((attempt.saturating_sub(1)) as u32)
        .unwrap_or(u64::MAX);
    Duration::from_millis(base.saturating_mul(multiplier).min(max))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{retry_async, retry_sync};
    use crate::core::config::RetryConfig;

    #[tokio::test]
    async fn retry_async_retries_until_success() {
        let attempts = AtomicUsize::new(0);
        let config = RetryConfig {
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
        let config = RetryConfig {
            attempts: 4,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
        };

        let err = retry_sync(
            &config,
            "test sync",
            |_| {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>("fatal")
            },
            |_| false,
        )
        .unwrap_err();

        assert_eq!(err, "fatal");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
