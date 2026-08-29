pub use sekai_asset_pipeline::{retry_async, retry_sync, RetryPolicy};

use crate::core::config::RetryConfig;

impl RetryPolicy for RetryConfig {
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{retry_async, retry_sync};
    use crate::core::config::RetryConfig;

    #[tokio::test]
    async fn retry_async_accepts_the_application_retry_config() {
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
    fn retry_sync_accepts_the_application_retry_config() {
        let attempts = AtomicUsize::new(0);
        let config = RetryConfig {
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
