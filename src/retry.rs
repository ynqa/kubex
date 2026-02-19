use std::{future::Future, num::NonZeroUsize, time::Duration};

use kube::Error as KubeError;
use tokio::time::sleep;

mod api;
pub use api::ApiRetryExt;

/// Retry attempt limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryLimit {
    /// Retry without an attempt cap.
    Unlimited,
    /// Retry up to the specified number of attempts.
    Finite(NonZeroUsize),
}

/// Retry policy used by [`ApiRetryExt`].
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Maximum number of attempts including the first call.
    pub max_attempts: RetryLimit,
    /// Initial wait duration before the next retry.
    pub initial_backoff: Duration,
    /// Upper bound for exponential backoff wait.
    pub max_backoff: Duration,
    /// Multiplication factor for exponential backoff.
    pub backoff_multiplier: f64,
    /// Error classifier for retry decisions.
    pub is_retryable: fn(&KubeError) -> bool,
}

impl RetryPolicy {
    pub fn with_max_attempts(mut self, max_attempts: NonZeroUsize) -> Self {
        self.max_attempts = RetryLimit::Finite(max_attempts);
        self
    }

    pub fn with_unlimited_attempts(mut self) -> Self {
        self.max_attempts = RetryLimit::Unlimited;
        self
    }

    pub fn with_initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.initial_backoff = initial_backoff;
        self
    }

    pub fn with_max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    pub fn with_backoff_multiplier(mut self, backoff_multiplier: f64) -> Self {
        self.backoff_multiplier = backoff_multiplier.max(1.0);
        self
    }

    pub fn with_retryable(mut self, is_retryable: fn(&KubeError) -> bool) -> Self {
        self.is_retryable = is_retryable;
        self
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: RetryLimit::Finite(NonZeroUsize::new(5).unwrap()),
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(5),
            backoff_multiplier: 2.0,
            is_retryable: default_retryable_error,
        }
    }
}

/// Default retry condition for [`kube::Error`].
///
/// For `Api` errors, retries only on transient HTTP status codes (`408`, `429`, `5xx`).
/// For other error types (transport/serialization/etc.), retries are enabled by default.
pub fn default_retryable_error(error: &KubeError) -> bool {
    match error {
        KubeError::Api(response) => matches!(response.code, 408 | 429 | 500..=599),
        _ => true,
    }
}

fn next_backoff(current: Duration, policy: &RetryPolicy) -> Duration {
    current
        .mul_f64(policy.backoff_multiplier.max(1.0))
        .min(policy.max_backoff)
}

/// Retry utility for [`kube::Error`].
pub async fn retry_with_policy<T, F, Fut>(
    policy: &RetryPolicy,
    mut operation: F,
) -> Result<T, KubeError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, KubeError>>,
{
    let max_attempts = policy.max_attempts;
    let mut backoff = policy.initial_backoff.min(policy.max_backoff);
    let mut attempts = 0usize;

    loop {
        attempts = attempts.saturating_add(1);
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let exhausted = match max_attempts {
                    RetryLimit::Unlimited => false,
                    RetryLimit::Finite(max_attempts) => attempts >= max_attempts.get(),
                };
                if exhausted || !(policy.is_retryable)(&error) {
                    return Err(error);
                }
                sleep(backoff).await;
                backoff = next_backoff(backoff, policy);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use kube::{Error as KubeError, core::Status};

    use super::{RetryPolicy, default_retryable_error, retry_with_policy};

    fn max_attempts(attempts: usize) -> NonZeroUsize {
        NonZeroUsize::new(attempts).expect("max attempts must be > 0")
    }

    fn api_error(code: u16) -> KubeError {
        KubeError::Api(
            Status::failure(&format!("status={code}"), "Test")
                .with_code(code)
                .boxed(),
        )
    }

    #[tokio::test]
    async fn retries_until_success() {
        let policy = RetryPolicy::default()
            .with_max_attempts(max_attempts(5))
            .with_initial_backoff(Duration::ZERO)
            .with_max_backoff(Duration::ZERO);

        let mut attempts = 0usize;
        let result = retry_with_policy(&policy, || {
            attempts += 1;
            let current = attempts;
            async move {
                if current < 3 {
                    Err(api_error(500))
                } else {
                    Ok(current)
                }
            }
        })
        .await
        .expect("retry should eventually succeed");

        assert_eq!(result, 3);
        assert_eq!(attempts, 3);
    }

    #[tokio::test]
    async fn retries_indefinitely_with_unlimited_limit() {
        let policy = RetryPolicy::default()
            .with_max_attempts(max_attempts(2))
            .with_unlimited_attempts()
            .with_initial_backoff(Duration::ZERO)
            .with_max_backoff(Duration::ZERO);

        let mut attempts = 0usize;
        let result = retry_with_policy(&policy, || {
            attempts += 1;
            let current = attempts;
            async move {
                if current < 7 {
                    Err(api_error(503))
                } else {
                    Ok(current)
                }
            }
        })
        .await
        .expect("unlimited retry should eventually succeed");

        assert_eq!(result, 7);
        assert_eq!(attempts, 7);
    }

    #[tokio::test]
    async fn stops_on_non_retryable_api_error() {
        let policy = RetryPolicy::default()
            .with_max_attempts(max_attempts(5))
            .with_initial_backoff(Duration::ZERO)
            .with_max_backoff(Duration::ZERO);

        let mut attempts = 0usize;
        let err = retry_with_policy::<(), _, _>(&policy, || {
            attempts += 1;
            async { Err(api_error(404)) }
        })
        .await
        .expect_err("404 should not be retried");

        match err {
            KubeError::Api(response) => assert_eq!(response.code, 404),
            _ => panic!("expected api error"),
        }
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn exhausts_attempts_on_retryable_error() {
        let policy = RetryPolicy::default()
            .with_max_attempts(max_attempts(3))
            .with_initial_backoff(Duration::ZERO)
            .with_max_backoff(Duration::ZERO);

        let mut attempts = 0usize;
        let err = retry_with_policy::<(), _, _>(&policy, || {
            attempts += 1;
            async { Err(api_error(503)) }
        })
        .await
        .expect_err("retryable error should eventually exhaust attempts");

        match err {
            KubeError::Api(response) => assert_eq!(response.code, 503),
            _ => panic!("expected api error"),
        }
        assert_eq!(attempts, 3);
    }

    #[test]
    fn default_retryable_classifies_api_codes() {
        assert!(default_retryable_error(&api_error(408)));
        assert!(default_retryable_error(&api_error(429)));
        assert!(default_retryable_error(&api_error(500)));
        assert!(!default_retryable_error(&api_error(404)));
    }
}
