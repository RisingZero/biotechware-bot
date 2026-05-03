use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::sleep;
use tracing::{debug, warn};

use crate::errors::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Transient,
    Auth,
    Permanent,
}

#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub base: Duration,
    pub cap: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base: Duration::from_millis(500),
            cap: Duration::from_secs(5),
        }
    }
}

pub fn classify(e: &Error) -> Class {
    if let Error::Http(http) = e {
        if http.is_timeout() || http.is_connect() {
            return Class::Transient;
        }
    }
    if e.is_transport_error() {
        return Class::Transient;
    }
    match e.http_status() {
        Some(401) | Some(403) => Class::Auth,
        Some(429) => Class::Transient,
        Some(s) if (500..600).contains(&s) => Class::Transient,
        Some(_) => Class::Permanent,
        None => match e {
            Error::Redis(_) => Class::Permanent,
            _ => Class::Permanent,
        },
    }
}

/// Run `op` with retry semantics defined by `cfg`. On a classified `Auth`
/// error, `on_auth` is invoked exactly once to recover (typically a re-login),
/// then `op` is retried once outside the transient budget. Transient errors
/// are retried with jittered exponential backoff up to `cfg.max_attempts`
/// total attempts. Permanent errors are returned immediately.
pub async fn with_retry<F, Fut, T, A, AFut>(
    cfg: RetryConfig,
    endpoint: &'static str,
    on_auth: A,
    mut op: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
    A: Fn() -> AFut,
    AFut: Future<Output = Result<()>>,
{
    let mut attempt: u32 = 0;
    let mut auth_recovered = false;
    loop {
        attempt += 1;
        match op().await {
            Ok(v) => {
                record_outcome(endpoint, "success");
                return Ok(v);
            }
            Err(e) => {
                let class = classify(&e);
                match class {
                    Class::Permanent => {
                        record_outcome(endpoint, "permanent");
                        return Err(e);
                    }
                    Class::Auth => {
                        record_outcome(endpoint, "auth");
                        if auth_recovered {
                            // Already tried re-login once; treat as permanent
                            // to prevent infinite re-login loops.
                            return Err(e);
                        }
                        warn!(endpoint, error = %e, "auth error, attempting recovery");
                        if let Err(auth_err) = on_auth().await {
                            // If recovery itself fails permanently, surface it.
                            if classify(&auth_err) == Class::Permanent {
                                return Err(auth_err);
                            }
                            // Otherwise let the loop see this as a fresh
                            // transient and back off.
                            return Err(auth_err);
                        }
                        auth_recovered = true;
                        // Don't consume attempt budget; retry immediately.
                        attempt -= 1;
                        continue;
                    }
                    Class::Transient => {
                        if attempt >= cfg.max_attempts {
                            record_outcome(endpoint, "exhausted");
                            warn!(
                                endpoint,
                                attempts = attempt,
                                error = %e,
                                "retries exhausted"
                            );
                            return Err(e);
                        }
                        record_outcome(endpoint, "transient");
                        let delay = backoff_delay(cfg.base, cfg.cap, attempt);
                        debug!(
                            endpoint,
                            attempt,
                            delay_ms = delay.as_millis() as u64,
                            error = %e,
                            "transient error, retrying"
                        );
                        sleep(delay).await;
                    }
                }
            }
        }
    }
}

fn record_outcome(endpoint: &'static str, outcome: &'static str) {
    if let Some(m) = crate::metrics::try_get() {
        m.retry_attempts_total
            .with_label_values(&[endpoint, outcome])
            .inc();
    }
}

/// Exponential backoff with full jitter: delay in [0, min(cap, base * 2^(n-1))].
fn backoff_delay(base: Duration, cap: Duration, attempt: u32) -> Duration {
    if base.is_zero() {
        return Duration::ZERO;
    }
    let exp = attempt.saturating_sub(1).min(16);
    let raw = base.saturating_mul(1u32 << exp);
    let bounded = raw.min(cap);
    let nanos = bounded.as_nanos() as u64;
    if nanos == 0 {
        return Duration::ZERO;
    }
    let r = pseudo_random_u64();
    Duration::from_nanos(r % nanos.max(1))
}

/// Tiny non-cryptographic PRNG seeded from the system clock. Adequate for
/// jitter; we don't pull in a `rand` crate just for this.
fn pseudo_random_u64() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1);
    // splitmix64
    let mut x = nanos.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn cfg_no_sleep(max: u32) -> RetryConfig {
        RetryConfig {
            max_attempts: max,
            base: Duration::ZERO,
            cap: Duration::ZERO,
        }
    }

    fn bad(status: u16) -> Error {
        Error::BadStatus {
            endpoint: "test",
            status,
        }
    }

    #[test]
    fn classify_5xx_transient() {
        assert_eq!(classify(&bad(500)), Class::Transient);
        assert_eq!(classify(&bad(502)), Class::Transient);
        assert_eq!(classify(&bad(599)), Class::Transient);
    }

    #[test]
    fn classify_429_transient() {
        assert_eq!(classify(&bad(429)), Class::Transient);
    }

    #[test]
    fn classify_auth() {
        assert_eq!(classify(&bad(401)), Class::Auth);
        assert_eq!(classify(&bad(403)), Class::Auth);
        assert_eq!(
            classify(&Error::LoginFailed { status: 401 }),
            Class::Auth
        );
    }

    #[test]
    fn classify_other_4xx_permanent() {
        assert_eq!(classify(&bad(400)), Class::Permanent);
        assert_eq!(classify(&bad(404)), Class::Permanent);
        assert_eq!(classify(&bad(418)), Class::Permanent);
    }

    #[tokio::test]
    async fn succeeds_first_attempt() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let result: Result<u32> = with_retry(
            cfg_no_sleep(3),
            "test",
            || async { Ok(()) },
            move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(42)
                }
            },
        )
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_5xx_then_succeeds() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let result: Result<u32> = with_retry(
            cfg_no_sleep(3),
            "test",
            || async { Ok(()) },
            move || {
                let c = c.clone();
                async move {
                    let n = c.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        Err(bad(503))
                    } else {
                        Ok(7)
                    }
                }
            },
        )
        .await;
        assert_eq!(result.unwrap(), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn exhausts_on_persistent_5xx() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let result: Result<u32> = with_retry(
            cfg_no_sleep(3),
            "test",
            || async { Ok(()) },
            move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err(bad(503))
                }
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn returns_immediately_on_permanent() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let result: Result<u32> = with_retry(
            cfg_no_sleep(3),
            "test",
            || async { Ok(()) },
            move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err(bad(404))
                }
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn auth_triggers_recovery_then_succeeds() {
        let op_calls = Arc::new(AtomicUsize::new(0));
        let auth_calls = Arc::new(AtomicUsize::new(0));
        let oc = op_calls.clone();
        let ac = auth_calls.clone();
        let result: Result<u32> = with_retry(
            cfg_no_sleep(3),
            "test",
            move || {
                let ac = ac.clone();
                async move {
                    ac.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            move || {
                let oc = oc.clone();
                async move {
                    let n = oc.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        Err(bad(401))
                    } else {
                        Ok(99)
                    }
                }
            },
        )
        .await;
        assert_eq!(result.unwrap(), 99);
        assert_eq!(op_calls.load(Ordering::SeqCst), 2);
        assert_eq!(auth_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn auth_with_failing_recovery_returns_error() {
        let op_calls = Arc::new(AtomicUsize::new(0));
        let oc = op_calls.clone();
        let result: Result<u32> = with_retry(
            cfg_no_sleep(3),
            "test",
            || async { Err(bad(403)) }, // recovery itself permanent
            move || {
                let oc = oc.clone();
                async move {
                    oc.fetch_add(1, Ordering::SeqCst);
                    Err(bad(401))
                }
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(op_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn auth_does_not_loop_forever() {
        // Op keeps returning 401 even after recovery. Should fail after one
        // retry post-recovery, not loop.
        let op_calls = Arc::new(AtomicUsize::new(0));
        let oc = op_calls.clone();
        let result: Result<u32> = with_retry(
            cfg_no_sleep(3),
            "test",
            || async { Ok(()) },
            move || {
                let oc = oc.clone();
                async move {
                    oc.fetch_add(1, Ordering::SeqCst);
                    Err(bad(401))
                }
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(op_calls.load(Ordering::SeqCst), 2);
    }
}
