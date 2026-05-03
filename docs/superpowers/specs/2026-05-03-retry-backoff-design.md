# Retry & Backoff Design

**Date:** 2026-05-03
**Status:** Approved
**Scope:** Make the bot resilient to upstream Biotechware API outages without
spamming Telegram or hammering the upstream when it is sick.

## Problem

The current Rust implementation runs `tick()` on a fixed interval (180s checker,
21600s resumes) and on error simply logs and sleeps the same interval. During a
multi-hour upstream outage this produces a constant rate of failed requests
against a sick API and writes identical errors to logs, with no operator-visible
signal in Telegram. The Python originals have the same behavior. We want:

1. Short-lived blips (one bad request) to be invisible.
2. Sustained outages to back off so we stop hammering the upstream.
3. The operator to be told *once* when the bot is degraded and *once* when it
   recovers — never repeatedly.
4. Bad credentials and other permanent errors to surface fast, not be hidden by
   retry loops.

## Approach: two layers

### Layer A — per-request retry with classification

Wraps `fetch_page` and post-login `ensure_login`. Catches transient flakiness
without the operator ever seeing it.

- **Max attempts:** 3 (1 original + 2 retries).
- **Backoff:** exponential with jitter, base 500ms, cap 5s
  (~0.5s, ~1s, ~2s between attempts).
- **Classifier:**
  - `Transient` → retry: connect/read timeouts, connection-refused/reset,
    HTTP 5xx, HTTP 429.
  - `Auth` → re-login + 1 extra retry: HTTP 401, 403. The auth retry doesn't
    count against the 3-attempt budget; if it fails with another Transient we
    fall back to the normal retry loop.
  - `Permanent` → return immediately: HTTP 4xx (except 401/403/429),
    JSON decode errors, anything we can't classify.

### Layer B — per-tick outage tracker

Wraps the whole `tick()` call inside `checker::run` / `resumes::run`. Catches
sustained outages and prevents log spam.

- **Healthy interval:** unchanged (180s checker / 21600s resumes).
- **Backoff schedule on consecutive failure:**
  60s → 120s → 240s → 480s → 900s (cap).
- **Reset:** on first successful tick, the next delay returns to the healthy
  interval immediately.
- **Telegram alert threshold:** 3 consecutive failed ticks before sending the
  degraded notification.
- **Recovery alert:** sent on the first successful tick after a degraded state
  was announced. Never sent if no degraded alert was sent.

The two layers compose: Layer A returns `Err` only after exhausting retries, at
which point Layer B sees the failed tick and may either continue counting
toward the threshold or eventually announce the outage.

## Architecture

### New files

- **`src/retry.rs`** — Layer A primitives.
- **`src/tasks/outage.rs`** — Layer B state machine.

### Modified files

- **`src/biotech/mod.rs`** — `fetch_page` wraps the request in `with_retry`. A
  new `force_login()` bypasses the TTL check and is called as the auth-recovery
  callback.
- **`src/tasks/checker.rs`** / **`src/tasks/resumes.rs`** — replace fixed
  `sleep(interval)` with `sleep(tracker.next_delay())`; call
  `tracker.record_success(&telegram)` or `tracker.record_failure(&telegram)`
  after each tick.
- **`src/metrics.rs`** — add outage gauges and retry counter (see below).
- **`src/errors.rs`** — add a method that returns the HTTP status code for
  `BadStatus`, used by the classifier. (No new variants required; existing ones
  already carry enough information.)
- **`src/main.rs`** — declare `mod retry;`.
- **`src/tasks/mod.rs`** — declare `pub mod outage;`.

## Components

### `retry.rs`

```rust
pub enum Class { Transient, Auth, Permanent }

pub fn classify(e: &Error) -> Class;

pub async fn with_retry<F, Fut, T, A, AFut>(
    endpoint: &'static str,
    on_auth: A,
    mut op: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
    A: Fn() -> AFut,
    AFut: Future<Output = Result<()>>;
```

`with_retry` runs `op()`. On `Ok`, returns. On `Err`:

- `Permanent` → return the error.
- `Auth` → call `on_auth()` once (re-login). If `on_auth` itself errors with
  `Permanent`, return that. Then retry `op()` once. This auth-recovery retry
  is a one-shot that runs *in addition* to the transient budget; it does not
  consume an attempt.
- `Transient` → exponential-jittered sleep, retry, up to 3 total attempts.
  After exhaustion, return the last error.

Each attempt increments the metric
`retry_attempts_total{endpoint, outcome}` where outcome ∈
`{success, transient, auth, permanent, exhausted}`.

### `tasks/outage.rs`

```rust
pub struct Tracker {
    task: &'static str,
    consecutive_failures: u32,
    announced: bool,
    healthy_interval: Duration,
    schedule: &'static [Duration],   // [60, 120, 240, 480, 900] seconds
    alert_threshold: u32,            // 3
}

impl Tracker {
    pub fn new(task: &'static str, healthy_interval: Duration) -> Self;
    pub fn next_delay(&self) -> Duration;
    pub async fn record_success(&mut self, telegram: &TelegramBot);
    pub async fn record_failure(&mut self, telegram: &TelegramBot);
}
```

State transitions:

- `record_failure`:
  - `consecutive_failures += 1`
  - bump `consecutive_failures{task}` gauge
  - if `consecutive_failures == alert_threshold` and not `announced`:
    send `⚠️ API Biotechware non raggiungibile (task: {task}).` to Telegram,
    set `announced = true`, set `outage_state{task} = 1`.
- `record_success`:
  - bump `last_success_timestamp_seconds{task}` gauge
  - if `announced`: send `✅ API Biotechware ripristinata (task: {task}).`,
    set `announced = false`, set `outage_state{task} = 0`.
  - reset `consecutive_failures = 0`, gauge to 0.
- `next_delay`:
  - if `consecutive_failures == 0` → `healthy_interval`
  - else `schedule[min(consecutive_failures - 1, schedule.len() - 1)]`

Telegram alerts use `silent = false` so the operator sees them on the device.

## Metrics

New series, all carrying the existing `user` label and a `task` label:

- `biotech_outage_state{task} 0|1` (gauge)
- `biotech_consecutive_failures{task}` (gauge)
- `biotech_last_success_timestamp_seconds{task}` (gauge)
- `biotech_retry_attempts_total{endpoint, outcome}` (counter)

## Error classification details

The classifier inspects `errors::Error`:

- `Http(reqwest::Error)` →
  `Transient` if `is_timeout()` or `is_connect()` or no status,
  else delegate by status (`5xx | 429 → Transient`, `401 | 403 → Auth`,
  else `Permanent`).
- `BadStatus { status }` → same status-based mapping.
- `LoginFailed { status }` → `Auth` if `status == 401 || 403`, else `Permanent`.
  (Realistically a `LoginFailed` would have come from a specific status; this
  is a defensive default.)
- `Redis(_)` → never reaches the HTTP classifier (Redis ops live outside
  `with_retry`). If one ever does, classify as `Permanent` so we surface the
  bug rather than silently retrying.

## Testing

Unit tests inline (`#[cfg(test)]`):

- `retry::classify` — one assertion per variant.
- `retry::with_retry` —
  - succeeds on first attempt without sleep
  - retries 5xx and succeeds on second attempt
  - exhausts on persistent 503
  - returns immediately on 404
  - on 401: calls `on_auth` once and retries; second call success returns Ok
  - on 401 with `on_auth` returning Permanent: returns the auth error
- `outage::Tracker` —
  - `next_delay` returns healthy interval at consecutive=0
  - schedule values at consecutive=1..=5 and 6 (capped)
  - `record_failure` × 3 sends one degraded alert; ×4 does not send another
  - `record_success` after announced sends one recovery; second success without
    failure in between does not re-send
  - `record_success` resets `consecutive_failures` to 0

No integration tests against the live API.

## Out of scope

- Per-endpoint differentiated retry budgets (we use one budget for all biotech
  HTTP calls).
- Circuit breaker that fully halts the bot after N hours of outage.
- Cookie persistence to Redis (separate ticket).
- Rate-limiting upstream beyond the natural rate limit imposed by Layer B
  backoff.

## Implementation sequence

1. `retry.rs` with classifier + tests.
2. `tasks/outage.rs` with `Tracker` + tests.
3. Wire Layer A into `biotech/mod.rs` (`fetch_page`, `force_login`).
4. Wire Layer B into `tasks/checker.rs` and `tasks/resumes.rs`.
5. Add the new metrics to `metrics.rs`.
6. `cargo check` and `cargo test`.

Steps 3–4 are mechanical edit work and will be dispatched to Haiku subagents.
Steps 1–2 and the metrics additions stay on the main agent (test design and
state-machine logic).
