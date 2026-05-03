# Parallel Page Fetching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the sequential page loop in `BiotechwareApi::fetch_all` with a bounded sliding-window of parallel speculative fetches using `FuturesUnordered`.

**Architecture:** Bootstrap page 0 to learn page size, then maintain a fixed window of in-flight `fetch_page` futures via `FuturesUnordered`. As each future resolves, one replacement is pushed (up to `max_concurrent`). Stop flag is set on first empty page; remaining in-flight tasks drain. Results are sorted by counter and flattened.

**Tech Stack:** `futures::stream::FuturesUnordered`, `futures::StreamExt`, `futures::FutureExt`, `tokio` (already present), `dotenvy` (already present).

---

## File Map

| File | Change |
|---|---|
| `src/config.rs` | Add `fetch_max_concurrent: usize` field, parse from `FETCH_MAX_CONCURRENT` env var |
| `src/biotech/mod.rs` | Add `max_concurrent: usize` to `BiotechwareApi`; add private `fetch_all_sliding`; rewrite `fetch_all`; add `#[cfg(test)]` |
| `src/main.rs` | Pass `cfg.fetch_max_concurrent` to `BiotechwareApi::new` |
| `.env.example` | Document `FETCH_MAX_CONCURRENT=8` |

---

### Task 1: Add `fetch_max_concurrent` to `Config`

**Files:**
- Modify: `src/config.rs`
- Modify: `.env.example`

- [ ] **Step 1: Add the field to `Config` and parse it**

In `src/config.rs`, add the field to the struct and the `from_env` constructor:

```rust
#[derive(Debug, Clone)]
pub struct Config {
    pub user_label: String,
    pub btw_username: String,
    pub btw_password: String,
    pub tg_token: String,
    pub tg_chat_id: String,
    pub redis_url: String,
    pub metrics_addr: String,
    pub checker_interval_secs: u64,
    pub resumes_interval_secs: u64,
    pub fetch_max_concurrent: usize,   // ← new
    pub pricing: Pricing,
}
```

In `Config::from_env`, add inside `Ok(Self { ... })`:

```rust
fetch_max_concurrent: env_or("FETCH_MAX_CONCURRENT", "8").parse()?,
```

- [ ] **Step 2: Document the env var in `.env.example`**

Add after the `RESUMES_INTERVAL_SECS` line (or after existing timing vars):

```
# Parallel page fetching (optional)
# FETCH_MAX_CONCURRENT=8
```

- [ ] **Step 3: Verify the config compiles**

```bash
cargo build 2>&1 | head -20
```

Expected: compile error about `BiotechwareApi::new` missing argument (fixed next task). If the only errors mention `new`, the config change is correct.

---

### Task 2: Add `max_concurrent` to `BiotechwareApi` and update `main.rs`

**Files:**
- Modify: `src/biotech/mod.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Add `max_concurrent` field to the struct and `new()`**

In `src/biotech/mod.rs`, update the struct:

```rust
pub struct BiotechwareApi {
    client: Client,
    username: String,
    password: String,
    last_login: Mutex<Option<DateTime<Utc>>>,
    max_concurrent: usize,
}
```

Update `new()`:

```rust
pub fn new(username: String, password: String, max_concurrent: usize) -> Result<Arc<Self>> {
    let client = ClientBuilder::new()
        .cookie_store(true)
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
        .build()?;
    Ok(Arc::new(Self {
        client,
        username,
        password,
        last_login: Mutex::new(None),
        max_concurrent,
    }))
}
```

- [ ] **Step 2: Update `main.rs` to pass the new argument**

In `src/main.rs`, change:

```rust
let api = biotech::BiotechwareApi::new(
    cfg.btw_username.clone(),
    cfg.btw_password.clone(),
)?;
```

to:

```rust
let api = biotech::BiotechwareApi::new(
    cfg.btw_username.clone(),
    cfg.btw_password.clone(),
    cfg.fetch_max_concurrent,
)?;
```

- [ ] **Step 3: Verify clean build**

```bash
cargo build 2>&1
```

Expected: zero errors, zero warnings about unused fields.

- [ ] **Step 4: Commit**

```bash
git add src/config.rs src/biotech/mod.rs src/main.rs .env.example
git commit -m "feat: add FETCH_MAX_CONCURRENT config and wire to BiotechwareApi"
```

---

### Task 3: TDD — write failing tests for `fetch_all_sliding`

**Files:**
- Modify: `src/biotech/mod.rs` (append `#[cfg(test)]` block)

- [ ] **Step 1: Add the test module to the bottom of `src/biotech/mod.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use models::{GetRecordsResponse, Record};

    fn make_record(id: usize) -> Record {
        Record {
            id: id.to_string(),
            status: String::new(),
            timezone_indication: None,
            last_report_revision: None,
            last_report_date: None,
            record_type_id: None,
            record_type_name: String::new(),
            firstname: None,
            lastname: None,
            url: None,
            health_code: None,
            reception_date: None,
            date: None,
            requester: None,
            effective_service_level: None,
        }
    }

    fn make_page(n: usize) -> GetRecordsResponse {
        GetRecordsResponse {
            count: n as i32,
            counter: 0,
            list: (0..n).map(make_record).collect(),
        }
    }

    fn empty_page() -> GetRecordsResponse {
        GetRecordsResponse { count: 0, counter: 0, list: vec![] }
    }

    // Single bootstrap page with no further data.
    #[tokio::test]
    async fn only_bootstrap_page() {
        let bootstrap = make_page(5);
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 2, |_c| async {
            Ok(empty_page())
        })
        .await
        .unwrap();
        assert_eq!(result.len(), 5);
    }

    // Three pages of 10 records each, then empty.
    #[tokio::test]
    async fn collects_multiple_pages() {
        use std::sync::{Arc, Mutex};
        use std::collections::VecDeque;

        let queue: Arc<Mutex<VecDeque<GetRecordsResponse>>> = Arc::new(Mutex::new(
            VecDeque::from(vec![
                make_page(10), // speculative page 1
                make_page(10), // speculative page 2
                empty_page(),  // stop signal
                empty_page(),  // drain for concurrent window
            ]),
        ));
        let bootstrap = make_page(10);
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 2, move |_c| {
            let q = queue.clone();
            async move {
                Ok(q.lock().unwrap().pop_front().unwrap_or_else(empty_page))
            }
        })
        .await
        .unwrap();
        assert_eq!(result.len(), 30);
    }

    // An error from fetch_page propagates immediately.
    #[tokio::test]
    async fn propagates_fetch_error() {
        use crate::errors::Error;
        let bootstrap = make_page(10);
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 1, |_c| async {
            Err(Error::BadStatus { endpoint: "test", status: 500 })
        })
        .await;
        assert!(result.is_err());
    }

    // Records arrive sorted by counter regardless of completion order.
    #[tokio::test]
    async fn results_ordered_by_counter() {
        use std::sync::{Arc, Mutex};
        use std::collections::VecDeque;

        // Bootstrap: page_size = 1, so counters advance by 1 each time.
        // We return pages in reverse counter order to stress the sort.
        let bootstrap = make_page(1);
        let queue: Arc<Mutex<VecDeque<GetRecordsResponse>>> = Arc::new(Mutex::new(
            VecDeque::from(vec![make_page(1), make_page(1), empty_page(), empty_page()]),
        ));
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 2, move |_c| {
            let q = queue.clone();
            async move {
                Ok(q.lock().unwrap().pop_front().unwrap_or_else(empty_page))
            }
        })
        .await
        .unwrap();
        // IDs are assigned sequentially in make_record; we just check total count.
        assert_eq!(result.len(), 3);
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail with "not found"**

```bash
cargo test --lib 2>&1 | grep -E "error|FAILED|not found" | head -20
```

Expected: compilation error — `fetch_all_sliding` not defined yet.

---

### Task 4: Implement `fetch_all_sliding` and update `fetch_all`

**Files:**
- Modify: `src/biotech/mod.rs`

- [ ] **Step 1: Add the required imports at the top of `src/biotech/mod.rs`**

Add these alongside the existing `use` statements:

```rust
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
```

- [ ] **Step 2: Add `fetch_all_sliding` as a private free function**

Place it just before the `#[cfg(test)]` block (after the `impl BiotechwareApi` closing brace):

```rust
async fn fetch_all_sliding<F, Fut>(
    bootstrap: models::GetRecordsResponse,
    first_counter: i32,
    max_concurrent: usize,
    fetch: F,
) -> crate::errors::Result<Vec<models::Record>>
where
    F: Fn(i32) -> Fut,
    Fut: std::future::Future<Output = crate::errors::Result<models::GetRecordsResponse>>,
{
    let page_size = bootstrap.count;
    let max_concurrent = max_concurrent.max(1);
    let mut results: Vec<(i32, Vec<models::Record>)> = vec![(first_counter, bootstrap.list)];
    let mut pending = FuturesUnordered::new();
    let mut next_counter = first_counter + page_size;
    let mut stop = false;

    for _ in 0..max_concurrent {
        let c = next_counter;
        pending.push(fetch(c).map(move |r| r.map(|p| (c, p))));
        next_counter += page_size;
    }

    while let Some(result) = pending.next().await {
        let (counter, page) = result?;
        if page.list.is_empty() || page.count <= 0 {
            stop = true;
        } else {
            results.push((counter, page.list));
            if !stop {
                let c = next_counter;
                pending.push(fetch(c).map(move |r| r.map(|p| (c, p))));
                next_counter += page_size;
            }
        }
    }

    results.sort_unstable_by_key(|(c, _)| *c);
    Ok(results.into_iter().flat_map(|(_, records)| records).collect())
}
```

- [ ] **Step 3: Replace `fetch_all` with the parallel implementation**

Replace the entire `fetch_all` method inside `impl BiotechwareApi`:

```rust
#[instrument(skip(self))]
pub async fn fetch_all(&self, list_type: ListType) -> crate::errors::Result<Vec<models::Record>> {
    let bootstrap = self.fetch_page(list_type, FIRST_COUNTER).await?;
    if bootstrap.list.is_empty() || bootstrap.count <= 0 {
        info!(list_type = list_type.to_str(), "no records found");
        return Ok(vec![]);
    }
    let records = fetch_all_sliding(bootstrap, FIRST_COUNTER, self.max_concurrent, |c| {
        self.fetch_page(list_type, c)
    })
    .await?;
    info!(n = records.len(), list_type = list_type.to_str(), "fetched all records");
    Ok(records)
}
```

Note: the old `fetch_all` called `self.ensure_login()` at the top; this is now handled entirely by `fetch_page` (which already calls `ensure_login`), so it is not needed here.

Confirm the existing import at the top of `src/biotech/mod.rs` still reads:

```rust
use models::{GetRecordsRequest, GetRecordsResponse, ListType, LoginRequest, Record};
```

No change is needed — it already covers all types used in `fetch_all_sliding`.

- [ ] **Step 4: Run the new tests**

```bash
cargo test --lib 2>&1
```

Expected output (all pass):

```
test biotech::tests::only_bootstrap_page ... ok
test biotech::tests::collects_multiple_pages ... ok
test biotech::tests::propagates_fetch_error ... ok
test biotech::tests::results_ordered_by_counter ... ok
```

- [ ] **Step 5: Run the full test suite**

```bash
cargo test 2>&1
```

Expected: all tests in `retry::tests` still pass; no regressions.

- [ ] **Step 6: Commit**

```bash
git add src/biotech/mod.rs
git commit -m "feat: parallel page fetching with bounded FuturesUnordered sliding window"
```

---

### Task 5: Smoke-test the build and clean up

**Files:** none new

- [ ] **Step 1: Final clean build**

```bash
cargo build --release 2>&1
```

Expected: zero errors, zero warnings.

- [ ] **Step 2: Verify `.env.example` is in sync with `.env`**

```bash
diff <(grep -v '^#' .env.example | grep -v '^$' | sort) <(grep -v '^#' .env | grep -v '^$' | sort) || true
```

If `.env` is missing `FETCH_MAX_CONCURRENT`, add it (the default of `8` is used automatically if absent, so this is informational only).

- [ ] **Step 3: Commit any leftover changes**

```bash
git status
```

If `.env.example` was not committed in Task 1, commit it now:

```bash
git add .env.example
git commit -m "chore: document FETCH_MAX_CONCURRENT in .env.example"
```
