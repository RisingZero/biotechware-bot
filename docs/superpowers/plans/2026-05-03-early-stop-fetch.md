# Early-Stop Filtered Fetch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-record filter predicate to `fetch_all` so that `resumes::tick` stops fetching pages early once a configurable grace window of consecutive non-matching pages is exhausted, instead of fetching all 500+ pages and filtering in memory.

**Architecture:** A new `FetchOptions` struct carries an optional `record_filter: Option<Box<dyn Fn(&Record) -> bool + Send + Sync>>`. The private `fetch_all_sliding` function consults this filter per page: it partitions each page's records, tracks consecutive pages with zero matches (`grace_misses`), and sets the stop flag once `EARLY_STOP_GRACE_PAGES` (a module-level constant = 5) consecutive misses occur. `resumes::tick` passes a this-month predicate; `checker::tick` passes `FetchOptions::default()` (no behaviour change).

**Tech Stack:** Rust, tokio, existing `FuturesUnordered` sliding window in `src/biotech/mod.rs`.

---

## File Map

| File | Change |
|---|---|
| `src/biotech/mod.rs` | Add `EARLY_STOP_GRACE_PAGES` const; add `pub FetchOptions` struct; update `fetch_all` signature; update `fetch_all_sliding` with filter + grace logic; update existing tests to pass `FetchOptions::default()`; add 3 new tests |
| `src/tasks/checker.rs` | Pass `FetchOptions::default()` to `fetch_all` |
| `src/tasks/resumes.rs` | Import `FetchOptions`; pass this-month predicate to `fetch_all`; remove post-fetch filter block; `this_month` becomes `Vec<Record>` |

---

### Task 1: TDD — write failing tests for the filter + grace logic

**Files:**
- Modify: `src/biotech/mod.rs` (append to existing `#[cfg(test)]` block)

- [ ] **Step 1: Append the three new tests to the `#[cfg(test)]` block**

Open `src/biotech/mod.rs`. Find the `#[cfg(test)]` block at the bottom (it already has `make_record`, `make_page`, `empty_page` helpers and 4 existing tests). Append these three tests **inside** the `mod tests { }` block, before the closing `}`:

```rust
    // Filter keeps only records matching the predicate; non-matching records
    // are discarded even when the page is non-empty.
    #[tokio::test]
    async fn filter_keeps_only_matching_records() {
        use std::sync::{Arc, Mutex};
        use std::collections::VecDeque;

        // Bootstrap: records 0..5 (ids "0".."4")
        // Speculative page: records 5..10 (ids "5".."9")
        // Then empty to stop.
        // Predicate: keep records with even numeric id.
        // Matching: 0, 2, 4, 6, 8 → 5 records.
        let bootstrap = GetRecordsResponse {
            count: 5,
            counter: 0,
            list: (0..5).map(make_record).collect(),
        };
        let queue: Arc<Mutex<VecDeque<GetRecordsResponse>>> = Arc::new(Mutex::new(
            VecDeque::from(vec![
                GetRecordsResponse {
                    count: 5,
                    counter: 0,
                    list: (5..10).map(make_record).collect(),
                },
                empty_page(),
                empty_page(),
            ]),
        ));
        let opts = FetchOptions {
            record_filter: Some(Box::new(|r: &Record| {
                r.id.parse::<usize>().map(|n| n % 2 == 0).unwrap_or(false)
            })),
        };
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 2, opts, move |_c| {
            let q = queue.clone();
            async move { Ok(q.lock().unwrap().pop_front().unwrap_or_else(empty_page)) }
        })
        .await
        .unwrap();

        assert_eq!(result.len(), 5);
        assert!(result.iter().all(|r| r.id.parse::<usize>().unwrap() % 2 == 0));
    }

    // After EARLY_STOP_GRACE_PAGES consecutive pages with no matching records
    // the fetch stops; pages beyond the grace window are never requested.
    #[tokio::test]
    async fn grace_window_stops_after_n_misses() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let pages_fetched = Arc::new(AtomicUsize::new(0));
        let pf = pages_fetched.clone();

        // Bootstrap: 1 matching record (id=0, even).
        let bootstrap = GetRecordsResponse {
            count: 1,
            counter: 0,
            list: vec![make_record(0)],
        };
        // Predicate: even ids match.
        let opts = FetchOptions {
            record_filter: Some(Box::new(|r: &Record| {
                r.id.parse::<usize>().map(|n| n % 2 == 0).unwrap_or(false)
            })),
        };
        // Every speculative page returns one non-matching record (id=1, odd).
        // After EARLY_STOP_GRACE_PAGES pages the stop fires.
        // Pages beyond that should never be fetched.
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 1, opts, move |_c| {
            let count = pf.fetch_add(1, Ordering::Relaxed);
            async move {
                if count < EARLY_STOP_GRACE_PAGES {
                    Ok(GetRecordsResponse { count: 1, counter: 0, list: vec![make_record(1)] })
                } else {
                    // Must not be reached.
                    Ok(GetRecordsResponse { count: 1, counter: 0, list: vec![make_record(2)] })
                }
            }
        })
        .await
        .unwrap();

        // Only the bootstrap match is returned.
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "0");
        // Exactly EARLY_STOP_GRACE_PAGES speculative pages were fetched.
        assert_eq!(pages_fetched.load(Ordering::Relaxed), EARLY_STOP_GRACE_PAGES);
    }

    // Grace counter resets to zero whenever a matching record is found, so a
    // sequence of miss/match/miss/match pages does not trigger early stop.
    #[tokio::test]
    async fn grace_counter_resets_on_match() {
        use std::sync::{Arc, Mutex};
        use std::collections::VecDeque;

        // Pattern (speculative pages): (GRACE-1) misses, 1 match, (GRACE-1) misses,
        // 1 match, then GRACE misses to finally trigger the stop.
        // Bootstrap: 1 match (id=0).
        // Expected matches: bootstrap + 2 cycle matches = 3.
        let mut pages: Vec<GetRecordsResponse> = Vec::new();
        for _ in 0..2 {
            for _ in 0..(EARLY_STOP_GRACE_PAGES - 1) {
                pages.push(GetRecordsResponse { count: 1, counter: 0, list: vec![make_record(1)] });
            }
            pages.push(GetRecordsResponse { count: 1, counter: 0, list: vec![make_record(2)] });
        }
        for _ in 0..EARLY_STOP_GRACE_PAGES {
            pages.push(GetRecordsResponse { count: 1, counter: 0, list: vec![make_record(1)] });
        }
        pages.push(empty_page()); // safety drain

        let queue: Arc<Mutex<VecDeque<GetRecordsResponse>>> =
            Arc::new(Mutex::new(VecDeque::from(pages)));

        let bootstrap = GetRecordsResponse {
            count: 1,
            counter: 0,
            list: vec![make_record(0)],
        };
        let opts = FetchOptions {
            record_filter: Some(Box::new(|r: &Record| {
                r.id.parse::<usize>().map(|n| n % 2 == 0).unwrap_or(false)
            })),
        };
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 1, opts, move |_c| {
            let q = queue.clone();
            async move { Ok(q.lock().unwrap().pop_front().unwrap_or_else(empty_page)) }
        })
        .await
        .unwrap();

        // bootstrap (0) + 2 cycle matches = 3 even-id records
        assert_eq!(result.len(), 3);
        assert!(result.iter().all(|r| r.id.parse::<usize>().unwrap() % 2 == 0));
    }
```

- [ ] **Step 2: Confirm the tests fail to compile**

```bash
cargo test --lib 2>&1 | grep -E "^error" | head -10
```

Expected: errors about `FetchOptions` not found. No other errors.

---

### Task 2: Implement `FetchOptions`, `EARLY_STOP_GRACE_PAGES`, and update `fetch_all_sliding` / `fetch_all`

**Files:**
- Modify: `src/biotech/mod.rs`

- [ ] **Step 1: Add the constant alongside the other module-level constants**

Find the block of constants at the top of `src/biotech/mod.rs` (around line 18). Add after `HTTP_TIMEOUT_SECS`:

```rust
const EARLY_STOP_GRACE_PAGES: usize = 5;
```

- [ ] **Step 2: Add `FetchOptions` after the constants, before `pub struct BiotechwareApi`**

```rust
pub struct FetchOptions {
    pub record_filter: Option<Box<dyn Fn(&Record) -> bool + Send + Sync>>,
}

impl Default for FetchOptions {
    fn default() -> Self {
        Self { record_filter: None }
    }
}
```

- [ ] **Step 3: Update `fetch_all` signature to accept `opts: FetchOptions`**

Replace the current `fetch_all` method (lines 168–181) with:

```rust
#[instrument(skip(self))]
pub async fn fetch_all(&self, list_type: ListType, opts: FetchOptions) -> crate::errors::Result<Vec<Record>> {
    let bootstrap = self.fetch_page(list_type, FIRST_COUNTER).await?;
    if bootstrap.list.is_empty() || bootstrap.count <= 0 {
        info!(list_type = list_type.to_str(), "no records found");
        return Ok(vec![]);
    }
    let records = fetch_all_sliding(bootstrap, FIRST_COUNTER, self.max_concurrent, opts, |c| {
        self.fetch_page(list_type, c)
    })
    .await?;
    info!(n = records.len(), list_type = list_type.to_str(), "fetched all records");
    Ok(records)
}
```

- [ ] **Step 4: Replace `fetch_all_sliding` with the filter + grace implementation**

Replace the entire `fetch_all_sliding` function (everything from `async fn fetch_all_sliding` down to its closing `}`) with:

```rust
async fn fetch_all_sliding<F, Fut>(
    bootstrap: GetRecordsResponse,
    first_counter: i32,
    max_concurrent: usize,
    opts: FetchOptions,
    fetch: F,
) -> crate::errors::Result<Vec<Record>>
where
    F: Fn(i32) -> Fut,
    Fut: std::future::Future<Output = crate::errors::Result<GetRecordsResponse>> + Send,
{
    let page_size = bootstrap.count;
    let max_concurrent = max_concurrent.max(1);

    let bootstrap_records: Vec<Record> = match &opts.record_filter {
        Some(f) => bootstrap.list.into_iter().filter(|r| f(r)).collect(),
        None => bootstrap.list,
    };

    let mut results: Vec<(i32, Vec<Record>)> = vec![(first_counter, bootstrap_records)];
    let mut pending: FuturesUnordered<
        std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::errors::Result<(i32, GetRecordsResponse)>> + Send>,
        >,
    > = FuturesUnordered::new();
    let mut next_counter = first_counter + page_size;
    let mut stop = false;
    let mut grace_misses: usize = 0;

    for _ in 0..max_concurrent {
        let c = next_counter;
        pending.push(Box::pin(fetch(c).map(move |r| r.map(|p| (c, p)))));
        next_counter += page_size;
    }

    while let Some(result) = pending.next().await {
        let (counter, page) = result?;
        if page.list.is_empty() || page.count <= 0 {
            stop = true;
        } else {
            let matched: Vec<Record> = match &opts.record_filter {
                Some(f) => page.list.into_iter().filter(|r| f(r)).collect(),
                None => page.list,
            };

            if opts.record_filter.is_some() {
                if matched.is_empty() {
                    grace_misses += 1;
                    if grace_misses >= EARLY_STOP_GRACE_PAGES {
                        stop = true;
                    }
                } else {
                    grace_misses = 0;
                }
            }

            results.push((counter, matched));

            if !stop {
                let c = next_counter;
                pending.push(Box::pin(fetch(c).map(move |r| r.map(|p| (c, p)))));
                next_counter += page_size;
            }
        }
    }

    results.sort_unstable_by_key(|(c, _)| *c);
    Ok(results.into_iter().flat_map(|(_, records)| records).collect())
}
```

- [ ] **Step 5: Update existing test calls to pass `FetchOptions::default()`**

The four existing tests call `fetch_all_sliding` without the `opts` argument. Add `FetchOptions::default()` as the fourth argument (before the closure) in each:

`only_bootstrap_page`:
```rust
let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 2, FetchOptions::default(), |_c| async {
    Ok(empty_page())
})
```

`collects_multiple_pages`:
```rust
let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 2, FetchOptions::default(), move |_c| {
```

`propagates_fetch_error`:
```rust
let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 1, FetchOptions::default(), |_c| async {
```

`results_ordered_by_counter`:
```rust
let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 2, FetchOptions::default(), move |_c| {
```

- [ ] **Step 6: Run all lib tests**

```bash
cargo test --lib 2>&1
```

Expected: all 7 tests pass (4 old + 3 new). Zero compile errors.

```
test biotech::tests::only_bootstrap_page ... ok
test biotech::tests::collects_multiple_pages ... ok
test biotech::tests::propagates_fetch_error ... ok
test biotech::tests::results_ordered_by_counter ... ok
test biotech::tests::filter_keeps_only_matching_records ... ok
test biotech::tests::grace_window_stops_after_n_misses ... ok
test biotech::tests::grace_counter_resets_on_match ... ok
```

- [ ] **Step 7: Commit**

```bash
git add src/biotech/mod.rs
git commit -m "feat: add FetchOptions with per-record filter and grace-window early stop"
```

---

### Task 3: Update callers — `checker.rs` and `resumes.rs`

**Files:**
- Modify: `src/tasks/checker.rs`
- Modify: `src/tasks/resumes.rs`

- [ ] **Step 1: Update `checker.rs` to pass `FetchOptions::default()`**

In `src/tasks/checker.rs`, add the import alongside the existing `use crate::biotech::BiotechwareApi;`:

```rust
use crate::biotech::{BiotechwareApi, FetchOptions};
```

In `tick`, change line 67:
```rust
let records = api.fetch_all(ListType::Unreported, FetchOptions::default()).await?;
```

- [ ] **Step 2: Update `resumes.rs` to pass the this-month predicate**

In `src/tasks/resumes.rs`, update the import from:
```rust
use crate::biotech::BiotechwareApi;
```
to:
```rust
use crate::biotech::{BiotechwareApi, FetchOptions};
```

In `tick`, replace the block from `let records = api.fetch_all(...)` through the closing of the `let this_month` filter (lines 73–81) with:

```rust
let opts = FetchOptions {
    record_filter: Some(Box::new(move |r: &Record| {
        r.last_report_date
            .map(|d| d.month() == month && d.year() == year)
            .unwrap_or(false)
    })),
};
let this_month: Vec<Record> = api.fetch_all(ListType::Reported, opts).await?;
```

The downstream code (`let ids`, `for r in &this_month`, `this_month.len()`) requires no further changes — `this_month.iter()` still yields `&Record` in all cases.

- [ ] **Step 3: Verify clean build**

```bash
cargo build 2>&1
```

Expected: zero errors, zero warnings.

- [ ] **Step 4: Run the full test suite**

```bash
cargo test 2>&1
```

Expected: all tests pass (7 lib tests + all retry tests).

- [ ] **Step 5: Commit**

```bash
git add src/tasks/checker.rs src/tasks/resumes.rs
git commit -m "feat: early-stop resumes fetch using this-month record filter"
```
