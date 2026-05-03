# Early-Stop Filtered Fetch — Design Spec

**Date**: 2026-05-03  
**Status**: Approved

## Problem

`resumes::tick` calls `fetch_all(Reported)` which fetches all pages (up to 500+), then
filters to current-month records in memory after the fact. This causes the resumes task to
run indefinitely when historical data is large.

## Goal

Add a per-record filter predicate to the unified `fetch_all` path. When set, the sliding
window collects only matching records and stops early once a configurable grace window of
consecutive pages with no matches is exhausted. The predicate doubles as both a record
filter and an early-stop signal — no post-fetch filtering is needed at the call site.

## Scope

- `src/biotech/mod.rs` — core changes (constant, struct, sliding window logic, tests)
- `src/tasks/resumes.rs` — pass filter predicate, remove post-fetch filter
- `src/tasks/checker.rs` — pass `FetchOptions::default()` (no behaviour change)

## Design

### `FetchOptions`

A new public struct in `src/biotech/mod.rs`:

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

`BiotechwareApi::fetch_all` gains a second parameter:

```rust
pub async fn fetch_all(&self, list_type: ListType, opts: FetchOptions) -> Result<Vec<Record>>
```

### Grace window constant

```rust
const EARLY_STOP_GRACE_PAGES: usize = 5;
```

Defined at module level in `src/biotech/mod.rs`. Not exposed to callers.

### Sliding window logic (updated)

`fetch_all_sliding` receives `opts: FetchOptions` in addition to its existing parameters.
A `grace_misses: usize` counter is added alongside the existing `stop: bool` flag.

Per resolved page:

```
matched = if opts.record_filter is Some(f):
              page.list filtered by f
          else:
              page.list (all records)

if page is API-empty (count <= 0 || list.is_empty()):
    stop = true                            // hard stop: no more data from API

elif opts.record_filter is Some:
    if matched.is_empty():
        grace_misses += 1
        if grace_misses >= EARLY_STOP_GRACE_PAGES:
            stop = true                    // soft stop: grace window exhausted
    else:
        grace_misses = 0                   // reset on any match — handles unordered sets
    push matched to results (even if empty this page)

else:
    push all records to results            // no filter: existing behaviour
```

When `stop` fires (hard or soft), no new pages are submitted. In-flight tasks drain as
before — at most `max_concurrent - 1` extra requests at the tail.

The grace counter resets on any match to correctly handle unordered record sets: a page
with matches that appears after a gap of misses continues the scan rather than
prematurely terminating.

### Call sites

**`resumes::tick`** — before:
```rust
let records = api.fetch_all(ListType::Reported).await?;
let this_month: Vec<&Record> = records.iter().filter(|r| {
    r.last_report_date
        .map(|d| d.month() == month && d.year() == year)
        .unwrap_or(false)
}).collect();
```

After:
```rust
let opts = FetchOptions {
    record_filter: Some(Box::new(move |r: &Record| {
        r.last_report_date
            .map(|d| d.month() == month && d.year() == year)
            .unwrap_or(false)
    })),
};
let this_month = api.fetch_all(ListType::Reported, opts).await?;
// records are already filtered — post-fetch filter removed
```

**`checker::tick`** — unchanged behaviour, just passes default options:
```rust
let records = api.fetch_all(ListType::Unreported, FetchOptions::default()).await?;
```

## Error Handling

No new error variants. A soft stop (grace window exhausted) is not an error — it simply
terminates the fetch early and returns accumulated matched records. The `Result<Vec<Record>>`
contract of `fetch_all` is unchanged.

## Tests

New tests added to the `#[cfg(test)]` block in `src/biotech/mod.rs`:

| Test | What it verifies |
|---|---|
| `filter_keeps_only_matching_records` | Predicate that keeps even-id records; result contains only those records |
| `grace_window_stops_after_n_misses` | After `EARLY_STOP_GRACE_PAGES` consecutive miss pages, pages beyond that are never fetched |
| `grace_counter_resets_on_match` | Alternating miss/match pages; all matching records returned (counter never reaches threshold) |
| Existing tests | Pass unchanged with `FetchOptions::default()` |
