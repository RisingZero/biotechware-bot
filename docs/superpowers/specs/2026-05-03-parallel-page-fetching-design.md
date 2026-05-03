# Parallel Page Fetching — Design Spec

**Date**: 2026-05-03  
**Status**: Approved

## Goal

Reduce the wall-clock time of `BiotechwareApi::fetch_all` by fetching record pages
in parallel. The API uses cursor-based pagination (each page's counter is derived
from the previous response), but the page size is nearly fixed, which allows
speculative parallel fetching with bounded concurrency.

## Scope

Optimisation is contained entirely within `fetch_all` in `src/biotech/mod.rs`.
The two task loops (`checker`, `resumes`) are unchanged. The public signature of
`fetch_all` is unchanged.

## Configuration

One new field added to the existing `Config` struct (`src/config.rs`):

```rust
pub fetch_max_concurrent: usize,  // default: 8
```

Sourced from the environment variable `FETCH_MAX_CONCURRENT`. Added to
`.env.example`. `BiotechwareApi` receives the value at construction time and
stores it as a plain `usize` field.

## Architecture

### Phase 1 — Bootstrap

`fetch_all` calls `fetch_page(list_type, FIRST_COUNTER)` sequentially. This
single blocking call establishes:

- The actual page size (`page.count`) used as the speculative stride.
- The first batch of records.

If the bootstrap page is empty, `fetch_all` returns `Ok(vec![])` immediately.

### Phase 2 — JoinSet Sliding Window

A `tokio::task::JoinSet<Result<(i32, GetRecordsResponse)>>` maintains a fixed
window of in-flight page fetches. Each spawned task receives:

- `Arc<BiotechwareApi>` (cloned from `self`) — no shared mutable state.
- `list_type: ListType` — `Copy`, zero-cost.
- `counter: i32` — the speculative offset for this task.

The task returns `(counter, response)` so results can be re-ordered after
collection.

**Loop invariant**: `join_set.len() <= max_concurrent` at all times.

```
next_counter = bootstrap_counter + page_size
stop = false
results = [(FIRST_COUNTER, bootstrap_records)]

fill_window():
    while join_set.len() < max_concurrent && !stop:
        spawn task for next_counter
        next_counter += page_size

fill_window()

while let Some(result) = join_set.join_next().await:
    match result:
        Err(_)             → propagate immediately (JoinSet dropped, tasks cancelled)
        Ok(Err(e))         → propagate immediately
        Ok(Ok((c, page))) →
            if page.list.is_empty() || page.count <= 0:
                stop = true          # end of data; drain remaining in-flight tasks
            else:
                results.push((c, page.list))
                fill_window()        # refill one slot

sort results by counter
flatten into Vec<Record>
return
```

### Stopping Condition

Setting `stop = true` prevents new tasks from being submitted. Already in-flight
tasks (at most `max_concurrent - 1`) run to completion; their empty results are
discarded. Short pages (count > 0 but < stride) are collected and also set `stop`.

### Ownership Summary

| Value | How shared |
|---|---|
| `Arc<BiotechwareApi>` | cloned per task; session mutex inside handles concurrent logins |
| `ListType` | `Copy` |
| `JoinSet` | owned by `fetch_all` stack frame |
| results buffer | owned by `fetch_all` stack frame; tasks never touch it |

No `Mutex`, no `Arc<Mutex<Vec>>`, no channels needed — the `JoinSet` acts as the
synchronisation point.

## Error Handling

| Situation | Behaviour |
|---|---|
| `fetch_page` returns `Err` | Propagated immediately; `JoinSet` dropped cancels remaining tasks |
| Task panics (`JoinError`) | Converted to `anyhow::Error`, propagated |
| Bootstrap page empty | `Ok(vec![])` returned immediately |
| Partial last page (count < stride but > 0) | Records collected; stop flag set |
| Tail in-flight tasks after stop | Results collected if non-empty; discarded if empty |

The `Result<Vec<Record>>` return type and error contract of `fetch_all` are
unchanged. Callers require no modification.

## Files Changed

| File | Change |
|---|---|
| `src/config.rs` | Add `fetch_max_concurrent: usize` field, parse from env |
| `src/biotech/mod.rs` | Add `max_concurrent: usize` field; rewrite `fetch_all` with JoinSet logic |
| `.env.example` | Add `FETCH_MAX_CONCURRENT=8` |

## Non-Changes

- `fetch_page` — unchanged
- `retry::with_retry` — unchanged
- `checker::tick`, `resumes::tick` — unchanged
- Redis usage — unchanged (not used for intra-call coordination)
- `BiotechwareApi::new` signature — gains `max_concurrent` parameter from config
