# Checker: queue summary + expiry detail Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enrich checker Telegram messages with a full unreported-queue summary and a "next to expire" detail line, and fix the all-clear logic so it only fires when the queue is completely empty.

**Architecture:** Two files change. `src/biotech/models.rs` gains `sla_duration` (SLA string → chrono::Duration) and `Record::deadline` (reception_date + SLA → DateTime<Utc>). `src/tasks/checker.rs` gains three new render helpers, updates `render_new` to include a queue summary block, and replaces the existing all-clear branch with a decision tree that emits a queue-update message when work remains.

**Tech Stack:** Rust, chrono, tokio, existing codebase patterns.

---

## File map

| File | Change |
|------|--------|
| `src/biotech/models.rs` | Add `sla_duration` free fn + `Record::deadline` method |
| `src/tasks/checker.rs` | Add `find_next_expiring`, `render_expiry_line`, `render_queue_summary`; update `render_new`; replace `render_all_clear`; update `tick` |

---

### Task 1: Add `sla_duration` to `src/biotech/models.rs`

**Files:**
- Modify: `src/biotech/models.rs`

- [ ] **Step 1: Write the failing tests**

Append a `#[cfg(test)]` module at the bottom of `src/biotech/models.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sla_ecg20() {
        assert_eq!(sla_duration(Some("TR-ECG-20")), Some(chrono::Duration::minutes(20)));
    }

    #[test]
    fn sla_ecg90() {
        assert_eq!(sla_duration(Some("TR-ECG-90")), Some(chrono::Duration::minutes(90)));
    }

    #[test]
    fn sla_ecg_day() {
        assert_eq!(sla_duration(Some("TR-ECG-DAY")), Some(chrono::Duration::hours(24)));
    }

    #[test]
    fn sla_hc24() {
        assert_eq!(sla_duration(Some("TR-HC-24")), Some(chrono::Duration::hours(24)));
    }

    #[test]
    fn sla_hc48() {
        assert_eq!(sla_duration(Some("TR-HC-48")), Some(chrono::Duration::hours(48)));
    }

    #[test]
    fn sla_unknown_and_none() {
        assert_eq!(sla_duration(Some("TR-HP-SOMETHING")), None);
        assert_eq!(sla_duration(None), None);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p biotechware-bot sla_ 2>&1 | tail -20
```

Expected: compilation error — `sla_duration` not defined.

- [ ] **Step 3: Add `sla_duration` to `src/biotech/models.rs`**

Add after the `is_long_term` function (before any `mod tests` block):

```rust
/// Returns the SLA window for a service-level string, matched by substring.
/// Returns `None` for service levels with no defined SLA.
pub fn sla_duration(sl: Option<&str>) -> Option<chrono::Duration> {
    let s = sl?;
    if s.contains("ECG-20") {
        Some(chrono::Duration::minutes(20))
    } else if s.contains("ECG-90") {
        Some(chrono::Duration::minutes(90))
    } else if s.contains("ECG-DAY") {
        Some(chrono::Duration::hours(24))
    } else if s.contains("HC-24") {
        Some(chrono::Duration::hours(24))
    } else if s.contains("HC-48") {
        Some(chrono::Duration::hours(48))
    } else {
        None
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p biotechware-bot sla_ 2>&1 | tail -20
```

Expected: `6 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/biotech/models.rs
git commit -m "feat: add sla_duration helper to models"
```

---

### Task 2: Add `Record::deadline` to `src/biotech/models.rs`

**Files:**
- Modify: `src/biotech/models.rs`

- [ ] **Step 1: Write the failing tests**

Add a helper and three tests inside the existing `#[cfg(test)] mod tests` block in `src/biotech/models.rs`:

```rust
    fn record_with_deadline(id: &str, reception: &str, sl: &str) -> Record {
        Record {
            id: id.to_string(),
            reception_date: Some(reception.to_string()),
            effective_service_level: Some(sl.to_string()),
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
            date: None,
            requester: None,
        }
    }

    #[test]
    fn deadline_ecg20_adds_20_min() {
        let r = record_with_deadline("1", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        let expected = chrono::DateTime::parse_from_rfc3339("2026-05-04T10:20:00+00:00")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(r.deadline(), Some(expected));
    }

    #[test]
    fn deadline_none_when_no_sla() {
        let r = record_with_deadline("2", "2026-05-04T10:00:00.000+0000", "TR-HP-SOMETHING");
        assert!(r.deadline().is_none());
    }

    #[test]
    fn deadline_none_when_no_reception_date() {
        let mut r = record_with_deadline("3", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        r.reception_date = None;
        assert!(r.deadline().is_none());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p biotechware-bot deadline_ 2>&1 | tail -20
```

Expected: compilation error — `deadline` method not defined on `Record`.

- [ ] **Step 3: Add `Record::deadline` method**

Add an `impl Record` block after the struct definition in `src/biotech/models.rs` (before the `ListType` enum):

```rust
impl Record {
    /// Computes the SLA deadline as reception_date + sla_duration.
    /// Returns None if reception_date is absent/unparseable or the service level has no SLA.
    pub fn deadline(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        let date_str = self.reception_date.as_deref()?;
        let parsed = chrono::DateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S%.f%z").ok()?;
        let duration = sla_duration(self.effective_service_level.as_deref())?;
        Some(parsed.with_timezone(&chrono::Utc) + duration)
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p biotechware-bot deadline_ 2>&1 | tail -20
```

Expected: `3 passed`.

- [ ] **Step 5: Run all tests to check for regressions**

```bash
cargo test -p biotechware-bot 2>&1 | tail -20
```

Expected: all existing tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/biotech/models.rs
git commit -m "feat: add Record::deadline computed from reception_date and SLA"
```

---

### Task 3: Add `find_next_expiring` and `render_expiry_line` to `src/tasks/checker.rs`

**Files:**
- Modify: `src/tasks/checker.rs`

- [ ] **Step 1: Write the failing tests**

Append a `#[cfg(test)]` module at the bottom of `src/tasks/checker.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn record_with_sl(id: &str, reception: &str, sl: &str) -> Record {
        Record {
            id: id.to_string(),
            reception_date: Some(reception.to_string()),
            effective_service_level: Some(sl.to_string()),
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
            date: None,
            requester: None,
        }
    }

    #[test]
    fn find_next_expiring_returns_earliest_deadline() {
        // r1: ECG-90 at 10:00 → deadline 11:30
        // r2: ECG-20 at 10:00 → deadline 10:20  ← earliest
        let r1 = record_with_sl("1", "2026-05-04T10:00:00.000+0000", "TR-ECG-90");
        let r2 = record_with_sl("2", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        let records = vec![r1, r2];
        let next = find_next_expiring(&records).unwrap();
        assert_eq!(next.id, "2");
    }

    #[test]
    fn find_next_expiring_skips_records_with_no_sla() {
        let r1 = record_with_sl("1", "2026-05-04T09:00:00.000+0000", "TR-HP-SOMETHING");
        let r2 = record_with_sl("2", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        let records = vec![r1, r2];
        let next = find_next_expiring(&records).unwrap();
        assert_eq!(next.id, "2");
    }

    #[test]
    fn find_next_expiring_empty_slice() {
        assert!(find_next_expiring(&[]).is_none());
    }

    #[test]
    fn render_expiry_line_not_yet_expired() {
        let r = record_with_sl("1284221", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        // deadline = 10:20, now = 10:08 → 12 min remaining
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-04T10:08:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let line = render_expiry_line(&r, now);
        assert_eq!(line, "⏰ Prossima scadenza: TR-ECG-20 — #1284221 — scade tra 12 min");
    }

    #[test]
    fn render_expiry_line_already_expired() {
        let r = record_with_sl("1284221", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        // deadline = 10:20, now = 10:25 → 5 min past
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-04T10:25:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let line = render_expiry_line(&r, now);
        assert_eq!(line, "🔴 Prossima scadenza: TR-ECG-20 — #1284221 — scaduta da 5 min");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p biotechware-bot find_next_expiring 2>&1 | tail -20
cargo test -p biotechware-bot render_expiry_line 2>&1 | tail -20
```

Expected: compilation errors — functions not defined.

- [ ] **Step 3: Add `chrono::Utc` import to `src/tasks/checker.rs`**

Replace the existing import block at the top of `src/tasks/checker.rs`:

```rust
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::time::sleep;
use tracing::{error, info, instrument};

use crate::biotech::models::{is_long_term, is_short_term, ListType, Record};
use crate::biotech::{BiotechwareApi, FetchOptions};
use crate::cache::Cache;
use crate::config::Config;
use crate::telegram::TelegramBot;
use crate::tasks::outage::{Alert, Tracker};
```

- [ ] **Step 4: Add `find_next_expiring` and `render_expiry_line` after the `sorted_lines` function**

```rust
fn find_next_expiring(records: &[Record]) -> Option<&Record> {
    records
        .iter()
        .filter(|r| r.deadline().is_some())
        .min_by_key(|r| r.deadline().unwrap())
}

fn render_expiry_line(record: &Record, now: DateTime<Utc>) -> String {
    let sl = record.effective_service_level.as_deref().unwrap_or("?");
    let deadline = record.deadline().unwrap();
    let diff_mins = (deadline - now).num_minutes();
    if diff_mins >= 0 {
        format!(
            "⏰ Prossima scadenza: {sl} — #{} — scade tra {} min",
            record.id, diff_mins
        )
    } else {
        format!(
            "🔴 Prossima scadenza: {sl} — #{} — scaduta da {} min",
            record.id, -diff_mins
        )
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cargo test -p biotechware-bot find_next_expiring 2>&1 | tail -20
cargo test -p biotechware-bot render_expiry_line 2>&1 | tail -20
```

Expected: `3 passed` + `2 passed`.

- [ ] **Step 6: Run all tests to check for regressions**

```bash
cargo test -p biotechware-bot 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/tasks/checker.rs
git commit -m "feat: add find_next_expiring and render_expiry_line to checker"
```

---

### Task 4: Add `render_queue_summary` to `src/tasks/checker.rs`

**Files:**
- Modify: `src/tasks/checker.rs`

- [ ] **Step 1: Write the failing tests**

Add two tests inside the existing `#[cfg(test)] mod tests` block in `src/tasks/checker.rs`:

```rust
    #[test]
    fn render_queue_summary_without_expiry() {
        let mut counts = HashMap::new();
        counts.insert("TR-ECG-20".to_string(), 2);
        counts.insert("TR-HC-24".to_string(), 1);
        let s = render_queue_summary(&counts, 3, None);
        assert!(s.contains("Coda attuale (3):"));
        assert!(s.contains("TR-ECG-20: 2"));
        assert!(s.contains("TR-HC-24: 1"));
        assert!(!s.contains("scadenza"));
    }

    #[test]
    fn render_queue_summary_with_expiry_line() {
        let mut counts = HashMap::new();
        counts.insert("TR-ECG-20".to_string(), 1);
        let expiry = "⏰ Prossima scadenza: TR-ECG-20 — #99 — scade tra 5 min".to_string();
        let s = render_queue_summary(&counts, 1, Some(expiry));
        assert!(s.contains("Coda attuale (1):"));
        assert!(s.contains("scadenza"));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p biotechware-bot render_queue_summary 2>&1 | tail -20
```

Expected: compilation error — function not defined.

- [ ] **Step 3: Add `render_queue_summary` after `render_expiry_line`**

```rust
fn render_queue_summary(
    counts: &HashMap<String, usize>,
    total: usize,
    expiry_line: Option<String>,
) -> String {
    let mut parts = vec![
        format!("Coda attuale ({total}):"),
        sorted_lines(counts).join("\n"),
    ];
    if let Some(line) = expiry_line {
        parts.push(line);
    }
    parts.join("\n")
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p biotechware-bot render_queue_summary 2>&1 | tail -20
```

Expected: `2 passed`.

- [ ] **Step 5: Run all tests**

```bash
cargo test -p biotechware-bot 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/tasks/checker.rs
git commit -m "feat: add render_queue_summary to checker"
```

---

### Task 5: Update `render_new` in `src/tasks/checker.rs`

**Files:**
- Modify: `src/tasks/checker.rs`

- [ ] **Step 1: Write the failing test**

Add inside the `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn render_new_includes_summary_block() {
        let mut new_counts = HashMap::new();
        new_counts.insert("TR-ECG-20".to_string(), 1);
        let mut all_counts = HashMap::new();
        all_counts.insert("TR-ECG-20".to_string(), 2);
        all_counts.insert("TR-HC-24".to_string(), 1);
        let s = render_new(&new_counts, &all_counts, 3, None);
        assert!(s.contains("Nuovi ECG disponibili"));
        assert!(s.contains("TR-ECG-20: 1"));      // new counts section
        assert!(s.contains("Coda attuale (3):"));  // summary block
        assert!(s.contains("TR-HC-24: 1"));        // full queue, not just new
    }
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p biotechware-bot render_new_includes_summary_block 2>&1 | tail -20
```

Expected: compilation error — wrong number of arguments to `render_new`.

- [ ] **Step 3: Replace `render_new` with the updated signature and body**

Find and replace the existing `render_new` function (currently lines ~192-197 of `src/tasks/checker.rs`):

**Old:**
```rust
fn render_new(new_counts: &HashMap<String, usize>) -> String {
    format!(
        "Nuovi ECG disponibili\n{}",
        sorted_lines(new_counts).join("\n")
    )
}
```

**New:**
```rust
fn render_new(
    new_counts: &HashMap<String, usize>,
    all_counts: &HashMap<String, usize>,
    total: usize,
    expiry_line: Option<String>,
) -> String {
    format!(
        "Nuovi ECG disponibili\n{}\n\n{}",
        sorted_lines(new_counts).join("\n"),
        render_queue_summary(all_counts, total, expiry_line)
    )
}
```

- [ ] **Step 4: Fix the call site in `tick`**

In the `tick` function (around line 109), the call `telegram.send(&render_new(&new_counts), false).await;` must be updated. That step is done in Task 6 — for now just verify the test compiles and passes after updating the signature (the call site will fail to compile until Task 6; that's expected):

```bash
cargo test -p biotechware-bot render_new_includes_summary_block 2>&1 | tail -20
```

Expected: compilation error at the call site in `tick` (not in the test). Note the error is a different line — the test itself would compile if the function signature were the only issue, but the stale call site will prevent compilation. Proceed to Task 6 immediately.

---

### Task 6: Update `tick` decision tree and remove `render_all_clear`

**Files:**
- Modify: `src/tasks/checker.rs`

- [ ] **Step 1: Add `use crate::biotech::models::sla_duration` if not already imported**

`sla_duration` is used by `Record::deadline` inside models.rs, not directly in checker.rs — no additional import needed. `is_long_term` is used only by the old `render_all_clear`; it can be removed from the import line once that function is deleted.

- [ ] **Step 2: Remove `render_all_clear` and drop unused `is_long_term` import**

Delete the entire `render_all_clear` function from `src/tasks/checker.rs`:

```rust
// DELETE this entire function:
fn render_all_clear(counts: &HashMap<String, usize>) -> String {
    let long_term: HashMap<String, usize> = counts
        .iter()
        .filter(|(sl, _)| is_long_term(Some(sl.as_str())))
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    if long_term.is_empty() {
        "ECG tutti refertati, niente di altro :D\nNiente di altro.".to_string()
    } else {
        format!(
            "ECG tutti refertati, niente di nuovo :D\nResoconto attuale:\n{}",
            sorted_lines(&long_term).join("\n")
        )
    }
}
```

Also remove `is_long_term` from the import at the top:

```rust
// Old:
use crate::biotech::models::{is_long_term, is_short_term, ListType, Record};
// New:
use crate::biotech::models::{is_short_term, ListType, Record};
```

- [ ] **Step 3: Rewrite the relevant section of `tick`**

The current `tick` body (lines ~88-126 of `src/tasks/checker.rs`) reads:

```rust
    if *first_tick {
        let msg = render_summary(&counts, records.len());
        telegram.send(&msg, false).await;
        *first_tick = false;
    } else {
        let new: Vec<&Record> = records
            .iter()
            .filter(|r| !known_ids.contains(&r.id))
            .collect();
        let removed_count = known_ids.iter().filter(|id| !current_ids.contains(*id)).count();

        if !new.is_empty() {
            let mut new_counts: HashMap<String, usize> = HashMap::new();
            for r in &new {
                let bucket = bucket_of(r);
                *new_counts.entry(bucket.clone()).or_insert(0) += 1;
                crate::metrics::get()
                    .new_records_total
                    .with_label_values(&[&bucket])
                    .inc();
            }
            telegram.send(&render_new(&new_counts), false).await;
        }

        // "Tutti refertati" rule: something was removed AND no short-term ECG
        // remains in queue.
        let any_short_term_remaining = counts
            .keys()
            .any(|sl| is_short_term(Some(sl.as_str())));
        if removed_count > 0 && !any_short_term_remaining {
            telegram
                .send(&render_all_clear(&counts), false)
                .await;
        }
    }
```

Replace with:

```rust
    if *first_tick {
        let msg = render_summary(&counts, records.len());
        telegram.send(&msg, false).await;
        *first_tick = false;
    } else {
        let now = Utc::now();
        let new: Vec<&Record> = records
            .iter()
            .filter(|r| !known_ids.contains(&r.id))
            .collect();
        let removed_count = known_ids.iter().filter(|id| !current_ids.contains(*id)).count();

        let expiry_line = find_next_expiring(&records)
            .map(|r| render_expiry_line(r, now));

        if !new.is_empty() {
            let mut new_counts: HashMap<String, usize> = HashMap::new();
            for r in &new {
                let bucket = bucket_of(r);
                *new_counts.entry(bucket.clone()).or_insert(0) += 1;
                crate::metrics::get()
                    .new_records_total
                    .with_label_values(&[&bucket])
                    .inc();
            }
            telegram
                .send(&render_new(&new_counts, &counts, records.len(), expiry_line.clone()), false)
                .await;
        }

        if removed_count > 0 {
            let any_short_term_remaining = counts
                .keys()
                .any(|sl| is_short_term(Some(sl.as_str())));
            if !any_short_term_remaining && records.is_empty() {
                telegram
                    .send("ECG tutti refertati, niente di altro :D", false)
                    .await;
            } else {
                telegram
                    .send(&render_queue_summary(&counts, records.len(), expiry_line), false)
                    .await;
            }
        }
    }
```

- [ ] **Step 4: Build to verify compilation**

```bash
cargo build -p biotechware-bot 2>&1 | tail -30
```

Expected: no errors.

- [ ] **Step 5: Run all tests**

```bash
cargo test -p biotechware-bot 2>&1 | tail -30
```

Expected: all tests pass (including the new ones from Tasks 1-5 and the existing fetch/sliding-window tests in `src/biotech/mod.rs`).

- [ ] **Step 6: Commit**

```bash
git add src/tasks/checker.rs
git commit -m "feat: enrich checker messages with queue summary, expiry detail, and updated all-clear logic"
```

---

## Self-review

**Spec coverage:**
- ✅ `sla_duration` covers ECG-20/90/DAY and HC-24/48 (Tasks 1)
- ✅ `Record::deadline` parses ISO 8601 reception_date with variable fractional seconds (Task 2)
- ✅ `find_next_expiring` picks earliest deadline (Task 3)
- ✅ `render_expiry_line` shows id not name, expired vs not-yet variants (Task 3)
- ✅ `render_queue_summary` builds full queue block with optional expiry (Task 4)
- ✅ `render_new` prepends new-arrivals block then full summary (Task 5)
- ✅ Queue-shrink message fires when `removed_count > 0` and work remains (Task 6)
- ✅ All-clear fires only when queue is completely empty (Task 6)

**Placeholder scan:** No TBDs, no "implement later", no "similar to task N" — all code is shown in full.

**Type consistency:**
- `render_queue_summary` defined in Task 4 with signature `(&HashMap<String,usize>, usize, Option<String>) -> String` — called identically in Tasks 5 and 6. ✓
- `render_new` defined in Task 5 with four args — call site updated in Task 6. ✓
- `find_next_expiring` returns `Option<&Record>` — used as `.map(|r| render_expiry_line(r, now))` in Task 6. ✓
- `render_expiry_line` takes `(&Record, DateTime<Utc>)` — called with `(r, now)` where `now: DateTime<Utc>`. ✓
