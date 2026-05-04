# Checker: queue summary + expiry detail in Telegram messages

**Date:** 2026-05-04

## Problem

The checker's "new records" Telegram message only lists newly arrived records by bucket/count. Operators have no immediate view of the full unreported queue, and there is no indication of which record is closest to breaching its SLA deadline. Additionally, when the operator clears short-term ECGs but HC/HP records remain, the bot sends a misleading all-clear instead of showing what work is left.

## Goals

1. Append a full queue summary to the "new records" message.
2. Add a single "next to expire" detail line to that same message.
3. Emit a neutral queue-update message whenever the queue shrinks (not just when it fully clears).
4. Fire the celebratory all-clear only when the queue is completely empty.

## Out of scope

- Changes to the resumes/billing task.
- Startup message (already shows full queue).
- Outage/recovery alerts.

---

## Section 1 — `Record` model (`src/biotech/models.rs`)

### `sla_duration`

```
pub fn sla_duration(sl: Option<&str>) -> Option<chrono::Duration>
```

Matches by `contains` on the service-level string:

| Pattern   | Duration |
|-----------|----------|
| `ECG-20`  | 20 min   |
| `ECG-90`  | 90 min   |
| `ECG-DAY` | 24 h     |
| `HC-24`   | 24 h     |
| `HC-48`   | 48 h     |
| anything else | `None` |

### `Record::deadline`

```
pub fn deadline(&self) -> Option<DateTime<Utc>>
```

- Parses `self.reception_date` (an `Option<String>`) with format `"%Y-%m-%dT%H:%M:%S%.f%z"` (variable fractional seconds, no-colon UTC offset such as `+0000`).
- Calls `sla_duration(self.effective_service_level.as_deref())`.
- Returns `Some(reception + duration)` if both succeed, otherwise `None`.

---

## Section 2 — `checker.rs` changes

### New helpers

**`find_next_expiring<'a>(records: &'a [Record]) -> Option<&'a Record>`**
Filters records that return `Some` from `deadline()`, sorts ascending by deadline, returns the first. Records without a computable deadline are ignored.

**`render_expiry_line(record: &Record, now: DateTime<Utc>) -> String`**
Formats a single line:
- Not yet expired: `"⏰ Prossima scadenza: {sl} — #{id} — scade tra {N} min"`
- Already past: `"🔴 Prossima scadenza: {sl} — #{id} — scaduta da {N} min"`

Where `{sl}` is `effective_service_level` (or `"?"` if absent) and `{id}` is `record.id`.

**`render_queue_summary(counts: &HashMap<String, usize>, total: usize, expiry: Option<String>) -> String`**
Returns:
```
Coda attuale ({total}):
{sorted_lines}
{expiry_line_if_some}
```
Reuses the existing `sorted_lines` helper. `expiry` is a pre-rendered string from `render_expiry_line`.

### Updated `render_new`

Becomes two blocks joined by a blank line:

```
Nuovi ECG disponibili
{new_lines}

{render_queue_summary(...)}
```

### New `render_queue_update`

Same as `render_queue_summary` but standalone — no "Nuovi ECG" prefix. Used when records decrease but the caller decides not to send a full `render_new`.

### Updated `tick` decision tree

After computing `new`, `removed_count`, and `counts`:

```
if !new.is_empty():
    send render_new(new_counts, all_records, counts, now)

if removed_count > 0:
    let any_short_term = counts.keys().any(|sl| is_short_term(...))
    if !any_short_term && total == 0:
        send "ECG tutti refertati, niente di altro :D"   // all-clear
    else:
        send render_queue_update(counts, total, expiry)  // work remains
```

`new` and `removed_count > 0` are independent — both branches can fire in the same tick (e.g. one record arrives and another is cleared simultaneously).

---

## Message examples

**New records message:**
```
Nuovi ECG disponibili
TR-ECG-20: 1

Coda attuale (4):
TR-ECG-20: 2
TR-ECG-90: 1
TR-HC-24: 1
⏰ Prossima scadenza: TR-ECG-20 — #1284221 — scade tra 12 min
```

**Queue update (operator cleared some records, short-term still remain):**
```
Coda attuale (3):
TR-ECG-20: 1
TR-ECG-90: 1
TR-HC-24: 1
🔴 Prossima scadenza: TR-ECG-20 — #1284221 — scaduta da 5 min
```

**Queue update (no short-term ECGs left, HC still in queue):**
```
Coda attuale (1):
TR-HC-24: 1
```
(no expiry line — HC-24 has a deadline too, so it would appear if it has one)

**All-clear (queue empty):**
```
ECG tutti refertati, niente di altro :D
```
