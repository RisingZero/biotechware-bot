use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::time::sleep;
use tracing::{error, info, instrument};

use crate::biotech::models::{is_short_term, ListType, Record};
use crate::biotech::{BiotechwareApi, FetchOptions};
use crate::cache::Cache;
use crate::config::Config;
use crate::telegram::TelegramBot;
use crate::tasks::outage::{Alert, Tracker};

const UNREPORTED: &str = "unreported";
const UNKNOWN_BUCKET: &str = "UNKNOWN";

pub async fn run(
    api: Arc<BiotechwareApi>,
    telegram: Arc<TelegramBot>,
    mut cache: Cache,
    cfg: Arc<Config>,
) {
    let mut tracker = Tracker::new("checker", Duration::from_secs(cfg.checker_interval_secs));
    let mut first_tick = true;
    info!(interval_secs = cfg.checker_interval_secs, "checker started");
    loop {
        let timer = crate::metrics::get()
            .task_iteration_duration
            .with_label_values(&["checker"])
            .start_timer();
        match tick(&api, &telegram, &mut cache, &mut first_tick).await {
            Ok(()) => {
                let alert = tracker.record_success();
                handle_alert(&telegram, alert, "checker").await;
            }
            Err(e) => {
                error!(error = ?e, "checker tick failed");
                let alert = tracker.record_failure();
                handle_alert(&telegram, alert, "checker").await;
            }
        }
        timer.observe_duration();
        sleep(tracker.next_delay()).await;
    }
}

async fn handle_alert(telegram: &TelegramBot, alert: Alert, task: &str) {
    match alert {
        Alert::None => {}
        Alert::Degraded => {
            telegram.send(&format!("⚠️ API Biotechware non raggiungibile (task: {}).", task), false).await;
        }
        Alert::Recovered => {
            telegram.send(&format!("✅ API Biotechware ripristinata (task: {}).", task), false).await;
        }
    }
}

#[instrument(skip_all)]
async fn tick(
    api: &BiotechwareApi,
    telegram: &TelegramBot,
    cache: &mut Cache,
    first_tick: &mut bool,
) -> anyhow::Result<()> {
    let records = api.fetch_all(ListType::Unreported, FetchOptions::default()).await?;

    // Persist (id -> service_level) so the resumes task can resolve specific
    // ECG subtypes even though its endpoint returns the generic name only.
    let levels: Vec<(String, String)> = records
        .iter()
        .filter_map(|r| {
            r.effective_service_level
                .clone()
                .map(|sl| (r.id.clone(), sl))
        })
        .collect();
    cache.store_service_levels(&levels).await?;

    let current_ids: HashSet<String> =
        records.iter().map(|r| r.id.clone()).collect();
    let known_ids = cache.known_ids(UNREPORTED).await.unwrap_or_default();

    let counts = count_by_bucket(&records);
    publish_gauges(&counts);

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
            .and_then(|r| should_show_expiry_line(now, r));

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

    let ids: Vec<String> = records.iter().map(|r| r.id.clone()).collect();
    cache.replace_known_ids(UNREPORTED, &ids).await?;

    info!(
        total = records.len(),
        first_tick = *first_tick,
        "checker tick done"
    );
    Ok(())
}

fn bucket_of(r: &Record) -> String {
    r.effective_service_level
        .clone()
        .unwrap_or_else(|| UNKNOWN_BUCKET.to_string())
}

fn count_by_bucket(records: &[Record]) -> HashMap<String, usize> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for r in records {
        *counts.entry(bucket_of(r)).or_insert(0) += 1;
    }
    counts
}

fn publish_gauges(counts: &HashMap<String, usize>) {
    let m = crate::metrics::get();
    for (k, v) in counts {
        m.records_seen
            .with_label_values(&[UNREPORTED, k])
            .set(*v as f64);
    }
}

fn priority(bucket: &str) -> u8 {
    if is_short_term(Some(bucket)) {
        0
    } else {
        1
    }
}

fn sorted_lines(counts: &HashMap<String, usize>) -> Vec<String> {
    let mut entries: Vec<(&String, &usize)> = counts.iter().collect();
    entries.sort_by(|a, b| {
        priority(a.0)
            .cmp(&priority(b.0))
            .then_with(|| a.0.cmp(b.0))
    });
    entries
        .into_iter()
        .map(|(k, v)| format!("{k}: {v}"))
        .collect()
}

fn find_next_expiring(records: &[Record]) -> Option<&Record> {
    records
        .iter()
        .filter(|r| r.deadline().is_some())
        .min_by_key(|r| r.deadline().unwrap())
}

fn should_show_expiry_line(now: DateTime<Utc>, record: &Record) -> Option<String> {
    let deadline = record.deadline()?;
    let diff_mins = (deadline - now).num_minutes();
    // Only show expiry line if remaining time < 5 minutes
    if diff_mins < 5 {
        Some(render_expiry_line(record, deadline, now))
    } else {
        None
    }
}

fn render_expiry_line(record: &Record, deadline: DateTime<Utc>, now: DateTime<Utc>) -> String {
    let sl = record.effective_service_level.as_deref().unwrap_or("?");
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

fn render_summary(counts: &HashMap<String, usize>, total: usize) -> String {
    if total == 0 {
        return "Bot avviato. Nessun ECG in coda.".to_string();
    }
    format!(
        "Bot avviato. ECG attualmente in coda ({total}):\n{}",
        sorted_lines(counts).join("\n")
    )
}

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
        let deadline = r.deadline().unwrap();
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-04T10:08:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let line = render_expiry_line(&r, deadline, now);
        assert_eq!(line, "⏰ Prossima scadenza: TR-ECG-20 — #1284221 — scade tra 12 min");
    }

    #[test]
    fn render_expiry_line_already_expired() {
        let r = record_with_sl("1284221", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        let deadline = r.deadline().unwrap();
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-04T10:25:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let line = render_expiry_line(&r, deadline, now);
        assert_eq!(line, "🔴 Prossima scadenza: TR-ECG-20 — #1284221 — scaduta da 5 min");
    }

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

    #[test]
    fn render_new_includes_summary_block() {
        let mut new_counts = HashMap::new();
        new_counts.insert("TR-ECG-20".to_string(), 1);
        let mut all_counts = HashMap::new();
        all_counts.insert("TR-ECG-20".to_string(), 2);
        all_counts.insert("TR-HC-24".to_string(), 1);
        let s = render_new(&new_counts, &all_counts, 3, None);
        assert!(s.contains("Nuovi ECG disponibili"));
        assert!(s.contains("TR-ECG-20: 1"));
        assert!(s.contains("Coda attuale (3):"));
        assert!(s.contains("TR-HC-24: 1"));
    }

    #[test]
    fn expiry_line_filtering_hides_when_more_than_5_mins() {
        // TR-ECG-20 has 20 minute SLA, reception at 10:00:00
        // so deadline is at 10:20:00
        // If now is 10:08:00, remaining = 12 minutes
        let r = record_with_sl("1284221", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-04T10:08:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let result = should_show_expiry_line(now, &r);
        assert!(result.is_none(), "Expiry line should be hidden when > 5 minutes remain");
    }

    #[test]
    fn expiry_line_filtering_shows_when_less_than_5_mins() {
        // TR-ECG-20 has 20 minute SLA, reception at 10:00:00
        // so deadline is at 10:20:00
        // If now is 10:17:00, remaining = 3 minutes (< 5)
        let r = record_with_sl("1284221", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-04T10:17:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let result = should_show_expiry_line(now, &r);
        assert!(result.is_some(), "Expiry line should be shown when < 5 minutes remain");
        assert!(result.unwrap().contains("scade tra 3 min"));
    }

    #[test]
    fn expiry_line_filtering_shows_at_exactly_boundary() {
        // TR-ECG-20 has 20 minute SLA, reception at 10:00:00
        // so deadline is at 10:20:00
        // If now is 10:16:00, remaining = 4 minutes (< 5)
        let r = record_with_sl("1284221", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-04T10:16:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let result = should_show_expiry_line(now, &r);
        assert!(result.is_some(), "Expiry line should be shown at 4 minutes (boundary)");
    }

    #[test]
    fn expiry_line_filtering_hides_when_at_5_min_boundary() {
        // TR-ECG-20 has 20 minute SLA, reception at 10:00:00
        // so deadline is at 10:20:00
        // If now is 10:15:00, remaining = exactly 5 minutes (should be hidden, < not <=)
        let r = record_with_sl("1284221", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-04T10:15:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let result = should_show_expiry_line(now, &r);
        assert!(result.is_none(), "Expiry line should be hidden when exactly 5 minutes remain");
    }

    #[test]
    fn expiry_line_filtering_shows_when_expired() {
        // TR-ECG-20 has 20 minute SLA, reception at 10:00:00
        // so deadline is at 10:20:00
        // If now is 10:25:00, already expired by 5 minutes (still < 5 mins elapsed? No.)
        // Actually, 10:25 - 10:20 = 5 minutes expired, and diff_mins = -5, which is < 5
        let r = record_with_sl("1284221", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-04T10:25:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let result = should_show_expiry_line(now, &r);
        assert!(result.is_some(), "Expiry line should be shown when expired (negative time is < 5)");
        assert!(result.unwrap().contains("scaduta"));
    }
}
