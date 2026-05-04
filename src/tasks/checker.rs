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
    let s = Some(bucket);
    if is_short_term(s) {
        0
    } else if is_long_term(s) {
        2
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

fn render_new(new_counts: &HashMap<String, usize>) -> String {
    format!(
        "Nuovi ECG disponibili\n{}",
        sorted_lines(new_counts).join("\n")
    )
}

fn render_all_clear(counts: &HashMap<String, usize>) -> String {
    let long_term: HashMap<String, usize> = counts
        .iter()
        .filter(|(sl, _)| is_long_term(Some(sl.as_str())))
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    if long_term.is_empty() {
        "ECG tutti refertati, niente di nuovo :D\nNiente di altro.".to_string()
    } else {
        format!(
            "ECG tutti refertati, niente di nuovo :D\nResoconto attuale:\n{}",
            sorted_lines(&long_term).join("\n")
        )
    }
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
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-04T10:08:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let line = render_expiry_line(&r, now);
        assert_eq!(line, "⏰ Prossima scadenza: TR-ECG-20 — #1284221 — scade tra 12 min");
    }

    #[test]
    fn render_expiry_line_already_expired() {
        let r = record_with_sl("1284221", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-04T10:25:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let line = render_expiry_line(&r, now);
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
}
