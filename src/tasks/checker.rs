use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;
use tracing::{error, info, instrument};

use crate::biotech::models::{is_long_term, is_short_term, ListType, Record};
use crate::biotech::BiotechwareApi;
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
    let records = api.fetch_all(ListType::Unreported).await?;

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
