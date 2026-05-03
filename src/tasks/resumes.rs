use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{Datelike, Utc};
use tokio::time::sleep;
use tracing::{error, info, instrument};

use crate::biotech::models::{ListType, Record};
use crate::biotech::{BiotechwareApi, FetchOptions};
use crate::cache::Cache;
use crate::config::Config;
use crate::telegram::TelegramBot;
use crate::tasks::outage::{Alert, Tracker};

/// Small offset so we don't fire both bots at the same instant on startup.
const STARTUP_OFFSET_SECS: u64 = 5;

pub async fn run(
    api: Arc<BiotechwareApi>,
    telegram: Arc<TelegramBot>,
    mut cache: Cache,
    cfg: Arc<Config>,
) {
    sleep(Duration::from_secs(STARTUP_OFFSET_SECS)).await;
    info!(interval_secs = cfg.resumes_interval_secs, "resumes started");
    telegram.send("Bot resoconti avviato.", true).await;
    let mut tracker = Tracker::new("resumes", Duration::from_secs(cfg.resumes_interval_secs));
    loop {
        let timer = crate::metrics::get()
            .task_iteration_duration
            .with_label_values(&["resumes"])
            .start_timer();
        match tick(&api, &telegram, &mut cache, &cfg).await {
            Ok(()) => {
                let alert = tracker.record_success();
                handle_alert(&telegram, alert, "resumes").await;
            }
            Err(e) => {
                error!(error = ?e, "resumes tick failed");
                let alert = tracker.record_failure();
                handle_alert(&telegram, alert, "resumes").await;
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
    cfg: &Config,
) -> anyhow::Result<()> {
    let now = Utc::now();
    let month = now.month();
    let year = now.year();

    let records = api.fetch_all(ListType::Reported, FetchOptions::default()).await?;
    let this_month: Vec<&Record> = records
        .iter()
        .filter(|r| {
            r.last_report_date
                .map(|d| d.month() == month && d.year() == year)
                .unwrap_or(false)
        })
        .collect();

    // Look up cached service-level mapping populated by the checker.
    let ids: Vec<String> = this_month.iter().map(|r| r.id.clone()).collect();
    let levels = cache.get_service_levels(&ids).await.unwrap_or_default();

    // Resolve effective record_type_name per record; prefer the cached service
    // level mapping when available so that ECG subtypes (20/90/DAY) are
    // priced correctly instead of collapsing into "ECG a riposo".
    let mut by_type: HashMap<String, usize> = HashMap::new();
    for r in &this_month {
        let name = match levels.get(&r.id) {
            Some(sl) => cfg
                .pricing
                .service_level_to_type
                .get(sl)
                .cloned()
                .unwrap_or_else(|| r.record_type_name.clone()),
            None => r.record_type_name.clone(),
        };
        *by_type.entry(name).or_insert(0) += 1;
    }

    let mut total_eur = 0.0;
    let mut lines: Vec<String> = Vec::new();
    let mut sorted: Vec<(&String, &usize)> = by_type.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    for (name, count) in sorted {
        match cfg.pricing.record_type_prices.get(name) {
            Some(price) => {
                let amount = price * (*count as f64);
                lines.push(format!("{name}: {amount:.2} €"));
                total_eur += amount;
            }
            None => {
                lines.push(format!("{name}: ? (prezzo non configurato, n={count})"));
            }
        }
    }
    let total_taxed = total_eur * (1.0 - cfg.pricing.tax_rate);
    lines.push(format!(
        "Totale (al netto della ritenuta): {total_taxed:.2} €"
    ));

    let month_label = format!("{year:04}-{month:02}");
    let m = crate::metrics::get();
    m.resumes_total_eur
        .with_label_values(&[&month_label, "gross"])
        .set(total_eur);
    m.resumes_total_eur
        .with_label_values(&[&month_label, "net"])
        .set(total_taxed);

    let msg = format!("Resoconto {month_label}:\n{}", lines.join("\n"));
    telegram.send(&msg, true).await;
    info!(
        total_eur,
        total_taxed,
        n_records = this_month.len(),
        "resumes tick complete"
    );
    Ok(())
}
