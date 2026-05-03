mod biotech;
mod cache;
mod config;
mod errors;
mod metrics;
mod retry;
mod server;
mod tasks;
mod telegram;

use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cfg = Arc::new(config::Config::from_env()?);
    metrics::init(&cfg.user_label);

    info!(user = %cfg.user_label, "starting biotechware-bot");

    let telegram = Arc::new(telegram::TelegramBot::new(
        cfg.tg_token.clone(),
        cfg.tg_chat_id.clone(),
    ));
    let api = biotech::BiotechwareApi::new(
        cfg.btw_username.clone(),
        cfg.btw_password.clone(),
    )?;
    let cache_checker = cache::Cache::connect(&cfg.redis_url, &cfg.user_label).await?;
    let cache_resumes = cache::Cache::connect(&cfg.redis_url, &cfg.user_label).await?;

    let metrics_addr: std::net::SocketAddr = cfg
        .metrics_addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid METRICS_ADDR {}: {e}", cfg.metrics_addr))?;
    let metrics_task = tokio::spawn(async move {
        if let Err(e) = server::serve(metrics_addr).await {
            error!(error = ?e, "metrics server stopped");
        }
    });

    let checker_task = {
        let api = api.clone();
        let tg = telegram.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move { tasks::checker::run(api, tg, cache_checker, cfg).await })
    };
    let resumes_task = {
        let api = api.clone();
        let tg = telegram.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move { tasks::resumes::run(api, tg, cache_resumes, cfg).await })
    };

    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received, aborting tasks");
    metrics_task.abort();
    checker_task.abort();
    resumes_task.abort();
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,biotechware_bot=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}
