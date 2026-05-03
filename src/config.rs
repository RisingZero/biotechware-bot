use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Config {
    pub user_label: String,
    pub btw_username: String,
    pub btw_password: String,
    pub tg_token: String,
    pub tg_chat_id: String,
    pub redis_url: String,
    pub metrics_addr: String,
    pub checker_interval_secs: u64,
    pub resumes_interval_secs: u64,
    pub pricing: Pricing,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Pricing {
    pub tax_rate: f64,
    pub record_type_prices: HashMap<String, f64>,
    #[serde(default)]
    pub service_level_to_type: HashMap<String, String>,
}

impl Pricing {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read pricing file {}: {}", path.display(), e))?;
        let p: Pricing = toml::from_str(&s)
            .map_err(|e| anyhow::anyhow!("parse pricing file {}: {}", path.display(), e))?;
        Ok(p)
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();
        let pricing_path = env_or("PRICING_FILE", "pricing.toml");
        let pricing = Pricing::load(Path::new(&pricing_path))?;
        Ok(Self {
            user_label: env_or("USER_LABEL", "default"),
            btw_username: env_req("BTW_USERNAME")?,
            btw_password: env_req("BTW_PASSWORD")?,
            tg_token: env_req("TG_TOKEN")?,
            tg_chat_id: env_req("TG_CHAT_ID")?,
            redis_url: env_or("REDIS_URL", "redis://127.0.0.1:6379"),
            metrics_addr: env_or("METRICS_ADDR", "0.0.0.0:9090"),
            checker_interval_secs: env_or("CHECKER_INTERVAL_SECS", "180").parse()?,
            resumes_interval_secs: env_or("RESUMES_INTERVAL_SECS", "21600").parse()?,
            pricing,
        })
    }
}

fn env_req(k: &str) -> anyhow::Result<String> {
    std::env::var(k).map_err(|_| anyhow::anyhow!("missing env var: {k}"))
}

fn env_or(k: &str, default: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| default.to_string())
}
