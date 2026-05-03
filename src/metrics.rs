use prometheus::{
    histogram_opts, labels, opts, GaugeVec, HistogramVec, IntCounterVec, Registry,
};
use std::sync::OnceLock;

pub struct Metrics {
    pub registry: Registry,
    pub login_total: IntCounterVec,
    pub fetch_pages_total: IntCounterVec,
    pub fetch_duration: HistogramVec,
    pub records_seen: GaugeVec,
    pub new_records_total: IntCounterVec,
    pub resumes_total_eur: GaugeVec,
    pub telegram_send_total: IntCounterVec,
    pub redis_ops_total: IntCounterVec,
    pub task_iteration_duration: HistogramVec,
    pub http_errors_total: IntCounterVec,
    pub outage_state: GaugeVec,
    pub consecutive_failures: GaugeVec,
    pub last_success_timestamp_seconds: GaugeVec,
    pub retry_attempts_total: IntCounterVec,
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

pub fn init(user_label: &str) {
    let _ = METRICS.set(Metrics::new(user_label));
}

pub fn get() -> &'static Metrics {
    METRICS
        .get()
        .expect("metrics not initialized; call metrics::init first")
}

/// Like `get`, but returns `None` if metrics haven't been initialized.
/// Useful from helpers that may run in unit tests where the global
/// registry is never set up.
pub fn try_get() -> Option<&'static Metrics> {
    METRICS.get()
}

impl Metrics {
    fn new(user: &str) -> Self {
        let registry = Registry::new_custom(
            Some("biotech".to_string()),
            Some(labels! { "user".to_string() => user.to_string() }),
        )
        .expect("create prometheus registry");

        let login_total =
            IntCounterVec::new(opts!("login_total", "Login attempts"), &["result"]).unwrap();
        let fetch_pages_total = IntCounterVec::new(
            opts!("fetch_pages_total", "Pages fetched from get_other_records"),
            &["list_type"],
        )
        .unwrap();
        let fetch_duration = HistogramVec::new(
            histogram_opts!("fetch_duration_seconds", "Page fetch duration"),
            &["list_type"],
        )
        .unwrap();
        let records_seen = GaugeVec::new(
            opts!("records_seen", "Current count of records observed"),
            &["list_type", "bucket"],
        )
        .unwrap();
        let new_records_total = IntCounterVec::new(
            opts!("new_records_total", "Newly observed records"),
            &["bucket"],
        )
        .unwrap();
        let resumes_total_eur = GaugeVec::new(
            opts!("resumes_total_eur", "Resumes EUR total for the current month"),
            &["month", "kind"],
        )
        .unwrap();
        let telegram_send_total = IntCounterVec::new(
            opts!("telegram_send_total", "Telegram message sends"),
            &["result"],
        )
        .unwrap();
        let redis_ops_total =
            IntCounterVec::new(opts!("redis_ops_total", "Redis operations"), &["op", "result"])
                .unwrap();
        let task_iteration_duration = HistogramVec::new(
            histogram_opts!("task_iteration_duration_seconds", "Task iteration duration"),
            &["task"],
        )
        .unwrap();
        let http_errors_total = IntCounterVec::new(
            opts!("http_errors_total", "HTTP errors by endpoint"),
            &["endpoint", "status"],
        )
        .unwrap();
        let outage_state = GaugeVec::new(
            opts!("outage_state", "1 when the task is in announced-outage state"),
            &["task"],
        )
        .unwrap();
        let consecutive_failures = GaugeVec::new(
            opts!("consecutive_failures", "Consecutive tick failures"),
            &["task"],
        )
        .unwrap();
        let last_success_timestamp_seconds = GaugeVec::new(
            opts!(
                "last_success_timestamp_seconds",
                "Unix timestamp of last successful tick"
            ),
            &["task"],
        )
        .unwrap();
        let retry_attempts_total = IntCounterVec::new(
            opts!(
                "retry_attempts_total",
                "Outcomes of retry attempts per endpoint"
            ),
            &["endpoint", "outcome"],
        )
        .unwrap();

        registry.register(Box::new(login_total.clone())).unwrap();
        registry
            .register(Box::new(fetch_pages_total.clone()))
            .unwrap();
        registry.register(Box::new(fetch_duration.clone())).unwrap();
        registry.register(Box::new(records_seen.clone())).unwrap();
        registry
            .register(Box::new(new_records_total.clone()))
            .unwrap();
        registry
            .register(Box::new(resumes_total_eur.clone()))
            .unwrap();
        registry
            .register(Box::new(telegram_send_total.clone()))
            .unwrap();
        registry.register(Box::new(redis_ops_total.clone())).unwrap();
        registry
            .register(Box::new(task_iteration_duration.clone()))
            .unwrap();
        registry
            .register(Box::new(http_errors_total.clone()))
            .unwrap();
        registry.register(Box::new(outage_state.clone())).unwrap();
        registry
            .register(Box::new(consecutive_failures.clone()))
            .unwrap();
        registry
            .register(Box::new(last_success_timestamp_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(retry_attempts_total.clone()))
            .unwrap();

        Self {
            registry,
            login_total,
            fetch_pages_total,
            fetch_duration,
            records_seen,
            new_records_total,
            resumes_total_eur,
            telegram_send_total,
            redis_ops_total,
            task_iteration_duration,
            http_errors_total,
            outage_state,
            consecutive_failures,
            last_success_timestamp_seconds,
            retry_attempts_total,
        }
    }
}
