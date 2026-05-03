use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{info, warn};

/// Result of a state transition. The caller is responsible for actually
/// sending the Telegram alert; the tracker only decides whether one should
/// be sent. This keeps the tracker pure and unit-testable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Alert {
    None,
    Degraded,
    Recovered,
}

const DEFAULT_SCHEDULE: &[Duration] = &[
    Duration::from_secs(60),
    Duration::from_secs(120),
    Duration::from_secs(240),
    Duration::from_secs(480),
    Duration::from_secs(900),
];

const DEFAULT_ALERT_THRESHOLD: u32 = 3;

pub struct Tracker {
    task: &'static str,
    consecutive_failures: u32,
    announced: bool,
    healthy_interval: Duration,
    schedule: &'static [Duration],
    alert_threshold: u32,
}

impl Tracker {
    pub fn new(task: &'static str, healthy_interval: Duration) -> Self {
        Self::with_config(
            task,
            healthy_interval,
            DEFAULT_SCHEDULE,
            DEFAULT_ALERT_THRESHOLD,
        )
    }

    pub fn with_config(
        task: &'static str,
        healthy_interval: Duration,
        schedule: &'static [Duration],
        alert_threshold: u32,
    ) -> Self {
        Self {
            task,
            consecutive_failures: 0,
            announced: false,
            healthy_interval,
            schedule,
            alert_threshold,
        }
    }

    pub fn next_delay(&self) -> Duration {
        if self.consecutive_failures == 0 {
            return self.healthy_interval;
        }
        let idx = (self.consecutive_failures as usize - 1).min(self.schedule.len() - 1);
        self.schedule[idx]
    }

    pub fn record_failure(&mut self) -> Alert {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if let Some(m) = crate::metrics::try_get() {
            m.consecutive_failures
                .with_label_values(&[self.task])
                .set(self.consecutive_failures as f64);
        }
        if !self.announced && self.consecutive_failures >= self.alert_threshold {
            self.announced = true;
            if let Some(m) = crate::metrics::try_get() {
                m.outage_state.with_label_values(&[self.task]).set(1.0);
            }
            warn!(
                task = self.task,
                consecutive = self.consecutive_failures,
                "outage threshold reached, announcing degraded state"
            );
            return Alert::Degraded;
        }
        Alert::None
    }

    pub fn record_success(&mut self) -> Alert {
        let was_announced = self.announced;
        self.consecutive_failures = 0;
        self.announced = false;
        if let Some(m) = crate::metrics::try_get() {
            m.consecutive_failures
                .with_label_values(&[self.task])
                .set(0.0);
            m.outage_state.with_label_values(&[self.task]).set(0.0);
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);
            m.last_success_timestamp_seconds
                .with_label_values(&[self.task])
                .set(now);
        }
        if was_announced {
            info!(task = self.task, "recovered from outage");
            Alert::Recovered
        } else {
            Alert::None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t() -> Tracker {
        Tracker::new("test", Duration::from_secs(180))
    }

    #[test]
    fn next_delay_healthy_when_no_failures() {
        let tr = t();
        assert_eq!(tr.next_delay(), Duration::from_secs(180));
    }

    #[test]
    fn next_delay_follows_schedule() {
        let mut tr = t();
        tr.record_failure();
        assert_eq!(tr.next_delay(), Duration::from_secs(60));
        tr.record_failure();
        assert_eq!(tr.next_delay(), Duration::from_secs(120));
        tr.record_failure();
        assert_eq!(tr.next_delay(), Duration::from_secs(240));
        tr.record_failure();
        assert_eq!(tr.next_delay(), Duration::from_secs(480));
        tr.record_failure();
        assert_eq!(tr.next_delay(), Duration::from_secs(900));
    }

    #[test]
    fn next_delay_caps_at_last_schedule_step() {
        let mut tr = t();
        for _ in 0..20 {
            tr.record_failure();
        }
        assert_eq!(tr.next_delay(), Duration::from_secs(900));
    }

    #[test]
    fn announces_after_threshold() {
        let mut tr = t();
        assert_eq!(tr.record_failure(), Alert::None);
        assert_eq!(tr.record_failure(), Alert::None);
        assert_eq!(tr.record_failure(), Alert::Degraded);
    }

    #[test]
    fn does_not_re_announce() {
        let mut tr = t();
        tr.record_failure();
        tr.record_failure();
        assert_eq!(tr.record_failure(), Alert::Degraded);
        assert_eq!(tr.record_failure(), Alert::None);
        assert_eq!(tr.record_failure(), Alert::None);
    }

    #[test]
    fn recovery_alert_only_if_announced() {
        let mut tr = t();
        // Two failures, no announcement, then success: no recovery alert
        tr.record_failure();
        tr.record_failure();
        assert_eq!(tr.record_success(), Alert::None);
    }

    #[test]
    fn recovery_alert_after_announcement() {
        let mut tr = t();
        tr.record_failure();
        tr.record_failure();
        tr.record_failure();
        assert_eq!(tr.record_success(), Alert::Recovered);
    }

    #[test]
    fn second_success_does_not_re_recover() {
        let mut tr = t();
        tr.record_failure();
        tr.record_failure();
        tr.record_failure();
        assert_eq!(tr.record_success(), Alert::Recovered);
        assert_eq!(tr.record_success(), Alert::None);
    }

    #[test]
    fn success_resets_failure_count() {
        let mut tr = t();
        tr.record_failure();
        tr.record_failure();
        tr.record_success();
        assert_eq!(tr.next_delay(), Duration::from_secs(180));
    }

    #[test]
    fn re_outage_after_recovery_re_alerts() {
        let mut tr = t();
        for _ in 0..3 {
            tr.record_failure();
        }
        tr.record_success();
        // New outage should announce again
        assert_eq!(tr.record_failure(), Alert::None);
        assert_eq!(tr.record_failure(), Alert::None);
        assert_eq!(tr.record_failure(), Alert::Degraded);
    }
}
