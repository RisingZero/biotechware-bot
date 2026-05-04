use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};

#[derive(Serialize, Debug)]
pub struct LoginRequest {
    pub login: String,
    pub password: String,
}

#[derive(Serialize, Debug)]
pub struct GetRecordsRequest {
    pub counter: i32,
    pub type_log: String,
    pub list_type: ListType,
    pub record_types_filter: String,
    #[serde(rename = "searchFilter")]
    pub search_filter: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct GetRecordsResponse {
    pub count: i32,
    #[serde(default)]
    pub counter: i32,
    pub list: Vec<Record>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Record {
    #[serde(default)]
    pub status: String,
    pub id: String,
    #[serde(default, alias = "timezone_indication")]
    pub timezone_indication: Option<String>,
    #[serde(default)]
    pub last_report_revision: Option<String>,
    #[serde(default, with = "report_date_format")]
    pub last_report_date: Option<DateTime<Utc>>,
    #[serde(default)]
    pub record_type_id: Option<i32>,
    #[serde(default, alias = "record_type_name")]
    pub record_type_name: String,
    #[serde(default)]
    pub firstname: Option<String>,
    #[serde(default)]
    pub lastname: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub health_code: Option<String>,
    #[serde(default)]
    pub reception_date: Option<String>,
    #[serde(default)]
    pub date: Option<String>,
    #[serde(default)]
    pub requester: Option<String>,
    #[serde(default)]
    pub effective_service_level: Option<String>,
}

impl Record {
    pub fn deadline(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        let date_str = self.reception_date.as_deref()?;
        let parsed = chrono::DateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S%.f%z").ok()?;
        let duration = sla_duration(self.effective_service_level.as_deref())?;
        Some(parsed.with_timezone(&chrono::Utc) + duration)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ListType {
    Reported,
    Unreported,
}

impl ListType {
    pub fn to_str(&self) -> &'static str {
        match self {
            ListType::Reported => "reportedRecords",
            ListType::Unreported => "unreportedRecords",
        }
    }
}

impl Serialize for ListType {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.to_str())
    }
}

mod report_date_format {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    const FORMAT: &str = "%d/%m/%Y %H:%M";

    #[allow(dead_code)]
    pub fn serialize<S: Serializer>(
        d: &Option<DateTime<Utc>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match d {
            Some(d) => s.serialize_str(&d.format(FORMAT).to_string()),
            None => s.serialize_str("NA"),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<DateTime<Utc>>, D::Error> {
        let s = String::deserialize(d)?;
        if s.is_empty() || s == "NA" {
            return Ok(None);
        }
        match NaiveDateTime::parse_from_str(&s, FORMAT) {
            Ok(ndt) => Ok(Some(DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))),
            Err(_) => Ok(None),
        }
    }
}

/// Records whose service-level corresponds to short turnaround ECG (20/90 min,
/// or same-day). These are time-sensitive and the operator wants quick alerts.
pub fn is_short_term(service_level: Option<&str>) -> bool {
    match service_level {
        Some(s) => s.contains("ECG-20") || s.contains("ECG-90") || s.contains("ECG-DAY"),
        None => false,
    }
}

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

    #[test]
    fn deadline_none_when_reception_date_malformed() {
        let mut r = record_with_deadline("4", "2026-05-04T10:00:00.000+0000", "TR-ECG-20");
        r.reception_date = Some("not-a-date".to_string());
        assert!(r.deadline().is_none());
    }
}
