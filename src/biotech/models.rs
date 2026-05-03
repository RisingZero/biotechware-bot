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

/// Records that are long-term holter (HC) or HP categories — handled with
/// less urgency.
pub fn is_long_term(service_level: Option<&str>) -> bool {
    match service_level {
        Some(s) => {
            s.contains("HP") || s.contains("HC") || s.contains("NO")
        }
        None => false,
    }
}
