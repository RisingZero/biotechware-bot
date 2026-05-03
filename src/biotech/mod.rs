pub mod models;

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::{Client, ClientBuilder, StatusCode};
use tokio::sync::Mutex;
use tracing::{debug, info, instrument, warn};

use crate::errors::{Error, Result};
use crate::retry::{self, RetryConfig};
use models::{GetRecordsRequest, GetRecordsResponse, ListType, LoginRequest, Record};

const API_URL: &str = "https://portal.biotechware.com";
const TYPE_LOG: &str = "physician";
const SESSION_TTL_HOURS: i64 = 1;
/// Negative starting counter: returns the records that the web UI normally
/// shows in the static HTML table. Without this we'd miss the first ~10
/// records on each fetch.
const FIRST_COUNTER: i32 = -10;
const HTTP_TIMEOUT_SECS: u64 = 30;

pub struct BiotechwareApi {
    client: Client,
    username: String,
    password: String,
    last_login: Mutex<Option<DateTime<Utc>>>,
    max_concurrent: usize,
}

impl BiotechwareApi {
    pub fn new(username: String, password: String, max_concurrent: usize) -> Result<Arc<Self>> {
        let client = ClientBuilder::new()
            .cookie_store(true)
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .build()?;
        Ok(Arc::new(Self {
            client,
            username,
            password,
            last_login: Mutex::new(None),
            max_concurrent,
        }))
    }

    #[instrument(skip(self), fields(user = %self.username))]
    async fn force_login(&self) -> Result<()> {
        let resp = self
            .client
            .post(format!(
                "{API_URL}/login_handler?__logins=0&came_from=%2F"
            ))
            .form(&LoginRequest {
                login: self.username.clone(),
                password: self.password.clone(),
            })
            .send()
            .await?;
        let status = resp.status();
        if status != StatusCode::OK {
            crate::metrics::get()
                .login_total
                .with_label_values(&["fail"])
                .inc();
            warn!(%status, "login failed");
            return Err(Error::LoginFailed {
                status: status.as_u16(),
            });
        }
        crate::metrics::get()
            .login_total
            .with_label_values(&["ok"])
            .inc();
        let mut g = self.last_login.lock().await;
        *g = Some(Utc::now());
        info!("logged in to biotechware");
        Ok(())
    }

    #[instrument(skip(self), fields(user = %self.username))]
    async fn ensure_login(&self) -> Result<()> {
        let needs = {
            let g = self.last_login.lock().await;
            match *g {
                Some(t) => Utc::now() - t > chrono::Duration::hours(SESSION_TTL_HOURS),
                None => true,
            }
        };
        if !needs {
            debug!("session valid, skipping login");
            return Ok(());
        }
        self.force_login().await
    }

    #[instrument(skip(self))]
    async fn fetch_page(
        &self,
        list_type: ListType,
        counter: i32,
    ) -> Result<GetRecordsResponse> {
        self.ensure_login().await?;

        debug!(list_type = list_type.to_str(), counter, "fetching page");
        let api = &*self;
        let client = self.client.clone();
        let timer = crate::metrics::get()
            .fetch_duration
            .with_label_values(&[list_type.to_str()])
            .start_timer();
        let result = retry::with_retry(
            RetryConfig::default(),
            "get_other_records",
            || {
                let api = api;
                api.force_login()
            },
            move || {
                let client = client.clone();
                async move {
                    let resp = client
                        .get(format!("{API_URL}/manage/ecg/get_other_records"))
                        .query(&GetRecordsRequest {
                            counter,
                            type_log: TYPE_LOG.to_string(),
                            list_type,
                            record_types_filter: String::new(),
                            search_filter: String::new(),
                        })
                        .send()
                        .await?;
                    let status = resp.status();
                    if !status.is_success() {
                        crate::metrics::get()
                            .http_errors_total
                            .with_label_values(&["get_other_records", &status.as_u16().to_string()])
                            .inc();
                        return Err(Error::BadStatus {
                            endpoint: "get_other_records",
                            status: status.as_u16(),
                        });
                    }
                    Ok(resp.json::<GetRecordsResponse>().await?)
                }
            },
        )
        .await?;
        timer.observe_duration();

        debug!(
            list_type = list_type.to_str(),
            count = result.count,
            next_counter = result.counter,
            "page received"
        );
        crate::metrics::get()
            .fetch_pages_total
            .with_label_values(&[list_type.to_str()])
            .inc();

        Ok(result)
    }

    /// Fetch every record of a given list type. Starts at counter = FIRST_COUNTER
    /// to capture the records the API otherwise serves only via the HTML table,
    /// then advances the counter by the number of items returned in each page
    /// until the API reports zero.
    #[instrument(skip(self))]
    pub async fn fetch_all(&self, list_type: ListType) -> Result<Vec<Record>> {
        self.ensure_login().await?;
        let mut all = Vec::new();
        let mut counter = FIRST_COUNTER;
        loop {
            let page = self.fetch_page(list_type, counter).await?;
            if page.count <= 0 || page.list.is_empty() {
                break;
            }
            debug!(
                list_type = list_type.to_str(),
                page_count = page.count,
                total_so_far = all.len() + page.list.len(),
                counter,
                "page fetched"
            );
            counter += page.count;
            all.extend(page.list);
        }
        info!(n = all.len(), list_type = list_type.to_str(), "fetched all records");
        Ok(all)
    }
}
