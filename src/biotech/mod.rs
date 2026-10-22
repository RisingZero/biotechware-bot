pub mod models;

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::{Client, ClientBuilder, StatusCode};
use tokio::sync::Mutex;
use tracing::{debug, info, instrument, warn};

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

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
const EARLY_STOP_GRACE_PAGES: usize = 10;

pub struct FetchOptions {
    pub record_filter: Option<Box<dyn Fn(&Record) -> bool + Send + Sync>>,
}

impl Default for FetchOptions {
    fn default() -> Self {
        Self { record_filter: None }
    }
}

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

    #[instrument(skip(self, opts))]
    pub async fn fetch_all(&self, list_type: ListType, opts: FetchOptions) -> crate::errors::Result<Vec<Record>> {
        let bootstrap = self.fetch_page(list_type, FIRST_COUNTER).await?;
        if bootstrap.list.is_empty() || bootstrap.count <= 0 {
            info!(list_type = list_type.to_str(), "no records found");
            return Ok(vec![]);
        }
        // A bootstrap page with fewer records than the initial counter offset
        // means the entire dataset fits in this single response — no further
        // pages exist. Apply the filter and return immediately.
        if bootstrap.count < -FIRST_COUNTER {
            let records: Vec<Record> = match &opts.record_filter {
                Some(f) => bootstrap.list.into_iter().filter(|r| f(r)).collect(),
                None => bootstrap.list,
            };
            info!(n = records.len(), list_type = list_type.to_str(), "fetched all records (single page)");
            return Ok(records);
        }
        let records = fetch_all_sliding(bootstrap, FIRST_COUNTER, self.max_concurrent, opts, |c| {
            self.fetch_page(list_type, c)
        })
        .await?;
        info!(n = records.len(), list_type = list_type.to_str(), "fetched all records");
        Ok(records)
    }
}

async fn fetch_all_sliding<F, Fut>(
    bootstrap: GetRecordsResponse,
    first_counter: i32,
    max_concurrent: usize,
    opts: FetchOptions,
    fetch: F,
) -> crate::errors::Result<Vec<Record>>
where
    F: Fn(i32) -> Fut,
    Fut: std::future::Future<Output = crate::errors::Result<GetRecordsResponse>> + Send,
{
    let page_size = bootstrap.count;
    let max_concurrent = max_concurrent.max(1);

    let bootstrap_records: Vec<Record> = match &opts.record_filter {
        Some(f) => bootstrap.list.into_iter().filter(|r| f(r)).collect(),
        None => bootstrap.list,
    };

    let mut results: Vec<(i32, Vec<Record>)> = vec![(first_counter, bootstrap_records)];
    let mut pending: FuturesUnordered<
        std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::errors::Result<(i32, GetRecordsResponse)>> + Send>,
        >,
    > = FuturesUnordered::new();
    let mut next_counter = first_counter + page_size;
    let mut stop = false;
    let mut grace_misses: usize = 0;

    for _ in 0..max_concurrent {
        let c = next_counter;
        pending.push(Box::pin(fetch(c).map(move |r| r.map(|p| (c, p)))));
        next_counter += page_size;
    }

    while let Some(result) = pending.next().await {
        let (counter, page) = result?;
        if page.list.is_empty() || page.count <= 0 {
            stop = true;
        } else {
            let matched: Vec<Record> = match &opts.record_filter {
                Some(f) => page.list.into_iter().filter(|r| f(r)).collect(),
                None => page.list,
            };

            if opts.record_filter.is_some() {
                if matched.is_empty() {
                    grace_misses += 1;
                    if grace_misses >= EARLY_STOP_GRACE_PAGES {
                        stop = true;
                    }
                } else {
                    grace_misses = 0;
                }
            }

            results.push((counter, matched));

            if !stop {
                let c = next_counter;
                pending.push(Box::pin(fetch(c).map(move |r| r.map(|p| (c, p)))));
                next_counter += page_size;
            }
        }
    }

    results.sort_unstable_by_key(|(c, _)| *c);
    Ok(results.into_iter().flat_map(|(_, records)| records).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use models::{GetRecordsResponse, Record};

    fn make_record(id: usize) -> Record {
        Record {
            id: id.to_string(),
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
            reception_date: None,
            date: None,
            requester: None,
            effective_service_level: None,
        }
    }

    fn make_page(n: usize) -> GetRecordsResponse {
        GetRecordsResponse {
            count: n as i32,
            counter: 0,
            list: (0..n).map(make_record).collect(),
        }
    }

    fn empty_page() -> GetRecordsResponse {
        GetRecordsResponse { count: 0, counter: 0, list: vec![] }
    }

    // Single bootstrap page with no further data.
    #[tokio::test]
    async fn only_bootstrap_page() {
        let bootstrap = make_page(5);
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 2, FetchOptions::default(), |_c| async {
            Ok(empty_page())
        })
        .await
        .unwrap();
        assert_eq!(result.len(), 5);
    }

    // Three pages of 10 records each, then empty.
    #[tokio::test]
    async fn collects_multiple_pages() {
        use std::sync::{Arc, Mutex};
        use std::collections::VecDeque;

        let queue: Arc<Mutex<VecDeque<GetRecordsResponse>>> = Arc::new(Mutex::new(
            VecDeque::from(vec![
                make_page(10), // speculative page 1
                make_page(10), // speculative page 2
                empty_page(),  // stop signal
                empty_page(),  // drain for concurrent window
            ]),
        ));
        let bootstrap = make_page(10);
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 2, FetchOptions::default(), move |_c| {
            let q = queue.clone();
            async move {
                Ok(q.lock().unwrap().pop_front().unwrap_or_else(empty_page))
            }
        })
        .await
        .unwrap();
        assert_eq!(result.len(), 30);
    }

    // An error from fetch_page propagates immediately.
    #[tokio::test]
    async fn propagates_fetch_error() {
        use crate::errors::Error;
        let bootstrap = make_page(10);
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 1, FetchOptions::default(), |_c| async {
            Err(Error::BadStatus { endpoint: "test", status: 500 })
        })
        .await;
        assert!(result.is_err());
    }

    // Records arrive sorted by counter regardless of completion order.
    #[tokio::test]
    async fn results_ordered_by_counter() {
        use std::sync::{Arc, Mutex};
        use std::collections::VecDeque;

        let bootstrap = make_page(1);
        let queue: Arc<Mutex<VecDeque<GetRecordsResponse>>> = Arc::new(Mutex::new(
            VecDeque::from(vec![make_page(1), make_page(1), empty_page(), empty_page()]),
        ));
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 2, FetchOptions::default(), move |_c| {
            let q = queue.clone();
            async move {
                Ok(q.lock().unwrap().pop_front().unwrap_or_else(empty_page))
            }
        })
        .await
        .unwrap();
        assert_eq!(result.len(), 3);
    }

    // Filter keeps only records matching the predicate; non-matching records
    // are discarded even when the page is non-empty.
    #[tokio::test]
    async fn filter_keeps_only_matching_records() {
        use std::sync::{Arc, Mutex};
        use std::collections::VecDeque;

        let bootstrap = GetRecordsResponse {
            count: 5,
            counter: 0,
            list: (0..5).map(make_record).collect(),
        };
        let queue: Arc<Mutex<VecDeque<GetRecordsResponse>>> = Arc::new(Mutex::new(
            VecDeque::from(vec![
                GetRecordsResponse {
                    count: 5,
                    counter: 0,
                    list: (5..10).map(make_record).collect(),
                },
                empty_page(),
                empty_page(),
            ]),
        ));
        let opts = FetchOptions {
            record_filter: Some(Box::new(|r: &Record| {
                r.id.parse::<usize>().map(|n| n % 2 == 0).unwrap_or(false)
            })),
        };
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 2, opts, move |_c| {
            let q = queue.clone();
            async move { Ok(q.lock().unwrap().pop_front().unwrap_or_else(empty_page)) }
        })
        .await
        .unwrap();

        assert_eq!(result.len(), 5);
        assert!(result.iter().all(|r| r.id.parse::<usize>().unwrap() % 2 == 0));
    }

    // After EARLY_STOP_GRACE_PAGES consecutive pages with no matching records
    // the fetch stops; pages beyond the grace window are never requested.
    #[tokio::test]
    async fn grace_window_stops_after_n_misses() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let pages_fetched = Arc::new(AtomicUsize::new(0));
        let pf = pages_fetched.clone();

        let bootstrap = GetRecordsResponse {
            count: 1,
            counter: 0,
            list: vec![make_record(0)],
        };
        let opts = FetchOptions {
            record_filter: Some(Box::new(|r: &Record| {
                r.id.parse::<usize>().map(|n| n % 2 == 0).unwrap_or(false)
            })),
        };
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 1, opts, move |_c| {
            let count = pf.fetch_add(1, Ordering::Relaxed);
            async move {
                if count < EARLY_STOP_GRACE_PAGES {
                    Ok(GetRecordsResponse { count: 1, counter: 0, list: vec![make_record(1)] })
                } else {
                    Ok(GetRecordsResponse { count: 1, counter: 0, list: vec![make_record(2)] })
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "0");
        assert_eq!(pages_fetched.load(Ordering::Relaxed), EARLY_STOP_GRACE_PAGES);
    }

    // Grace counter resets to zero whenever a matching record is found, so a
    // sequence of miss/match/miss/match pages does not trigger early stop.
    #[tokio::test]
    async fn grace_counter_resets_on_match() {
        use std::sync::{Arc, Mutex};
        use std::collections::VecDeque;

        let mut pages: Vec<GetRecordsResponse> = Vec::new();
        for _ in 0..2 {
            for _ in 0..(EARLY_STOP_GRACE_PAGES - 1) {
                pages.push(GetRecordsResponse { count: 1, counter: 0, list: vec![make_record(1)] });
            }
            pages.push(GetRecordsResponse { count: 1, counter: 0, list: vec![make_record(2)] });
        }
        for _ in 0..EARLY_STOP_GRACE_PAGES {
            pages.push(GetRecordsResponse { count: 1, counter: 0, list: vec![make_record(1)] });
        }
        pages.push(empty_page());

        let queue: Arc<Mutex<VecDeque<GetRecordsResponse>>> =
            Arc::new(Mutex::new(VecDeque::from(pages)));

        let bootstrap = GetRecordsResponse {
            count: 1,
            counter: 0,
            list: vec![make_record(0)],
        };
        let opts = FetchOptions {
            record_filter: Some(Box::new(|r: &Record| {
                r.id.parse::<usize>().map(|n| n % 2 == 0).unwrap_or(false)
            })),
        };
        let result = fetch_all_sliding(bootstrap, FIRST_COUNTER, 1, opts, move |_c| {
            let q = queue.clone();
            async move { Ok(q.lock().unwrap().pop_front().unwrap_or_else(empty_page)) }
        })
        .await
        .unwrap();

        assert_eq!(result.len(), 3);
        assert!(result.iter().all(|r| r.id.parse::<usize>().unwrap() % 2 == 0));
    }
}
