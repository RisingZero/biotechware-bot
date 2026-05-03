pub mod models;

use models::SendMessageBody;
use reqwest::Client;
use std::time::Duration;
use tracing::{debug, instrument, warn};

pub struct TelegramBot {
    client: Client,
    token: String,
    chat_id: String,
}

impl TelegramBot {
    pub fn new(token: String, chat_id: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .expect("build telegram http client");
        Self {
            client,
            token,
            chat_id,
        }
    }

    #[instrument(skip(self, text), fields(chat_id = %self.chat_id, len = text.len(), silent))]
    pub async fn send(&self, text: &str, silent: bool) {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.token);
        let body = SendMessageBody {
            chat_id: self.chat_id.clone(),
            text: text.to_string(),
            disable_web_page_preview: true,
            disable_notification: silent,
        };
        match self.client.post(&url).json(&body).send().await {
            Ok(r) if r.status().is_success() => {
                debug!(len = text.len(), "telegram message sent");
                crate::metrics::get()
                    .telegram_send_total
                    .with_label_values(&["ok"])
                    .inc();
            }
            Ok(r) => {
                warn!(status = %r.status(), "telegram non-success");
                crate::metrics::get()
                    .telegram_send_total
                    .with_label_values(&["non2xx"])
                    .inc();
            }
            Err(e) => {
                warn!(error = %e, "telegram send failed");
                crate::metrics::get()
                    .telegram_send_total
                    .with_label_values(&["err"])
                    .inc();
            }
        }
    }
}
