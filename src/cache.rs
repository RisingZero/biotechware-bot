use redis::{aio::ConnectionManager, AsyncCommands, Client};
use std::collections::{HashMap, HashSet};

const KNOWN_TTL_SECS: i64 = 60 * 60 * 24 * 7;
const LEVELS_TTL_SECS: i64 = 60 * 60 * 24 * 60;

#[derive(Clone)]
pub struct Cache {
    conn: ConnectionManager,
    user: String,
}

impl Cache {
    pub async fn connect(url: &str, user: &str) -> anyhow::Result<Self> {
        let client = Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self {
            conn,
            user: user.to_string(),
        })
    }

    fn k_known(&self, list: &str) -> String {
        format!("biotech:{}:known:{}", self.user, list)
    }

    fn k_levels(&self) -> String {
        format!("biotech:{}:service_levels", self.user)
    }

    pub async fn known_ids(&mut self, list: &str) -> anyhow::Result<HashSet<String>> {
        let key = self.k_known(list);
        match self.conn.smembers::<_, Vec<String>>(&key).await {
            Ok(v) => {
                crate::metrics::get()
                    .redis_ops_total
                    .with_label_values(&["smembers", "ok"])
                    .inc();
                Ok(v.into_iter().collect())
            }
            Err(e) => {
                crate::metrics::get()
                    .redis_ops_total
                    .with_label_values(&["smembers", "err"])
                    .inc();
                Err(e.into())
            }
        }
    }

    pub async fn replace_known_ids(&mut self, list: &str, ids: &[String]) -> anyhow::Result<()> {
        let key = self.k_known(list);
        let mut pipe = redis::pipe();
        pipe.atomic().del(&key);
        if !ids.is_empty() {
            pipe.sadd(&key, ids);
            pipe.expire(&key, KNOWN_TTL_SECS);
        }
        let result: redis::RedisResult<()> = pipe.query_async(&mut self.conn).await;
        let label = if result.is_ok() { "ok" } else { "err" };
        crate::metrics::get()
            .redis_ops_total
            .with_label_values(&["replace_known", label])
            .inc();
        result.map_err(Into::into)
    }

    pub async fn store_service_levels(
        &mut self,
        items: &[(String, String)],
    ) -> anyhow::Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let key = self.k_levels();
        let pairs: Vec<(&str, &str)> =
            items.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let _: () = self.conn.hset_multiple(&key, &pairs).await?;
        let _: () = self.conn.expire(&key, LEVELS_TTL_SECS).await?;
        crate::metrics::get()
            .redis_ops_total
            .with_label_values(&["hset_multiple", "ok"])
            .inc();
        Ok(())
    }

    pub async fn get_service_levels(
        &mut self,
        ids: &[String],
    ) -> anyhow::Result<HashMap<String, String>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let key = self.k_levels();
        let vals: Vec<Option<String>> = self.conn.hget(&key, ids).await?;
        crate::metrics::get()
            .redis_ops_total
            .with_label_values(&["hget", "ok"])
            .inc();
        Ok(ids
            .iter()
            .zip(vals)
            .filter_map(|(k, v)| v.map(|v| (k.clone(), v)))
            .collect())
    }
}
