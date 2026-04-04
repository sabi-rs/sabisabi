use anyhow::Context;
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
use serde::Serialize;
use serde::de::DeserializeOwned;

#[derive(Clone)]
pub(crate) enum HotCache {
    Disabled,
    Redis(RedisCache),
}

#[derive(Clone)]
pub(crate) struct RedisCache {
    connection: MultiplexedConnection,
    key_prefix: String,
    ttl_seconds: u64,
}

impl HotCache {
    pub(crate) async fn from_redis_url(
        redis_url: Option<&str>,
        ttl_seconds: u64,
    ) -> anyhow::Result<Self> {
        match redis_url.map(str::trim).filter(|value| !value.is_empty()) {
            Some(redis_url) => Ok(Self::Redis(
                RedisCache::connect(redis_url, ttl_seconds).await?,
            )),
            None => Ok(Self::Disabled),
        }
    }

    pub(crate) async fn get_json<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        match self {
            Self::Disabled => None,
            Self::Redis(cache) => match cache.get_json(key).await {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(cache_key = key, "redis cache read failed: {error}");
                    None
                }
            },
        }
    }

    pub(crate) async fn set_json<T: Serialize>(&self, key: &str, value: &T) {
        match self {
            Self::Disabled => {}
            Self::Redis(cache) => {
                if let Err(error) = cache.set_json(key, value).await {
                    tracing::warn!(cache_key = key, "redis cache write failed: {error}");
                }
            }
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        matches!(self, Self::Redis(_))
    }
}

impl RedisCache {
    async fn connect(redis_url: &str, ttl_seconds: u64) -> anyhow::Result<Self> {
        let client = Client::open(redis_url).context("failed to create redis client")?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .context("failed to connect to redis")?;
        Ok(Self {
            connection,
            key_prefix: String::from("sabisabi"),
            ttl_seconds,
        })
    }

    async fn get_json<T: DeserializeOwned>(&self, key: &str) -> anyhow::Result<Option<T>> {
        let mut connection = self.connection.clone();
        let payload: Option<String> = connection
            .get(self.scoped_key(key))
            .await
            .context("failed to GET cached key")?;
        payload
            .map(|value| {
                serde_json::from_str::<T>(&value).context("failed to decode cached json payload")
            })
            .transpose()
    }

    async fn set_json<T: Serialize>(&self, key: &str, value: &T) -> anyhow::Result<()> {
        let mut connection = self.connection.clone();
        let payload =
            serde_json::to_string(value).context("failed to encode cached json payload")?;
        let _: () = connection
            .set_ex(self.scoped_key(key), payload, self.ttl_seconds)
            .await
            .context("failed to SETEX cached key")?;
        Ok(())
    }

    fn scoped_key(&self, key: &str) -> String {
        format!("{}:{}", self.key_prefix, key)
    }
}
