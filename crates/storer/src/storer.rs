use crate::watch::WatchEvent;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use std::pin::Pin;
use base64::{engine::general_purpose, Engine as _};

#[derive(Debug, Clone)]
pub struct GetOptions {
    pub commit: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListOptions {
    pub commit: Option<String>,
    pub watch_only: bool,
    pub limit: Option<u32>,
    pub continue_token: Option<String>,
}

//pub type StorerValue<T> = Arc<T>;

#[async_trait]
pub trait Storer<K, T>: Send + Sync
where
    K: Eq + std::hash::Hash + Clone + Debug + Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    async fn get(
        &self,
        key: K,
        opts: Option<GetOptions>,
    ) -> Result<T, Box<dyn Error + Send + Sync>>;
    async fn list(
        &self,
        opts: Option<ListOptions>,
    ) -> Result<
        (
            Pin<Box<dyn Stream<Item = (K, T)> + Send + Sync>>,
            u64,                  // resource_version
            Option<(String, u64)>,       // (continue_token, remaining_items)
        ),
        Box<dyn Error + Send + Sync>
    >;
    async fn list_keys(&self, opts: Option<ListOptions>) -> (Vec<K>, u64);
    async fn len(&self, opts: Option<ListOptions>) -> usize;
    async fn apply(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>>;
    async fn create(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>>;
    async fn update(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>>;
    async fn delete(&self, key: K) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn watch(
        &self,
        ctx: oneshot::Receiver<()>,
        opts: Option<ListOptions>,
    ) -> Result<(oneshot::Sender<()>, ReceiverStream<WatchEvent<T>>), Box<dyn Error + Send + Sync>>;
}

#[async_trait::async_trait]
impl<S, K, T> Storer<K, T> for Arc<S>
where
    S: Storer<K, T> + Send + Sync + 'static,
    K: Eq + std::hash::Hash + Clone + Debug + Send + Sync + 'static,
    T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
{
    async fn get(
        &self,
        key: K,
        opts: Option<GetOptions>,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
        self.deref().get(key, opts).await
    }

    async fn list(
        &self,
        opts: Option<ListOptions>,
    ) -> Result<
        (
            Pin<Box<dyn Stream<Item = (K, T)> + Send + Sync>>,
            u64,                    // resource_version
            Option<(String, u64)>,  // (continue_token, remaining_items)
        ),
        Box<dyn Error + Send + Sync>
    > {
        self.deref().list(opts).await
    }

    async fn list_keys(&self, opts: Option<ListOptions>) -> (Vec<K>, u64) {
        self.deref().list_keys(opts).await
    }

    async fn len(&self, opts: Option<ListOptions>) -> usize {
        self.deref().len(opts).await
    }

    async fn apply(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        self.deref().apply(key, value).await
    }

    async fn create(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        self.deref().create(key, value).await
    }

    async fn update(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        self.deref().update(key, value).await
    }

    async fn delete(&self, key: K) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.deref().delete(key).await
    }

    async fn watch(
        &self,
        ctx: oneshot::Receiver<()>,
        opts: Option<ListOptions>,
    ) -> Result<(oneshot::Sender<()>, ReceiverStream<WatchEvent<T>>), Box<dyn Error + Send + Sync>>
    {
        self.deref().watch(ctx, opts).await
    }
}


pub fn decode_continue_token(token: &str) -> Option<(u64, String)> {
    let decoded = general_purpose::STANDARD.decode(token).ok()?;
    let decoded_str = String::from_utf8(decoded).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(&decoded_str).ok()?;
    
    let rv = parsed["rv"].as_u64()?;
    let start = parsed["start"].as_str()?.to_string();
    
    Some((rv, start))
}

pub fn encode_continue_token(rv: u64, start: &str) -> String {
    let data = serde_json::json!({
        "rv": rv,
        "start": start
    });

    let json_string = serde_json::to_string(&data).unwrap_or_default();
    general_purpose::STANDARD.encode(json_string)
}
