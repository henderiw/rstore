use crate::watch::WatchEvent;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug, Clone)]
pub struct GetOptions {
    pub commit: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListOptions {
    pub commit: Option<String>,
    pub watch_only: bool,
}

pub type StorerValue<T> = Arc<T>;

#[async_trait]
pub trait Storer<K, T>
where
    K: Eq + std::hash::Hash + Clone + Debug + Send + Sync + 'static,
    T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
{
    async fn get(&self, key: K, opts: Option<GetOptions>)
        -> Result<StorerValue<T>, Box<dyn Error>>;
    async fn list(
        &self,
        visitor_fn: Box<dyn Fn(K, StorerValue<T>) + Send>,
        opts: Option<ListOptions>,
    ) -> Result<(), Box<dyn Error>>;
    async fn list_keys(&self, opts: Option<ListOptions>) -> Vec<String>;
    async fn len(&self, opts: Option<ListOptions>) -> usize;
    async fn apply(&self, key: K, value: T) -> Result<StorerValue<T>, Box<dyn Error>>;
    async fn create(&self, key: K, value: T) -> Result<StorerValue<T>, Box<dyn Error>>;
    async fn update(&self, key: K, value: T) -> Result<StorerValue<T>, Box<dyn Error>>;
    async fn delete(&self, key: K) -> Result<(), Box<dyn Error>>;
    async fn watch(
        &self,
        ctx: oneshot::Receiver<()>,
        opts: Option<ListOptions>,
    ) -> Result<
        (
            oneshot::Sender<()>,
            ReceiverStream<WatchEvent<K, StorerValue<T>>>,
        ),
        Box<dyn Error>,
    >;
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
    ) -> Result<StorerValue<T>, Box<dyn Error>> {
        self.deref().get(key, opts).await
    }

    async fn list(
        &self,
        visitor_fn: Box<dyn Fn(K, StorerValue<T>) + Send>,
        opts: Option<ListOptions>,
    ) -> Result<(), Box<dyn Error>> {
        self.deref().list(visitor_fn, opts).await
    }

    async fn list_keys(&self, opts: Option<ListOptions>) -> Vec<String> {
        self.deref().list_keys(opts).await
    }

    async fn len(&self, opts: Option<ListOptions>) -> usize {
        self.deref().len(opts).await
    }

    async fn apply(&self, key: K, value: T) -> Result<StorerValue<T>, Box<dyn Error>> {
        self.deref().apply(key, value).await
    }

    async fn create(&self, key: K, value: T) -> Result<StorerValue<T>, Box<dyn Error>> {
        self.deref().create(key, value).await
    }

    async fn update(&self, key: K, value: T) -> Result<StorerValue<T>, Box<dyn Error>> {
        self.deref().update(key, value).await
    }

    async fn delete(&self, key: K) -> Result<(), Box<dyn Error>> {
        self.deref().delete(key).await
    }

    async fn watch(
        &self,
        ctx: oneshot::Receiver<()>,
        opts: Option<ListOptions>,
    ) -> Result<
        (
            oneshot::Sender<()>,
            ReceiverStream<WatchEvent<K, StorerValue<T>>>,
        ),
        Box<dyn Error>,
    > {
        self.deref().watch(ctx, opts).await
    }
}
