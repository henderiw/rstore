use crate::watch::WatchEvent;
use async_trait::async_trait;
use std::error::Error;
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct GetOptions {
    pub commit: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListOptions {
    pub commit: Option<String>,
    pub watch_only: bool,
}

#[async_trait]
pub trait Storer<K, T>
where
    K: Eq + std::hash::Hash + Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    async fn get(&self, key: K, opts: Option<GetOptions>) -> Result<T, Box<dyn Error>>;
    async fn list(
        &self,
        visitor_fn: Box<dyn Fn(K, T) + Send>,
        opts: Option<ListOptions>,
    ) -> Result<(), Box<dyn Error>>;
    async fn list_keys(&self, opts: Option<ListOptions>) -> Vec<String>;
    async fn len(&self, opts: Option<ListOptions>) -> usize;
    async fn apply(&self, key: K, value: T) -> Result<T, Box<dyn Error>>;
    async fn create(&self, key: K, value: T) -> Result<T, Box<dyn Error>>;
    async fn update(&self, key: K, value: T) -> Result<T, Box<dyn Error>>;
    async fn delete(&self, key: K) -> Result<(), Box<dyn Error>>;
    async fn watch(
        &self,
        ctx: tokio::sync::oneshot::Receiver<()>,
        opts: Option<ListOptions>,
    ) -> Result<
        (
            tokio::sync::oneshot::Sender<()>,
            tokio_stream::wrappers::ReceiverStream<WatchEvent<K, T>>,
        ),
        Box<dyn Error>,
    >;
}
