//use crate::storer::StorerValue;
//use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
pub enum WatchEvent<K, T> {
    Added(K, T),
    Modified(K, T),
    Deleted(K, T),
    Error,
}

impl<K, T> WatchEvent<K, T> {
    /// Provides a reference to the inner object, if present.
    pub fn as_ref(&self) -> Option<(&K, &T)> {
        match self {
            WatchEvent::Added(key, value) => Some((key, value)),
            WatchEvent::Modified(key, value) => Some((key, value)),
            WatchEvent::Deleted(key, value) => Some((key, value)),
            WatchEvent::Error => None,
        }
    }
}
