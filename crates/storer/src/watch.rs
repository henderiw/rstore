//use crate::storer::StorerValue;
//use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
pub enum WatchEvent<T> {
    Added(T),
    Modified(T),
    Deleted(T),
    Error,
}

impl<T> WatchEvent<T> {
    /// Provides a reference to the inner object, if present.
    pub fn as_ref(&self) -> Option<&T> {
        match self {
            WatchEvent::Added(value) => Some(value),
            WatchEvent::Modified(value) => Some(value),
            WatchEvent::Deleted(value) => Some(value),
            WatchEvent::Error => None,
        }
    }
}
