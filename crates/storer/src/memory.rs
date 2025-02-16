use crate::error::{AlreadyExists, NotFoundError};
use crate::storer::{GetOptions, ListOptions, Storer, StorerValue};
use crate::watch::WatchEvent;
use crate::watcher_manager::WatcherManager;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::cmp::{Eq, Ord};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

pub struct Memory<K, T>
where
    K: Ord + Eq + Hash + Clone + Debug + Send + Sync + 'static,
    T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
{
    store: Arc<RwLock<HashMap<K, StorerValue<T>>>>,
    watcher_manager: Arc<WatcherManager<K, Arc<T>>>,
    event_sender: mpsc::Sender<WatchEvent<K, Arc<T>>>,
}

impl<K, T> Memory<K, T>
where
    K: Ord + Eq + Hash + Clone + Debug + Send + Sync + 'static,
    T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
{
    pub fn new() -> Self {
        let store: Arc<RwLock<HashMap<K, StorerValue<T>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (watcher_manager, event_sender) = WatcherManager::new(store.clone(), 1024);
        Self {
            store: store,
            watcher_manager: Arc::new(watcher_manager),
            event_sender: event_sender,
        }
    }
    async fn notify_watcher_manager(&self, event: WatchEvent<K, StorerValue<T>>) {
        if self.watcher_manager.is_running().await {
            _ = self.event_sender.send(event).await;
            return;
        }
    }
}

#[async_trait]
impl<K, T> Storer<K, T> for Memory<K, T>
where
    K: Ord + Eq + Hash + Clone + Debug + Send + Sync + 'static,
    T: Serialize + DeserializeOwned + Debug + Send + Sync + 'static,
{
    async fn get(
        &self,
        key: K,
        _opts: Option<GetOptions>,
    ) -> Result<StorerValue<T>, Box<dyn Error>> {
        let store = self.store.read().await;
        store
            .get(&key)
            .cloned() // Clone the value (e.g., Arc)
            .ok_or_else(|| {
                Box::new(NotFoundError {
                    key: format!("{:?}", key),
                }) as Box<dyn Error>
            })
    }

    async fn list(
        &self,
        visitor_fn: Box<dyn Fn(K, StorerValue<T>) + Send>,
        _opts: Option<ListOptions>,
    ) -> Result<(), Box<dyn Error>> {
        let store = self.store.read().await;
        for (key, value) in store.iter() {
            let key_cloned = key.clone();
            let value_cloned = value.clone();
            visitor_fn(key_cloned, value_cloned);
        }
        Ok(())
    }

    async fn list_keys(&self, _opts: Option<ListOptions>) -> Vec<String> {
        let store = self.store.read().await;
        store.keys().map(|key| format!("{:?}", key)).collect()
    }

    async fn len(&self, _opts: Option<ListOptions>) -> usize {
        let store = self.store.read().await;
        store.len()
    }

    async fn apply(&self, key: K, value: T) -> Result<StorerValue<T>, Box<dyn Error>> {
        let exists = self.get(key.clone(), None).await.is_ok();
        let mut store = self.store.write().await;
        let arc_value = Arc::new(value);
        store.insert(key.clone(), arc_value.clone());
        if !exists {
            self.notify_watcher_manager(WatchEvent::Added(key.clone(), arc_value.clone()))
                .await;
        } else {
            self.notify_watcher_manager(WatchEvent::Modified(key.clone(), arc_value.clone()))
                .await;
        }
        Ok(arc_value)
    }

    async fn create(&self, key: K, value: T) -> Result<StorerValue<T>, Box<dyn Error>> {
        let mut store = self.store.write().await;
        if store.contains_key(&key) {
            return Err(Box::new(AlreadyExists {
                key: format!("{:?}", key),
            }) as Box<dyn Error>);
        }
        let arc_value = Arc::new(value);
        store.insert(key.clone(), arc_value.clone());
        self.notify_watcher_manager(WatchEvent::Added(key.clone(), arc_value.clone()))
            .await;
        Ok(arc_value)
    }

    async fn update(&self, key: K, value: T) -> Result<StorerValue<T>, Box<dyn Error>> {
        let mut store = self.store.write().await;
        if !store.contains_key(&key) {
            return Err(Box::new(NotFoundError {
                key: format!("{:?}", key),
            }) as Box<dyn Error>);
        }
        let arc_value = Arc::new(value);
        store.insert(key.clone(), arc_value.clone());
        self.notify_watcher_manager(WatchEvent::Modified(key.clone(), arc_value.clone()))
            .await;
        Ok(arc_value)
    }

    async fn delete(&self, key: K) -> Result<(), Box<dyn Error>> {
        let mut store = self.store.write().await;
        let value = store.remove(&key).ok_or_else(|| {
            Box::new(NotFoundError {
                key: format!("{:?}", key),
            }) as Box<dyn Error>
        })?;
        self.notify_watcher_manager(WatchEvent::Deleted(key.clone(), value.clone()))
            .await;
        Ok(())
    }

    async fn watch(
        &self,
        _ctx: tokio::sync::oneshot::Receiver<()>,
        options: Option<ListOptions>,
    ) -> Result<
        (
            tokio::sync::oneshot::Sender<()>,
            tokio_stream::wrappers::ReceiverStream<WatchEvent<K, StorerValue<T>>>,
        ),
        Box<dyn Error>,
    > {
        //unimplemented!("Watch functionality is not implemented yet.")
        //let catchup = opts.map_or(true, |o| !o.watch);

        //let filter_fn = move |_value: &T| true; // Customize filtering logic if needed

        // Add watcher to the manager
        let manager = self.watcher_manager.clone();
        manager.add_watcher(options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Import the `Memory` struct and related traits
    use tokio;

    #[tokio::test]
    async fn test_create_and_get() {
        let memory = Memory::<String, String>::new();

        // Create a key-value pair
        memory
            .create("key1".to_string(), "value1".to_string())
            .await
            .expect("Failed to create key1");

        // Retrieve the value
        let value = memory
            .get("key1".to_string(), None)
            .await
            .expect("Failed to get key1");
        assert_eq!(*value, "value1".to_string());
    }

    #[tokio::test]
    async fn test_create_duplicate_key() {
        let memory = Memory::<String, String>::new();

        // Create a key-value pair
        memory
            .create("key1".to_string(), "value1".to_string())
            .await
            .expect("Failed to create key1");

        // Attempt to create the same key again
        let result = memory
            .create("key1".to_string(), "value2".to_string())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_existing_key() {
        let memory = Memory::<String, String>::new();

        // Create a key-value pair
        memory
            .create("key1".to_string(), "value1".to_string())
            .await
            .expect("Failed to create key1");

        // Update the key
        memory
            .update("key1".to_string(), "new_value1".to_string())
            .await
            .expect("Failed to update key1");

        // Verify the update
        let value = memory
            .get("key1".to_string(), None)
            .await
            .expect("Failed to get key1");
        assert_eq!(*value, "new_value1".to_string());
    }

    #[tokio::test]
    async fn test_update_nonexistent_key() {
        let memory = Memory::<String, String>::new();

        // Attempt to update a non-existent key
        let result = memory
            .update("key1".to_string(), "value1".to_string())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_key() {
        let memory = Memory::<String, String>::new();

        // Create a key-value pair
        memory
            .create("key1".to_string(), "value1".to_string())
            .await
            .expect("Failed to create key1");

        // Delete the key
        memory
            .delete("key1".to_string())
            .await
            .expect("Failed to delete key1");

        // Verify deletion
        let result = memory.get("key1".to_string(), None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_keys() {
        let memory = Memory::<String, String>::new();

        // Create multiple keys
        memory
            .create("key1".to_string(), "value1".to_string())
            .await
            .expect("Failed to create key1");
        memory
            .create("key2".to_string(), "value2".to_string())
            .await
            .expect("Failed to create key2");

        // List keys
        let keys = memory.list_keys(None).await;
        let expected_keys = vec!["\"key1\"", "\"key2\""]; // Keys are formatted as strings
        assert_eq!(keys, expected_keys);
    }

    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_list_with_visitor() {
        let memory = Memory::<String, String>::new();

        // Create multiple keys
        memory
            .create("key1".to_string(), "value1".to_string())
            .await
            .expect("Failed to create key1");
        memory
            .create("key2".to_string(), "value2".to_string())
            .await
            .expect("Failed to create key2");

        // Use Arc<Mutex<Vec<T>>> for thread-safe shared mutability
        let local_results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = local_results.clone();

        memory
            .list(
                Box::new(move |key, value| {
                    // Spawn a new async task to perform the mutation asynchronously
                    let results_clone = results_clone.clone();
                    tokio::spawn(async move {
                        let mut results = results_clone.lock().await; // Use async lock
                        results.push((key, (*value).clone()));
                    });
                }),
                None,
            )
            .await
            .expect("Failed to list keys");

        // Allow spawned tasks to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify results
        let mut results = local_results.lock().await;
        results.sort();
        assert_eq!(
            *results,
            vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn test_len() {
        let memory = Memory::<String, String>::new();

        // Initially empty
        assert_eq!(memory.len(None).await, 0);

        // Add keys
        memory
            .create("key1".to_string(), "value1".to_string())
            .await
            .expect("Failed to create key1");
        memory
            .create("key2".to_string(), "value2".to_string())
            .await
            .expect("Failed to create key2");

        // Verify length
        assert_eq!(memory.len(None).await, 2);
    }
}
