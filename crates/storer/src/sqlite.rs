use crate::storer::{GetOptions, ListOptions, Storer};
use crate::watch::WatchEvent;
use crate::watcher_manager::WatcherManager;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use serde_json;
use sqlx::sqlite::SqlitePool;
use std::cmp::{Eq, Ord};
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;

pub struct SqliteStorer<K, T>
where
    K: Eq + std::hash::Hash + Clone + Debug + Send + Sync + Ord + ToString + From<String> + 'static,
    T: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    pool: SqlitePool,
    //_phantom: std::marker::PhantomData<(K, T)>,
    watcher_manager: Option<Arc<WatcherManager<K, T>>>,
}

impl<K, T> SqliteStorer<K, T>
where
    K: Eq + std::hash::Hash + Clone + Debug + Send + Sync + Ord + ToString + From<String> + 'static,
    T: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    pub async fn new(db_url: &str) -> Result<Arc<Self>, Box<dyn Error>> {
        let pool = SqlitePool::connect(db_url).await?;

        let pool_clone = pool.clone();

        // Ensure the table exists
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );",
        )
        .execute(&pool)
        .await?;

        // Step 2: Create a storer without WatcherManager to please the watcher manager
        let storer: Arc<dyn Storer<K, T>> = Arc::new(Self {
            pool: pool_clone,
            watcher_manager: None,
        });

        // Create WatcherManager
        let watcher_manager = Arc::new(WatcherManager::new(Arc::clone(&storer), 1024));

        Ok(Arc::new(Self {
            pool,
            watcher_manager: Some(watcher_manager),
        }))
    }

    async fn notify_watcher_manager(&self, event: WatchEvent<K, T>) {
        if let Some(watcher_manager) = &self.watcher_manager {
            if watcher_manager.is_running().await {
                if let Err(e) = watcher_manager.event_channel_tx().send(event).await {
                    tracing::warn!("Failed to send event to watcher manager: {:?}", e);
                }
            }
        }
    }
}

#[async_trait]
impl<K, T> Storer<K, T> for SqliteStorer<K, T>
where
    K: ToString + From<String> + Eq + std::hash::Hash + Clone + Debug + Send + Sync + Ord + 'static,
    T: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    async fn get(&self, key: K, _opts: Option<GetOptions>) -> Result<T, Box<dyn Error + Send + Sync>> {
        let row: (String,) = sqlx::query_as("SELECT value FROM store WHERE key = ?")
            .bind(key.to_string())
            .fetch_one(&self.pool)
            .await?;

        let deserialized_value: T = serde_json::from_str(&row.0)?;
        Ok(deserialized_value)
    }

    async fn list(
        &self,
        visitor_fn: Box<dyn Fn(K, T) + Send>,
        _opts: Option<ListOptions>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let rows = sqlx::query_as::<_, (String, String)>("SELECT key, value FROM store")
            .fetch_all(&self.pool)
            .await?;

        for (key, value) in rows {
            let deserialized_value: T = serde_json::from_str(&value)?;
            visitor_fn(K::from(key), deserialized_value);
        }

        Ok(())
    }

    async fn list_keys(&self, _opts: Option<ListOptions>) -> Vec<String> {
        let rows = sqlx::query_as::<_, (String,)>("SELECT key FROM store")
            .fetch_all(&self.pool)
            .await
            .unwrap_or_default();

        rows.into_iter().map(|(key,)| key).collect()
    }

    async fn len(&self, _opts: Option<ListOptions>) -> usize {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM store")
            .fetch_one(&self.pool)
            .await
            .unwrap_or((0,));

        row.0 as usize
    }

    async fn apply(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        let serialized_value = serde_json::to_string(&value)?;

        let result = sqlx::query(
            "INSERT INTO store (key, value) VALUES (?, ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value;",
        )
        .bind(key.to_string())
        .bind(serialized_value)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 1 {
            self.notify_watcher_manager(WatchEvent::Added(key.clone(), value.clone()))
                .await;
        } else {
            self.notify_watcher_manager(WatchEvent::Modified(key.clone(), value.clone()))
                .await;
        }

        Ok(value)
    }

    async fn create(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        let serialized_value = serde_json::to_string(&value)?;

        sqlx::query("INSERT INTO store (key, value) VALUES (?, ?);")
            .bind(key.to_string())
            .bind(serialized_value)
            .execute(&self.pool)
            .await?;

        self.notify_watcher_manager(WatchEvent::Added(key.clone(), value.clone()))
            .await;
        Ok(value)
    }

    async fn update(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        let serialized_value = serde_json::to_string(&value)?;

        self.get(key.clone(), None).await?;
            
        sqlx::query("UPDATE store SET value = ? WHERE key = ?;")
            .bind(serialized_value)
            .bind(key.to_string())
            .execute(&self.pool)
            .await?;

        self.notify_watcher_manager(WatchEvent::Modified(key.clone(), value.clone()))
            .await;
        Ok(value)
    }

    async fn delete(&self, key: K) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Fetch the value before deletion
        let value = match self.get(key.clone(), None).await {
            Ok(v) => v, // Key exists, proceed with deletion
            Err(_) => return Ok(()), // Key not found, return `Ok(())` silently
        };

        sqlx::query("DELETE FROM store WHERE key = ?;")
            .bind(key.to_string())
            .execute(&self.pool)
            .await?;

        // Step 3: Notify the watcher manager
        self.notify_watcher_manager(WatchEvent::Deleted(key.clone(), value))
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
            tokio_stream::wrappers::ReceiverStream<WatchEvent<K, T>>,
        ),
        Box<dyn Error + Send + Sync>,
    > {
        // Add watcher to the manager
        match &self.watcher_manager {
            None => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "WatcherManager is not initialized",
            ))),
            Some(watcher_manager) => {
                let manager = watcher_manager.clone();
                manager.add_watcher(options).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Import `SqliteStorer` and related traits
    use tokio::sync::oneshot;
    use tokio::sync::Mutex;
    use tokio_stream::StreamExt;

    type TestKey = String;
    type TestValue = String;

    async fn setup_db() -> Arc<SqliteStorer<TestKey, TestValue>> {
        let db_url = "sqlite://:memory:"; // Use in-memory SQLite for testing
        let store = SqliteStorer::new(db_url)
            .await
            .expect("Failed to create SqliteStorer");
        store
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let store = setup_db().await;

        let key = "key1".to_string();
        let value = "value1".to_string();
        store
            .create(key.clone(), value.clone())
            .await
            .expect("Failed to create");

        let retrieved = store.get(key.clone(), None).await.expect("Failed to get");
        assert_eq!(retrieved, value);
    }

    #[tokio::test]
    async fn test_create_duplicate() {
        let store = setup_db().await;

        let key = "key1".to_string();
        let value = "value1".to_string();
        store
            .create(key.clone(), value.clone())
            .await
            .expect("Failed to create");

        let result = store.create(key.clone(), "new_value".to_string()).await;
        assert!(result.is_err(), "Duplicate key should result in error");
    }

    #[tokio::test]
    async fn test_update_existing() {
        let store = setup_db().await;

        let key = "key1".to_string();
        store
            .create(key.clone(), "value1".to_string())
            .await
            .expect("Failed to create");

        let new_value = "new_value1".to_string();
        store
            .update(key.clone(), new_value.clone())
            .await
            .expect("Failed to update");

        let retrieved = store
            .get(key.clone(), None)
            .await
            .expect("Failed to get updated value");
        assert_eq!(retrieved, new_value);
    }

    #[tokio::test]
    async fn test_update_non_existing() {
        let store = setup_db().await;

        let result = store.update("key1".to_string(), "value1".to_string()).await;
        assert!(
            result.is_err(),
            "Updating a non-existent key should result in error"
        );
    }

    #[tokio::test]
    async fn test_delete_existing() {
        let store = setup_db().await;

        let key = "key1".to_string();
        store
            .create(key.clone(), "value1".to_string())
            .await
            .expect("Failed to create");

        store.delete(key.clone()).await.expect("Failed to delete");

        let result = store.get(key.clone(), None).await;
        assert!(result.is_err(), "Deleted key should not be retrievable");
    }

    #[tokio::test]
    async fn test_delete_non_existing() {
        let store = setup_db().await;

        let result = store.delete("key1".to_string()).await;
        assert!(result.is_ok(), "Deleting a non-existent key should be fine");
    }

    #[tokio::test]
    async fn test_list_keys() {
        let store = setup_db().await;

        store
            .create("key1".to_string(), "value1".to_string())
            .await
            .expect("Failed to create key1");
        store
            .create("key2".to_string(), "value2".to_string())
            .await
            .expect("Failed to create key2");

        let keys = store.list_keys(None).await;
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));
    }

    #[tokio::test]
    async fn test_list_values() {
        let store = setup_db().await;

        store
            .create("key1".to_string(), "value1".to_string())
            .await
            .expect("Failed to create key1");
        store
            .create("key2".to_string(), "value2".to_string())
            .await
            .expect("Failed to create key2");

        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = results.clone();

        store
            .list(
                Box::new(move |key, value| {
                    // Spawn a new async task to perform the mutation asynchronously
                    let results_clone = results_clone.clone();
                    tokio::spawn(async move {
                        let mut results = results_clone.lock().await; // Use async lock
                        results.push((key, value.clone()));
                    });
                }),
                None,
            )
            .await
            .expect("Failed to list");

        // Allow spawned tasks to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Lock mutex before asserting
        let results = results.lock().await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(_, v)| v == "value1"));
        assert!(results.iter().any(|(_, v)| v == "value2"));
    }

    #[tokio::test]
    async fn test_len() {
        let store = setup_db().await;

        assert_eq!(store.len(None).await, 0);

        store
            .create("key1".to_string(), "value1".to_string())
            .await
            .expect("Failed to create key1");
        store
            .create("key2".to_string(), "value2".to_string())
            .await
            .expect("Failed to create key2");

        assert_eq!(store.len(None).await, 2);
    }

    #[tokio::test]
    async fn test_apply_create_modify() {
        let store = setup_db().await;

        let key = "key1".to_string();
        let value1 = "value1".to_string();
        store
            .apply(key.clone(), value1.clone())
            .await
            .expect("Failed to apply create");

        let retrieved = store
            .get(key.clone(), None)
            .await
            .expect("Failed to get after apply");
        assert_eq!(retrieved, value1);

        let value2 = "value2".to_string();
        store
            .apply(key.clone(), value2.clone())
            .await
            .expect("Failed to apply modify");

        let retrieved = store
            .get(key.clone(), None)
            .await
            .expect("Failed to get after modify");
        assert_eq!(retrieved, value2);
    }

    #[tokio::test]
    async fn test_watch_events() {
        let store = setup_db().await;
        // Step 1: Start the watch stream
        let watch_options = Some(ListOptions {
            commit: None,
            watch_only: false,
        });
        let (_stop_tx, mut watch_stream) = store
            .watch(oneshot::channel().1, watch_options)
            .await
            .expect("Failed to start watch");

        // Step 2: Ensure the watcher is running before proceeding
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Step 3: Create a new key-value pair (should trigger a watch event)
        let key = "key1".to_string();
        let value = "value1".to_string();
        store
            .create(key.clone(), value.clone())
            .await
            .expect("Failed to create");

        // Step 4: Wait for the event with a timeout
        match tokio::time::timeout(tokio::time::Duration::from_secs(1), watch_stream.next()).await {
            Ok(Some(event)) => match event {
                WatchEvent::Added(k, v) => {
                    assert_eq!(k, key);
                    assert_eq!(v, value);
                }
                _ => panic!("Unexpected watch event"),
            },
            Ok(None) => panic!("Expected watch event but got None"),
            Err(_) => panic!("Timed out waiting for a watch event"),
        }
    }
}
