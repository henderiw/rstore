use crate::storer::{GetOptions, ListOptions, Storer};
use crate::watcher_manager::WatcherManager;
use crate::watch::WatchEvent;
use crate::{decode_continue_token, encode_continue_token};
use async_trait::async_trait;
use sqlx::{Pool, Sqlite};
use std::error::Error;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::{Stream, wrappers::{ReceiverStream, UnboundedReceiverStream}};
use sqlx::sqlite::SqlitePool;
use serde::{de::DeserializeOwned, Serialize};
use core_apim::APIError;
use tokio::sync::oneshot;
use sqlx::Row;
use tokio::sync::mpsc;

/*
CREATE TABLE IF NOT EXISTS resources (
    key TEXT NOT NULL,
    version INTEGER NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (key, version)
);

CREATE TABLE IF NOT EXISTS versions (
    resource_version INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    version INTEGER NOT NULL,
    FOREIGN KEY (key, version) REFERENCES resources(key, version)
);
*/

pub struct ImmutableSqliteStore<K, T>
where
    K: Eq + std::hash::Hash + Clone + Debug + Send + Sync + Ord + Into<String> + From<String> + 'static,
    T: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    pool: Pool<Sqlite>,
    watcher_manager: Option<Arc<WatcherManager<K, T>>>,
}

impl<K, T> ImmutableSqliteStore<K, T>
where
    K: Eq + std::hash::Hash + Clone + Debug + Send + Sync + Ord + Into<String> + From<String> + 'static,
    T: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    pub async fn new(db_url: &str) -> Result<Arc<Self>, Box<dyn Error + Send + Sync>> {
        let pool = SqlitePool::connect(db_url).await?;
        let pool_clone = pool.clone();

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS resources (
                key TEXT NOT NULL,
                version INTEGER NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (key, version)
            );",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS versions (
                resource_version INTEGER NOT NULL,
                key TEXT NOT NULL,
                version INTEGER NOT NULL,
                PRIMARY KEY (resource_version, key)
            );",
        )
        .execute(&pool)
        .await
        .unwrap();

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

    async fn notify_watcher_manager(&self, event: WatchEvent<T>) {
        if let Some(watcher_manager) = &self.watcher_manager {
            if watcher_manager.is_running().await {
                if let Err(e) = watcher_manager.event_channel_tx().send(event).await {
                    tracing::warn!("Failed to send event to watcher manager: {:?}", e);
                }
            }
        }
    }

    async fn fetch_latest_version(&self) -> Result<u64, Box<dyn Error + Send + Sync>> {
        Ok(sqlx::query_scalar::<Sqlite, i64>(
            "SELECT COALESCE(MAX(resource_version), 0) FROM versions;"
        )
        .fetch_one(&self.pool)
        .await?
        .try_into()?)
    }

}

#[async_trait]
impl<K, T> Storer<K, T> for ImmutableSqliteStore<K, T>
where
    K: Eq + std::hash::Hash + Clone + Debug + Send + Sync + Ord + Into<String> + From<String> + 'static,
    T: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    async fn get(
        &self,
        key: K,
        _opts: Option<GetOptions>,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
        let key_str: String = key.into();
        let row: Option<(String,)> = sqlx::query_as::<_, (String,)>(
            "SELECT value FROM resources 
             WHERE key = ? 
             ORDER BY version DESC LIMIT 1;",
        )
        .bind(&key_str)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((json_value,)) => {
                let value: T = serde_json::from_str(&json_value)?;
                Ok(value)
            }
            None => Err(Box::new(APIError::NotFound(format!(
                "Key not found: {:?}",
                key_str
            )))),
        }
    }

    async fn apply(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        let key_str: String = key.clone().into();
        let serialized_value = serde_json::to_string(&value)?;

        let mut tx = self.pool.begin().await?;

        // ✅ Check if the resource exists in `versions`
        let exists_in_versions: Option<i64> = sqlx::query_scalar(
            "SELECT COUNT(*) FROM versions WHERE key = ?;"
        )
        .bind(&key_str)
        .fetch_one(&mut *tx)
        .await?;

        let is_create = exists_in_versions == Some(0); // 🚨 Determines if this is a "CREATE" operation

        // ✅ Fetch latest resource version
        let latest_version: u64 = sqlx::query_scalar::<Sqlite, i64>(
            "SELECT COALESCE(MAX(resource_version), 0) FROM versions;"
        )
        .fetch_one(&mut *tx)
        .await?
        .try_into()
        .expect("Database returned negative resource version");

        let new_version = latest_version.saturating_add(1);
        if new_version == u64::MAX {
            return Err("🚨 Resource version overflow detected!".into());
        }

        // ✅ Step 1: Insert new resource entry
        sqlx::query(
            "INSERT INTO resources (key, version, value) VALUES (?, ?, ?);"
        )
        .bind(&key_str)
        .bind(new_version as i64)
        .bind(&serialized_value)
        .execute(&mut *tx)
        .await?;

        // ✅ Step 2: Clone all previous version mappings (for continuity)
        sqlx::query(
            "INSERT INTO versions (resource_version, key, version)
             SELECT ?, key, version FROM versions WHERE resource_version = ?;"
        )
        .bind(new_version as i64)
        .bind(latest_version as i64)
        .execute(&mut *tx)
        .await?;

        // ✅ Step 3: Update the new version mapping for the modified key
        sqlx::query(
            "INSERT OR REPLACE INTO versions (resource_version, key, version) VALUES (?, ?, ?);"
        )
        .bind(new_version as i64)
        .bind(&key_str)
        .bind(new_version as i64)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        println!("📌 After applying key `{}`:", key_str);
        println!("{}", self.dump_store().await);

        // ✅ Notify Watcher Manager
        if is_create {
            self.notify_watcher_manager(WatchEvent::Added(value.clone()))
                .await;
        } else {
            self.notify_watcher_manager(WatchEvent::Modified(value.clone()))
                .await;
        }

        Ok(value)
    }

    async fn delete(&self, key: K) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key_str: String = key.clone().into();

        let mut tx = self.pool.begin().await?;

        // ✅ Step 1: Fetch the latest resource version
        let latest_version: u64 = sqlx::query_scalar::<Sqlite, i64>(
            "SELECT COALESCE(MAX(resource_version), 0) FROM versions;"
        )
        .fetch_one(&mut *tx)
        .await?
        .try_into()
        .expect("Database returned negative resource version");

        let new_version = latest_version.saturating_add(1);
        if new_version == u64::MAX {
            return Err("🚨 Resource version overflow detected!".into());
        }

        // ✅ Step 2: Check if the key exists in resources
        let latest_value: Option<(String,)> = sqlx::query_as::<_, (String,)>(
            "SELECT value FROM resources WHERE key = ? ORDER BY version DESC LIMIT 1;"
        )
        .bind(&key_str)
        .fetch_optional(&mut *tx)
        .await?;

        // ✅ If key does not exist, return early
        let latest_value = match latest_value {
            Some((json_value,)) => serde_json::from_str::<T>(&json_value)?,
            None => return Ok(()), // Key not found, return silently
        };

        // ✅ Step 3: Copy all previous keys **except the deleted one**
        sqlx::query(
            "INSERT INTO versions (resource_version, key, version)
            SELECT ?, key, version FROM versions WHERE resource_version = ? AND key != ?;"
        )
        .bind(new_version as i64)
        .bind(latest_version as i64)
        .bind(&key_str)
        .execute(&mut *tx)
        .await?;

        // ✅ Step 4: Delete only the **latest version** of the key in `resources`
        sqlx::query("DELETE FROM resources WHERE key = ?;")
            .bind(&key_str)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        // ✅ Notify Watcher Manager if delete was successful
        self.notify_watcher_manager(WatchEvent::Deleted(latest_value))
            .await;

        Ok(())

    }


    async fn list(
        &self,
        opts: Option<ListOptions>,
    ) ->Result<
        (   
            Pin<Box<dyn Stream<Item = (K, T)> + Send + Sync>>, 
            u64,
            Option<(String, u64)>,
        ), Box<dyn Error + Send + Sync>> {
        let pool = self.pool.clone();
    
        // ✅ **Step 1: Determine Resource Version & Start Key**
        // if latest version is not present provide use the latest
        let latest_version = self.fetch_latest_version().await?;
        let (effective_version, start_key) = opts
            .as_ref()
            .and_then(|o| o.continue_token.as_ref())
            .and_then(|token| decode_continue_token(token))
            .unwrap_or_else(|| (latest_version, "".to_string()));


        // ✅ **Step 2: Fetch keys and versions**
        let rows = sqlx::query(
            "SELECT DISTINCT key FROM versions WHERE resource_version = ? ORDER BY key ASC;"
        )
        .bind(effective_version as i64)
        .fetch_all(&pool)
        .await?;

        let mut sorted_keys: Vec<K> = rows
            .into_iter()
            .map(|row| row.try_get::<String, _>("key").unwrap().into())
            .collect();
        
        sorted_keys.sort();

        // get the pagination limit
        let limit = opts.as_ref().and_then(|o| o.limit).map(|l| l as usize).unwrap_or(usize::MAX);
        let start_key_converted: K = start_key.into();
        let start_index = sorted_keys.iter().position(|key| key == &start_key_converted).unwrap_or(0);


        // ✅ Step 5: **Precompute last_key and continue token before creating the stream**
        // doing this inside the stream was not working
        let next_continue_token = if start_index + limit < sorted_keys.len() {
            sorted_keys
                .get(start_index + limit) // ✅ Safely access the next key after the current batch
                .cloned()
                .map(|key| {
                    let remaining_items = (sorted_keys.len() - (start_index + limit)) as u64;
                    (encode_continue_token(effective_version, &key.into()), remaining_items)
                })
        } else {
            None // ✅ No more pages, stop pagination
        };

        // ✅ **Step 4: Use a channel to fetch data asynchronously**
        let (tx, rx) = mpsc::unbounded_channel();
        let resource_store = self.pool.clone();
        
        tokio::spawn(async move {
            let mut count = 0;
            for key in sorted_keys.iter().skip(start_index) {
                if count >= limit {
                    break;
                }
                let version = sqlx::query_scalar::<_, i64>(
                    "SELECT version FROM versions WHERE key = ? AND resource_version = ?;"
                )
                .bind(&key.clone().into())
                .bind(effective_version as i64)
                .fetch_one(&resource_store)
                .await
                .ok();

                if let Some(version) = version {
                    let row: Option<(String,)> = sqlx::query_as::<_, (String,)>(
                        "SELECT value FROM resources WHERE key = ? AND version = ?;"
                    )
                    .bind(&key.clone().into())
                    .bind(version)
                    .fetch_optional(&resource_store)
                    .await
                    .ok()
                    .flatten();

                    if let Some((value_json,)) = row {
                        if let Ok(value) = serde_json::from_str::<T>(&value_json) {
                            let _ = tx.send((key.clone(), value)); // Send to the stream
                            count += 1;
                        }
                    }
                }
            }
        });

        // ✅ Convert the receiver to a stream
        let stream = UnboundedReceiverStream::new(rx);

        Ok((Box::pin(stream), effective_version, next_continue_token))
    }

    async fn list_keys(&self, _opts: Option<ListOptions>) -> (Vec<K>, u64) {
        // inexpensive uses internally uses Arc<Inner>
        let pool = self.pool.clone();

        // ✅ Fetch latest resource version **as u64**
        let latest_version: u64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(resource_version), 0) FROM versions;"
        )
        .fetch_one(&pool)
        .await
        .unwrap_or(0) as u64; // Explicit cast

        // ✅ Fetch distinct keys
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT DISTINCT key FROM versions;"
        )
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        (rows.into_iter().map(|(key,)| key.into()).collect(), latest_version)
    }

    async fn len(&self, _opts: Option<ListOptions>) -> usize {
        let count: Option<(i64,)> = sqlx::query_as::<_, (i64,)>(
            "SELECT COUNT(DISTINCT key) FROM versions;",
        )
        .fetch_optional(&self.pool)
        .await
        .unwrap_or(Some((0,)));

        count.map_or(0, |(c,)| c as usize)
    }

    async fn create(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        self.apply(key, value).await
    }

    async fn update(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        self.apply(key, value).await
    }

    async fn watch(
        &self,
        _ctx: oneshot::Receiver<()>,
        options: Option<ListOptions>,
    ) -> Result<(oneshot::Sender<()>, ReceiverStream<WatchEvent<T>>), Box<dyn Error + Send + Sync>> {
        match &self.watcher_manager {
            None => Err(Box::new(APIError::InternalServerError(
                "WatcherManager is not initialized".to_string(),
            ))),
            Some(watcher_manager) => {
                let manager = watcher_manager.clone();
                manager.add_watcher(options).await
            }
        }
    }
}

impl<K, T> ImmutableSqliteStore<K, T>
where
    K: Eq + std::hash::Hash + Clone + Debug + Send + Sync + Ord + Into<String> + From<String> + 'static,
    T: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    /// Fetches and formats store contents for debugging **without blocking**
    pub async fn dump_store(&self) -> String {
        let mut output = String::new();

        // ✅ **Fetch Versions Table**
        let versions = sqlx::query(
            "SELECT resource_version, key, version FROM versions ORDER BY resource_version ASC, key ASC;"
        )
        .fetch_all(&self.pool)
        .await
        .unwrap_or_else(|_| vec![]);

        // ✅ **Fetch Resources Table**
        let resources = sqlx::query(
            "SELECT key, version, value FROM resources ORDER BY key ASC, version ASC;"
        )
        .fetch_all(&self.pool)
        .await
        .unwrap_or_else(|_| vec![]);

        output.push_str("\n📌 **SQLite Store Contents** 📌\n");

        // ✅ **Group keys by Resource Version (RV)**
        output.push_str("\n🔹 **Version Store (Keys by Resource Version):**\n");
        let mut version_map: std::collections::BTreeMap<i64, Vec<String>> = std::collections::BTreeMap::new();

        for row in &versions {
            let rv: i64 = row.try_get("resource_version").unwrap_or(0);
            let key: String = row.try_get("key").unwrap_or_else(|_| "unknown".to_string());

            version_map.entry(rv).or_insert_with(Vec::new).push(key);
        }

        for (rv, keys) in &version_map {
            output.push_str(&format!("  🏷️ RV {} -> Keys: {:?}\n", rv, keys));
        }

        // ✅ **Print Resource Store**
        output.push_str("\n🔹 **Resource Store (Actual Data by Key & Version):**\n");
        let mut sorted_resources: Vec<_> = resources.iter().collect();
        sorted_resources.sort_by_key(|row| row.try_get::<i64, _>("version").unwrap_or(0)); // Sort by version

        for row in sorted_resources {
            let key: String = row.try_get("key").unwrap_or_else(|_| "unknown".to_string());
            let version: i64 = row.try_get("version").unwrap_or(0);
            let value: String = row.try_get("value").unwrap_or_else(|_| "<empty>".to_string());

            output.push_str(&format!("  🔑 Key: {} | 📌 Version: {} | 📦 Value: {}\n", key, version, value));
        }

        output
    }
}

#[tokio::test]
async fn test_sqlite_pagination() -> Result<(), Box<dyn Error + Send + Sync>> {
    use crate::storer::{ListOptions, Storer};

    // ✅ Step 1: Setup In-Memory SQLite Database
    let db_url = "sqlite::memory:";  // Use in-memory SQLite for faster testing
    let store = ImmutableSqliteStore::<String, String>::new(db_url).await?;

    println!("📌 Before inserts:\n{}", store.dump_store().await);

    // ✅ Step 2: Insert Test Records
    let test_data = vec![
        ("key1".to_string(), "value1".to_string()),
        ("key2".to_string(), "value2".to_string()),
        ("key3".to_string(), "value3".to_string()),
    ];

    for (key, value) in &test_data {
        store.apply(key.clone(), value.clone()).await?;
    }

    // ✅ Print Store After Inserts
    println!("📌 After inserts:\n{}", store.dump_store().await);

    // ✅ Step 3: Test Full Listing (Limit > Total Records)
    let list_options = ListOptions {
        commit: None,
        watch_only: false,
        limit: Some(10),  // Larger than total records
        continue_token: None,
    };

    println!("list pagination limit 10");

    let collected_keys = client_list(&store, list_options).await?;

    println!("collected_keys {:?}", collected_keys);

    // ✅ Step 4: Test Paginated Listing (Limit = 1)
    let list_options = ListOptions {
        commit: None,
        watch_only: false,
        limit: Some(1),  // Only fetch 1 record per request
        continue_token: None,
    };

    let collected_keys = client_list(&store, list_options).await?;

    println!("collected_keys {:?}", collected_keys);

    // ✅ Step 5: Delete a Key (`key2`)
    println!("\n🗑 **Deleting key2...**");
    store.delete("key2".to_string()).await?;

    // ✅ Step 4: Test Paginated Listing (Limit = 1)
    let list_options = ListOptions {
        commit: None,
        watch_only: false,
        limit: Some(1),  // Only fetch 1 record per request
        continue_token: None,
    };

    let collected_keys = client_list(&store, list_options).await?;
    println!("collected_keys {:?}", collected_keys);
    Ok(())
}


use tokio_stream::StreamExt;
pub async fn client_list<K, T>(
    store: &ImmutableSqliteStore<K, T>, 
    mut list_options: ListOptions
) -> Result<Vec<K>, Box<dyn Error + Send + Sync>>
where
    K: Eq + std::hash::Hash + Clone + Debug + Send + Sync + Ord + Into<String> + From<String> + 'static,
    T: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static,
{
    let mut collected_keys = Vec::new();
    let mut first_iteration = true;

    loop {
        // ✅ Fetch the list using `store.list()`
        let (mut stream, version, continue_token) = store.list(Some(list_options.clone())).await?;

        if first_iteration {
            println!("list pagination limit {:?}", list_options.limit);
            first_iteration = false;
        }

        while let Some((key, value)) = stream.next().await {
            println!(
                "version {} key {:?}, value {:?} continue {:?}",
                version, key, value, continue_token
            );
            collected_keys.push(key);
        }

        // ✅ Handle continue token for pagination
        if let Some(token) = continue_token {
            list_options.continue_token = Some(token.0); // Update `continue_token` for the next request
        } else {
            break;  // ✅ Stop if there are no more pages
        }
    }

    Ok(collected_keys)
}