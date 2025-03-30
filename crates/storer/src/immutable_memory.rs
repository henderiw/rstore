use crate::storer::{GetOptions, ListOptions, Storer};
use crate::watch::WatchEvent;
use crate::watcher_manager::WatcherManager;
use crate::{decode_continue_token, encode_continue_token};
use async_trait::async_trait;
use core_apim::APIError;
use std::cmp::{Eq, Ord};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::pin::Pin;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tokio::sync::oneshot;
use async_stream::stream;

#[derive(Clone)]
pub struct ImmutableMemoryStore<K, T>
where
    K: Ord + Eq + Hash + Clone + Debug + Send + Sync + From<String> + Into<String> + 'static,
    T: Debug + Clone + Send + Sync + 'static,
{
    /// Tracks the global version (`resource_version`) -> key -> specific instance version
    version_store: HashMap<u64, HashMap<K, u64>>,
    /// Stores **all** versions of each resource, indexed by key & version.
    resource_store: HashMap<(K, u64), T>,
    /// Global **monotonic resource version**
    head: u64,
}

impl<K, T> ImmutableMemoryStore<K, T>
where
    K: Ord + Eq + Hash + Clone + Debug + Send + Sync + From<String> + Into<String> + 'static,
    T: Debug + Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        // Step 4: Rebuild `storer` with `watcher_manager`
        let mut initial_version = HashMap::new();
        initial_version.insert(0, HashMap::new());
        Self {
            version_store: initial_version,
            resource_store: HashMap::new(),
            head: 0,
        }
    }

    /// Retrieves the **latest version** of a resource.
    pub fn get(&self, key: &K, _opts: Option<GetOptions>) -> Option<T> {
        let latest_mapping = self.version_store.get(&self.head)?;

        // Find the latest recorded version for the given key
        let latest_version = latest_mapping.get(key)?;

        // Fetch the resource instance at that version
        self.resource_store.get(&(key.clone(), *latest_version)).cloned()
    }

    /// **Returns an async stream over stored resources**
    pub fn list(
        &self,
        opts: Option<ListOptions>,
    ) -> (
            Pin<Box<dyn Stream<Item = (K, T)> + Send + Sync>>, 
            u64,
            Option<(String, u64)>, // Continue token + remaining items
        ) { 

        // **Step 1: Determine resource version**
        let (effective_version, start_key) = opts
            .as_ref()
            .and_then(|o| o.continue_token.as_ref())
            .and_then(|token| decode_continue_token(token))
            .unwrap_or((self.head, "".to_string()));

        println!("list effective version {}, start_key {}", effective_version, start_key);

        // Step 2: Fetch the key->version map for the effective version
        let version_map = self.version_store.get(&effective_version).cloned().unwrap_or_default();
        
        // Step 3: Create a sorted key list
        let mut sorted_keys: Vec<K> = version_map.keys().cloned().collect();
        sorted_keys.sort();

        // get the pagination limit
        let limit = opts.as_ref().and_then(|o| o.limit).map(|l| l as usize).unwrap_or(usize::MAX);

        // Step 4: Find the correct starting index
        // Convert start_key into `K`
        let start_key_converted: K = start_key.into(); 
        // Step 4: Find the correct starting index
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

        // Step 6: Create async stream for pagination
        let resource_store = self.resource_store.clone();
        let stream = stream! {
            let mut count = 0;
            for key in sorted_keys.iter().skip(start_index) {
                if count >= limit {
                    break; // Stop when limit is reached
                }

                if let Some(&version) = version_map.get(key) {
                    if let Some(value) = resource_store.get(&(key.clone(), version)) {
                        yield (key.clone(), value.clone());
                        count += 1;
                    }
                }
            }
        };


        // ✅ Return final values
        (Box::pin(stream), effective_version, next_continue_token)
    }

    /// **Inserts or updates a resource**
    /// - **Returns `true`** if **created** (new key)
    /// - **Returns `false`** if **updated** (existing key)
    pub fn apply(&mut self, key: K, value: T) -> bool {
        // ✅ Clone previous version mapping to keep history
        let mut new_mapping = self.version_store.get(&self.head)
            .cloned()
            .unwrap_or_default();

        self.head += 1;
        let resource_version = self.head;

        let is_new = !self.version_store.get(&self.head)
            .map_or(false, |mapping| mapping.contains_key(&key));

        // Insert the **new resource version** into the store
        self.resource_store.insert((key.clone(), resource_version), value);

        // ✅ Insert the new key with the current version
        new_mapping.insert(key.clone(), resource_version);

        // ✅ Track this new version
        self.version_store.insert(resource_version, new_mapping);

        println!("📌 After applying key `{}`:", key.clone().into());
        println!("{}", self.dump_store());

        is_new
    }

    /// **Deletes a resource**
    /// - **Returns `Some(T)`** if the resource was deleted
    /// - **Returns `None`** if the resource didn't exist
    pub fn delete(&mut self, key: &K) -> Option<T> {
        // Check if the resource exists before modifying state
        let latest_mapping = self.version_store.get(&self.head)?.clone();
        let deleted_version = latest_mapping.get(key)?;

        self.head += 1;
        let new_version = self.head;

        // Clone the previous version mappings
        let mut new_mapping = latest_mapping.clone();
        new_mapping.remove(key);

        // Remove from `resource_store`
        //let deleted_value = self.resource_store.remove(&(key.clone(), *deleted_version));

        // Store the new version mapping
        self.version_store.insert(new_version, new_mapping);

        // ✅ Return the deleted value, but don't remove from `resource_store`
        self.resource_store.get(&(key.clone(), *deleted_version)).cloned()
    }

    pub fn len(&self) -> usize {
        self.version_store.get(&self.head).map_or(0, |latest_mapping| latest_mapping.len())
    }

    pub fn keys(&self) -> (Vec<K>, u64) {
        let latest_version = self.head;
        (self.version_store
            .get(&latest_version) // Get the latest version mapping
            .map_or_else(Vec::new, |latest_mapping| latest_mapping.keys().cloned().collect())
            , latest_version)
    }
}

pub struct ImmutableMemory<K, T>
where
    K: Ord + Eq + Hash + Clone + Debug + Send + Sync + From<String> + Into<String> + 'static,
    T: Debug + Clone + Send + Sync + 'static,
{
    /// stores versions with resource instances keys/versions
    store: Arc<RwLock<ImmutableMemoryStore<K, T>>>,

    watcher_manager: Option<Arc<WatcherManager<K, T>>>,
}


impl<K, T> ImmutableMemory<K, T>
where
    K: Ord + Eq + Hash + Clone + Debug + Send + Sync + From<String> + Into<String> + 'static,
    T: Debug + Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        // Step 1: Create store
       let store = Arc::new(RwLock::new(ImmutableMemoryStore::new()));

        // Step 2: Create a storer without WatcherManager to please the watcher manager
        let storer: Arc<dyn Storer<K, T>> = Arc::new(Self {
            store: Arc::clone(&store),
            watcher_manager: None,
        });

        let watcher_manager = Arc::new(
            WatcherManager::new(
                Arc::clone(&storer), 
                1024,
            ));

        // Step 4: Rebuild `storer` with `watcher_manager`
        Self {
            store,
            watcher_manager: Some(watcher_manager),
        }
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
}

#[async_trait]
impl<K, T> Storer<K, T> for ImmutableMemory<K, T>
where
    K: Ord + Eq + Hash + Clone + Debug + Send + Sync + From<String> + Into<String> + 'static,
    T: Debug + Clone + Send + Sync + 'static,
{
    async fn get(
        &self,
        key: K,
        opts: Option<GetOptions>,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
        let store = self.store.read().await;
        store.get(&key, opts).ok_or_else(|| {
            Box::new(APIError::NotFound(format!("key: {:?}", key))) as Box<dyn Error + Send + Sync>
        })

    }

    async fn list(
        &self,
        opts: Option<ListOptions>,
    ) -> Result<
        (   
            Pin<Box<dyn Stream<Item = (K, T)> + Send + Sync>>, 
            u64,
            Option<(String, u64)>,
        ), Box<dyn Error + Send + Sync>> {
        let store = self.store.read().await;
        Ok(store.list(opts))
    }

    async fn list_keys(&self, _opts: Option<ListOptions>) -> (Vec<K>, u64) {
        let store = self.store.read().await;
        store.keys()
    }
    async fn len(&self, _opts: Option<ListOptions>) -> usize {
        let store = self.store.read().await;
        store.len()
    }
    async fn apply(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        let mut store = self.store.write().await;
        let is_new = store.apply(key.clone(), value.clone());

        if is_new {
            self.notify_watcher_manager(WatchEvent::Added(value.clone()))
            .await;
        } else {
            self.notify_watcher_manager(WatchEvent::Modified(value.clone()))
            .await;
        }

        Ok(value)
    }
    async fn create(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        self.apply(key, value).await
    }
    async fn update(&self, key: K, value: T) -> Result<T, Box<dyn Error + Send + Sync>> {
        self.apply(key, value).await
    }
    async fn delete(&self, key: K) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut store = self.store.write().await;
        if let Some(value) = store.delete(&key) {
            self.notify_watcher_manager(WatchEvent::Deleted(value.clone()))
            .await;
        }
        Ok(())

    }
    async fn watch(
        &self,
        _ctx: oneshot::Receiver<()>,
        opts: Option<ListOptions>,
    ) -> Result<(oneshot::Sender<()>, ReceiverStream<WatchEvent<T>>), Box<dyn Error + Send + Sync>> {
        match &self.watcher_manager {
            None => Err(Box::new(APIError::InternalServerError(
                "WatcherManager is not initialized".to_string(),
            ))),
            Some(watcher_manager) => {
                let manager = watcher_manager.clone();
                manager.add_watcher(opts).await
            }
        }
    }
}

impl<K, T> ImmutableMemoryStore<K, T>
where
    K: Ord + Eq + Hash + Clone + Debug + Send + Sync + From<String> + Into<String> + 'static,
    T: Debug + Clone + Send + Sync + 'static,
{
    pub fn dump_store(&self) -> String {
        let mut output = String::new();
        output.push_str("🔹 **Version Store:**\n");

        for (rv, mapping) in &self.version_store {
            output.push_str(&format!("  🏷️ RV: {} -> {:?}\n", rv, mapping));
        }

        output.push_str("🔹 **Resource Store:**\n");
        for ((key, version), value) in &self.resource_store {
            output.push_str(&format!("  🔑 Key: {:?} | 📌 Version: {} | 📦 Value: {:?}\n", key, version, value));
        }

        output
    }
}

#[tokio::test]
async fn test_memory_pagination() -> Result<(), Box<dyn Error + Send + Sync>> {
    use crate::storer::{ListOptions, Storer};
    use tokio_stream::StreamExt;

    // ✅ Step 1: Setup In-Memory SQLite Database
    let store = ImmutableMemory::<String, String>::new();

    // ✅ Step 2: Insert Test Records
    let test_data = vec![
        ("key1".to_string(), "value1".to_string()),
        ("key2".to_string(), "value2".to_string()),
        ("key3".to_string(), "value3".to_string()),
    ];

    for (key, value) in &test_data {
        store.apply(key.clone(), value.clone()).await?;
    }

    // ✅ Step 3: Test Full Listing (Limit > Total Records)
    let list_options = ListOptions {
        commit: None,
        watch_only: false,
        limit: Some(10),  // Larger than total records
        continue_token: None,
    };

    let (mut stream, version, continue_token) = store.list(Some(list_options)).await?;
    println!("version {}", version);
    let mut results = Vec::new();

    while let Some((key, value)) = stream.next().await {
        println!("key {}, value {}", key, value);
        results.push((key, value));
    }

    assert_eq!(results.len(), test_data.len(), "Should return all records");
    assert!(continue_token.is_none(), "Continue token should be None");
    
    // ✅ Step 4: Test Paginated Listing (Limit = 1)
    let mut list_options = ListOptions {
        commit: None,
        watch_only: false,
        limit: Some(1),  // Only fetch 1 record per request
        continue_token: None,
    };

    let mut collected_keys = Vec::new();
    //let mut next_token = None;
    println!("\n🔄 **Starting Pagination Test** 🔄");
    loop {
        let (mut stream, version, continue_token) = store.list(Some(list_options.clone())).await?;

        println!("version {}", version);
        if let Some((key, value)) = stream.next().await {
            println!("🔹 Paginated key: {}, value: {}", key, value);
            collected_keys.push(key);
        }

        if let Some(token) = continue_token {
            let next_token: Option<String> = Some(token.0); // Save token for next iteration
            list_options.continue_token = Some(next_token.clone().unwrap()); // Update for next query
            println!("➡️ Continue Token: {}", next_token.clone().unwrap());
        } else {
            println!("✅ **No more pages, finished pagination**");
            break;  // Stop when no more continue tokens
        }
    }

    assert_eq!(collected_keys.len(), test_data.len(), "Should fetch all records in multiple queries");
    assert_eq!(collected_keys.sort(), test_data.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>().sort(), "All keys should match expected order");
    
    // ✅ Step 5: Delete a Key (`key2`)
    println!("\n🗑 **Deleting key2...**");
    store.delete("key2".to_string()).await?;

    // ✅ Step 6: Verify listing after deletion
    let list_options = ListOptions {
        commit: None,
        watch_only: false,
        limit: Some(10),
        continue_token: None,
    };

    let (mut stream, version, _continue_token) = store.list(Some(list_options)).await?;
    println!("📌 **Version After Delete**: {}", version);
    let mut results_after_delete = Vec::new();

    while let Some((key, value)) = stream.next().await {
        println!("🔹 Key: {}, Value: {}", key, value);
        results_after_delete.push((key, value));
    }

    assert_eq!(results_after_delete.len(), 2, "❌ Should have 2 records after deletion");
    assert!(
        !results_after_delete.iter().any(|(k, _)| k == "key2"),
        "❌ `key2` should not be present after deletion"
    );

    // ✅ Step 7: Test Paginated Listing Again After Deletion
    let mut list_options = ListOptions {
        commit: None,
        watch_only: false,
        limit: Some(1),
        continue_token: None,
    };

    let mut collected_keys_after_delete = Vec::new();
    println!("\n🔄 **Starting Pagination Test After Deletion** 🔄");
    loop {
        let (mut stream, version, continue_token) = store.list(Some(list_options.clone())).await?;

        println!("🔍 Listing Version: {}", version);
        if let Some((key, value)) = stream.next().await {
            println!("🔹 Paginated Key: {}, Value: {}", key, value);
            collected_keys_after_delete.push(key);
        }

        if let Some(token) = continue_token {
            let next_token: Option<String> = Some(token.0);
            list_options.continue_token = Some(next_token.clone().unwrap());
            println!("➡️ Continue Token: {}", next_token.unwrap());
        } else {
            println!("✅ **No more pages, finished pagination**");
            break;
        }
    }

    assert_eq!(collected_keys_after_delete.len(), 2, "❌ Pagination should return 2 records after delete");
    Ok(())
}