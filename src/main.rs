use std::sync::Arc;
use storer::watch::WatchEvent;
use storer::Memory;
use storer::Storer; // Import the Storer trait
use tokio_stream::StreamExt;

/*
#[tokio::main]
async fn main() {
    let storer = Arc::new(Memory::<String, String>::new());

    // Task 1: Insert some keys
    let storer_clone = storer.clone();
    let task1 = tokio::spawn(async move {
        storer_clone
            .update("key1".to_string(), "value1".to_string())
            .await
            .expect("Failed to create key1");
        storer_clone
            .create("key2".to_string(), "value2".to_string())
            .await
            .expect("Failed to create key2");
        println!("Task 1: Created key1 and key2");
    });

    // Task 2: Read a key
    let storer_clone = storer.clone();
    let task2 = tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await; // Ensure task1 runs first
        match storer_clone.get("key1".to_string(), None).await {
            Ok(value) => println!("Task 2: Got value for key1: {}", value),
            Err(err) => println!("Task 2: Error fetching key1: {}", err),
        }
        match storer_clone.get("key2".to_string(), None).await {
            Ok(value) => println!("Task 2: Got value for key1: {}", value),
            Err(err) => println!("Task 2: Error fetching key1: {}", err),
        }
    });

    // Task 3: Update a key
    let storer_clone = storer.clone();
    let task3 = tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // Ensure task1 runs first
        storer_clone
            .update("key1".to_string(), "updated_value1".to_string())
            .await
            .expect("Failed to update key1");
        println!("Task 3: Updated key1 to updated_value1");
    });

    // Task 4: Delete a key
    let storer_clone = storer.clone();
    let task4 = tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await; // Ensure task1 runs first
        storer_clone
            .delete("key2".to_string())
            .await
            .expect("Failed to delete key2");
        println!("Task 4: Deleted key2");
    });

    // Task 5: List all keys
    let storer_clone = storer.clone();
    let task5 = tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await; // Ensure all modifications are complete
        let keys = storer_clone.list_keys(None).await;
        println!("Task 5: Remaining keys: {:?}", keys);
    });

    // Wait for all tasks to complete
    let _ = tokio::join!(task1, task2, task3, task4, task5);
}
*/

#[tokio::main]
async fn main() {
    // Create the Memory store
    let memory = Arc::new(Memory::<String, String>::new());

    // Create initial key-value pair
    memory
        .create("key1".to_string(), "value1".to_string())
        .await
        .expect("Failed to create key1");

    // Simulate some operations
    /*
    memory
        .apply("key1".to_string(), "new_value1".to_string())
        .await
        .expect("Failed to modify key1");
    */

    memory
        .apply("key2".to_string(), "value2".to_string())
        .await
        .expect("Failed to create key2");

    /*
    memory
        .delete("key1".to_string())
        .await
        .expect("Failed to delete key1");
    */

    // Start a watcher
    let (stop_signal, mut event_stream) = memory
        .watch(tokio::sync::oneshot::channel().1, None)
        .await
        .expect("Failed to create watcher");

    // Spawn a task to handle incoming events
    let event_task = tokio::spawn(async move {
        while let Some(event) = event_stream.next().await {
            match event {
                WatchEvent::Added(value) => {
                    println!("Event: Added => value {}", value)
                }
                WatchEvent::Modified(value) => {
                    println!("Event: Modified => value {}", value)
                }
                WatchEvent::Deleted(value) => {
                    println!("Event: Deleted => value {}", value)
                }
                WatchEvent::Error => eprintln!("Event: Error occurred"),
            }
        }
    });

    // Wait for some time to observe events
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    memory
        .apply("key3".to_string(), "value3".to_string())
        .await
        .expect("Failed to apply key3");

    memory
        .apply("key4".to_string(), "value4".to_string())
        .await
        .expect("Failed to apply key4");

    memory
        .apply("key3".to_string(), "value3.1".to_string())
        .await
        .expect("Failed to apply key3");

    // Wait for some time to observe events
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Stop the watcher
    let _ = stop_signal.send(());

    // Ensure the watcher stops
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    println!("Watcher stopped. Exiting application.");

    let (mut stream, _resource_version, _continue_token) = memory.list(None).await.expect("Failed to list keys");
    while let Some((key, value)) = stream.next().await {
        println!("Data key {} value {}", key, value);

        //let local_results_clone = Arc::clone(&local_results);
        //tokio::spawn(async move {
        //    let mut results = local_results_clone.lock().await;
        //    results.push((key, value));
        //});
    }

    /* 
    let list_fn = Box::new(|key: String, value: String| {
        println!("Data key {} value {}", key, value);
    });
    memory.list(list_fn, None).await.unwrap();
    */
    // Ensure event processing task completes
    event_task.await.expect("Event task failed");
}
