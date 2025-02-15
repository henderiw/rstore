use crate::storer::ListOptions;
use crate::watch::WatchEvent;
use std::cmp::{Eq, Ord};
use std::collections::HashMap;
use std::error::Error;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, RwLock};
use tokio::task;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tokio_stream::StreamExt;

pub struct WatcherManager<K, T> {
    store: Arc<RwLock<HashMap<K, T>>>,
    broadcast_channel: broadcast::Sender<WatchEvent<K, T>>,
    event_channel: Arc<Mutex<mpsc::Receiver<WatchEvent<K, T>>>>,
    max_watchers: usize,
    running: Arc<Mutex<bool>>,
}

impl<K, T> WatcherManager<K, T>
where
    K: Ord + Eq + Hash + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    pub fn new(
        store: Arc<RwLock<HashMap<K, T>>>,
        max_watchers: usize,
    ) -> (Self, mpsc::Sender<WatchEvent<K, T>>) {
        let (broadcast_channel_tx, _) = broadcast::channel(100);
        let (event_channel_tx, event_channel_rx) = mpsc::channel(100);
        (
            Self {
                store: store,
                broadcast_channel: broadcast_channel_tx,
                event_channel: Arc::new(Mutex::new(event_channel_rx)),
                max_watchers: max_watchers,
                running: Arc::new(Mutex::new(false)),
            },
            event_channel_tx,
        )
    }

    pub async fn is_running(&self) -> bool {
        let running = self.running.lock().await;
        *running
    }

    pub async fn add_watcher(
        self: Arc<Self>,
        opts: Option<ListOptions>,
    ) -> Result<(oneshot::Sender<()>, ReceiverStream<WatchEvent<K, T>>), Box<dyn Error>> {
        if self.broadcast_channel.receiver_count() >= self.max_watchers {
            return Err("Max watchers reached".to_string().into());
        }

        // Ensure the manager thread is running
        self.ensure_manager_thread_running().await;

        // Parse options
        let watch_only = opts.as_ref().map_or(false, |o| o.watch_only);

        // Create the watcher channels
        let (stop_tx, stop_rx) = oneshot::channel();
        let (watch_tx, watch_rx) = tokio::sync::mpsc::channel(100);
        let broadcast_stream = BroadcastStream::new(self.broadcast_channel.subscribe());

        // Initialize shared state for backlog and catch-up flag
        let backlog = Arc::new(Mutex::new(Vec::new()));
        let catchup_done = Arc::new(Mutex::new(false));

        // Start the catch-up phase if required
        if !watch_only {
            self.start_catchup_phase(watch_tx.clone(), backlog.clone(), catchup_done.clone())
                .await;
        }

        // Start processing live events
        self.spawn_watcher_task(
            stop_rx,
            broadcast_stream,
            watch_tx,
            backlog,
            catchup_done,
            watch_only,
        );

        Ok((stop_tx, ReceiverStream::new(watch_rx)))
    }
    async fn ensure_manager_thread_running(self: &Arc<Self>) {
        let mut running = self.running.lock().await;
        if !*running {
            *running = true;
            self.clone().start_manager_thread();
        }
    }

    async fn start_catchup_phase(
        &self,
        watch_tx: tokio::sync::mpsc::Sender<WatchEvent<K, T>>,
        backlog: Arc<Mutex<Vec<WatchEvent<K, T>>>>,
        catchup_done: Arc<Mutex<bool>>,
    ) {
        let store = self.store.clone();
        tokio::spawn(async move {
            let store_lock = store.read().await;
            let mut keys: Vec<_> = store_lock.keys().cloned().collect();
            keys.sort();

            for key in keys {
                if let Some(value) = store_lock.get(&key) {
                    let event = WatchEvent::Added(key.clone(), value.clone());
                    if watch_tx.send(event).await.is_err() {
                        return; // Consumer dropped
                    }
                }
            }

            let mut backlog_lock = backlog.lock().await;
            for event in backlog_lock.drain(..) {
                if watch_tx.send(event).await.is_err() {
                    return; // Consumer dropped
                }
            }

            let mut catchup_done_lock = catchup_done.lock().await;
            *catchup_done_lock = true;
        });
    }

    fn spawn_watcher_task(
        &self,
        mut stop_rx: oneshot::Receiver<()>,
        mut broadcast_stream: BroadcastStream<WatchEvent<K, T>>,
        watch_tx: tokio::sync::mpsc::Sender<WatchEvent<K, T>>,
        backlog: Arc<Mutex<Vec<WatchEvent<K, T>>>>,
        catchup_done: Arc<Mutex<bool>>,
        watch_only: bool,
    ) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Process live events
                    Some(event) = broadcast_stream.next() => {
                        match event {
                            Ok(event) => {
                                let catchup_done_lock = catchup_done.lock().await;

                                if *catchup_done_lock || watch_only {
                                    if watch_tx.send(event.clone()).await.is_err() {
                                        break; // Consumer dropped
                                    }
                                } else {
                                    let mut backlog_lock = backlog.lock().await;
                                    backlog_lock.push(event.clone());
                                }
                            }
                            Err(_) => break,
                        }
                    }

                    // Stop the watcher when the stop signal is received
                    _ = &mut stop_rx => {
                        break;
                    }
                }
            }
        });
    }

    fn start_manager_thread(self: Arc<Self>) {
        let running = Arc::clone(&self.running);
        let event_channel = Arc::clone(&self.event_channel);
        let broadcast_channel = self.broadcast_channel.clone();

        task::spawn(async move {
            loop {
                {
                    tokio::select! {
                        // Process events from the memory store
                        Some(event) = async {
                            let mut event_channel_lock = event_channel.lock().await;
                            event_channel_lock.recv().await
                        } => {
                            let _ = broadcast_channel.send(event.clone());
                        }

                        else => {
                            let active_subscribers = broadcast_channel.receiver_count();
                            if active_subscribers == 0 {
                                *running.lock().await = false;
                                break;
                            }
                        }

                        // Periodically clean up stopped watchers
                        /*
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                            // If no watchers remain, stop the manager thread
                            let active_subscribers = broadcast_channel.receiver_count();
                            if active_subscribers == 0 {
                                *running.lock().await = false;
                                break;
                            }
                        }
                        */
                    }
                }
            }
        });
    }
}
