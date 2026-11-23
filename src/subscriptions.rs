use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

use futures::{
    future::{AbortHandle, Abortable},
    FutureExt, StreamExt,
};
use std::panic::AssertUnwindSafe;
use serde::Serialize;
use serde_json::Value;

use crate::registry::ValueStream;

/// A message sent to the webview over the subscription's IPC channel.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Event {
    Next { value: Value },
    Error { message: String },
    Complete,
}

struct Entry {
    webview: String,
    window: String,
    /// The source subscribed to, for scope checks on later commands about this subscription.
    source: String,
    abort: AbortHandle,
}

/// Tracks the live subscriptions and which webview/window owns each, so they can be torn down
/// when the frontend unsubscribes, reloads, or its window is destroyed.
#[derive(Default)]
pub(crate) struct Subscriptions {
    next_id: AtomicU64,
    active: Mutex<HashMap<u64, Entry>>,
}

impl Subscriptions {
    /// Pumps `stream` into `sink` on the async runtime until the stream ends, the sink reports
    /// the receiver is gone (returns `false`), or the subscription is cancelled.
    pub fn spawn<F>(
        self: &Arc<Self>,
        webview: String,
        window: String,
        source: String,
        mut stream: ValueStream,
        sink: F,
    ) -> u64
    where
        F: Fn(Event) -> bool + Send + Sync + 'static,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (abort, registration) = AbortHandle::new_pair();
        self.lock().insert(id, Entry { webview, window, source, abort });

        let this = Arc::clone(self);
        let sink = Arc::new(sink);
        let pump_sink = Arc::clone(&sink);
        let this_sink = sink;
        tauri::async_runtime::spawn(async move {
            let sink = pump_sink;
            let pump = async move {
                while let Some(item) = stream.next().await {
                    let event = match item {
                        Ok(value) => Event::Next { value },
                        Err(message) => {
                            sink(Event::Error { message });
                            return;
                        }
                    };
                    if !sink(event) {
                        return;
                    }
                }
                sink(Event::Complete);
            };
            // A panicking user stream must not skip the cleanup below (or leave the frontend waiting).
            if AssertUnwindSafe(Abortable::new(pump, registration)).catch_unwind().await.is_err() {
                let sink = Arc::clone(&this_sink);
                sink(Event::Error { message: "the stream panicked".into() });
            }
            this.lock().remove(&id);
        });
        id
    }

    /// The source of a subscription owned by `webview`, if it exists.
    pub fn source_of(&self, id: u64, webview: &str) -> Option<String> {
        self.lock().get(&id).filter(|e| e.webview == webview).map(|e| e.source.clone())
    }

    /// Cancels a subscription, but only if it belongs to `webview` (webviews can't cancel each other's).
    pub fn cancel_owned(&self, id: u64, webview: &str) -> bool {
        let mut active = self.lock();
        match active.get(&id) {
            Some(entry) if entry.webview == webview => {
                active.remove(&id).unwrap().abort.abort();
                true
            }
            _ => false,
        }
    }

    pub fn cancel_webview(&self, label: &str) -> usize {
        self.cancel_where(|e| e.webview == label)
    }

    pub fn cancel_window(&self, label: &str) -> usize {
        self.cancel_where(|e| e.window == label)
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    fn cancel_where(&self, pred: impl Fn(&Entry) -> bool) -> usize {
        let mut active = self.lock();
        let ids: Vec<u64> = active.iter().filter(|(_, e)| pred(e)).map(|(id, _)| *id).collect();
        for id in &ids {
            active.remove(id).unwrap().abort.abort();
        }
        ids.len()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<u64, Entry>> {
        self.active.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Reactive, ReactiveState};
    use serde_json::json;
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn collector() -> (impl Fn(Event) -> bool + Send + 'static, mpsc::UnboundedReceiver<Event>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (move |e| tx.send(e).is_ok(), rx)
    }

    async fn recv(rx: &mut mpsc::UnboundedReceiver<Event>) -> Option<Event> {
        tokio::time::timeout(Duration::from_secs(2), rx.recv()).await.expect("timed out waiting for event")
    }

    async fn wait_until(f: impl Fn() -> bool) {
        for _ in 0..200 {
            if f() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("condition not reached");
    }

    #[tokio::test]
    async fn state_subscription_emits_and_cancels() {
        let reactive = Reactive::default();
        let state = ReactiveState::new(0);
        reactive.state("counter", &state);

        let (sink, mut rx) = collector();
        let stream = reactive.open("counter", Value::Null).unwrap();
        let id = reactive.subscriptions.spawn("main".into(), "main".into(), "s".into(), stream, sink);

        assert_eq!(recv(&mut rx).await, Some(Event::Next { value: json!(0) }));
        state.set(5);
        assert_eq!(recv(&mut rx).await, Some(Event::Next { value: json!(5) }));

        assert!(!reactive.subscriptions.cancel_owned(id, "other-webview"));
        assert!(reactive.subscriptions.cancel_owned(id, "main"));
        assert_eq!(reactive.active_subscriptions(), 0);
        // The pump task drops the sink once aborted, closing the channel.
        assert_eq!(recv(&mut rx).await, None);
    }

    #[tokio::test]
    async fn finite_stream_completes_and_is_removed() {
        let reactive = Reactive::default();
        reactive.stream("nums", |n: u32| futures::stream::iter(0..n));

        let (sink, mut rx) = collector();
        let stream = reactive.open("nums", json!(2)).unwrap();
        reactive.subscriptions.spawn("w".into(), "w".into(), "s".into(), stream, sink);

        assert_eq!(recv(&mut rx).await, Some(Event::Next { value: json!(0) }));
        assert_eq!(recv(&mut rx).await, Some(Event::Next { value: json!(1) }));
        assert_eq!(recv(&mut rx).await, Some(Event::Complete));
        wait_until(|| reactive.active_subscriptions() == 0).await;
    }

    #[tokio::test]
    async fn stream_error_is_terminal() {
        let reactive = Reactive::default();
        reactive.try_stream("flaky", |_: ()| {
            futures::stream::iter(vec![Ok(1), Err("boom"), Ok(3)])
        });

        let (sink, mut rx) = collector();
        let stream = reactive.open("flaky", Value::Null).unwrap();
        reactive.subscriptions.spawn("w".into(), "w".into(), "s".into(), stream, sink);

        assert_eq!(recv(&mut rx).await, Some(Event::Next { value: json!(1) }));
        assert_eq!(recv(&mut rx).await, Some(Event::Error { message: "boom".into() }));
        assert_eq!(recv(&mut rx).await, None);
    }

    #[tokio::test]
    async fn panicking_stream_reports_an_error_and_is_cleaned_up() {
        let reactive = Reactive::default();
        reactive.stream("boom", |_: ()| {
            futures::stream::iter(0..3u8).map(|n| if n == 1 { panic!("user stream bug") } else { n })
        });

        let (sink, mut rx) = collector();
        let stream = reactive.open("boom", Value::Null).unwrap();
        reactive.subscriptions.spawn("w".into(), "w".into(), "boom".into(), stream, sink);

        assert_eq!(recv(&mut rx).await, Some(Event::Next { value: json!(0) }));
        assert_eq!(recv(&mut rx).await, Some(Event::Error { message: "the stream panicked".into() }));
        wait_until(|| reactive.active_subscriptions() == 0).await;
    }

    #[tokio::test]
    async fn source_of_respects_ownership() {
        let reactive = Reactive::default();
        reactive.state("s", &ReactiveState::new(0));
        let stream = reactive.open("s", Value::Null).unwrap();
        let id = reactive.subscriptions.spawn("a".into(), "win".into(), "s".into(), stream, |_| true);
        assert_eq!(reactive.subscriptions.source_of(id, "a").as_deref(), Some("s"));
        assert_eq!(reactive.subscriptions.source_of(id, "b"), None);
        assert_eq!(reactive.subscriptions.source_of(id + 1, "a"), None);
    }

    #[tokio::test]
    async fn dead_sink_stops_the_pump() {
        let reactive = Reactive::default();
        let state = ReactiveState::new(0);
        reactive.state("s", &state);

        let stream = reactive.open("s", Value::Null).unwrap();
        reactive.subscriptions.spawn("w".into(), "w".into(), "s".into(), stream, |_| false);
        wait_until(|| reactive.active_subscriptions() == 0).await;
    }

    #[tokio::test]
    async fn window_teardown_only_hits_that_window() {
        let reactive = Reactive::default();
        let state = ReactiveState::new(0);
        reactive.state("s", &state);

        for (webview, window) in [("a", "win-1"), ("b", "win-1"), ("c", "win-2")] {
            let stream = reactive.open("s", Value::Null).unwrap();
            reactive.subscriptions.spawn(webview.into(), window.into(), "s".into(), stream, |_| true);
        }
        assert_eq!(reactive.active_subscriptions(), 3);
        assert_eq!(reactive.subscriptions.cancel_window("win-1"), 2);
        assert_eq!(reactive.active_subscriptions(), 1);
        assert_eq!(reactive.subscriptions.cancel_webview("c"), 1);
        assert_eq!(reactive.active_subscriptions(), 0);
    }
}
