//! Frontend -> Rust streams. The webview pushes values; a Rust handler consumes them as a
//! [`SinkStream`]. Pushes are awaited by the frontend client one batch at a time, and the channel
//! is bounded, so a slow handler applies backpressure all the way to JS.
//!
//! A handler only *completes* (its stream ends with `None`) when the frontend calls `complete()`.
//! If the writer fails or disappears (producer error, page reload, window closed) the handler is
//! *aborted*: its future is dropped, so code after its loop (e.g. a commit) never runs.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    task::{Context, Poll},
};

use futures::{
    future::{AbortHandle, BoxFuture},
    Stream,
};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::{Error, Result};

/// How many pushed values may be buffered before pushes wait for the handler.
pub(crate) const SINK_BUFFER: usize = 64;

/// The values the frontend pushes into a sink, in order. Ends (`None`) only when the frontend calls
/// `complete()`. If the writer fails or goes away instead, the handler is aborted and this stream never
/// ends: it can't be mistaken for a complete one.
pub struct SinkStream<T> {
    pub(crate) rx: mpsc::Receiver<T>,
    /// Set *before* the sender is dropped on abort, so an end-of-channel caused by an abort is
    /// never reported as `None`, even to a poll already in progress on another thread.
    pub(crate) aborted: Arc<AtomicBool>,
}

impl<T> Stream for SinkStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if self.aborted.load(Ordering::SeqCst) {
            return Poll::Pending; // the abort wakes the task; `Abortable` then drops the handler
        }
        let next = self.rx.poll_recv(cx);
        if self.aborted.load(Ordering::SeqCst) {
            return Poll::Pending;
        }
        next
    }
}

pub(crate) type PushFn = Arc<dyn Fn(Vec<Value>) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// A sink handler that has been started for one frontend writer.
pub(crate) struct OpenedSink {
    pub push: PushFn,
    /// Resolves with how the handler ended.
    pub done: oneshot::Receiver<HandlerOutcome>,
    /// Cancels the handler (drops its future) when the writer did not complete normally.
    pub abort: AbortHandle,
    /// Shared with the handler's [`SinkStream`]; see there.
    pub aborted: Arc<AtomicBool>,
}

/// How a sink handler ended.
pub(crate) enum HandlerOutcome {
    /// Returned `Ok` (normally after its stream ended, or on its own, e.g. after an early `break`).
    Completed,
    /// Returned `Err`.
    Failed(String),
    /// Was aborted before it finished.
    Aborted,
}

/// What `sink_abort` actually did. A handler may have finished on its own before the abort arrived;
/// the frontend must then learn the real outcome instead of being told the run was abandoned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AbortOutcome {
    /// The handler was stopped before finishing; it never completed.
    Aborted,
    /// The handler had already completed successfully (it may have committed).
    Completed,
}

fn outcome_of(done: std::result::Result<HandlerOutcome, oneshot::error::RecvError>) -> Result<AbortOutcome> {
    match done {
        Ok(HandlerOutcome::Completed) => Ok(AbortOutcome::Completed),
        Ok(HandlerOutcome::Aborted) => Ok(AbortOutcome::Aborted),
        Ok(HandlerOutcome::Failed(message)) => Err(Error::SinkFailed(message)),
        Err(_) => Err(Error::SinkFailed("sink handler panicked".into())),
    }
}

/// Aborts an entry's handler. The order matters: flag, then abort, then drop the sender (with the entry).
fn abort_entry(entry: Entry) {
    entry.sink.aborted.store(true, Ordering::SeqCst);
    entry.sink.abort.abort();
    drop(entry);
}

struct Entry {
    webview: String,
    window: String,
    /// The sink's source name, for scope checks on push/close.
    source: String,
    sink: OpenedSink,
}

#[derive(Default)]
pub(crate) struct Sinks {
    next_id: AtomicU64,
    active: Mutex<HashMap<u64, Entry>>,
}

impl Sinks {
    pub fn insert(&self, webview: String, window: String, source: String, sink: OpenedSink) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.lock().insert(id, Entry { webview, window, source, sink });
        id
    }

    /// The source name and push function of a sink owned by `webview`.
    pub fn push_fn(&self, id: u64, webview: &str) -> Result<(String, PushFn)> {
        match self.lock().get(&id) {
            Some(e) if e.webview == webview => Ok((e.source.clone(), Arc::clone(&e.sink.push))),
            _ => Err(Error::UnknownSink(id)),
        }
    }

    /// The source name of a sink owned by `webview`.
    pub fn source_of(&self, id: u64, webview: &str) -> Result<String> {
        match self.lock().get(&id) {
            Some(e) if e.webview == webview => Ok(e.source.clone()),
            _ => Err(Error::UnknownSink(id)),
        }
    }

    /// Ends the stream (the handler sees `None`) and waits for the handler's result.
    pub async fn close(&self, id: u64, webview: &str) -> Result<()> {
        let entry = {
            let mut active = self.lock();
            match active.get(&id) {
                Some(e) if e.webview == webview => active.remove(&id).unwrap(),
                _ => return Err(Error::UnknownSink(id)),
            }
        };
        let Entry { sink: OpenedSink { push, done, .. }, .. } = entry;
        drop(push); // drops the sender => end of stream
        match outcome_of(done.await)? {
            AbortOutcome::Completed => Ok(()),
            // Aborted concurrently (e.g. a reload) after we took the entry: not a completion.
            AbortOutcome::Aborted => Err(Error::SinkFailed("sink was aborted".into())),
        }
    }

    /// Aborts a sink owned by `webview`: the handler's future is dropped, so it never runs the code
    /// after its loop (a writer that failed or went away must not look like a completed one).
    ///
    /// Then waits for the handler and reports what really happened: if it had already finished on
    /// its own, that is `Completed` (or its `SinkFailed` error), not `Aborted`.
    pub async fn abort(&self, id: u64, webview: &str) -> Result<AbortOutcome> {
        let entry = {
            let mut active = self.lock();
            match active.get(&id) {
                Some(e) if e.webview == webview => active.remove(&id).unwrap(),
                _ => return Err(Error::UnknownSink(id)),
            }
        };
        let Entry { sink: OpenedSink { push, done, abort, aborted }, .. } = entry;
        aborted.store(true, Ordering::SeqCst); // same order as abort_entry: flag, abort, drop sender
        abort.abort();
        drop(push);
        outcome_of(done.await)
    }

    /// Removes and aborts one sink (used when a reload raced its opening).
    pub fn discard(&self, id: u64) {
        if let Some(e) = self.lock().remove(&id) {
            abort_entry(e);
        }
    }

    /// Aborts every sink owned by a webview / window: their writer is gone without completing.
    pub fn release_webview(&self, label: &str) -> usize {
        self.release_where(|e| e.webview == label)
    }

    pub fn release_window(&self, label: &str) -> usize {
        self.release_where(|e| e.window == label)
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    fn release_where(&self, pred: impl Fn(&Entry) -> bool) -> usize {
        let mut active = self.lock();
        let ids: Vec<u64> = active.iter().filter(|(_, e)| pred(e)).map(|(id, _)| *id).collect();
        for id in &ids {
            abort_entry(active.remove(id).unwrap());
        }
        ids.len()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<u64, Entry>> {
        self.active.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// The plugin is going away (app teardown) with writers still open: they never completed, so abort
/// their handlers rather than letting the dropped senders end their streams normally.
impl Drop for Sinks {
    fn drop(&mut self) {
        let active = std::mem::take(&mut *self.lock());
        for (_, entry) in active {
            abort_entry(entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Error, Reactive, ReactiveState};
    use futures::StreamExt;
    use serde_json::json;
    use std::time::Duration;

    async fn push(reactive: &Reactive, id: u64, values: Vec<serde_json::Value>) -> crate::Result<()> {
        let (_, push) = reactive.sinks.push_fn(id, "w")?;
        push(values).await
    }

    #[tokio::test]
    async fn values_arrive_in_order_and_close_returns_handler_result() {
        let reactive = Reactive::default();
        let total = ReactiveState::new(Vec::<u32>::new());
        let t = total.clone();
        reactive.sink("collect", move |prefix: String, mut values: crate::SinkStream<u32>| {
            let t = t.clone();
            async move {
                assert_eq!(prefix, "p");
                while let Some(v) = values.next().await {
                    tokio::time::sleep(Duration::from_millis(1)).await; // slow consumer
                    t.update(|all| all.push(v));
                }
            }
        });

        let id = reactive.open_sink("collect", json!("p"), "w".into(), "w".into()).unwrap();
        assert_eq!(reactive.active_sinks(), 1);
        // More values than the buffer: pushes must wait on the handler, not drop anything.
        push(&reactive, id, (0..200).map(|n| json!(n)).collect()).await.unwrap();
        push(&reactive, id, vec![json!(200)]).await.unwrap();
        reactive.sinks.close(id, "w").await.unwrap();
        assert_eq!(total.get(), (0..=200).collect::<Vec<u32>>());
        assert_eq!(reactive.active_sinks(), 0);
    }

    #[tokio::test]
    async fn handler_error_and_invalid_values() {
        let reactive = Reactive::default();
        reactive.try_sink("strict", |_: (), mut values: crate::SinkStream<u32>| async move {
            let mut expected = 0;
            while let Some(v) = values.next().await {
                if v != expected {
                    return Err(format!("out of order: expected {expected}, got {v}"));
                }
                expected += 1;
            }
            Ok(())
        });

        // A handler error: further pushes report the sink closed, and close returns the error.
        let id = reactive.open_sink("strict", serde_json::Value::Null, "w".into(), "w".into()).unwrap();
        push(&reactive, id, vec![json!(0), json!(5)]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(matches!(push(&reactive, id, vec![json!(6)]).await, Err(Error::SinkClosed(_))));
        match reactive.sinks.close(id, "w").await {
            Err(Error::SinkFailed(m)) => assert!(m.contains("expected 1, got 5"), "{m}"),
            other => panic!("unexpected {other:?}"),
        }

        // An invalid value: rejected, and the sink is aborted (not merely skipped), so nothing
        // further is accepted and the handler never completes.
        let id = reactive.open_sink("strict", serde_json::Value::Null, "w".into(), "w".into()).unwrap();
        assert!(matches!(push(&reactive, id, vec![json!(0), json!("nope")]).await, Err(Error::InvalidValue { .. })));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(matches!(push(&reactive, id, vec![json!(1)]).await, Err(Error::SinkClosed(_))));
        match reactive.sinks.close(id, "w").await {
            Err(Error::SinkFailed(m)) => assert!(m.contains("aborted"), "{m}"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn abort_and_release_drop_the_handler_before_its_commit() {
        let reactive = Reactive::default();
        let committed = ReactiveState::new(Vec::<u32>::new());
        let c = committed.clone();
        reactive.sink("upload", move |_: (), mut rows: crate::SinkStream<u32>| {
            let c = c.clone();
            async move {
                let mut batch = Vec::new();
                while let Some(r) = rows.next().await {
                    batch.push(r);
                }
                c.set(batch); // "commit": must only happen for a completed writer
            }
        });

        // Aborted by the writer (producer error).
        let id = reactive.open_sink("upload", serde_json::Value::Null, "w".into(), "w".into()).unwrap();
        push(&reactive, id, vec![json!(1), json!(2)]).await.unwrap();
        assert!(matches!(reactive.sinks.abort(id, "other").await, Err(Error::UnknownSink(_))));
        assert_eq!(reactive.sinks.abort(id, "w").await.unwrap(), super::AbortOutcome::Aborted);
        assert!(matches!(reactive.sinks.close(id, "w").await, Err(Error::UnknownSink(_))));

        // Released by a reload / window close.
        let id2 = reactive.open_sink("upload", serde_json::Value::Null, "w".into(), "win".into()).unwrap();
        push(&reactive, id2, vec![json!(3)]).await.unwrap();
        reactive.sinks.release_window("win");

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(committed.get().is_empty(), "an aborted handler committed: {:?}", committed.get());
        assert_eq!(reactive.active_sinks(), 0);

        // A completed writer does commit.
        let id3 = reactive.open_sink("upload", serde_json::Value::Null, "w".into(), "w".into()).unwrap();
        push(&reactive, id3, vec![json!(7)]).await.unwrap();
        reactive.sinks.close(id3, "w").await.unwrap();
        assert_eq!(committed.get(), vec![7]);
    }

    /// Stress the race the review loop found: release while the handler is mid-poll on another
    /// worker thread (just woken by a push). An aborted handler must never reach its commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn release_racing_a_busy_handler_never_commits() {
        let reactive = Reactive::default();
        let commits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = commits.clone();
        reactive.sink("upload", move |_: (), mut rows: crate::SinkStream<u32>| {
            let c = c.clone();
            async move {
                while rows.next().await.is_some() {}
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst); // the "commit"
            }
        });
        for i in 0..300 {
            let window = format!("win-{i}");
            let id = reactive.open_sink("upload", serde_json::Value::Null, "w".into(), window.clone()).unwrap();
            push(&reactive, id, vec![json!(1)]).await.unwrap(); // wakes the handler...
            reactive.sinks.release_window(&window); // ...and releases while it may be running
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 0, "an aborted handler committed");
    }

    fn counting_upload(reactive: &Reactive) -> std::sync::Arc<std::sync::atomic::AtomicUsize> {
        let commits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = commits.clone();
        reactive.sink("upload", move |_: (), mut rows: crate::SinkStream<u32>| {
            let c = c.clone();
            async move {
                while rows.next().await.is_some() {}
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst); // the "commit"
            }
        });
        commits
    }

    /// A misbehaving client sends an invalid push and a close at the same time: the close may remove
    /// the entry first, but the push aborts the handler itself, so the truncated run never commits.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invalid_push_racing_close_never_commits() {
        let reactive = Reactive::default();
        let commits = counting_upload(&reactive);
        for _ in 0..100 {
            let id = reactive.open_sink("upload", serde_json::Value::Null, "w".into(), "w".into()).unwrap();
            let (_, push) = reactive.sinks.push_fn(id, "w").unwrap();
            let (pushed, closed) = tokio::join!(push(vec![json!(1), json!("bad")]), reactive.sinks.close(id, "w"));
            assert!(matches!(pushed, Err(Error::InvalidValue { .. })));
            assert!(closed.is_err(), "a close racing an invalid push must not report success");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    /// Tearing the plugin down with writers still open aborts their handlers.
    #[tokio::test]
    async fn dropping_the_registry_aborts_open_sinks() {
        let reactive = Reactive::default();
        let commits = counting_upload(&reactive);
        let id = reactive.open_sink("upload", serde_json::Value::Null, "w".into(), "w".into()).unwrap();
        push(&reactive, id, vec![json!(1)]).await.unwrap();
        drop(reactive);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    /// Backpressure: with a handler that isn't reading, a push can only buffer SINK_BUFFER values;
    /// the rest of the push waits (so the frontend, which keeps one push in flight, is slowed down).
    #[tokio::test]
    async fn a_push_beyond_the_buffer_waits_for_the_handler() {
        let reactive = Reactive::default();
        let (go_tx, go_rx) = tokio::sync::oneshot::channel::<()>();
        let go = std::sync::Mutex::new(Some(go_rx));
        reactive.sink("slow", move |_: (), mut rows: crate::SinkStream<u32>| {
            let go = go.lock().unwrap().take().unwrap();
            async move {
                let _ = go.await; // don't read anything until released
                while rows.next().await.is_some() {}
            }
        });
        let id = reactive.open_sink("slow", serde_json::Value::Null, "w".into(), "w".into()).unwrap();
        let (_, push_fn) = reactive.sinks.push_fn(id, "w").unwrap();

        let within: Vec<_> = (0..super::SINK_BUFFER as u32).map(|n| json!(n)).collect();
        tokio::time::timeout(Duration::from_secs(2), push_fn(within)).await.expect("fits in the buffer").unwrap();

        let mut more = Box::pin(push_fn(vec![json!(999)]));
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut more).await.is_err(),
            "a push beyond the buffer must wait for the handler"
        );
        go_tx.send(()).unwrap(); // the handler starts reading...
        tokio::time::timeout(Duration::from_secs(2), more).await.expect("...and the push completes").unwrap();
        drop(push_fn); // it holds a sender: close can only end the stream once every sender is gone
        tokio::time::timeout(Duration::from_secs(2), reactive.sinks.close(id, "w"))
            .await
            .expect("close completes")
            .unwrap();
    }

    /// A handler that finishes on its own (early break + commit) before the abort arrives: the abort
    /// must report that, not claim the run was abandoned. One that failed reports its error.
    #[tokio::test]
    async fn abort_reports_a_handler_that_already_finished() {
        let reactive = Reactive::default();
        reactive.try_sink("first", |fail: bool, mut rows: crate::SinkStream<u32>| async move {
            rows.next().await; // take one value, then stop on its own
            if fail {
                Err("gave up".to_string())
            } else {
                Ok(())
            }
        });
        for (fail, expect_err) in [(false, false), (true, true)] {
            let id = reactive.open_sink("first", json!(fail), "w".into(), "w".into()).unwrap();
            push(&reactive, id, vec![json!(1)]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await; // handler has returned
            match reactive.sinks.abort(id, "w").await {
                Ok(outcome) => {
                    assert!(!expect_err);
                    assert_eq!(outcome, super::AbortOutcome::Completed);
                }
                Err(Error::SinkFailed(m)) => {
                    assert!(expect_err);
                    assert!(m.contains("gave up"), "{m}");
                }
                Err(other) => panic!("unexpected {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn ownership_kind_and_release() {
        let reactive = Reactive::default();
        reactive.sink("s", |_: (), mut v: crate::SinkStream<u8>| async move { while v.next().await.is_some() {} });
        reactive.state("st", &ReactiveState::new(1));

        assert!(matches!(reactive.open_sink("st", serde_json::Value::Null, "w".into(), "w".into()), Err(Error::NotASink(_))));
        assert!(matches!(reactive.open("s", serde_json::Value::Null), Err(Error::NotSubscribable(_))));
        assert!(matches!(reactive.open_sink("s", json!(1), "w".into(), "w".into()), Err(Error::InvalidArgs { .. })));

        let a = reactive.open_sink("s", serde_json::Value::Null, "a".into(), "win".into()).unwrap();
        let _b = reactive.open_sink("s", serde_json::Value::Null, "b".into(), "win".into()).unwrap();
        let _c = reactive.open_sink("s", serde_json::Value::Null, "c".into(), "other".into()).unwrap();
        assert!(matches!(reactive.sinks.push_fn(a, "b"), Err(Error::UnknownSink(_))));
        assert!(matches!(reactive.sinks.close(a, "b").await, Err(Error::UnknownSink(_))));
        assert_eq!(reactive.sinks.release_window("win"), 2);
        assert_eq!(reactive.sinks.release_webview("c"), 1);
        assert_eq!(reactive.active_sinks(), 0);
    }
}
