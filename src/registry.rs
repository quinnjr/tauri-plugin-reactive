use std::{
    collections::HashMap,
    fmt::Display,
    future::Future,
    sync::{Arc, RwLock},
};

use futures::{stream::BoxStream, FutureExt, Stream, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

#[cfg(feature = "typegen")]
use crate::typegen::{args_ref, type_ref, TsSource};
use crate::{
    sinks::{OpenedSink, SinkStream, Sinks, SINK_BUFFER},
    state::ReactiveState,
    subscriptions::Subscriptions,
    typegen::MaybeTs,
    Error, Result,
};

pub(crate) type ValueStream = BoxStream<'static, std::result::Result<Value, String>>;

type OpenFn = dyn Fn(Value) -> Result<ValueStream> + Send + Sync;
type GetFn = dyn Fn() -> Result<Value> + Send + Sync;
type SetFn = dyn Fn(Value) -> Result<()> + Send + Sync;
type SinkOpenFn = dyn Fn(Value) -> Result<OpenedSink> + Send + Sync;

struct Source {
    /// Subscribable (states and streams).
    open: Option<Box<OpenFn>>,
    /// Frontend -> Rust (sinks).
    sink: Option<Box<SinkOpenFn>>,
    get: Option<Box<GetFn>>,
    set: Option<Box<SetFn>>,
    #[cfg(feature = "typegen")]
    ts: TsSource,
}

/// The registry of named sources exposed to the frontend. Access it with
/// [`ReactiveExt::reactive`](crate::ReactiveExt::reactive).
///
/// Registering a name that already exists replaces the previous source (existing subscriptions
/// keep running against the old one until they end).
#[derive(Default)]
pub struct Reactive {
    sources: RwLock<HashMap<String, Arc<Source>>>,
    pub(crate) subscriptions: Arc<Subscriptions>,
    pub(crate) sinks: Sinks,
    pub(crate) pages: crate::pages::Pages,
}

fn to_value<T: Serialize>(value: &T) -> std::result::Result<Value, String> {
    serde_json::to_value(value).map_err(|e| format!("failed to serialize value: {e}"))
}

fn state_source<T>(state: &ReactiveState<T>, set: Option<Box<SetFn>>) -> Source
where
    T: Serialize + MaybeTs + Clone + Send + Sync + 'static,
{
    let for_open = state.clone();
    let for_get = state.clone();
    Source {
        open: Some(Box::new(move |_args| Ok(for_open.stream().map(|v| to_value(&v)).boxed()))),
        sink: None,
        get: Some(Box::new(move || Ok(for_get.with(|v| serde_json::to_value(v))?))),
        #[cfg(feature = "typegen")]
        ts: TsSource::State { writable: set.is_some(), value: type_ref::<T> },
        set,
    }
}

impl Reactive {
    /// Exposes a read-only state. Subscribers get the current value immediately, then every change.
    pub fn state<T>(&self, name: impl Into<String>, state: &ReactiveState<T>) -> &Self
    where
        T: Serialize + MaybeTs + Clone + Send + Sync + 'static,
    {
        self.insert(name.into(), state_source(state, None));
        self
    }

    /// Exposes a state the frontend can also `set`.
    pub fn writable_state<T>(&self, name: impl Into<String>, state: &ReactiveState<T>) -> &Self
    where
        T: Serialize + DeserializeOwned + MaybeTs + Clone + Send + Sync + 'static,
    {
        self.writable_state_with(name, state, |_| Ok::<(), std::convert::Infallible>(()))
    }

    /// Exposes a writable state whose incoming values from the frontend are checked by `validate`
    /// first. Rejected values never reach the state, and the error is returned to the caller.
    pub fn writable_state_with<T, V, E>(&self, name: impl Into<String>, state: &ReactiveState<T>, validate: V) -> &Self
    where
        T: Serialize + DeserializeOwned + MaybeTs + Clone + Send + Sync + 'static,
        V: Fn(&T) -> std::result::Result<(), E> + Send + Sync + 'static,
        E: Display,
    {
        let name = name.into();
        let target = state.clone();
        let source_name = name.clone();
        let invalid = move |message: String| Error::InvalidValue { name: source_name.clone(), message };
        let set = move |value: Value| -> Result<()> {
            let value: T = serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?;
            validate(&value).map_err(|e| invalid(e.to_string()))?;
            target.set(value);
            Ok(())
        };
        self.insert(name, state_source(state, Some(Box::new(set))));
        self
    }

    /// Exposes a stream factory. Each frontend subscription calls `factory` with its deserialized
    /// arguments (use `()` for none), so the stream is created lazily and dropped on unsubscribe.
    pub fn stream<A, T, S, F>(&self, name: impl Into<String>, factory: F) -> &Self
    where
        A: DeserializeOwned + MaybeTs + 'static,
        T: Serialize + MaybeTs + 'static,
        S: Stream<Item = T> + Send + 'static,
        F: Fn(A) -> S + Send + Sync + 'static,
    {
        self.try_stream(name, move |args: A| factory(args).map(Ok::<T, std::convert::Infallible>))
    }

    /// Like [`stream`](Self::stream), but items are `Result`s. An `Err` is sent to the frontend
    /// as an error and ends the subscription.
    pub fn try_stream<A, T, E, S, F>(&self, name: impl Into<String>, factory: F) -> &Self
    where
        A: DeserializeOwned + MaybeTs + 'static,
        T: Serialize + MaybeTs + 'static,
        E: Display + 'static,
        S: Stream<Item = std::result::Result<T, E>> + Send + 'static,
        F: Fn(A) -> S + Send + Sync + 'static,
    {
        let name = name.into();
        let source_name = name.clone();
        let open = move |args: Value| -> Result<ValueStream> {
            let args: A = serde_json::from_value(args).map_err(|e| Error::InvalidArgs {
                name: source_name.clone(),
                message: e.to_string(),
            })?;
            Ok(factory(args)
                .map(|item| match item {
                    Ok(value) => to_value(&value),
                    Err(e) => Err(e.to_string()),
                })
                .boxed())
        };
        self.insert(
            name,
            Source {
                open: Some(Box::new(open)),
                sink: None,
                get: None,
                set: None,
                #[cfg(feature = "typegen")]
                ts: TsSource::Stream { value: type_ref::<T>, args: args_ref::<A> },
            },
        );
        self
    }

    /// Exposes a frontend -> Rust stream. Each frontend writer (`rx.sink(name, args)`) starts one
    /// run of `handler` with its deserialized arguments and a [`SinkStream`] of the values it
    /// pushes, in order. The stream ends (`None`) when the writer calls `complete()`. If the writer
    /// fails or goes away instead (producer error, page reload, window closed), the handler is
    /// aborted: its future is dropped without the stream ending.
    ///
    /// ```no_run
    /// # use tauri_plugin_reactive::{Reactive, SinkStream};
    /// # use futures::StreamExt;
    /// # let reactive = Reactive::default();
    /// reactive.sink("log", |level: String, mut lines: SinkStream<String>| async move {
    ///     while let Some(line) = lines.next().await {
    ///         println!("[{level}] {line}");
    ///     }
    /// });
    /// ```
    pub fn sink<A, T, F, Fut>(&self, name: impl Into<String>, handler: F) -> &Self
    where
        A: DeserializeOwned + MaybeTs + 'static,
        T: DeserializeOwned + MaybeTs + Send + 'static,
        F: Fn(A, SinkStream<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.try_sink(name, move |args: A, values: SinkStream<T>| {
            handler(args, values).map(Ok::<(), std::convert::Infallible>)
        })
    }

    /// Like [`sink`](Self::sink), but the handler returns a `Result`. An `Err` ends the sink:
    /// further pushes fail, and the frontend's `complete()` rejects with the error message.
    pub fn try_sink<A, T, E, F, Fut>(&self, name: impl Into<String>, handler: F) -> &Self
    where
        A: DeserializeOwned + MaybeTs + 'static,
        T: DeserializeOwned + MaybeTs + Send + 'static,
        E: Display + 'static,
        F: Fn(A, SinkStream<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), E>> + Send + 'static,
    {
        let name = name.into();
        let source_name = name.clone();
        let open = move |args: Value| -> Result<OpenedSink> {
            let args: A = serde_json::from_value(args).map_err(|e| Error::InvalidArgs {
                name: source_name.clone(),
                message: e.to_string(),
            })?;
            let (tx, rx) = tokio::sync::mpsc::channel::<T>(SINK_BUFFER);
            let (done_tx, done) = tokio::sync::oneshot::channel();
            let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let run = handler(args, SinkStream { rx, aborted: Arc::clone(&aborted) });
            let (abort, registration) = futures::future::AbortHandle::new_pair();
            tauri::async_runtime::spawn(async move {
                let result = match futures::future::Abortable::new(run, registration).await {
                    Ok(Ok(())) => crate::sinks::HandlerOutcome::Completed,
                    Ok(Err(e)) => crate::sinks::HandlerOutcome::Failed(e.to_string()),
                    Err(_aborted) => crate::sinks::HandlerOutcome::Aborted,
                };
                let _ = done_tx.send(result);
            });

            let name = source_name.clone();
            let (push_aborted, push_abort) = (Arc::clone(&aborted), abort.clone());
            let push = move |values: Vec<Value>| {
                let tx = tx.clone();
                let name = name.clone();
                let (aborted, abort) = (Arc::clone(&push_aborted), push_abort.clone());
                async move {
                    for value in values {
                        let value: T = match serde_json::from_value(value) {
                            Ok(value) => value,
                            Err(e) => {
                                // Values before this one were delivered: abort right here (flag first,
                                // as on every abort path), so even a concurrent close can't end the
                                // stream normally and let the handler commit a truncated run.
                                aborted.store(true, std::sync::atomic::Ordering::SeqCst);
                                abort.abort();
                                return Err(Error::InvalidValue { name: name.clone(), message: e.to_string() });
                            }
                        };
                        // Waits while the buffer is full: backpressure on the frontend's push.
                        tx.send(value).await.map_err(|_| Error::SinkClosed(name.clone()))?;
                    }
                    Ok(())
                }
                .boxed()
            };
            Ok(OpenedSink { push: Arc::new(push), done, abort, aborted })
        };
        self.insert(
            name,
            Source {
                open: None,
                sink: Some(Box::new(open)),
                get: None,
                set: None,
                #[cfg(feature = "typegen")]
                ts: TsSource::Sink { value: type_ref::<T>, args: args_ref::<A> },
            },
        );
        self
    }

    /// TypeScript bindings for every registered source: the referenced type declarations plus a
    /// `Sources` map to pass to `createReactive<Sources>()`. Output is deterministic (sorted).
    #[cfg(feature = "typegen")]
    pub fn typescript(&self) -> std::result::Result<String, String> {
        let mut sources: Vec<(String, TsSource)> = self
            .sources
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(name, source)| (name.clone(), source.ts))
            .collect();
        sources.sort_by(|a, b| a.0.cmp(&b.0));
        crate::typegen::render(&sources)
    }

    /// Writes [`typescript`](Self::typescript) to `path`, creating parent directories.
    ///
    /// The file is only rewritten when its content changes (so dev servers don't reload
    /// needlessly). Returns `true` if it was written, so a test can fail when bindings are stale:
    /// `assert!(!reactive.export_typescript(path)?, "bindings were stale and have been regenerated")`.
    #[cfg(feature = "typegen")]
    pub fn export_typescript(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<bool> {
        let path = path.as_ref();
        let content = self.typescript().map_err(std::io::Error::other)?;
        if std::fs::read_to_string(path).is_ok_and(|existing| existing.replace("\r\n", "\n") == content) {
            return Ok(false);
        }
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(path, content)?;
        Ok(true)
    }

    /// Removes a source. New subscriptions to it fail; existing ones keep running.
    pub fn unregister(&self, name: &str) -> bool {
        self.sources.write().unwrap_or_else(|e| e.into_inner()).remove(name).is_some()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.sources.read().unwrap_or_else(|e| e.into_inner()).contains_key(name)
    }

    /// Number of live frontend subscriptions, across all webviews.
    pub fn active_subscriptions(&self) -> usize {
        self.subscriptions.len()
    }

    pub(crate) fn open(&self, name: &str, args: Value) -> Result<ValueStream> {
        let source = self.source(name)?;
        let open = source.open.as_ref().ok_or_else(|| Error::NotSubscribable(name.into()))?;
        open(args)
    }

    /// Starts the sink handler for one frontend writer and returns its id.
    pub(crate) fn open_sink(&self, name: &str, args: Value, webview: String, window: String) -> Result<u64> {
        let source = self.source(name)?;
        let open = source.sink.as_ref().ok_or_else(|| Error::NotASink(name.into()))?;
        Ok(self.sinks.insert(webview, window, name.to_owned(), open(args)?))
    }

    /// Number of open frontend -> Rust sinks, across all webviews.
    pub fn active_sinks(&self) -> usize {
        self.sinks.len()
    }

    /// Tears down everything a webview owns (its page reloaded or navigated away), and stops
    /// accepting new subscriptions/sinks from the old page's in-flight calls.
    pub(crate) fn release_webview(&self, label: &str, window: &str) {
        self.pages.retire(label, window);
        self.subscriptions.cancel_webview(label);
        self.sinks.release_webview(label);
    }

    /// Tears down everything a window's webviews own (the window was destroyed).
    pub(crate) fn release_window(&self, label: &str) {
        self.pages.close_window(label);
        self.subscriptions.cancel_window(label);
        self.sinks.release_window(label);
    }

    pub(crate) fn get(&self, name: &str) -> Result<Value> {
        let source = self.source(name)?;
        let get = source.get.as_ref().ok_or_else(|| Error::NotAState(name.into()))?;
        get()
    }

    pub(crate) fn set(&self, name: &str, value: Value) -> Result<()> {
        let source = self.source(name)?;
        let set = source.set.as_ref().ok_or_else(|| Error::ReadOnly(name.into()))?;
        set(value)
    }

    fn source(&self, name: &str) -> Result<Arc<Source>> {
        self.sources
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnknownSource(name.into()))
    }

    fn insert(&self, name: String, source: Source) {
        self.sources.write().unwrap_or_else(|e| e.into_inner()).insert(name, Arc::new(source));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Deserialize)]
    #[cfg_attr(feature = "typegen", derive(ts_rs::TS))]
    struct TickArgs {
        #[allow(dead_code)]
        interval_ms: u64,
    }

    fn registry() -> (Reactive, ReactiveState<i32>, ReactiveState<String>) {
        let reactive = Reactive::default();
        let count = ReactiveState::new(1);
        let name = ReactiveState::new("ann".to_string());
        reactive
            .state("count", &count)
            .writable_state_with("name", &name, |v: &String| {
                if v.is_empty() {
                    Err("name must not be empty")
                } else {
                    Ok(())
                }
            })
            .stream("ticks", |_: TickArgs| futures::stream::empty::<u64>());
        (reactive, count, name)
    }

    #[test]
    fn get_and_set() {
        let (reactive, _count, name) = registry();
        assert_eq!(reactive.get("count").unwrap(), json!(1));
        reactive.set("name", json!("bob")).unwrap();
        assert_eq!(name.get(), "bob");
        assert_eq!(reactive.get("name").unwrap(), json!("bob"));
    }

    #[test]
    fn errors() {
        let (reactive, _count, name) = registry();
        assert!(matches!(reactive.get("nope"), Err(Error::UnknownSource(_))));
        assert!(matches!(reactive.set("count", json!(2)), Err(Error::ReadOnly(_))));
        assert!(matches!(reactive.get("ticks"), Err(Error::NotAState(_))));
        assert!(matches!(reactive.set("name", json!(42)), Err(Error::InvalidValue { .. })));
        assert!(matches!(reactive.set("name", json!("")), Err(Error::InvalidValue { .. })));
        assert_eq!(name.get(), "ann");
        assert!(matches!(reactive.open("ticks", Value::Null), Err(Error::InvalidArgs { .. })));
        assert!(reactive.open("ticks", json!({ "interval_ms": 5 })).is_ok());
    }

    #[test]
    fn error_serializes_with_code() {
        let err = Error::UnknownSource("x".into());
        assert_eq!(
            serde_json::to_value(&err).unwrap(),
            json!({ "code": "UnknownSource", "message": "unknown reactive source `x`" })
        );
    }

    #[cfg(feature = "typegen")]
    #[test]
    fn typescript_bindings() {
        use ts_rs::TS;

        /// Player-facing settings.
        #[derive(Clone, Serialize, Deserialize, TS)]
        #[serde(rename_all = "camelCase")]
        struct Prefs {
            dark_mode: bool,
            volume: u8,
            theme: Theme,
        }
        #[derive(Clone, Serialize, Deserialize, TS)]
        #[serde(rename_all = "lowercase")]
        enum Theme {
            Light,
            Dark,
        }
        #[derive(Clone, Serialize, TS)]
        struct Reading {
            sensor: String,
            values: Vec<f64>,
            theme: Theme, // shared dependency, must be declared once
        }
        #[derive(Deserialize, TS)]
        #[serde(rename_all = "camelCase")]
        #[allow(dead_code)]
        struct Query {
            interval_ms: u64,
            #[ts(optional)]
            limit: Option<u32>,
        }

        let reactive = Reactive::default();
        reactive
            .state("count", &ReactiveState::new(0u64))
            .writable_state("prefs", &ReactiveState::new(Prefs { dark_mode: true, volume: 3, theme: Theme::Dark }))
            .stream("readings", |_: Query| futures::stream::empty::<Reading>())
            .stream("pings", |_: ()| futures::stream::empty::<Option<String>>())
            .stream("has-dash", |n: u64| futures::stream::iter(0..n))
            .sink("log", |_level: String, _lines: crate::SinkStream<Prefs>| async {});

        let ts = reactive.typescript().unwrap();
        let expected_sources = "export type Sources = {\n  \"count\": State<number>;\n  \"has-dash\": Stream<number, number>;\n  \"log\": Sink<Prefs, string>;\n  \"pings\": Stream<string | null>;\n  \"prefs\": WritableState<Prefs>;\n  \"readings\": Stream<Reading, Query>;\n};\n";
        assert!(ts.ends_with(expected_sources), "unexpected Sources:\n{ts}");
        assert!(ts.contains("import type { Sink, State, Stream, WritableState } from 'tauri-plugin-reactive-api';"), "{ts}");

        // Only the kinds actually used are imported (unused imports fail under noUnusedLocals).
        let only_state = Reactive::default();
        only_state.state("n", &ReactiveState::new(1u8));
        assert!(only_state.typescript().unwrap().contains("import type { State } from"));
        assert!(ts.contains("Player-facing settings."), "docs missing:\n{ts}");
        assert!(ts.contains("export type Prefs = { darkMode: boolean, volume: number, theme: Theme, };"), "{ts}");
        assert!(ts.contains("export type Theme = \"light\" | \"dark\";"), "{ts}");
        assert!(ts.contains("export type Query = { intervalMs: number, limit?: number, };"), "{ts}");
        assert_eq!(ts.matches("export type Theme =").count(), 1, "Theme declared once:\n{ts}");
        // Declarations sorted by name, deterministic output.
        let order: Vec<usize> = ["export type Prefs", "export type Query", "export type Reading", "export type Theme"]
            .iter()
            .map(|d| ts.find(d).unwrap())
            .collect();
        assert!(order.windows(2).all(|w| w[0] < w[1]), "{ts}");
        assert_eq!(ts, reactive.typescript().unwrap());

        // export_typescript only writes on change.
        let dir = std::env::temp_dir().join(format!("reactive-typegen-{}", std::process::id()));
        let path = dir.join("nested/bindings.ts");
        assert!(reactive.export_typescript(&path).unwrap());
        assert!(!reactive.export_typescript(&path).unwrap());
        reactive.state("extra", &ReactiveState::new(true));
        assert!(reactive.export_typescript(&path).unwrap());
        assert!(std::fs::read_to_string(&path).unwrap().contains("\"extra\": State<boolean>;"));
        std::fs::remove_dir_all(dir).ok();
    }

    #[cfg(feature = "typegen")]
    #[test]
    fn typescript_shared_declarations_are_not_collisions() {
        #[derive(Clone, Serialize, ts_rs::TS)]
        struct Wrapper<T: ts_rs::TS> {
            value: T,
        }
        #[derive(Clone, Serialize, ts_rs::TS)]
        struct Doc {
            meta: serde_json::Value,
        }
        let reactive = Reactive::default();
        reactive
            .state("a", &ReactiveState::new(Wrapper { value: 1u8 }))
            .state("b", &ReactiveState::new(Wrapper { value: String::new() }))
            .state("c", &ReactiveState::new(Wrapper { value: Wrapper { value: 1u8 } }))
            .state("json", &ReactiveState::new(serde_json::Value::Null))
            .state("doc", &ReactiveState::new(Doc { meta: serde_json::Value::Null }));
        let ts = reactive.typescript().unwrap();
        assert_eq!(ts.matches("export type Wrapper<").count(), 1, "{ts}");
        assert_eq!(ts.matches("export type JsonValue =").count(), 1, "{ts}");
        assert!(ts.contains("\"a\": State<Wrapper<number>>;"), "{ts}");
        assert!(ts.contains("\"b\": State<Wrapper<string>>;"), "{ts}");
        assert!(ts.contains("\"c\": State<Wrapper<Wrapper<number>>>;"), "{ts}");
    }

    #[cfg(feature = "typegen")]
    #[test]
    fn typescript_reserved_names_are_an_error() {
        #[derive(Clone, Serialize, ts_rs::TS)]
        enum State {
            Idle,
        }
        let reactive = Reactive::default();
        reactive.state("status", &ReactiveState::new(State::Idle));
        let err = reactive.typescript().unwrap_err();
        assert!(err.contains("`State`") && err.contains("rename"), "{err}");

        #[derive(Clone, Serialize, ts_rs::TS)]
        #[ts(rename = "RunState")]
        enum State2 {
            Idle,
        }
        let renamed = Reactive::default();
        renamed.state("status", &ReactiveState::new(State2::Idle));
        assert!(renamed.typescript().unwrap().contains("\"status\": State<RunState>;"));
    }

    #[cfg(feature = "typegen")]
    #[test]
    fn typescript_name_collision_is_an_error() {
        mod a {
            #[derive(Clone, serde::Serialize, ts_rs::TS)]
            pub struct Item(pub u8);
        }
        mod b {
            #[derive(Clone, serde::Serialize, ts_rs::TS)]
            pub struct Item(pub String);
        }
        let reactive = Reactive::default();
        reactive.state("a", &ReactiveState::new(a::Item(1))).state("b", &ReactiveState::new(b::Item("x".into())));
        let err = reactive.typescript().unwrap_err();
        assert!(err.contains("`Item`"), "{err}");
    }

    #[test]
    fn unregister() {
        let (reactive, ..) = registry();
        assert!(reactive.contains("count"));
        assert!(reactive.unregister("count"));
        assert!(!reactive.contains("count"));
    }
}
