//! Every command checks the source against the caller's capability scopes. Commands that open or
//! read/write (`subscribe`, `get`, `set`, `sink_open`) need an allow match and no deny match.
//! Commands acting on an already-open subscription/sink by id (`unsubscribe`, `sink_push`,
//! `sink_close`, `sink_abort`) check the source recorded at open against deny scopes only.

use serde_json::Value;
use tauri::{
    ipc::{Channel, CommandScope, GlobalScope},
    Runtime, State, Webview,
};

use crate::{
    scope::{Scopes, SourceScope},
    subscriptions::Event,
    Reactive, Result,
};

fn scopes(command: CommandScope<SourceScope>, global: GlobalScope<SourceScope>) -> Scopes {
    Scopes { command, global }
}

#[tauri::command]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn subscribe<R: Runtime>(
    webview: Webview<R>,
    reactive: State<'_, Reactive>,
    command_scope: CommandScope<SourceScope>,
    global_scope: GlobalScope<SourceScope>,
    source: String,
    args: Option<Value>,
    page: Option<String>,
    on_event: Channel<Event>,
) -> Result<u64> {
    scopes(command_scope, global_scope).check(&source)?;
    let label = webview.label();
    let window = webview.window().label().to_owned();
    let generation = reactive.pages.admit(label, &window, page.as_deref())?;
    let stream = reactive.open(&source, args.unwrap_or(Value::Null))?;
    let id = reactive.subscriptions.spawn(label.to_owned(), window, source, stream, move |event| on_event.send(event).is_ok());
    // A reload may have torn this webview down while we were opening: don't leave an orphan.
    if !reactive.pages.is_current(label, generation) {
        reactive.subscriptions.cancel_owned(id, label);
        return Err(crate::Error::StalePage);
    }
    Ok(id)
}

/// Ends a subscription owned by the calling webview. Unknown ids are a no-op (`false`).
#[tauri::command]
pub(crate) async fn unsubscribe<R: Runtime>(
    webview: Webview<R>,
    reactive: State<'_, Reactive>,
    command_scope: CommandScope<SourceScope>,
    global_scope: GlobalScope<SourceScope>,
    id: u64,
) -> Result<bool> {
    let Some(source) = reactive.subscriptions.source_of(id, webview.label()) else {
        return Ok(false);
    };
    scopes(command_scope, global_scope).check_not_denied(&source)?;
    Ok(reactive.subscriptions.cancel_owned(id, webview.label()))
}

#[tauri::command]
pub(crate) async fn get(
    reactive: State<'_, Reactive>,
    command_scope: CommandScope<SourceScope>,
    global_scope: GlobalScope<SourceScope>,
    source: String,
) -> Result<Value> {
    scopes(command_scope, global_scope).check(&source)?;
    reactive.get(&source)
}

#[tauri::command]
pub(crate) async fn set(
    reactive: State<'_, Reactive>,
    command_scope: CommandScope<SourceScope>,
    global_scope: GlobalScope<SourceScope>,
    source: String,
    value: Value,
) -> Result<()> {
    scopes(command_scope, global_scope).check(&source)?;
    reactive.set(&source, value)
}

#[tauri::command]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn sink_open<R: Runtime>(
    webview: Webview<R>,
    reactive: State<'_, Reactive>,
    command_scope: CommandScope<SourceScope>,
    global_scope: GlobalScope<SourceScope>,
    source: String,
    args: Option<Value>,
    page: Option<String>,
) -> Result<u64> {
    scopes(command_scope, global_scope).check(&source)?;
    let label = webview.label();
    let window = webview.window().label().to_owned();
    let generation = reactive.pages.admit(label, &window, page.as_deref())?;
    let id = reactive.open_sink(&source, args.unwrap_or(Value::Null), label.to_owned(), window)?;
    // A reload may have torn this webview down while we were opening: don't leave an orphan.
    if !reactive.pages.is_current(label, generation) {
        reactive.sinks.discard(id);
        return Err(crate::Error::StalePage);
    }
    Ok(id)
}

/// Pushes a batch of values, in order. Resolves once they are buffered for the handler, so the
/// frontend (which keeps one push in flight) is slowed down by a slow handler. A value that doesn't
/// deserialize fails the push with `InvalidValue` and aborts the sink.
#[tauri::command]
pub(crate) async fn sink_push<R: Runtime>(
    webview: Webview<R>,
    reactive: State<'_, Reactive>,
    command_scope: CommandScope<SourceScope>,
    global_scope: GlobalScope<SourceScope>,
    id: u64,
    values: Vec<Value>,
) -> Result<()> {
    let (source, push) = reactive.sinks.push_fn(id, webview.label())?;
    scopes(command_scope, global_scope).check_not_denied(&source)?;
    let result = push(values).await;
    if matches!(result, Err(crate::Error::InvalidValue { .. })) {
        // The push already aborted the handler; also free the entry.
        let _ = reactive.sinks.abort(id, webview.label()).await;
    }
    result
}

/// Aborts the sink: the handler's future is dropped (the writer failed, so this must not look
/// like a completed stream). Resolves with `aborted`, or `completed` if the handler had already
/// finished on its own (or rejects with its `SinkFailed` error).
#[tauri::command]
pub(crate) async fn sink_abort<R: Runtime>(
    webview: Webview<R>,
    reactive: State<'_, Reactive>,
    command_scope: CommandScope<SourceScope>,
    global_scope: GlobalScope<SourceScope>,
    id: u64,
) -> Result<crate::sinks::AbortOutcome> {
    let source = reactive.sinks.source_of(id, webview.label())?;
    scopes(command_scope, global_scope).check_not_denied(&source)?;
    reactive.sinks.abort(id, webview.label()).await
}

/// Ends the sink's stream and resolves with the handler's result.
#[tauri::command]
pub(crate) async fn sink_close<R: Runtime>(
    webview: Webview<R>,
    reactive: State<'_, Reactive>,
    command_scope: CommandScope<SourceScope>,
    global_scope: GlobalScope<SourceScope>,
    id: u64,
) -> Result<()> {
    let source = reactive.sinks.source_of(id, webview.label())?;
    scopes(command_scope, global_scope).check_not_denied(&source)?;
    reactive.sinks.close(id, webview.label()).await
}
