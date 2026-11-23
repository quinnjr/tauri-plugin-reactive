# tauri-plugin-reactive

Typed reactive state and streams across Tauri's Rust ↔ JS boundary.

Register a `ReactiveState<T>` or a `Stream` factory in Rust. Subscribe from the frontend as an
observable / store / React hook. Subscriptions are lazy, shared, and cleaned up automatically.

- **State**: current value on subscribe, then every change. Rapid updates are coalesced (latest wins).
- **Streams**: a factory called per subscription with typed args; dropped when nobody listens.
- **Writable state**: the frontend can `set`, with optional Rust-side validation.
- **Sinks (frontend → Rust streams)**: the frontend pushes values in order, and a Rust handler consumes
  them as a `Stream`, with backpressure.
- **Per-source permissions**: capability scopes decide which windows may read, write or push to which sources.
- **Sharing**: every subscriber to the same source and args shares one backend subscription.
- **Cleanup**: backend work stops when the last subscriber leaves, when the page reloads, or when the window is destroyed.
- **Framework integration**: React `useReactive`, Vue `useReactive`, Angular `reactiveSignal`,
  Svelte `$source` (no adapter needed), and RxJS `from(source)`.

## Rust

```toml
[dependencies]
tauri-plugin-reactive = "0.1"
```

```rust
use tauri_plugin_reactive::{ReactiveExt, ReactiveState};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Settings { theme: String, volume: u8 }

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickArgs { interval_ms: u64 }

tauri::Builder::default()
    .plugin(tauri_plugin_reactive::init())
    .setup(|app| {
        let downloads = ReactiveState::new(0u32);
        let settings = ReactiveState::new(Settings { theme: "dark".into(), volume: 50 });

        app.reactive()
            .state("downloads", &downloads)                       // read-only
            .writable_state_with("settings", &settings, |s: &Settings| {
                if s.volume > 100 { Err("volume must be <= 100") } else { Ok(()) }
            })
            .stream("ticks", |args: TickArgs| ticker(args.interval_ms))   // lazy, per-subscription
            .try_stream("logs", |path: String| tail_file(path));          // Err(e) -> frontend error

        // Drive the UI from anywhere in Rust:
        downloads.update(|n| *n += 1);
        Ok(())
    })
```

Add the permission to your capability (e.g. `src-tauri/capabilities/default.json`):

```json
{ "permissions": ["core:default", "reactive:default"] }
```

`reactive:default` allows every source. See [Permissions](#permissions) to restrict it.

## Frontend

```sh
npm i tauri-plugin-reactive-api
```

Give the client a `Sources` type describing what Rust registered. The best way is to
[generate it from Rust](#generated-typescript-types). It can also be written by hand:

```ts
import { createReactive, type State, type WritableState, type Stream } from 'tauri-plugin-reactive-api';

type Sources = {
  downloads: State<number>;
  settings: WritableState<{ theme: string; volume: number }>;
  ticks: Stream<number, { intervalMs: number }>;
  logs: Stream<string, string>;
};

export const rx = createReactive<Sources>({ keepAliveMs: 0 });
```

```ts
// Plain subscribe: returns a function that also has .unsubscribe()
const stop = rx.state('downloads').subscribe((n) => console.log(n));

// Writable state
await rx.state('settings').set({ theme: 'light', volume: 30 });
// rx.state('downloads').set(...)   <- type error: read-only

// Streams with typed args
rx.stream('ticks', { intervalMs: 500 }).subscribe({
  next: (t) => console.log(t),
  error: (e) => console.error(e.code, e.message),
  complete: () => console.log('done'),
});

// RxJS
import { from, map } from 'rxjs';
from(rx.stream('logs', '/var/log/app.log')).pipe(map((l) => l.toUpperCase()));
```

**React**

```tsx
import { useReactive } from 'tauri-plugin-reactive-api/react';

function Downloads() {
  const n = useReactive(rx.state('downloads'), 0);
  const tick = useReactive(rx.stream('ticks', { intervalMs: 1000 }));
  return <p>{n} downloads · tick {tick ?? '…'}</p>;
}
```

**Svelte**

```svelte
<script lang="ts">
  const downloads = rx.state('downloads');
</script>
<p>{$downloads ?? 0}</p>
```

For args that change, use `const tick = $derived(rx.stream('ticks', { intervalMs }))` with `{$tick}`.

**Vue**

```vue
<script setup lang="ts">
import { useReactive } from 'tauri-plugin-reactive-api/vue';
const props = defineProps<{ intervalMs: number }>();
const downloads = useReactive(rx.state('downloads'), 0);
// A getter re-subscribes when the args change:
const tick = useReactive(() => rx.stream('ticks', { intervalMs: props.intervalMs }));
</script>
<template><p>{{ downloads }} · {{ tick }}</p></template>
```

**Angular** (signals; works zoneless)

```ts
import { reactiveSignal } from 'tauri-plugin-reactive-api/angular';

@Component({ template: `{{ downloads() }} · {{ tick() }}` })
export class Downloads {
  intervalMs = input.required<number>();
  downloads = reactiveSignal(rx.state('downloads'), { initial: 0 });
  // A function re-subscribes when the signals it reads change:
  tick = reactiveSignal(() => rx.stream('ticks', { intervalMs: this.intervalMs() }));
}
```

`toSignal(from(source))` from `@angular/core/rxjs-interop` also works.

## Semantics

| | State | Stream |
|---|---|---|
| On subscribe | current value, then changes | values from now on (hot, shared) |
| Late subscriber (shared) | gets the latest value replayed | only future values |
| Backpressure | coalesced (latest wins) | every item is sent |
| `get()` / `set()` | yes / if writable | no |

- **Errors** from `try_stream` or a failed subscribe end the subscription. They are delivered to
  `observer.error` as a `ReactiveError` and are kept on `source.error`. Its `code` is one of:
  - `UnknownSource`
  - `Forbidden`
  - `InvalidArgs`
  - `InvalidValue`
  - `ReadOnly`
  - `NotAState`
  - `NotSubscribable`
  - `NotASink`
  - `StreamError`
  - `SinkClosed`
  - `SinkFailed`
  - `UnknownSink`
  - `StalePage`
  - `Aborted` (a sink writer was aborted)
- **Completion** of a finite stream calls `observer.complete`. Subscribing again starts a fresh backend subscription.
- **Two subscription styles:**
  - **Observer object** (`subscribe({ next, error, complete })`, as RxJS does): follows Observable
    semantics, so an error or completion ends that subscription.
  - **Callbacks** (`subscribe(next, error?, complete?)`, the store style used by Svelte and the
    adapters): told about the error or completion, but stays registered. It receives values again if
    the source reconnects, for example when another component subscribes. That keeps every component on a
    source consistent.
- **Error reporting:** an error is logged with `console.error` unless an observer-object subscriber has an `error`
  callback. Store-style callbacks don't count: Svelte passes `invalidate` as the second argument. React's
  `useReactiveState` returns `{ value, error }` and re-renders when the source errors.
- **Observer isolation:** an observer that throws doesn't stop the others; the exception is rethrown
  asynchronously. An observer unsubscribed during a dispatch isn't called for it.
- **Sharing key**: the source name plus the args, compared structurally (`{a:1,b:2}` ≡ `{b:2,a:1}`).
- **Teardown** is deferred: by a microtask by default, or `keepAliveMs`. So unsubscribe and
  resubscribe churn, such as React StrictMode or route changes, doesn't re-open backend work.
- **Ownership**: a webview can only cancel its own subscriptions.

## How it works

```
JS Source ──invoke(subscribe, {source, args, onEvent: Channel})──▶ registry.open(args) → Stream
   ▲                                                                       │ spawned pump task
   └──────────── Channel: {type:'next'|'error'|'complete'} ◀──────────────┘
JS last unsubscribe ──invoke(unsubscribe, id)──▶ abort task (drops the stream / watch receiver)
page reload / window destroyed ─────────────────▶ abort all tasks owned by that webview / window

JS SinkWriter ──invoke(sink_open, {source, args})──▶ spawn handler(args, SinkStream) ◀─┐
   next(v)… ──invoke(sink_push, {id, values: [batch]})──▶ bounded channel (waits when full) ┘
   complete() ──invoke(sink_close, id)──▶ end of stream → resolves with the handler's result
```

## Frontend → Rust streams (sinks)

A sink is the reverse of a stream. The frontend pushes values, and a Rust handler consumes them as a
`Stream`. Each writer the frontend opens starts its own run of the handler.

```rust
use futures::StreamExt;
use tauri_plugin_reactive::SinkStream;

#[derive(serde::Deserialize)]
struct Point { x: f64, y: f64 }

app.reactive()
    .sink("pointer", |_: (), mut points: SinkStream<Point>| async move {
        while let Some(p) = points.next().await { /* ... */ }
    })
    // try_sink: an Err ends the sink and is reported to the frontend.
    .try_sink("import", |file: String, mut rows: SinkStream<Row>| async move {
        while let Some(row) = rows.next().await { db.insert(&file, row)?; }
        Ok::<(), String>(())
    });
```

```ts
const pointer = rx.sink('pointer');
pad.onpointermove = (e) => pointer.next({ x: e.offsetX, y: e.offsetY });

// Writers are observers, so observables can be piped straight in:
from(rows).subscribe(rx.sink('import', 'data.csv'));

// complete() sends what's queued, ends the stream, and resolves with the handler's result:
await writer.complete(); // rejects with SinkFailed if the handler returned Err
```

- **Order** is preserved. Values passed to `next()` in the same tick go out as one batch, and only one
  batch is in flight at a time.
- **Backpressure:** the channel to the handler is bounded at 64 values, so a slow handler slows
  the writer down instead of piling up memory or IPC calls.
- **Failures:**
  - A value that doesn't deserialize fails the writer with `InvalidValue` and aborts the handler. The
    backend does this itself, whatever the client does next.
  - A handler `Err` fails it with `SinkFailed`.
  - Either way the backend sink is freed and `writer.closed` rejects.
- **Completing versus aborting:** the handler's stream ends normally (`None`) only when the writer calls
  `complete()`. If the writer fails instead, the handler is **aborted**: its future is dropped, so code after its loop
  (a commit, say) never runs. That covers `writer.abort()`, a producer error when piped from RxJS, an invalid value,
  a page reload and the window closing. A truncated upload never looks like a finished one.
- **`abort()` reports what really happened:**
  - **Already finished:** if the handler had already finished on its own (say, it broke out of its loop and
    committed), the outcome resolves (completed) or rejects with that handler's `SinkFailed` error, instead
    of claiming `Aborted`.
  - **After `complete()` sent its close:** calling `abort()` is too late, and the outcome is the close result.
- **Multi-webview limitation:** in a window with several webviews (Tauri's unstable multi-webview API), closing *one* child
  webview emits no event in Tauri 2.12. Its subscriptions and sinks are released when its window is destroyed or
  a page loads in it again.
- **Outcome:** `complete()`, `abort()` and `closed` return a promise-compatible `SinkOutcome`. A failure is logged with
  `console.error` unless someone awaits it or attaches a handler. That way a writer driven by RxJS, which discards the
  result, can't fail silently.

## Permissions

Every command checks the source against the window's capability scopes. A source is accessible
only if an **allow** entry matches it and no **deny** entry does. `source` is a glob, so `*` matches
any run of characters. `reactive:default` grants the commands with an allow-everything scope (`source: "*"`)
attached to each opening command. Everything is allowed, but **only in the windows that capability
covers**: a second capability can restrict other windows independently.

> Scopes must be attached to command permissions, as below, to stay per-window. Tauri applies a *global*
> scope (one from a permission that grants no command) to every window in the app, whichever capability
> granted it. The plugin honors global scopes, but use them only for app-wide rules, such as a global deny.

To deny a few sources, or a few operations, add deny scopes. Scopes on a command's permission apply only
to that command:

```json
"permissions": [
  "reactive:default",
  { "identifier": "reactive:allow-subscribe", "deny": [{ "source": "admin*" }] },
  { "identifier": "reactive:allow-get", "deny": [{ "source": "admin*" }] },
  { "identifier": "reactive:allow-set", "deny": [{ "source": "admin*" }, { "source": "motd" }] }
]
```

With this, `adminToken` is unreachable from this window and `motd` is readable but not writable.

To allow only specific sources, grant the commands without `reactive:default`, and list allow scopes:

```json
"permissions": [
  "reactive:allow-unsubscribe",
  { "identifier": "reactive:allow-subscribe", "allow": [{ "source": "clicks" }, { "source": "settings" }] },
  { "identifier": "reactive:allow-set", "allow": [{ "source": "settings" }] },
  { "identifier": "reactive:allow-sink-open", "allow": [{ "source": "pointer" }] },
  "reactive:allow-sink-push",
  "reactive:allow-sink-close",
  "reactive:allow-sink-abort"
]
```

Denied operations fail with `Forbidden`. Every command is scope-checked:

- **Opening, reading and writing** (`subscribe`, `get`, `set`, `sink_open`) need an allow match and no deny match.
- **Acting on something already open** (`unsubscribe`, `sink_push`, `sink_close`, `sink_abort`) checks the
  source it was opened for against **deny** scopes only. It already passed the full check when it
  was opened, so these can be granted plainly, as above. These commands also only act on the calling
  webview's own subscriptions and sinks.

The example apps' child windows use exactly this restrictive style. Their self-test checks it end to end. Capability files are validated against the `source`
scope schema, so editors can autocomplete it.

## Generated TypeScript types

With the `typegen` feature, the `Sources` type and every value and argument type are generated from
your Rust registrations using [`ts-rs`](https://crates.io/crates/ts-rs). The frontend then can't drift
from the backend: renaming a source or changing a field in Rust becomes a type error at every use in
the frontend.

```toml
tauri-plugin-reactive = { version = "0.1", features = ["typegen"] }
ts-rs = "12"
```

Derive `TS` on every type you register. The compiler enforces this at each `state(...)` / `stream(...)` call.
The derive needs `ts-rs` as a direct dependency; alternatively, put
`#[ts(crate = "tauri_plugin_reactive::ts_rs")]` on each type to use the plugin's re-export.

Distinct Rust types that produce the same declaration are emitted once. Examples are one generic type used
with several type arguments, or `serde_json::Value`. Only a name with two *different* declarations is an error.

```rust
use ts_rs::TS;

#[derive(Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
struct Settings { theme: Theme, volume: u8 }

#[derive(Deserialize, TS)]
#[serde(rename_all = "camelCase")]
struct TickArgs {
    interval_ms: u64,
    #[ts(optional)]            // `count?: number` instead of `count: number | null`
    count: Option<u64>,
}
```

Put all registrations in one function, so both the app and a test can call it:

```rust
pub fn register(reactive: &Reactive) { /* .state(...).stream(...) */ }

// In setup: regenerate on every debug run. The file is only written when it changes.
#[cfg(debug_assertions)]
app.reactive().export_typescript(concat!(env!("CARGO_MANIFEST_DIR"), "/../src/sources.gen.ts"))?;

// In tests / CI: fail when the committed bindings are stale (and regenerate them).
#[test]
fn bindings_up_to_date() {
    let reactive = Reactive::default();
    register(&reactive);
    assert!(!reactive.export_typescript(PATH).unwrap(), "bindings were stale; commit the regenerated file");
}
```

```ts
import { createReactive } from 'tauri-plugin-reactive-api';
import type { Sources } from './sources.gen';
export const rx = createReactive<Sources>();
```

The generated file is self-contained. It holds every referenced type declared once, plus `Sources`:

```ts
export type Settings = { theme: Theme, volume: number, };
export type Theme = "dark" | "light";
export type TickArgs = { intervalMs: number, count?: number, };

export type Sources = {
  "settings": WritableState<Settings>;
  "ticks": Stream<number, TickArgs>;
};
```

Notes:

- **Large integers:** `u64` and `i64` are emitted as `number`, which matches what serde actually sends.
- **Serde attributes are respected:** `rename_all`, enum tagging and so on.
- **Streams with no arguments:** a `()` argument becomes a stream you call with no arguments.
- **Name collisions:** two Rust types with the same name are reported as an error. Rename one
  with `#[ts(rename = "...")]`.
- **Dev mode:** if `tauri dev` watches your frontend folder, add the generated file to `.taurignore`.

## Example

[`examples/`](examples) has the same demo app in React, Svelte, Vue and Angular, with one shared Rust
backend. Each one's `npm run selftest` (and `selftest:dev` under `tauri dev`) runs 35 end-to-end checks over real
IPC: the library, that framework's adapter, permissions, sinks, cleanup on reload and window close, and the real demo UI
driven through the DOM.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
