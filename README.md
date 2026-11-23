# tauri-plugin-reactive

**Typed reactive state, streams and frontend-to-Rust sinks across Tauri's Rust ↔ JavaScript boundary.**

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![Tauri v2](https://img.shields.io/badge/Tauri-v2-24C8DB)](https://v2.tauri.app)
[![Rust 1.90+](https://img.shields.io/badge/rust-1.90%2B-orange)](https://www.rust-lang.org)

Hold state in Rust and have every window render it live. Stream data from Rust into the UI. Stream
user input back into Rust. All of it typed end to end, shared between components, permission-checked
per window, and cleaned up automatically when nobody is listening, a page reloads or a window closes.

```rust
// Rust
let downloads = ReactiveState::new(0u32);
app.reactive().state("downloads", &downloads);
downloads.update(|n| *n += 1); // every subscribed window re-renders
```

```tsx
// React (Vue, Angular, Svelte and RxJS work too)
const downloads = useReactive(rx.state('downloads'), 0);
```

---

## Contents

- [Why](#why)
- [Features](#features)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
- [Rust API](#rust-api)
- [JavaScript / TypeScript API](#javascript--typescript-api)
- [Framework integration](#framework-integration)
- [Frontend → Rust streams (sinks)](#frontend--rust-streams-sinks)
- [Permissions](#permissions)
- [Generated TypeScript types](#generated-typescript-types)
- [Lifecycle and cleanup](#lifecycle-and-cleanup)
- [Errors](#errors)
- [How it works](#how-it-works)
- [Examples](#examples)
- [Development](#development)
- [Limitations](#limitations)
- [FAQ](#faq)
- [License](#license)

---

## Why

Tauri gives you three ways to move data between Rust and the webview: commands (request/response),
events (fire-and-forget broadcast) and channels (one-off streams). Building *live, shared state* on top
of those is repetitive and easy to get subtly wrong:

- **Typing.** Nothing ties a JavaScript `listen('downloads')` to the Rust type behind it, so they drift.
- **Lifecycle.** Who stops the Rust work when a component unmounts, the page reloads or a window closes?
  Channel sends keep "succeeding" after a reload, so forgotten streams leak silently.
- **Sharing.** Ten components showing the same value shouldn't mean ten Rust tasks.
- **Late joiners.** A component mounted after the last event needs the current value, not the next one.
- **Permissions.** Tauri's ACL decides who may call a command, not which data a window may see.

This plugin handles all of that once. Rust registers named **sources**, and the frontend subscribes to
them by name with full types.

## Features

- **State** (`ReactiveState<T>`): subscribers get the current value, then every change. Rapid updates are
  coalesced (latest wins), so a hot value can't flood the IPC bridge.
- **Writable state:** the frontend can `set` it, with optional validation in Rust.
- **Streams:** a Rust factory runs **per subscription** with typed arguments, lazily, and is dropped when
  nobody listens.
- **Sinks (frontend → Rust):** the frontend pushes values and a Rust handler consumes them as a `Stream`.
  Order is preserved, backpressure is real, and an abandoned run can never look like a finished one.
- **Sharing:** every subscriber to the same source and arguments shares one Rust subscription.
- **Automatic cleanup:** on the last unsubscribe, page reload, window close and app shutdown.
- **Per-source, per-window permissions** through Tauri's own capability files.
- **Generated TypeScript types** from your Rust registrations (optional `typegen` feature).
- **Framework adapters:** React hooks, Vue composables, Angular signals, Svelte stores (no adapter
  needed) and RxJS interop.
- **Framework-agnostic core:** the plain client works anywhere, with no framework at all.

## Installation

> **Status:** `0.1.0`, not yet published to crates.io or npm. Until it is, install from GitHub as shown
> below. Once published, the plain `cargo add tauri-plugin-reactive` / `npm i tauri-plugin-reactive-api`
> will work.

### Requirements

| | Version |
|---|---|
| Tauri | 2.x |
| Rust | 1.90 or later |
| `@tauri-apps/api` | 2.x |
| React (optional) | 18 or later |
| Vue (optional) | 3.3 or later |
| Angular (optional) | 19 or later (signals; works zoneless) |
| Svelte (optional) | any (uses the store contract) |
| RxJS (optional) | 7 |

### Rust

```toml
# src-tauri/Cargo.toml
[dependencies]
tauri-plugin-reactive = { git = "https://github.com/quinnjr/tauri-plugin-reactive", branch = "main" }
# once published:
# tauri-plugin-reactive = "0.1"
```

Register the plugin:

```rust
fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_reactive::init())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

### JavaScript / TypeScript

```sh
npm install github:quinnjr/tauri-plugin-reactive#main
# once published:
# npm install tauri-plugin-reactive-api
```

The package builds itself on install from git. `main` holds released code; `develop` (the default branch) is the next release in progress. The package name is `tauri-plugin-reactive-api`, with
subpath exports for the adapters:

| Import | Contents |
|---|---|
| `tauri-plugin-reactive-api` | `createReactive`, schema types, `Source`, `SinkWriter`, errors |
| `tauri-plugin-reactive-api/react` | `useReactive`, `useReactiveState` |
| `tauri-plugin-reactive-api/vue` | `useReactive` |
| `tauri-plugin-reactive-api/angular` | `reactiveSignal` |

### Permissions

Grant the plugin in a capability file, for example `src-tauri/capabilities/default.json`:

```json
{
  "identifier": "default",
  "windows": ["main"],
  "permissions": ["core:default", "reactive:default"]
}
```

`reactive:default` allows every source in the windows that capability covers. See
[Permissions](#permissions) to restrict it.

## Quick start

A counter owned by Rust, a stream with arguments, and settings the UI can edit.

**Rust** (`src-tauri/src/main.rs`):

```rust
use std::time::Duration;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tauri::Manager;
use tauri_plugin_reactive::{ReactiveExt, ReactiveState};

#[derive(Clone, Serialize, Deserialize)]
struct Settings {
    theme: String,
    volume: u8,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickArgs {
    interval_ms: u64,
}

fn ticker(interval_ms: u64) -> impl Stream<Item = u64> + Send + 'static {
    futures::stream::unfold(0u64, move |n| async move {
        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
        Some((n, n + 1))
    })
}

#[tauri::command]
fn click(clicks: tauri::State<'_, ReactiveState<u64>>) {
    clicks.update(|n| *n += 1);
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_reactive::init())
        .invoke_handler(tauri::generate_handler![click])
        .setup(|app| {
            let clicks = ReactiveState::new(0u64);
            let settings = ReactiveState::new(Settings { theme: "dark".into(), volume: 50 });

            app.reactive()
                .state("clicks", &clicks) // read-only for the frontend
                .writable_state_with("settings", &settings, |s: &Settings| {
                    if s.volume > 100 { Err("volume must be 0-100") } else { Ok(()) }
                })
                .stream("ticks", |args: TickArgs| ticker(args.interval_ms));

            app.manage(clicks); // so commands (and any Rust code) can change it
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

**TypeScript** (`src/reactive.ts`):

```ts
import { createReactive, type State, type WritableState, type Stream } from 'tauri-plugin-reactive-api';

// Describes what Rust registered. Can be generated: see "Generated TypeScript types".
type Sources = {
  clicks: State<number>;
  settings: WritableState<{ theme: string; volume: number }>;
  ticks: Stream<number, { intervalMs: number }>;
};

export const rx = createReactive<Sources>();
```

**React** (`src/App.tsx`):

```tsx
import { invoke } from '@tauri-apps/api/core';
import { useReactive } from 'tauri-plugin-reactive-api/react';
import { rx } from './reactive';

export function App() {
  const clicks = useReactive(rx.state('clicks'), 0);
  const tick = useReactive(rx.stream('ticks', { intervalMs: 500 }));
  const settings = useReactive(rx.state('settings'));

  return (
    <main>
      <button onClick={() => invoke('click')}>Clicked {clicks} times</button>
      <p>Tick {tick ?? '…'}</p>
      {settings && (
        <input
          type="range" min={0} max={100} value={settings.volume}
          onChange={(e) => rx.state('settings').set({ ...settings, volume: +e.target.value })}
        />
      )}
    </main>
  );
}
```

Every window running this stays in sync with Rust. The `ticks` stream runs only while the component is
mounted, and two components asking for `{ intervalMs: 500 }` share one Rust stream.

## Core concepts

### Sources

Rust registers **sources** under names. There are four kinds:

| Kind | Rust | Frontend | Direction | Semantics |
|---|---|---|---|---|
| State | `.state(name, &state)` | `rx.state(name)` | Rust → JS | Current value, then every change |
| Writable state | `.writable_state(name, &state)` | `rx.state(name).set(v)` | both | As state, plus validated writes from JS |
| Stream | `.stream(name, factory)` | `rx.stream(name, args)` | Rust → JS | A fresh Rust stream per distinct `args` |
| Sink | `.sink(name, handler)` | `rx.sink(name, args)` | JS → Rust | Each writer runs the handler once |

### State vs. stream

| | State | Stream |
|---|---|---|
| On subscribe | current value, then changes | values from now on (hot and shared) |
| Late subscriber to a shared source | gets the latest value replayed | only future values |
| Fast producer | coalesced: subscribers see the latest | every item is sent |
| `get()` / `set()` | yes / if writable | no |
| Ends | never (until unsubscribed) | when the Rust stream ends (complete) or errors |

### Sharing

Calling `rx.state('x')` or `rx.stream('x', args)` twice with equal arguments returns **the same
`Source` object**. Arguments are compared structurally, so `{ a: 1, b: 2 }` equals `{ b: 2, a: 1 }`. All
subscribers to a source share **one** Rust subscription:

- It is opened by the first subscriber.
- It is closed shortly after the last one leaves.

This is why it's safe to call `rx.stream('ticks', { intervalMs })` inside a render function.

### Typing

`createReactive<Sources>()` takes a map from source name to kind:

```ts
type Sources = {
  name: State<T>;                // read-only state
  name: WritableState<T>;        // state the frontend can set
  name: Stream<T, Args = void>;  // stream; Args is what the Rust factory deserializes
  name: Sink<T, Args = void>;    // sink; T is what the frontend pushes
};
```

The client then knows the following at compile time:

- **Which names exist.**
- **Each source's kind:** `rx.stream('clicks')` is a type error if `clicks` is a state.
- **Value types.**
- **Required arguments:** `rx.stream('ticks')` without args is a type error when the stream takes some.
- **Whether `set` exists:** it does only on writable states.

You can write `Sources` by hand or [generate it from Rust](#generated-typescript-types).

## Rust API

Everything is reached through `app.reactive()`, which works on `App`, `AppHandle`, `Window` and
`Webview`, via the `ReactiveExt` trait.

### `ReactiveState<T>`

A value shared between Rust and every subscribed webview. Cheap to clone: all clones share the same
value. Built on `tokio::sync::watch`.

| Method | Description |
|---|---|
| `ReactiveState::new(value)` | Creates a state. `ReactiveState::default()` works when `T: Default`. |
| `get() -> T` | Clone of the current value (`T: Clone`). |
| `with(\|v\| ...)` | Reads the value by reference, without cloning. |
| `set(value)` | Replaces the value and notifies subscribers. |
| `set_if_changed(value) -> bool` | Replaces and notifies only if different (`T: PartialEq`). |
| `update(\|v\| ...)` | Mutates in place and notifies. |
| `update_if(\|v\| -> bool) -> bool` | Mutates in place; notifies only if the closure returns `true`. |
| `watch() -> watch::Receiver<T>` | A raw receiver for Rust-side consumers. |
| `stream()` | A `Stream` of the current value, then every change (for Rust-side consumers). |

Updates are **coalesced**: a subscriber that falls behind sees only the latest value. That bounds IPC
traffic for fast-changing state, like progress or cursor positions.

### Registering sources (`Reactive`)

All registration methods take `&self`, return `&Self` for chaining, and can be called at any time, not
only in `setup`. Registering an existing name replaces it. Subscriptions already open keep running
against the old source.

```rust
app.reactive()
    .state("clicks", &clicks)
    .writable_state("prefs", &prefs)
    .writable_state_with("settings", &settings, |s: &Settings| validate(s))
    .stream("ticks", |args: TickArgs| ticker(args))
    .try_stream("logs", |path: String| tail(path))
    .sink("pointer", |_: (), points: SinkStream<Point>| async move { /* ... */ })
    .try_sink("import", |file: String, rows: SinkStream<Row>| async move { /* ... */ Ok::<_, String>(()) });
```

| Method | Value bounds | Notes |
|---|---|---|
| `state(name, &ReactiveState<T>)` | `T: Serialize + Clone + Send + Sync` | Read-only for the frontend. |
| `writable_state(name, &ReactiveState<T>)` | plus `T: DeserializeOwned` | The frontend can `set`. |
| `writable_state_with(name, &state, validate)` | `validate: Fn(&T) -> Result<(), E>`, `E: Display` | Rejected values never reach the state; the error goes back to `set()`. |
| `stream(name, factory)` | `factory: Fn(A) -> impl Stream<Item = T>` | Called once per distinct `args`. Use `()` for no args. |
| `try_stream(name, factory)` | stream of `Result<T, E>` | An `Err` is sent to the frontend as an error and ends the subscription. |
| `sink(name, handler)` | `handler: Fn(A, SinkStream<T>) -> impl Future<Output = ()>` | See [sinks](#frontend--rust-streams-sinks). |
| `try_sink(name, handler)` | `Future<Output = Result<(), E>>` | An `Err` fails the writer with `SinkFailed`. |

Arguments (`A`) are deserialized from what the frontend passes, so a mismatch fails with `InvalidArgs`.
Values (`T`) are serialized with `serde_json`.

Other methods:

| Method | Description |
|---|---|
| `unregister(name) -> bool` | Removes a source. New subscriptions fail; existing ones keep running. |
| `contains(name) -> bool` | Whether a source is registered. |
| `active_subscriptions() -> usize` | Live frontend subscriptions, across all webviews. |
| `active_sinks() -> usize` | Open frontend → Rust sinks, across all webviews. |
| `typescript()` / `export_typescript(path)` | TypeScript bindings (feature `typegen`). See [below](#generated-typescript-types). |

**Driving state from anywhere.** Keep the `ReactiveState` (it's `Clone`), or `app.manage()` it, and change
it from commands, background tasks, file watchers or other plugins:

```rust
let progress = ReactiveState::new(0.0f32);
app.reactive().state("progress", &progress);

let p = progress.clone();
tauri::async_runtime::spawn(async move {
    for i in 0..=100 {
        p.set(i as f32 / 100.0);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
});
```

### Other exports

| Item | Description |
|---|---|
| `init()` | The plugin. |
| `ReactiveExt` | Adds `.reactive()` to `App`, `AppHandle`, `Window` and `Webview`. |
| `SinkStream<T>` | The `Stream` a sink handler receives. |
| `SourceScope` | The capability scope object: `{ "source": "<glob>" }`. |
| `Error`, `Result` | Command errors; `Error::code()` gives the code the frontend sees. |
| `Event` | The wire format of subscription events (`next` / `error` / `complete`). |
| `MaybeTs` | Bound on registered types: `ts_rs::TS` with `typegen`, nothing without. |
| `ts_rs`, `TS` | Re-exports (feature `typegen`). |

## JavaScript / TypeScript API

### `createReactive<Sources>(options?)`

```ts
const rx = createReactive<Sources>({ keepAliveMs: 0 });
```

| Option | Default | Description |
|---|---|---|
| `keepAliveMs` | `0` | How long to keep a Rust subscription open after its last subscriber leaves, so a quick re-subscribe (route change, list re-render) reuses it. `0` means "until the end of the current microtask", which already absorbs React StrictMode's double effects. |

Create **one client per app** (a module-level `export const rx`) so sharing works across components.

| Method | Returns |
|---|---|
| `rx.state(name)` | `StateSource<T>`, or `WritableStateSource<T>` for writable states |
| `rx.stream(name, args?)` | `Source<T>` (args required if the stream takes them) |
| `rx.sink(name, args?)` | `SinkWriter<T>`: a **new** writer on each call |

### `Source<T>`

Returned by `rx.state()` and `rx.stream()`.

| Member | Description |
|---|---|
| `subscribe(observer)` | Observer object `{ next?, error?, complete? }`. Follows Observable semantics: an error or completion ends this subscription. |
| `subscribe(next, error?, complete?)` | Callback ("store") style. Stays registered after an error or completion and receives values again if the source reconnects. |
| `value` | Latest value received, or `undefined`. Kept across reconnects until a new value arrives. |
| `hasValue` | Whether the current connection has delivered a value. |
| `error` | The `ReactiveError` that ended the last connection, if any. Cleared on reconnect. |
| `connected` | Whether a Rust subscription is open or opening. |
| `listen(onChange)` / `snapshot()` / `snapshotState()` | Stable functions for `useSyncExternalStore`-style integrations. `snapshotState()` returns `{ value, error }` as a new object whenever either changes. |
| `[Symbol.observable]()` | RxJS interop: `from(source)` works. |
| `get()` *(states)* | `Promise<T>`: reads the current value without subscribing. |
| `set(value)` *(writable states)* | `Promise<void>`: writes through Rust validation. Subscribers, including this one, get the value through the normal update path. |

`subscribe` returns an `Unsubscribe`: a function that also has an `.unsubscribe()` method, so it fits
both store and RxJS conventions.

```ts
const stop = rx.state('clicks').subscribe((n) => console.log('clicks', n));
stop(); // or stop.unsubscribe()

rx.stream('logs', '/var/log/app.log').subscribe({
  next: (line) => append(line),
  error: (e) => showError(e.code, e.message),
  complete: () => console.log('log ended'),
});

const current = await rx.state('settings').get();
await rx.state('settings').set({ ...current, volume: 30 });
```

### `SinkWriter<T>`

Returned by `rx.sink(name, args?)`. Each writer starts its own run of the Rust handler. See
[sinks](#frontend--rust-streams-sinks) for the semantics.

| Member | Description |
|---|---|
| `next(value)` | Queues a value. Values queued in the same tick go out as one batch. Ignored once completing or closed. |
| `complete()` | Sends what's queued, ends the stream, and returns a `SinkOutcome` that settles with the handler's result. |
| `abort(reason?)` | Abandons the run: drops queued values and aborts the handler. Returns a `SinkOutcome`. |
| `error(err)` | Observer `error`: aborts (used when piping from RxJS). |
| `closed` | A `SinkOutcome` that settles when the sink ends. |
| `isOpen` | Whether the writer still accepts values. |
| `failure` | The `ReactiveError` that ended the sink, if it failed. |

`SinkOutcome` behaves like a `Promise<void>`: `await`, `.then`, `.catch`, `.finally` and `Promise.all`
all work, and it type-checks wherever a `Promise<void>` is expected. If a failure is never awaited or
handled, it's reported with `console.error` rather than lost.

### `ReactiveError`

Every failure the client surfaces is a `ReactiveError` (it extends `Error`) with a string `code`. See
[Errors](#errors) for the codes.

```ts
try {
  await rx.state('settings').set({ theme: 'neon', volume: 200 });
} catch (e) {
  if (e instanceof ReactiveError && e.code === 'InvalidValue') showValidationMessage(e.message);
}
```

### Schema types

| Type | Meaning |
|---|---|
| `State<T>` | Read-only state |
| `WritableState<T>` | Writable state |
| `Stream<T, A = void>` | Stream with value `T` and arguments `A` |
| `Sink<T, A = void>` | Sink accepting `T`, with handler arguments `A` |

## Framework integration

### React

```tsx
import { useReactive, useReactiveState } from 'tauri-plugin-reactive-api/react';

function Downloads() {
  const count = useReactive(rx.state('downloads'), 0);               // T
  const tick = useReactive(rx.stream('ticks', { intervalMs: 1000 })); // T | undefined
  const { value: log, error } = useReactiveState(rx.stream('logs', path));
  return error ? <p>{error.message}</p> : <p>{count} · {tick} · {log}</p>;
}
```

- Built on `useSyncExternalStore`, so it's safe under concurrent rendering, with no tearing.
- **Fresh args are fine.** Passing a new `{ intervalMs }` object every render resolves to the same shared source.
- **StrictMode** double-mounting doesn't reopen Rust work.
- **Errors.** `useReactiveState` re-renders when the source errors; `useReactive` only returns the value.

### Vue

```vue
<script setup lang="ts">
import { ref } from 'vue';
import { useReactive } from 'tauri-plugin-reactive-api/vue';

const props = defineProps<{ intervalMs: number }>();
const downloads = useReactive(rx.state('downloads'), 0);  // Readonly<Ref<number>>
// A getter re-subscribes when what it reads changes, without remounting:
const tick = useReactive(() => rx.stream('ticks', { intervalMs: props.intervalMs }));
</script>

<template><p>{{ downloads }} · {{ tick ?? '…' }}</p></template>
```

- `useReactive` accepts a source, a ref to a source, or a getter.
- The subscription lives as long as the component or effect scope.
- **Sources are marked raw,** so wrapping one in `ref()` or `reactive()` state is safe: Vue never proxies it.

### Angular

```ts
import { Component, input } from '@angular/core';
import { reactiveSignal } from 'tauri-plugin-reactive-api/angular';

@Component({
  selector: 'app-downloads',
  template: `{{ downloads() }} · {{ tick() ?? '…' }}`,
})
export class DownloadsComponent {
  intervalMs = input.required<number>();
  downloads = reactiveSignal(rx.state('downloads'), { initial: 0 });  // Signal<number>
  // A function re-subscribes when the signals it reads change:
  tick = reactiveSignal(() => rx.stream('ticks', { intervalMs: this.intervalMs() }));
}
```

- `reactiveSignal` returns a read-only `Signal` and unsubscribes when the owning component or service is destroyed.
- **Where to call it:** in an injection context (field initializer or constructor), or pass `{ injector }`.
- **Zoneless:** works with `provideZonelessChangeDetection()`.
- **RxJS route:** sources are interop observables, so `toSignal(from(source))` also works.

### Svelte

No adapter is needed, because every `Source` is a Svelte store:

```svelte
<script lang="ts">
  let { intervalMs }: { intervalMs: number } = $props();
  const downloads = rx.state('downloads');
  // Re-derived when the prop changes; `$tick` re-subscribes to the new source.
  const tick = $derived(rx.stream('ticks', { intervalMs }));
</script>

<p>{$downloads ?? 0} · {$tick ?? '…'}</p>
```

### RxJS

```ts
import { from, bufferCount, map, fromEvent } from 'rxjs';

// Rust stream → RxJS pipeline (unsubscribing tears down the Rust stream)
from(rx.stream('ticks', { intervalMs: 100 }))
  .pipe(bufferCount(5), map((xs) => xs.reduce((a, b) => a + b, 0) / xs.length))
  .subscribe(console.log);

// RxJS → Rust sink (a SinkWriter is an observer)
fromEvent<PointerEvent>(pad, 'pointermove')
  .pipe(map((e) => ({ x: e.offsetX, y: e.offsetY })))
  .subscribe(rx.sink('pointer'));
```

### No framework

```ts
const el = document.querySelector('#clicks')!;
const stop = rx.state('clicks').subscribe((n) => (el.textContent = String(n)));
window.addEventListener('beforeunload', stop);
```

## Frontend → Rust streams (sinks)

A sink is the reverse of a stream: the frontend pushes values and a Rust handler consumes them.

```rust
use futures::StreamExt;
use tauri_plugin_reactive::SinkStream;

#[derive(serde::Deserialize)]
struct Point { x: f64, y: f64 }

app.reactive()
    // A handler that runs until the writer completes (or is aborted):
    .sink("pointer", |_: (), mut points: SinkStream<Point>| async move {
        while let Some(p) = points.next().await {
            track(p);
        }
    })
    // A fallible handler with arguments. The commit only runs for a completed upload:
    .try_sink("import", |file: String, mut rows: SinkStream<Row>| async move {
        let mut batch = Vec::new();
        while let Some(row) = rows.next().await {
            batch.push(row);
        }
        db.commit(&file, batch).map_err(|e| e.to_string())
    });
```

```ts
const writer = rx.sink('import', 'customers.csv');
for (const row of parsed) writer.next(row);
await writer.complete(); // resolves after db.commit(); rejects with SinkFailed if it returned Err

// Cancelled halfway? Nothing is committed:
writer.abort('user cancelled');
```

### Guarantees

- **Order.** Values arrive in the order `next()` was called. Values queued in the same tick are sent
  as one batch, and only one batch is in flight at a time.
- **Backpressure.** The channel to the handler holds 64 values. A slow handler makes pushes wait, so
  the writer slows down instead of piling up memory or IPC calls.
- **Completion is explicit.** The handler's stream ends normally (`None`) **only** when the writer calls
  `complete()`. `complete()` then resolves with the handler's own result.
- **Anything else aborts.** The handler's future is dropped, so code after its loop (a commit, say)
  never runs, and a truncated upload can never look finished. The cases are:
  - `writer.abort()`, or a producer error when the writer is piped from RxJS;
  - a value that doesn't deserialize (`InvalidValue`, enforced by the backend itself);
  - a page reload, the window closing, or the plugin's state being dropped at app teardown.

  The stream itself also refuses to end once aborted, even for a handler mid-poll on another thread.
- **`abort()` reports what actually happened:**
  - **Already finished:** if the handler had already finished on its own (it broke out early and
    committed, or failed), the outcome reflects that instead of claiming `Aborted`.
  - **Too late:** once `complete()` has sent its close, `abort()` has no effect, and the outcome is the
    close result.

### Errors from sinks

| Situation | Writer outcome |
|---|---|
| Handler returned `Ok` after `complete()` | resolves |
| Handler returned `Err(e)` | rejects `SinkFailed` (message is `e`) |
| A pushed value didn't deserialize | rejects `InvalidValue`; handler aborted |
| `abort()` / producer error | rejects `Aborted`; handler aborted |
| Not allowed by the capability | rejects `Forbidden` |
| The name is a state or stream | rejects `NotASink` |

## Permissions

Every command checks the source name against the calling window's capability scopes. A source is
accessible only if an **allow** entry matches it and no **deny** entry does. `source` is a glob: `*`
matches any run of characters (`"*"`, `"settings.*"`, `"*Stats"`).

### The default

`reactive:default` grants every command, with an allow-everything scope on each opening command. It
applies only to the windows and webviews of the capability that includes it.

| Permission | Grants |
|---|---|
| `reactive:default` | The 8 commands below; every source allowed (in this capability's windows) |
| `reactive:allow-subscribe-all-sources` | `subscribe` + `source: "*"` |
| `reactive:allow-get-all-sources` | `get` + `source: "*"` |
| `reactive:allow-set-all-sources` | `set` + `source: "*"` |
| `reactive:allow-sink-open-all-sources` | `sink_open` + `source: "*"` |
| `reactive:allow-<command>` / `reactive:deny-<command>` | One command, no scope (`subscribe`, `unsubscribe`, `get`, `set`, `sink-open`, `sink-push`, `sink-close`, `sink-abort`) |

### Deny a few sources or operations

Scopes attached to a command's permission apply to that command only:

```json
"permissions": [
  "reactive:default",
  { "identifier": "reactive:allow-subscribe", "deny": [{ "source": "admin*" }] },
  { "identifier": "reactive:allow-get", "deny": [{ "source": "admin*" }] },
  { "identifier": "reactive:allow-set", "deny": [{ "source": "admin*" }, { "source": "motd" }] }
]
```

With this, `adminToken` is unreachable from these windows, and `motd` is readable but not writable.

### Allow only specific sources

Leave out `reactive:default` and list allow scopes on the opening commands:

```json
{
  "identifier": "settings-window",
  "windows": ["settings"],
  "permissions": [
    { "identifier": "reactive:allow-subscribe", "allow": [{ "source": "settings" }, { "source": "theme" }] },
    { "identifier": "reactive:allow-set", "allow": [{ "source": "settings" }] },
    { "identifier": "reactive:allow-sink-open", "allow": [{ "source": "pointer" }] },
    "reactive:allow-unsubscribe",
    "reactive:allow-sink-push",
    "reactive:allow-sink-close",
    "reactive:allow-sink-abort"
  ]
}
```

### How commands are checked

| Commands | Check |
|---|---|
| `subscribe`, `get`, `set`, `sink_open` (opening, reading, writing) | allow match **and** no deny match |
| `unsubscribe`, `sink_push`, `sink_close`, `sink_abort` (acting on something already open) | the source it was opened for must not match a deny; plus the caller must own it |

The id-based commands can be granted plainly, as above, because what they act on already passed the
full check when it was opened. A webview can never act on another webview's subscriptions or sinks.
Denied operations fail with `Forbidden`.

> **Global scopes are app-wide.** A scope on a permission that grants *no* command becomes a Tauri
> *global* scope. Tauri applies global scopes to every window, whichever capability granted them. The
> plugin honors them, but use them only for app-wide rules (a global deny). Attach per-window rules to
> command permissions, as above. `reactive:default` does this.

Capability files are validated against the `source` scope schema, so editors autocomplete it.

## Generated TypeScript types

With the `typegen` feature, the `Sources` type and every value and argument type are generated from
your Rust registrations using [`ts-rs`](https://crates.io/crates/ts-rs). The frontend can't drift from the
backend: renaming a source or changing a field in Rust becomes a type error at every use in the frontend.

```toml
[dependencies]
tauri-plugin-reactive = { git = "https://github.com/quinnjr/tauri-plugin-reactive", branch = "main", features = ["typegen"] }
ts-rs = "12"
```

**1. Derive `TS`** on every type you register. The compiler enforces it at each registration call.

```rust
use ts_rs::TS;

#[derive(Clone, Copy, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
enum Theme { Dark, Light }

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

The derive needs `ts-rs` as a direct dependency. Alternatively, put
`#[ts(crate = "tauri_plugin_reactive::ts_rs")]` on each type.

**2. Register everything in one function**, so the app and a test can both call it:

```rust
pub const BINDINGS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../src/sources.gen.ts");

pub fn register(reactive: &Reactive) { /* .state(...).stream(...).sink(...) */ }

// In setup: regenerate on every debug run. The file is only written when it changes.
register(app.reactive());
#[cfg(debug_assertions)]
app.reactive().export_typescript(BINDINGS)?;

// In tests / CI: fail when the committed file is stale (and regenerate it).
#[test]
fn bindings_up_to_date() {
    let reactive = Reactive::default();
    register(&reactive);
    assert!(!reactive.export_typescript(BINDINGS).unwrap(), "bindings were stale; commit the result");
}
```

**3. Use it:**

```ts
import { createReactive } from 'tauri-plugin-reactive-api';
import type { Sources } from './sources.gen';
export const rx = createReactive<Sources>();
```

The generated file is self-contained:

```ts
// Generated by tauri-plugin-reactive from the sources registered in Rust. Do not edit.
import type { State, Stream, WritableState } from 'tauri-plugin-reactive-api';

export type Settings = { theme: Theme, volume: number, };
export type Theme = "dark" | "light";
export type TickArgs = { intervalMs: number, count?: number, };

export type Sources = {
  "clicks": State<number>;
  "settings": WritableState<Settings>;
  "ticks": Stream<number, TickArgs>;
};
```

- **Serde attributes are respected:** `rename_all`, enum tagging and so on.
- **Large integers:** `u64` and `i64` are emitted as `number`, which matches what `serde_json` sends.
- **No arguments:** a `()` argument means the source is called without arguments.
- **Shared types** are declared once. That includes a generic used with several type arguments, and
  `serde_json::Value`.
- **Errors.** Two different Rust types with the same name are an error, and so is a type named like the
  generated identifiers (`State`, `WritableState`, `Stream`, `Sink`, `Sources`). Rename with
  `#[ts(rename = "...")]`.
- **Dev mode.** If `tauri dev` watches your frontend folder, add the generated file to `.taurignore` so
  regenerating it doesn't restart the app.

## Lifecycle and cleanup

| Event | Subscriptions | Sinks |
|---|---|---|
| Last subscriber unsubscribes | closed after `keepAliveMs` (default: end of microtask) | n/a |
| `writer.complete()` | n/a | stream ends normally; handler result returned |
| `writer.abort()` / invalid value | n/a | handler aborted |
| Rust stream ends / errors | subscribers get `complete` / `error` | n/a |
| Page reload or navigation | all of that webview's are closed | all aborted |
| Window destroyed | all of that window's are closed | all aborted |
| Plugin state dropped (app teardown) | | all aborted |

Some details:

- **Deferred teardown** absorbs unsubscribe/re-subscribe churn, such as React StrictMode, list
  re-renders and route changes.
- **Late calls from a replaced page are refused.** A call the old page sent just before a reload, or a
  call from a window being destroyed, can't leave an orphan subscription. Each command records the
  page's generation and re-checks it after registering. The client also sends a random per-page token.
- **Stream panics are caught:** subscribers get an error and the entry is removed.

## Errors

| Code | When |
|---|---|
| `UnknownSource` | No source with that name is registered. |
| `Forbidden` | The capability doesn't allow this source for this command in this window. |
| `InvalidArgs` | Arguments didn't deserialize into the stream/sink argument type. |
| `InvalidValue` | A `set` value or sink value didn't deserialize, or `writable_state_with` validation rejected it. |
| `ReadOnly` | `set` on a read-only state. |
| `NotAState` | `get` on something that isn't a state. |
| `NotSubscribable` | Subscribing to a sink. |
| `NotASink` | Opening a writer to something that isn't a sink. |
| `StreamError` | A `try_stream` item was `Err` (ends the subscription). |
| `SinkFailed` | A sink handler returned `Err` (or panicked). |
| `SinkClosed` | Pushing to a sink whose handler already stopped. |
| `UnknownSink` | The sink id doesn't exist (already closed), or belongs to another webview. |
| `Aborted` | The writer was aborted. |
| `StalePage` | The call came from a page that has since been replaced. |
| `Serialize` | A value failed to serialize in Rust. |

**Reporting.** A Rust-side error on a subscription is delivered to observer-object `error` callbacks.
If nobody handles it, it's logged with `console.error`, never swallowed. Store-style callbacks
(Svelte's `subscribe(run, invalidate)`, the adapters) are notified but don't count as handling it.

## How it works

```
                        Rust (plugin)                                    WebView (client)
 ┌───────────────────────────────────────────────┐        ┌──────────────────────────────────────────┐
 │ Reactive registry: name → state/stream/sink   │        │ createReactive<Sources>()                │
 │                                               │        │   one canonical Source per name+args     │
 │ subscribe(source, args, page, Channel) ───────┼───◀────┼── first subscriber                       │
 │   scope check → open stream → pump task ──────┼──▶─────┼─▶ Channel: next / error / complete       │
 │ unsubscribe(id) ── abort pump ◀───────────────┼───◀────┼── last subscriber leaves (deferred)      │
 │                                               │        │                                          │
 │ sink_open(source, args, page) → spawn handler │◀───────┼── rx.sink(name, args)                    │
 │ sink_push(id, [batch]) → bounded channel (64) │◀───────┼── next(v)… one batch in flight          │
 │ sink_close(id) → end of stream → result ──────┼──▶─────┼─▶ complete() resolves / rejects         │
 │ sink_abort(id) → abort → real outcome ────────┼──▶─────┼─▶ abort() settles                       │
 │                                               │        │                                          │
 │ on page load / window destroyed: tear down ◀──┼── Tauri│                                          │
 └───────────────────────────────────────────────┘        └──────────────────────────────────────────┘
```

**IPC commands** (all under `plugin:reactive|`):

| Command | Arguments | Returns |
|---|---|---|
| `subscribe` | `source`, `args`, `page`, `onEvent` (Channel) | subscription id |
| `unsubscribe` | `id` | `bool` |
| `get` | `source` | value |
| `set` | `source`, `value` | |
| `sink_open` | `source`, `args`, `page` | sink id |
| `sink_push` | `id`, `values` (array) | |
| `sink_close` | `id` | (handler result) |
| `sink_abort` | `id` | `"aborted"` or `"completed"` |

Channel events are `{ "type": "next", "value": … }`, `{ "type": "error", "message": … }` or
`{ "type": "complete" }`. You don't need any of this to use the client. It's documented for other
clients and for debugging.

## Examples

[`examples/`](examples) contains the same demo app in **React, Svelte, Vue and Angular**, all sharing
one Rust backend. Each app shows:

- Rust-driven state, writable state with validation, and streams with arguments;
- RxJS pipelines, stream errors with retry, and a frontend → Rust pointer sink;
- per-window permissions, and cleanup when a child window closes.

```sh
cd examples
npm install
cd react                # or svelte / vue / angular
npm run tauri dev       # interactive demo
npm run selftest        # 35 end-to-end checks over real IPC, built app
npm run selftest:dev    # the same checks under `tauri dev`
```

The self-test runs inside the real webview and checks behavior against the backend's actual
subscription and sink counts. It covers the library, that framework's adapter, permissions (including
a restrictive child-window capability), sink abort semantics, cleanup on reload and window close, and
the demo UI driven through the DOM. See [examples/README.md](examples/README.md) for details.

## Development

```sh
# Rust: unit tests (with and without generated types), lints, packaging
cargo test -p tauri-plugin-reactive
cargo test -p tauri-plugin-reactive --features typegen
cargo clippy --workspace --all-targets

# TypeScript client: tests and type checks
npm install
npm test
npm run typecheck
npm run build             # emits dist-js/

# End to end, all four frameworks
cd examples && npm install && npm run selftest && npm run selftest:dev
```

**Contributing:** the project uses git-flow (`develop` is the integration branch; PRs go there). See
[CONTRIBUTING.md](CONTRIBUTING.md). AI coding agents: see [AGENTS.md](AGENTS.md).

After changing `guest-js/`, run `npm run lib` in `examples/`. It rebuilds the client, reinstalls it
into the examples, and clears the dev servers' dependency caches.

**Repository layout:**

```
src/            Rust plugin (registry, state, subscriptions, sinks, scopes, page tracking, typegen)
permissions/    Tauri permission definitions (autogenerated command permissions + the default set)
guest-js/       TypeScript client and the React / Vue / Angular adapters
tests-js/       Client tests (vitest, mocked IPC)
examples/       Four framework apps + shared backend, frontend, scripts and self-test
```

## Limitations

- **Multi-webview windows.** With Tauri's unstable multi-webview API, closing *one* child webview emits
  no event (Tauri 2.12). Its subscriptions and sinks are released when its window is destroyed or a page
  loads in it again.
- **A reload at the exact moment of a page's first call.** If a page's *very first* subscribe is still
  in flight when it reloads, it can't be told apart from the new page's first call, so it's admitted.
  Every later call from the old page is refused.
- **Tested on Windows.** The code is platform-independent, but the end-to-end suite has so far run only
  on Windows (WebView2).
- **Values are JSON.** Everything crosses IPC as JSON via `serde_json`. Binary data should be encoded,
  or sent through a regular command.

## FAQ

**How is this different from Tauri events?**
Events are untyped broadcasts with no current value, no per-subscriber lifecycle and no notion of a
source the frontend can ask for. Here the frontend subscribes by name, gets the current value
immediately, shares one Rust subscription per source, and stops the Rust work when it leaves.

**Can Rust code consume a `ReactiveState` too?**
Yes. `state.watch()` and `state.stream()` give Rust-side consumers the same updates.

**Where should `ReactiveState`s live?**
Wherever is convenient. They're cheap `Clone` handles. A common pattern is to create them in
`setup`, register them, and `app.manage()` them so commands can reach them.

**Can I register sources after startup, or per window?**
Yes: `app.reactive()` works at any time, from any `Manager`. Use [permissions](#permissions) to control
which windows can reach which sources.

**Does every component subscribing open a new Rust stream?**
No. Equal name and arguments share one Rust subscription, and it closes when the last subscriber leaves.

**What happens if the frontend sends a malformed value?**
Nothing panics. Bad arguments are rejected with `InvalidArgs`, and bad values with `InvalidValue`. For
sinks, a bad value also aborts the handler.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
