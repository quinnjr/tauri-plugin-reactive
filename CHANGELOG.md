# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). The Rust crate `tauri-plugin-reactive` and
the npm package `tauri-plugin-reactive-api` are released together with the same version.

## [Unreleased]

## [0.1.0] - 2026-11-23

First release.

### Rust plugin (`tauri-plugin-reactive`)

- **Sources** registered by name on the `Reactive` registry (`app.reactive()`):
  - `state`, `writable_state` and `writable_state_with` (Rust-side validation) for `ReactiveState<T>`.
    State updates are coalesced, so the latest value wins.
  - `stream` and `try_stream`: a factory runs per subscription with typed, deserialized arguments.
  - `sink` and `try_sink`: frontend → Rust streams consumed as a `SinkStream<T>`.
- **`ReactiveState<T>`**: a cheap-to-clone shared value with `get`, `with`, `set`, `set_if_changed`,
  `update`, `update_if`, `watch` and `stream`.
- **Sinks:**
  - **Delivery:** in order, with backpressure (a bounded 64-value channel).
  - **Completion is explicit.** Abort, an invalid value, page reload, window destroy and registry drop
    all abort the handler, so an abandoned run never reaches its commit.
  - **`sink_abort` reports what actually happened.**
- **Sharing and cleanup:**
  - One subscription per source and arguments.
  - Teardown on unsubscribe, page reload and window destroy.
  - A per-webview generation counter and page tokens refuse late calls from replaced pages and destroyed windows.
  - Stream panics are caught and reported.
- **Per-source permissions** through Tauri ACL command scopes (`{ "source": "<glob>" }`):
  - `reactive:default` allows every source, in the windows its capability covers.
  - Deny scopes can restrict individual commands.
  - Capability files are validated against the scope schema.
- **`typegen` feature:** generates the frontend's TypeScript `Sources` type and every value and argument
  type from the Rust registrations, using ts-rs 12, with a staleness check for CI.
- Stable error codes for every failure.

### TypeScript client (`tauri-plugin-reactive-api`)

- **`createReactive<Sources>()`:** a fully typed client. Names, kinds, value types, required arguments and
  `set` on writable states are all checked at compile time.
- **`Source<T>`:**
  - shared and reference-counted, with deferred teardown (`keepAliveMs`);
  - state replay for late subscribers;
  - Observable semantics for observer objects and store semantics for callbacks;
  - error reporting that never swallows errors;
  - RxJS interop (`from(source)`) and the Svelte store contract.
- **`SinkWriter<T>`:** batches values per tick with one push in flight. `complete()`, `abort()` and `closed`
  return a promise-compatible `SinkOutcome`, and failures that nobody handles are reported. It works as
  an RxJS observer.
- **Framework adapters:**
  - React: `useReactive`, `useReactiveState`
  - Vue: `useReactive`, which accepts sources through `ref()`, `reactive()` or getters
  - Angular: `reactiveSignal`, which works zoneless

### Examples and tooling

- The same demo app in React, Svelte, Vue and Angular, with a shared Rust backend and generated types.
- A 35-check end-to-end self-test run inside the real webview, in both built and `tauri dev` modes. It
  covers the library, each framework adapter, permissions (including a restrictive per-window
  capability), sink abort semantics, reload and window-close cleanup, and the demo UI driven through the
  DOM.

[Unreleased]: https://github.com/quinnjr/tauri-plugin-reactive/compare/v0.1.0...develop
[0.1.0]: https://github.com/quinnjr/tauri-plugin-reactive/releases/tag/v0.1.0
