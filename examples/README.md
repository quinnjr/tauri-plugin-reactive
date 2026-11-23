# Examples

The same demo app in four frameworks. Each one uses its framework's idiomatic integration:

| Example | Integration | Re-subscribe on arg change |
|---|---|---|
| [`react`](react) | `useReactive(source)` from `tauri-plugin-reactive-api/react` | new args → new (shared) source each render |
| [`svelte`](svelte) | none needed: a `Source` *is* a Svelte store (`$clicks`) | `$derived(rx.stream(...))` + `$tick` |
| [`vue`](vue) | `useReactive(source \| getter)` from `tauri-plugin-reactive-api/vue` | getter form: `useReactive(() => rx.stream(..., { intervalMs: props.intervalMs }))` |
| [`angular`](angular) | `reactiveSignal(source \| fn)` from `tauri-plugin-reactive-api/angular` (zoneless) | function form reads input signals; `toSignal(from(source))` also works |

## Layout

```
examples/
  shared/
    backend/     Rust crate with the demo backend (sources, commands). Every src-tauri calls its run().
    frontend/    sources.gen.ts (generated from backend/register()), the typed client (reactive.ts),
                 self-test + entry point (selftest.ts), styles.css
    scripts/     selftest.mjs runner
    icons/
  react/ svelte/ vue/ angular/
    src/         the framework UI + a small Probe component for the self-test
    src-tauri/   ~5 lines of Rust: reactive_example_backend::run(generate_context!())
```

This folder is an npm workspace, so React, Vue, Svelte and Angular are each installed once.
`.npmrc` sets `install-links=true`, so the library is installed as a packed copy of `../dist-js`
(as it would be from npm), not as a symlink.

## Running

```sh
cd examples
npm install
npm run lib                 # rebuild + reinstall the library after changing guest-js/ (also clears dev-server caches)

cd svelte                   # or react / vue / angular
npm run tauri dev           # interactive demo
npm run selftest            # end-to-end checks over real IPC, embedded build; exits 0/1
npm run selftest:dev        # the same checks under `tauri dev` (dev server + HMR)
```

`npm run selftest` / `npm run selftest:dev` from `examples/` runs all four in turn.

## Generated types

`shared/frontend/sources.gen.ts` is generated from `register()` in `shared/backend/src/lib.rs`. It is rewritten
on every debug run of any example (only when it changes), and checked by
`cargo test -p reactive-example-backend`, which fails and regenerates the file when it's stale.
To add a source, register it in Rust and run the app or that test; the frontend types follow.

## What the demo shows

| Panel | Feature |
|---|---|
| Header badge | `liveSubscriptions`: Rust publishes the plugin's own open-subscription count as reactive state |
| Read-only state | `clicks`: a command mutates `ReactiveState` in Rust, and the UI follows |
| Rust-driven state | `uptime`: updated by a background task |
| Writable state | `settings`: `set()` from the UI, validated in Rust (try volume 150) |
| Stream with args | `ticks`: two components with the same args share one backend subscription. Stop/start and watch the badge |
| RxJS interop | `from(rx.stream(...)).pipe(bufferCount, map)` |
| Stream errors | `flaky`: a Rust `Err` ends the stream with a `StreamError`; Retry resubscribes |
| Frontend → Rust stream | `pointer` sink: pointer moves and a 100-point burst are pushed to Rust, which aggregates them into `pointerStats` |
| Window cleanup | a child window holds a subscription and a sink; closing it releases both |

## Self-test

`npm run selftest` builds the frontend, embeds it in the app (`tauri/custom-protocol`), and runs
`shared/frontend/selftest.ts` inside the real webview. It makes 35 checks against the backend's actual
subscription count:

- values, Rust-side updates, `get` / `set`, and validation
- read-only rejection, stream args, completion, `StreamError`, `InvalidArgs` and `UnknownSource`
- sharing, and teardown on unsubscribe and on RxJS `take()`
- **a framework check**: mount the example's `Probe` component through its adapter, assert the DOM
  follows Rust-side changes, and assert unmounting releases all 3 backend subscriptions
- **permissions**: `adminToken` is denied by the capability (`Forbidden` for subscribe, get and set), and
  `motd` is readable but not writable (a deny scope on `reactive:allow-set` only)
- **sinks**: in-order delivery of more values than the buffer holds through a slow handler, handler errors (`SinkFailed`),
  invalid values, piping an RxJS observable, a producer error aborting the handler before its commit,
  and wrong-kind errors
- **a restrictive capability**: child windows (`capabilities/child.json`) get only what they use, in the README's
  restrictive style. A child page checks that its allowed operations work, including the id-based
  commands granted without scopes, and that other sources are `Forbidden`. It reports back over an event.
- cleanup of subscriptions **and sinks** when a window is destroyed and when the page reloads
- **the real demo UI**, mounted and driven through the DOM: every button, slider and select, with
  each panel checked against the backend (sharing, Stop/Start, interval changes, errors, Retry,
  child window)

`selftest:dev` also fails if `tauri dev` restarts the app mid-run. `/.taurignore` keeps the dev
watcher on Rust files only, so frontend and cache writes don't trigger a restart.

Results go to stdout. The script exits 0 only if the app printed `SELF-TEST PASSED`. An app that exits
without a verdict counts as a failure. Rust is compiled before the timed run, and a timed-out run kills the
whole process tree (app and dev server).
