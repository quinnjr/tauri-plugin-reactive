//! The demo backend shared by every framework example. Each example's `src-tauri` just calls [`run`].
//!
//! The frontend's types come from here: [`register`] defines every source, and
//! `shared/frontend/sources.gen.ts` is generated from it (on every debug run, and checked by
//! `cargo test -p reactive-example-backend`).

use std::time::Duration;

use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Manager};
use tauri_plugin_reactive::{Reactive, ReactiveExt, ReactiveState, SinkStream};
use ts_rs::TS;

/// Where the generated TypeScript bindings live (shared by all framework examples).
pub const BINDINGS_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../frontend/sources.gen.ts");

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum Theme {
    Dark,
    Light,
}

/// User-editable settings. Written from the UI, validated in Rust.
#[derive(Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    pub theme: Theme,
    /// 0-100
    pub volume: u8,
}

#[derive(Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TickArgs {
    pub interval_ms: u64,
    /// Stop (complete) after this many ticks; run forever if omitted.
    #[ts(optional)]
    pub count: Option<u64>,
}

#[derive(Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct FlakyArgs {
    pub interval_ms: u64,
    pub fail_after: u64,
}

#[derive(Clone, Copy, Deserialize, TS)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

/// Aggregated from the points the frontend pushes into the `pointer` sink.
#[derive(Clone, Default, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PointerStats {
    pub points: u64,
    pub distance: f64,
}

#[derive(Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SequenceArgs {
    /// Artificial per-value delay, to exercise backpressure.
    pub delay_ms: u64,
}

/// Handles to the state Rust code drives.
pub struct Handles {
    pub clicks: ReactiveState<u64>,
    pub uptime: ReactiveState<u64>,
    pub live_subscriptions: ReactiveState<usize>,
}

fn ticker(interval_ms: u64, count: Option<u64>) -> impl Stream<Item = u64> + Send + 'static {
    futures::stream::unfold(0u64, move |n| async move {
        if count.is_some_and(|c| n >= c) {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
        Some((n, n + 1))
    })
}

/// Registers every source the frontend can use. This is the single source of truth for the
/// generated TypeScript `Sources` type.
pub fn register(reactive: &Reactive) -> Handles {
    let handles = Handles {
        clicks: ReactiveState::new(0),
        uptime: ReactiveState::new(0),
        live_subscriptions: ReactiveState::new(0),
    };
    let settings = ReactiveState::new(Settings { theme: Theme::Dark, volume: 50 });
    let pointer_stats = ReactiveState::new(PointerStats::default());
    let sequence_total = ReactiveState::new(0u64);

    reactive
        .state("clicks", &handles.clicks)
        .state("uptime", &handles.uptime)
        .state("liveSubscriptions", &handles.live_subscriptions)
        // Unknown themes are already rejected by deserialization; only the range needs checking.
        .writable_state_with("settings", &settings, |s: &Settings| {
            if s.volume > 100 {
                Err(format!("volume must be 0-100, got {}", s.volume))
            } else {
                Ok(())
            }
        })
        .stream("ticks", |a: TickArgs| ticker(a.interval_ms, a.count))
        .try_stream("flaky", |a: FlakyArgs| {
            ticker(a.interval_ms, None).map(move |n| {
                if n >= a.fail_after {
                    Err(format!("sensor failed after {n} readings"))
                } else {
                    Ok(n)
                }
            })
        })
        // Locked down by the capability files: see src-tauri/capabilities/default.json.
        .writable_state("adminToken", &ReactiveState::new(String::from("s3cret")))
        .writable_state("motd", &ReactiveState::new(String::from("Welcome!")))
        // Frontend -> Rust: points pushed from the UI are aggregated here and published back.
        .state("pointerStats", &pointer_stats)
        .sink("pointer", move |_: (), mut points: SinkStream<Point>| {
            let stats = pointer_stats.clone();
            async move {
                let mut last: Option<Point> = None;
                while let Some(p) = points.next().await {
                    stats.update(|s| {
                        s.points += 1;
                        if let Some(l) = last {
                            s.distance += (p.x - l.x).hypot(p.y - l.y);
                        }
                    });
                    last = Some(p);
                }
            }
        })
        // Strict, slow consumer: values must be 0, 1, 2, ... A gap fails the sink.
        .state("sequenceTotal", &sequence_total)
        .try_sink("sequence", move |a: SequenceArgs, mut values: SinkStream<u32>| {
            let total = sequence_total.clone();
            async move {
                let mut expected = 0u32;
                while let Some(v) = values.next().await {
                    if v != expected {
                        return Err(format!("out of order: expected {expected}, got {v}"));
                    }
                    expected += 1;
                    if a.delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(a.delay_ms)).await;
                    }
                }
                total.set(u64::from(expected));
                Ok(())
            }
        });
    handles
}

/// App state that Rust code mutates; the UI just observes it.
struct AppState {
    clicks: ReactiveState<u64>,
}

/// Simulates some Rust-side event (e.g. a file watcher or device) by bumping a counter.
#[tauri::command]
fn click(state: tauri::State<'_, AppState>) {
    state.clicks.update(|n| *n += 1);
}

#[tauri::command]
fn active_subscriptions(app: AppHandle) -> usize {
    app.reactive().active_subscriptions()
}

#[tauri::command]
fn active_sinks(app: AppHandle) -> usize {
    app.reactive().active_sinks()
}

#[tauri::command]
fn is_self_test() -> bool {
    std::env::var_os("REACTIVE_SELF_TEST").is_some()
}

#[tauri::command]
fn self_test_report(app: AppHandle, passed: bool, report: String) {
    println!("{report}");
    println!("SELF-TEST {}", if passed { "PASSED" } else { "FAILED" });
    app.exit(if passed { 0 } else { 1 });
}

pub fn run(context: tauri::Context) {
    tauri::Builder::default()
        .plugin(tauri_plugin_reactive::init())
        .invoke_handler(tauri::generate_handler![click, active_subscriptions, active_sinks, is_self_test, self_test_report])
        .setup(|app| {
            let Handles { clicks, uptime, live_subscriptions } = register(app.reactive());

            // Keep the frontend's types in sync with what Rust registers while developing.
            #[cfg(debug_assertions)]
            match app.reactive().export_typescript(BINDINGS_PATH) {
                Ok(true) => println!("[reactive] regenerated {BINDINGS_PATH}"),
                Ok(false) => {}
                Err(e) => eprintln!("[reactive] failed to write TypeScript bindings: {e}"),
            }

            app.manage(AppState { clicks });

            // Background work that drives state from Rust.
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(250));
                let mut ticks = 0u64;
                loop {
                    interval.tick().await;
                    ticks += 1;
                    if ticks.is_multiple_of(4) {
                        uptime.update(|s| *s += 1);
                    }
                    live_subscriptions.set_if_changed(handle.reactive().active_subscriptions());
                }
            });
            Ok(())
        })
        .run(context)
        .expect("error while running tauri application");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fails (after regenerating the file) if the committed bindings don't match the Rust sources.
    #[test]
    fn typescript_bindings_are_up_to_date() {
        let reactive = Reactive::default();
        register(&reactive);
        let regenerated = reactive.export_typescript(BINDINGS_PATH).expect("write bindings");
        assert!(!regenerated, "{BINDINGS_PATH} was stale and has been regenerated; commit the result");
    }
}
