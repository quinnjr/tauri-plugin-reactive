//! Typed reactive state and streams across Tauri's Rust <-> JS boundary.
//!
//! Register named sources on the Rust side; subscribe to them from the frontend with
//! `tauri-plugin-reactive-api`. Subscriptions are lazy, shared per source on the JS side, and are
//! torn down automatically when the frontend unsubscribes, the page reloads, or the window closes.
//!
//! ```no_run
//! use tauri_plugin_reactive::{ReactiveExt, ReactiveState};
//! use futures::StreamExt;
//!
//! tauri::Builder::default()
//!     .plugin(tauri_plugin_reactive::init())
//!     .setup(|app| {
//!         let counter = ReactiveState::new(0i64);
//!         app.reactive()
//!             .writable_state("counter", &counter)
//!             .stream("ticks", |interval_ms: u64| {
//!                 futures::stream::unfold(0u64, move |n| async move {
//!                     tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
//!                     Some((n, n + 1))
//!                 })
//!             });
//!         // Any Rust code holding `counter` can now drive the UI:
//!         counter.update(|n| *n += 1);
//!         Ok(())
//!     });
//! ```

mod commands;
mod error;
mod pages;
mod registry;
mod scope;
mod sinks;
mod state;
mod subscriptions;
mod typegen;

use tauri::{
    plugin::{Builder, TauriPlugin},
    webview::PageLoadEvent,
    Manager, RunEvent, Runtime, WindowEvent,
};

pub use error::{Error, Result};
pub use registry::Reactive;
pub use scope::SourceScope;
pub use sinks::SinkStream;
pub use state::ReactiveState;
pub use subscriptions::Event;
pub use typegen::MaybeTs;

/// Re-export of ts-rs (feature `typegen`). `#[derive(TS)]` expands to paths under `ts_rs`, so either
/// add `ts-rs = "12"` to your own dependencies (simplest), or point the derive at this re-export on
/// each type with `#[ts(crate = "tauri_plugin_reactive::ts_rs")]`.
#[cfg(feature = "typegen")]
pub use ts_rs::{self, TS};

/// Access the [`Reactive`] registry from an `App`, `AppHandle`, `Window` or `Webview`.
pub trait ReactiveExt<R: Runtime> {
    fn reactive(&self) -> &Reactive;
}

impl<R: Runtime, M: Manager<R>> ReactiveExt<R> for M {
    fn reactive(&self) -> &Reactive {
        self.state::<Reactive>().inner()
    }
}

/// Initializes the plugin. The frontend also needs the `reactive:default` permission in its capability.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new("reactive")
        .invoke_handler(tauri::generate_handler![
            commands::subscribe,
            commands::unsubscribe,
            commands::get,
            commands::set,
            commands::sink_open,
            commands::sink_push,
            commands::sink_close,
            commands::sink_abort
        ])
        .setup(|app, _api| {
            app.manage(Reactive::default());
            Ok(())
        })
        // A reload/navigation discards the page's JS, so any subscriptions it held are orphaned.
        .on_page_load(|webview, payload| {
            if payload.event() == PageLoadEvent::Started {
                if let Some(reactive) = webview.try_state::<Reactive>() {
                    reactive.release_webview(webview.label(), webview.window().label());
                }
            }
        })
        .on_event(|app, event| {
            if let RunEvent::WindowEvent { label, event: WindowEvent::Destroyed, .. } = event {
                if let Some(reactive) = app.try_state::<Reactive>() {
                    reactive.release_window(label);
                }
            }
        })
        .build()
}
