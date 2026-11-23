#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

// The demo backend is shared by all framework examples: see examples/shared/backend.
fn main() {
    reactive_example_backend::run(tauri::generate_context!());
}
