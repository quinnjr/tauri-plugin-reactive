const COMMANDS: &[&str] = &["subscribe", "unsubscribe", "get", "set", "sink_open", "sink_push", "sink_close", "sink_abort"];

/// Mirrors `src/scope.rs::SourceScope`, so capability files get validation and autocomplete for
/// `{ "identifier": "reactive:...", "allow": [{ "source": "..." }] }`.
#[allow(dead_code)]
#[derive(schemars::JsonSchema)]
struct SourceScope {
    /// Name of a registered reactive source. `*` matches any run of characters, e.g. `"settings.*"`.
    source: String,
}

fn main() {
    tauri_plugin::Builder::new(COMMANDS)
        .global_scope_schema(schemars::schema_for!(SourceScope))
        .build();
}
