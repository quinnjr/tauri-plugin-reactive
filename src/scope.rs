use serde::Deserialize;
use tauri::ipc::{CommandScope, GlobalScope};

use crate::{Error, Result};

/// A capability scope entry naming the sources a window may use, e.g.
/// `{ "identifier": "reactive:allow-set", "allow": [{ "source": "settings" }] }`.
///
/// `source` is a glob where `*` matches any run of characters (`"*"`, `"settings.*"`, `"*Stats"`).
/// A source is accessible only if an allow entry matches it and no deny entry does. Scopes come
/// from the command's own permission (per-command, resolved per capability/window — this is what
/// `reactive:default` uses) and from scope-only permissions (global). Note that Tauri applies global
/// scopes app-wide, to every window, so use them only for rules meant for the whole app.
#[derive(Debug, Clone, Deserialize)]
pub struct SourceScope {
    pub source: String,
}

pub(crate) fn glob_match(pattern: &str, name: &str) -> bool {
    let (p, n) = (pattern.as_bytes(), name.as_bytes());
    let (mut pi, mut ni) = (0, 0);
    let mut backtrack: Option<(usize, usize)> = None;
    while ni < n.len() {
        if pi < p.len() && p[pi] == b'*' {
            backtrack = Some((pi, ni));
            pi += 1;
        } else if pi < p.len() && p[pi] == n[ni] {
            pi += 1;
            ni += 1;
        } else if let Some((star, matched)) = backtrack {
            pi = star + 1;
            ni = matched + 1;
            backtrack = Some((star, matched + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|&c| c == b'*')
}

pub(crate) fn is_allowed<'a>(
    name: &str,
    allows: impl IntoIterator<Item = &'a SourceScope>,
    denies: impl IntoIterator<Item = &'a SourceScope>,
) -> bool {
    allows.into_iter().any(|s| glob_match(&s.source, name)) && !denies.into_iter().any(|s| glob_match(&s.source, name))
}

/// The scopes that apply to one command invocation.
pub(crate) struct Scopes {
    pub command: CommandScope<SourceScope>,
    pub global: GlobalScope<SourceScope>,
}

impl Scopes {
    pub fn check(&self, name: &str) -> Result<()> {
        let allows = self.command.allows().iter().chain(self.global.allows()).map(|s| &**s);
        let denies = self.command.denies().iter().chain(self.global.denies()).map(|s| &**s);
        if is_allowed(name, allows, denies) {
            Ok(())
        } else {
            Err(Error::Forbidden(name.into()))
        }
    }

    /// For commands acting on an already-open subscription/sink (`unsubscribe`, `sink_push`,
    /// `sink_close`): its source passed the full check when it was opened, so only deny scopes
    /// apply here. Requiring an allow entry as well would break capabilities that scope only the
    /// opening commands and grant these three plainly.
    pub fn check_not_denied(&self, name: &str) -> Result<()> {
        let mut denies = self.command.denies().iter().chain(self.global.denies());
        if denies.any(|s| glob_match(&s.source, name)) {
            Err(Error::Forbidden(name.into()))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(source: &str) -> SourceScope {
        SourceScope { source: source.into() }
    }

    #[test]
    fn globs() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
        assert!(glob_match("settings", "settings"));
        assert!(!glob_match("settings", "settings2"));
        assert!(glob_match("settings.*", "settings.audio"));
        assert!(!glob_match("settings.*", "settings"));
        assert!(glob_match("*Stats", "pointerStats"));
        assert!(glob_match("a*b*c", "aXXbYYc"));
        assert!(!glob_match("a*b*c", "aXXbYY"));
        assert!(glob_match("a**", "a"));
    }

    #[test]
    fn allow_and_deny() {
        // Nothing allowed => nothing accessible.
        assert!(!is_allowed("x", &[], &[]));
        assert!(is_allowed("x", &[s("*")], &[]));
        // Deny wins over allow.
        assert!(!is_allowed("adminToken", &[s("*")], &[s("admin*")]));
        assert!(is_allowed("clicks", &[s("*")], &[s("admin*")]));
        // Narrow allow list.
        assert!(is_allowed("settings", &[s("settings"), s("clicks")], &[]));
        assert!(!is_allowed("uptime", &[s("settings"), s("clicks")], &[]));
    }
}
