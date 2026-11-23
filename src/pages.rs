//! Refuses subscriptions/sinks opened on behalf of a page that has already been replaced.
//!
//! On reload, the plugin tears down everything the webview owns when the new page starts loading.
//! But a `subscribe` / `sink_open` the old page sent just before can still be in progress, and
//! record its entry after that cleanup, leaving a subscription nobody will ever unsubscribe (IPC
//! channel sends keep succeeding after a reload). Two mechanisms close this:
//!
//! - **Generations.** Each reload bumps the webview's generation. A command takes the generation
//!   when it starts and re-checks it *after* recording its entry; if a reload happened in between,
//!   it removes the entry itself. Since cleanup runs after the bump, every interleaving ends with
//!   the entry removed: either cleanup sees it, or the re-check does.
//! - **Page tokens.** The JS client sends a random per-page token. Tokens seen before a reload are
//!   retired, so a late call from the old page that only *starts* after the reload is refused too.
//!   (The old page's very first call, if it starts after the reload, can't be told apart from the
//!   new page's first call and is admitted.)

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Mutex, MutexGuard},
};

use crate::{Error, Result};

/// How many retired tokens to remember per webview (one per past page load is plenty).
const RETIRED_LIMIT: usize = 32;

#[derive(Default)]
struct WebviewPages {
    window: String,
    generation: u64,
    /// The webview's window was destroyed. Kept (rather than removed) so a call that was already in
    /// flight is refused instead of recreating a fresh record; cleared when a page loads again in a
    /// webview with this label (a new window reusing it).
    closed: bool,
    current: HashSet<String>,
    retired: VecDeque<String>,
}

#[derive(Default)]
pub(crate) struct Pages(Mutex<HashMap<String, WebviewPages>>);

impl Pages {
    /// Admits a call from `webview` and returns the generation to re-check with [`Self::is_current`].
    /// Calls without a token (older clients) are only subject to the generation check.
    pub fn admit(&self, webview: &str, window: &str, token: Option<&str>) -> Result<u64> {
        let mut pages = self.lock();
        let page = pages.entry(webview.to_owned()).or_default();
        if page.closed {
            return Err(Error::StalePage);
        }
        page.window = window.to_owned();
        if let Some(token) = token {
            if page.retired.iter().any(|t| t == token) {
                return Err(Error::StalePage);
            }
            // Bounded: the official client sends one token per page. A frontend sending endless
            // distinct tokens must not grow memory (unrecorded tokens just aren't retired later).
            if page.current.len() < RETIRED_LIMIT {
                page.current.insert(token.to_owned());
            }
        }
        Ok(page.generation)
    }

    /// Whether no reload happened since [`Self::admit`] returned `generation`.
    pub fn is_current(&self, webview: &str, generation: u64) -> bool {
        self.lock().get(webview).is_some_and(|p| p.generation == generation)
    }

    /// The webview's page was replaced. Must run *before* its entries are torn down.
    pub fn retire(&self, webview: &str, window: &str) {
        let mut pages = self.lock();
        let page = pages.entry(webview.to_owned()).or_default();
        page.window = window.to_owned();
        page.generation += 1;
        page.closed = false; // a page is loading in this webview: it is live again
        let current = std::mem::take(&mut page.current);
        page.retired.extend(current);
        while page.retired.len() > RETIRED_LIMIT {
            page.retired.pop_front();
        }
    }

    /// The window was destroyed: close all of its webviews. In-flight calls fail either their
    /// `is_current` re-check (admitted before) or `admit` itself (arriving after).
    ///
    /// The webview named like the window (its default webview) is closed even if it has no record yet,
    /// so its very first call can't slip in after the destroy. Other webviews always have a record by
    /// the time they can send anything: their page load ran `retire` with the window label.
    pub fn close_window(&self, window: &str) {
        let mut pages = self.lock();
        pages.entry(window.to_owned()).or_default().window = window.to_owned();
        for page in pages.values_mut().filter(|p| p.window == window) {
            page.closed = true;
            page.generation += 1;
            // Retire (not drop) its tokens: a late call must stay refused even after a new window
            // reuses the label.
            let current = std::mem::take(&mut page.current);
            page.retired.extend(current);
            while page.retired.len() > RETIRED_LIMIT {
                page.retired.pop_front();
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, WebviewPages>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_page_tokens_are_refused_after_reload() {
        let pages = Pages::default();
        pages.admit("main", "main", Some("page-a")).unwrap();
        pages.admit("main", "main", None).unwrap();
        pages.retire("main", "main"); // reload
        assert!(matches!(pages.admit("main", "main", Some("page-a")), Err(Error::StalePage)));
        pages.admit("main", "main", Some("page-b")).unwrap();
        // Other webviews are unaffected.
        pages.admit("other", "other", Some("page-a")).unwrap();
        // A token first seen after the reload is admitted (can't be told apart from the new page).
        pages.admit("main", "main", Some("page-c")).unwrap();
        pages.retire("main", "main");
        assert!(matches!(pages.admit("main", "main", Some("page-b")), Err(Error::StalePage)));
        assert!(matches!(pages.admit("main", "main", Some("page-c")), Err(Error::StalePage)));
    }

    #[test]
    fn generation_detects_a_reload_during_a_call() {
        let pages = Pages::default();
        let g = pages.admit("main", "win", None).unwrap();
        assert!(pages.is_current("main", g));
        pages.retire("main", "main"); // reload lands while the call is still recording its entry
        assert!(!pages.is_current("main", g));
        let g2 = pages.admit("main", "win", Some("new-page")).unwrap();
        assert!(pages.is_current("main", g2));
    }

    #[test]
    fn close_window_closes_all_its_webviews() {
        let pages = Pages::default();
        let a = pages.admit("a", "win", None).unwrap();
        let b = pages.admit("b", "win", None).unwrap();
        let c = pages.admit("c", "other", None).unwrap();
        pages.close_window("win");
        // Admitted before the destroy: the re-check fails.
        assert!(!pages.is_current("a", a));
        assert!(!pages.is_current("b", b));
        assert!(pages.is_current("c", c));
        // Arriving after the destroy: refused, with or without a token.
        assert!(matches!(pages.admit("a", "win", None), Err(Error::StalePage)));
        assert!(matches!(pages.admit("b", "win", Some("t")), Err(Error::StalePage)));
        // A new window reusing the label loads a page first, which reopens it.
        pages.retire("a", "a");
        let a2 = pages.admit("a", "win", Some("new")).unwrap();
        assert!(pages.is_current("a", a2));
    }

    #[test]
    fn first_call_after_destroy_is_refused() {
        // A webview whose only record so far came from its first page load...
        let pages = Pages::default();
        pages.retire("w", "w");
        pages.close_window("w");
        assert!(matches!(pages.admit("w", "w", Some("t")), Err(Error::StalePage)));
        // ...and one with no record at all (destroyed before anything was recorded).
        pages.close_window("fresh");
        assert!(matches!(pages.admit("fresh", "fresh", None), Err(Error::StalePage)));
        // A child webview of the window, recorded at its page load, is closed too.
        pages.retire("child", "w2");
        pages.close_window("w2");
        assert!(matches!(pages.admit("child", "w2", None), Err(Error::StalePage)));
    }

    #[test]
    fn tokens_stay_refused_after_label_reuse() {
        let pages = Pages::default();
        pages.admit("w", "w", Some("old")).unwrap();
        pages.close_window("w");
        pages.retire("w", "w"); // a new window reusing the label loads its page
        assert!(matches!(pages.admit("w", "w", Some("old")), Err(Error::StalePage)));
        pages.admit("w", "w", Some("new")).unwrap();
    }

    #[test]
    fn retired_tokens_are_bounded() {
        let pages = Pages::default();
        for i in 0..100 {
            pages.admit("w", "w", Some(&format!("t{i}"))).unwrap();
            pages.retire("w", "w");
        }
        assert_eq!(pages.lock()["w"].retired.len(), RETIRED_LIMIT);
    }
}
