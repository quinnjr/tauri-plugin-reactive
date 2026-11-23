use std::sync::Arc;

use futures::Stream;
use tokio::sync::watch;

/// A value shared between Rust and every subscribed webview.
///
/// Cheap to clone (all clones share the same value). Updates are coalesced: a subscriber that
/// falls behind only sees the latest value, which keeps IPC traffic bounded for fast-changing state.
pub struct ReactiveState<T> {
    tx: Arc<watch::Sender<T>>,
}

impl<T> Clone for ReactiveState<T> {
    fn clone(&self) -> Self {
        Self { tx: Arc::clone(&self.tx) }
    }
}

impl<T: Default + Send + Sync + 'static> Default for ReactiveState<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Send + Sync + 'static> ReactiveState<T> {
    pub fn new(initial: T) -> Self {
        let (tx, _rx) = watch::channel(initial);
        Self { tx: Arc::new(tx) }
    }

    /// Returns a clone of the current value.
    pub fn get(&self) -> T
    where
        T: Clone,
    {
        self.tx.borrow().clone()
    }

    /// Reads the current value without cloning it.
    pub fn with<U>(&self, f: impl FnOnce(&T) -> U) -> U {
        f(&self.tx.borrow())
    }

    /// Replaces the value and notifies subscribers.
    pub fn set(&self, value: T) {
        self.tx.send_replace(value);
    }

    /// Replaces the value only if it differs, returning whether subscribers were notified.
    pub fn set_if_changed(&self, value: T) -> bool
    where
        T: PartialEq,
    {
        self.tx.send_if_modified(|current| {
            if *current == value {
                false
            } else {
                *current = value;
                true
            }
        })
    }

    /// Mutates the value in place and notifies subscribers.
    pub fn update(&self, f: impl FnOnce(&mut T)) {
        self.tx.send_modify(f);
    }

    /// Mutates the value in place; subscribers are notified only if `f` returns `true`.
    pub fn update_if(&self, f: impl FnOnce(&mut T) -> bool) -> bool {
        self.tx.send_if_modified(f)
    }

    /// A raw `watch` receiver, for Rust-side consumers.
    pub fn watch(&self) -> watch::Receiver<T> {
        self.tx.subscribe()
    }

    /// A stream that yields the current value immediately, then every subsequent change.
    pub fn stream(&self) -> impl Stream<Item = T> + Send + 'static
    where
        T: Clone,
    {
        futures::stream::unfold((self.tx.subscribe(), true), |(mut rx, first)| async move {
            if !first && rx.changed().await.is_err() {
                return None;
            }
            let value = rx.borrow_and_update().clone();
            Some((value, (rx, false)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn stream_yields_current_then_changes() {
        let state = ReactiveState::new(1);
        let mut stream = Box::pin(state.stream());
        assert_eq!(stream.next().await, Some(1));
        state.set(2);
        assert_eq!(stream.next().await, Some(2));
        state.update(|v| *v += 10);
        assert_eq!(stream.next().await, Some(12));
    }

    #[tokio::test]
    async fn stream_coalesces_rapid_updates() {
        let state = ReactiveState::new(0);
        let mut stream = Box::pin(state.stream());
        assert_eq!(stream.next().await, Some(0));
        for i in 1..=100 {
            state.set(i);
        }
        assert_eq!(stream.next().await, Some(100));
    }

    #[test]
    fn set_if_changed_skips_equal_values() {
        let state = ReactiveState::new("a".to_string());
        let mut rx = state.watch();
        rx.mark_unchanged();
        assert!(!state.set_if_changed("a".into()));
        assert!(!rx.has_changed().unwrap());
        assert!(state.set_if_changed("b".into()));
        assert!(rx.has_changed().unwrap());
    }
}
