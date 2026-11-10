//! The server's way out: what a stop asks of the requests that are under way.
//!
//! A server that is told to stop (SIGTERM from `systemctl stop`, which every deploy sends)
//! takes no new connections. What it does with the requests it is working on depends on what
//! they are:
//!
//! - A request that waits for news (`/sync`, sliding sync) has nothing to finish. It is
//!   answered at once with what it has, which is nothing, and the client asks the next
//!   server.
//! - A request that writes is a row of steps that belong together (see `see_writes_through`
//!   in the API crate). It is let finish, also when its client is gone, within a time limit.
//!
//! [`Closing`] is how the two sides find each other: the binary says that the stop has begun
//! and waits for the writes; the waiters listen for the first, and each write is counted from
//! start to end.

use std::sync::Arc;

use tokio::sync::watch;

/// Shared between the binary, which begins the stop, and the requests it concerns.
#[derive(Clone, Debug)]
pub struct Closing {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    begun: watch::Sender<bool>,
    writes: watch::Sender<usize>,
}

impl Default for Closing {
    fn default() -> Self {
        Self::new()
    }
}

impl Closing {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                begun: watch::Sender::new(false),
                writes: watch::Sender::new(0),
            }),
        }
    }

    /// The server is stopping. Everything that waits in [`Closing::begun`] goes on.
    pub fn begin(&self) {
        self.inner.begun.send_replace(true);
    }

    pub fn has_begun(&self) -> bool {
        *self.inner.begun.borrow()
    }

    /// Returns when the stop begins; at once when it already has.
    pub async fn begun(&self) {
        let mut rx = self.inner.begun.subscribe();
        // The sender lives in `self`, so this cannot fail while we wait.
        let _ = rx.wait_for(|begun| *begun).await;
    }

    /// Counts a request that writes for as long as the returned value lives.
    #[must_use = "the write is counted until this is dropped"]
    pub fn write(&self) -> Write {
        self.inner.writes.send_modify(|n| *n += 1);
        Write {
            inner: Arc::clone(&self.inner),
        }
    }

    /// How many requests that write are under way.
    pub fn writes(&self) -> usize {
        *self.inner.writes.borrow()
    }

    /// Returns when no request that writes is under way.
    pub async fn writes_done(&self) {
        let mut rx = self.inner.writes.subscribe();
        let _ = rx.wait_for(|n| *n == 0).await;
    }
}

/// A request that writes, counted until this is dropped. Dropping is what counts it out, so
/// a request that panics or is cancelled is counted out as well.
#[derive(Debug)]
pub struct Write {
    inner: Arc<Inner>,
}

impl Drop for Write {
    fn drop(&mut self) {
        self.inner.writes.send_modify(|n| *n -= 1);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const SHORT: Duration = Duration::from_millis(50);

    #[tokio::test]
    async fn waiters_go_on_when_the_stop_begins_and_afterwards_at_once() {
        let closing = Closing::new();
        assert!(
            tokio::time::timeout(SHORT, closing.begun()).await.is_err(),
            "nothing has begun"
        );
        let waiter = tokio::spawn({
            let closing = closing.clone();
            async move { closing.begun().await }
        });
        tokio::time::sleep(SHORT).await;
        assert!(!waiter.is_finished());
        closing.begin();
        tokio::time::timeout(SHORT, waiter).await.unwrap().unwrap();
        assert!(closing.has_begun());
        tokio::time::timeout(SHORT, closing.begun())
            .await
            .expect("one that comes late does not wait");
    }

    #[tokio::test]
    async fn the_writes_are_done_when_the_last_one_is_dropped() {
        let closing = Closing::new();
        tokio::time::timeout(SHORT, closing.writes_done())
            .await
            .expect("no writes: done");
        let first = closing.write();
        let second = closing.write();
        assert_eq!(closing.writes(), 2);
        drop(first);
        assert!(
            tokio::time::timeout(SHORT, closing.writes_done())
                .await
                .is_err(),
            "one is still under way"
        );
        drop(second);
        tokio::time::timeout(SHORT, closing.writes_done())
            .await
            .expect("done");
    }
}
