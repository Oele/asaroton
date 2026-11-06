//! Stream positions that are safe to hand out.
//!
//! Every stream (events, to-device messages, account data, receipts, ...) numbers its rows
//! from a sequence. A sync takes the highest number as "now" and gives it to the client as
//! the position to continue from. But a row gets its number when it is written, not when the
//! write is committed, and writes that do not concern the same room or user are not ordered
//! among each other. So a higher number can become visible before a lower one, and a sync
//! that looks in between moves its client past a row it never saw: an event that is missing
//! from a timeline for good, or a to-device message that the next sync deletes as delivered.
//!
//! This keeps "now" below every write that is under way. A write registers before it takes
//! its number, and notes as its floor the highest position that had been reported or written
//! by then; the number it is about to take lies above that. For as long as the write is
//! registered, the position reported for its stream does not go above its floor. Once every
//! write below a position is done, all the rows up to it can be seen, and it is reported.
//!
//! A reader has to learn the highest committed number first and ask here second
//! ([`Streams::settled`]). Rows up to the position it gets back are then all visible to
//! whatever it reads afterwards.
//!
//! Whoever wakes syncs for a row it has written has to wait until the row's position can be
//! reported ([`Streams::reportable`], [`Streams::past`]). A sync that is woken while an
//! earlier write still holds the position back finds nothing and goes back to waiting, and
//! nobody is left to wake it. The writer itself does not wait: it would hold up whoever
//! writes after it, for as long as the slowest write under way takes.
//!
//! A write that hangs does not hold everything back for ever: after `STALE` it is no longer
//! counted, and says so in the log. Its row may then be skipped, which is what this module
//! exists to prevent, but the alternative is that no sync on the server sees anything new.
//!
//! All of this lives in the process: asaroton runs as one process per database.

use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use crate::SyncStream;

/// How long a write may be under way before it stops holding its stream's position back.
const STALE: Duration = Duration::from_secs(30);

const STREAMS: usize = 7;

fn index(stream: SyncStream) -> usize {
    match stream {
        SyncStream::Events => 0,
        SyncStream::AccountData => 1,
        SyncStream::Receipts => 2,
        SyncStream::ToDevice => 3,
        SyncStream::DeviceLists => 4,
        SyncStream::ThreadSubscriptions => 5,
        SyncStream::ProfileUpdates => 6,
    }
}

#[derive(Debug, Default)]
struct State {
    /// The highest position that was reported to a reader.
    reported: i64,
    /// The highest number that a finished write was given.
    written: i64,
    /// The writes under way: their floor, a number to tell them apart, and when they began.
    under_way: BTreeMap<(i64, u64), Instant>,
    next: u64,
}

impl State {
    /// The lowest floor among the writes under way. A write that has been under way for too
    /// long is struck off first.
    fn lowest_floor(&mut self, stream: SyncStream) -> Option<i64> {
        while let Some((&key, began)) = self.under_way.iter().next() {
            if began.elapsed() < STALE {
                return Some(key.0);
            }
            self.under_way.remove(&key);
            tracing::warn!(
                ?stream,
                seconds = STALE.as_secs(),
                "a write has been under way for too long; sync no longer waits for it, and may miss what it writes"
            );
        }
        None
    }
}

/// See the module documentation.
#[derive(Debug, Default)]
pub(crate) struct Streams {
    states: [Mutex<State>; STREAMS],
    /// Told whenever a write ends.
    ended: tokio::sync::Notify,
}

impl Streams {
    fn state(&self, stream: SyncStream) -> std::sync::MutexGuard<'_, State> {
        self.states[index(stream)]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Tells the stream where it stands when the process starts. Without it the first write
    /// would take zero for its floor.
    pub(crate) fn seed(&self, stream: SyncStream, position: i64) {
        let mut state = self.state(stream);
        state.reported = state.reported.max(position);
    }

    /// Registers a write. To be called before the write takes its number from the sequence,
    /// and the returned value kept until the write is committed (or has failed).
    pub(crate) fn writing(&self, stream: SyncStream) -> Writing<'_> {
        let mut state = self.state(stream);
        // Both are numbers that were taken before this write takes its own.
        let floor = state.reported.max(state.written);
        let id = state.next;
        state.next += 1;
        state.under_way.insert((floor, id), Instant::now());
        Writing {
            streams: self,
            stream,
            key: (floor, id),
            wrote: None,
        }
    }

    /// The position to report for a stream whose highest committed number is `committed`.
    /// `committed` has to have been read before this is called.
    pub(crate) fn settled(&self, stream: SyncStream, committed: i64) -> i64 {
        let mut state = self.state(stream);
        let settled = match state.lowest_floor(stream) {
            Some(lowest) => committed.min(lowest),
            None => committed,
        };
        // What was reported once is not taken back. (It cannot lie above a floor either:
        // every write under way began at or above what had been reported by then.)
        state.reported = state.reported.max(settled);
        state.reported
    }

    /// Whether `position`, which the caller has written and committed, can be reported: no
    /// write that began before it was done is still under way.
    pub(crate) fn reportable(&self, stream: SyncStream, position: i64) -> bool {
        self.state(stream)
            .lowest_floor(stream)
            .is_none_or(|lowest| lowest >= position)
    }

    /// Returns once `position` can be reported (see [`Streams::reportable`]).
    pub(crate) async fn past(&self, stream: SyncStream, position: i64) {
        loop {
            // Listening before looking, so that a write that ends in between is not missed.
            let ended = self.ended.notified();
            tokio::pin!(ended);
            ended.as_mut().enable();
            if self.reportable(stream, position) {
                return;
            }
            // A write that hangs ends no wait; it is struck off when it is stale, which only
            // a look finds out.
            let _ = tokio::time::timeout(Duration::from_secs(1), ended).await;
        }
    }
}

/// A write that is under way (see [`Streams::writing`]). Dropping it ends the registration.
#[derive(Debug)]
pub(crate) struct Writing<'a> {
    streams: &'a Streams,
    stream: SyncStream,
    key: (i64, u64),
    wrote: Option<i64>,
}

impl Writing<'_> {
    /// Notes the number the write was given, once it is committed. Writes that begin after
    /// this one has ended start from there.
    pub(crate) fn wrote(&mut self, position: i64) {
        self.wrote = Some(self.wrote.map_or(position, |w| w.max(position)));
    }
}

impl Drop for Writing<'_> {
    fn drop(&mut self) {
        let mut state = self.streams.state(self.stream);
        if let Some(position) = self.wrote {
            state.written = state.written.max(position);
        }
        state.under_way.remove(&self.key);
        drop(state);
        self.streams.ended.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SyncStream as Stream;

    #[test]
    fn nothing_under_way_reports_what_is_committed() {
        let streams = Streams::default();
        streams.seed(Stream::Events, 10);
        assert_eq!(streams.settled(Stream::Events, 10), 10);
        assert_eq!(streams.settled(Stream::Events, 12), 12);
        // Other streams are their own.
        assert_eq!(streams.settled(Stream::ToDevice, 3), 3);
    }

    #[test]
    fn a_write_under_way_holds_the_position_below_its_number() {
        let streams = Streams::default();
        streams.seed(Stream::Events, 10);
        // Two writes take the numbers 11 and 12. The second commits first.
        let mut first = streams.writing(Stream::Events);
        let mut second = streams.writing(Stream::Events);
        second.wrote(12);
        drop(second);
        // 12 can be seen and 11 cannot: a reader must not be moved past 11.
        assert_eq!(streams.settled(Stream::Events, 12), 10);
        assert!(!streams.reportable(Stream::Events, 12));
        first.wrote(11);
        drop(first);
        assert_eq!(streams.settled(Stream::Events, 12), 12);
        assert!(streams.reportable(Stream::Events, 12));
    }

    #[test]
    fn the_position_follows_as_writes_finish_one_after_the_other() {
        let streams = Streams::default();
        streams.seed(Stream::Events, 10);
        // Writes that overlap without pause: each begins before the one before it ends.
        let mut a = streams.writing(Stream::Events);
        let mut b = streams.writing(Stream::Events);
        a.wrote(11);
        drop(a);
        // `b` began when 10 was the last position, so 11 is not reported yet.
        assert_eq!(streams.settled(Stream::Events, 11), 10);
        let mut c = streams.writing(Stream::Events);
        b.wrote(12);
        drop(b);
        // `c` began after 11 was written: it holds the position at 11, not at 10.
        assert_eq!(streams.settled(Stream::Events, 12), 11);
        // 11 can be reported now, 12 not yet: `c` began before 12 was written.
        assert!(streams.reportable(Stream::Events, 11));
        assert!(!streams.reportable(Stream::Events, 12));
        c.wrote(13);
        drop(c);
        assert_eq!(streams.settled(Stream::Events, 13), 13);
    }

    #[tokio::test]
    async fn whoever_waits_for_a_position_is_let_go_when_the_writes_before_it_are_done() {
        let streams = std::sync::Arc::new(Streams::default());
        streams.seed(Stream::ToDevice, 10);
        // The first write will get 11. The second gets 12 and commits first.
        let mut first = streams.writing(Stream::ToDevice);
        let mut second = streams.writing(Stream::ToDevice);
        second.wrote(12);
        drop(second);
        let waiting = {
            let streams = std::sync::Arc::clone(&streams);
            tokio::spawn(async move { streams.past(Stream::ToDevice, 12).await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !waiting.is_finished(),
            "12 cannot be reported while 11 is being written"
        );
        first.wrote(11);
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("the wait ends with the first write")
            .unwrap();
        // With nothing under way there is nothing to wait for.
        streams.past(Stream::ToDevice, 99).await;
    }

    #[test]
    fn a_reported_position_is_never_taken_back() {
        let streams = Streams::default();
        // A position that was written by something this process did not see is reported,
        // and a write that starts next takes it as its floor.
        assert_eq!(streams.settled(Stream::Receipts, 7), 7);
        let writing = streams.writing(Stream::Receipts);
        assert_eq!(streams.settled(Stream::Receipts, 7), 7);
        assert_eq!(streams.settled(Stream::Receipts, 9), 7);
        // A write that fails leaves no trace.
        drop(writing);
        assert_eq!(streams.settled(Stream::Receipts, 9), 9);
        // Rows that were deleted do not take the position back either.
        assert_eq!(streams.settled(Stream::Receipts, 2), 9);
    }

    #[test]
    fn a_write_that_hangs_is_struck_off() {
        let streams = Streams::default();
        streams.seed(Stream::Events, 10);
        let hanging = streams.writing(Stream::Events);
        // As if it had begun long ago.
        {
            let mut state = streams.state(Stream::Events);
            let key = *state.under_way.keys().next().unwrap();
            let long_ago = Instant::now().checked_sub(STALE + Duration::from_secs(1));
            state.under_way.insert(key, long_ago.unwrap());
        }
        assert_eq!(streams.settled(Stream::Events, 15), 15);
        // Its end, when it comes, is no trouble.
        drop(hanging);
        assert_eq!(streams.settled(Stream::Events, 16), 16);
    }
}
