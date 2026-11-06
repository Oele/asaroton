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
//! A writer, for its part, does not return to its caller before its own row can be reported
//! ([`Streams::past`]): the caller wakes the syncs that wait for the row, and a sync that is
//! woken while an earlier write still holds the position back would find nothing and go
//! back to waiting, with nobody left to wake it. The wait is for the writes that were under
//! way when this one finished, which is a matter of milliseconds.
//!
//! All of this lives in the process: asaroton runs as one process per database.

use std::{collections::BTreeMap, sync::Mutex, time::Duration};

/// How long a finished write waits for earlier ones before it returns regardless. A write
/// that hangs must not hold up every other one for good.
const LONGEST_WAIT: Duration = Duration::from_secs(2);

/// The streams whose positions go into sync tokens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stream {
    Events,
    /// Account data and push rules, which share a sequence.
    AccountData,
    Receipts,
    ToDevice,
    DeviceLists,
    ThreadSubscriptions,
    ProfileUpdates,
}

const STREAMS: usize = 7;

#[derive(Debug, Default)]
struct State {
    /// The highest position that was reported to a reader.
    reported: i64,
    /// The highest number that a finished write was given.
    written: i64,
    /// The floors of the writes under way, with how many share each.
    floors: BTreeMap<i64, u32>,
}

/// See the module documentation.
#[derive(Debug, Default)]
pub(crate) struct Streams {
    states: [Mutex<State>; STREAMS],
    /// Told whenever a write ends.
    ended: tokio::sync::Notify,
}

impl Streams {
    fn state(&self, stream: Stream) -> std::sync::MutexGuard<'_, State> {
        self.states[stream as usize]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Tells the stream where it stands when the process starts: the highest position in the
    /// database. Without it the first write would take zero for its floor.
    pub(crate) fn seed(&self, stream: Stream, position: i64) {
        let mut state = self.state(stream);
        state.reported = state.reported.max(position);
    }

    /// Registers a write. To be called before the write takes its number from the sequence,
    /// and the returned value kept until the write is committed (or has failed).
    pub(crate) fn writing(&self, stream: Stream) -> Writing<'_> {
        let mut state = self.state(stream);
        // Both are numbers that were taken before this write takes its own.
        let floor = state.reported.max(state.written);
        *state.floors.entry(floor).or_default() += 1;
        Writing {
            streams: self,
            stream,
            floor,
            wrote: None,
        }
    }

    /// The position to report for a stream whose highest committed number is `committed`.
    /// `committed` has to have been read before this is called.
    pub(crate) fn settled(&self, stream: Stream, committed: i64) -> i64 {
        let mut state = self.state(stream);
        let settled = match state.floors.keys().next() {
            Some(lowest) => committed.min(*lowest),
            None => committed,
        };
        // What was reported once is not taken back. (It cannot lie above a floor either:
        // every write under way began at or above what had been reported by then.)
        state.reported = state.reported.max(settled);
        state.reported
    }

    /// Returns once `position`, which the caller has written and committed, can be reported:
    /// no write that began before it was done is still under way. (Not for ever: see
    /// `LONGEST_WAIT`.)
    pub(crate) async fn past(&self, stream: Stream, position: i64) {
        let deadline = tokio::time::Instant::now() + LONGEST_WAIT;
        loop {
            // Listening before looking, so that a write that ends in between is not missed.
            let ended = self.ended.notified();
            tokio::pin!(ended);
            ended.as_mut().enable();
            let held_back = self
                .state(stream)
                .floors
                .keys()
                .next()
                .is_some_and(|lowest| *lowest < position);
            if !held_back || tokio::time::timeout_at(deadline, ended).await.is_err() {
                return;
            }
        }
    }
}

/// A write that is under way (see [`Streams::writing`]). Dropping it ends the registration.
#[derive(Debug)]
pub(crate) struct Writing<'a> {
    streams: &'a Streams,
    stream: Stream,
    floor: i64,
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
        if let Some(count) = state.floors.get_mut(&self.floor) {
            *count -= 1;
            if *count == 0 {
                state.floors.remove(&self.floor);
            }
        }
        drop(state);
        self.streams.ended.notify_waiters();
    }
}

impl Writing<'_> {
    /// Ends the write, which was given `position` and is committed, and waits until that
    /// position can be reported (see [`Streams::past`]).
    pub(crate) async fn done(mut self, position: i64) {
        let (streams, stream) = (self.streams, self.stream);
        self.wrote(position);
        drop(self);
        streams.past(stream, position).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        first.wrote(11);
        drop(first);
        assert_eq!(streams.settled(Stream::Events, 12), 12);
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
        c.wrote(13);
        drop(c);
        assert_eq!(streams.settled(Stream::Events, 13), 13);
    }

    #[tokio::test]
    async fn a_finished_write_waits_for_the_ones_that_began_before_it_was_done() {
        let streams = std::sync::Arc::new(Streams::default());
        streams.seed(Stream::ToDevice, 10);
        // The first write will get 11. The second gets 12 and commits first.
        let mut first = streams.writing(Stream::ToDevice);
        let second = {
            let streams = std::sync::Arc::clone(&streams);
            tokio::spawn(async move {
                let second = streams.writing(Stream::ToDevice);
                second.done(12).await;
            })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !second.is_finished(),
            "the second write does not return while 11 is being written"
        );
        assert_eq!(streams.settled(Stream::ToDevice, 12), 10);
        first.wrote(11);
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("it returns when the first write is done")
            .unwrap();
        assert_eq!(streams.settled(Stream::ToDevice, 12), 12);
        // With nothing under way there is nothing to wait for.
        streams.past(Stream::ToDevice, 99).await;
    }

    #[test]
    fn a_reported_position_is_never_taken_back() {
        let streams = Streams::default();
        // A position that was written by something this process did not see (a write whose
        // request was dropped after its commit) is reported, and a write that starts next
        // takes it as its floor.
        assert_eq!(streams.settled(Stream::Receipts, 7), 7);
        let writing = streams.writing(Stream::Receipts);
        assert_eq!(streams.settled(Stream::Receipts, 7), 7);
        assert_eq!(streams.settled(Stream::Receipts, 9), 7);
        // A write that fails leaves no trace.
        drop(writing);
        assert_eq!(streams.settled(Stream::Receipts, 9), 9);
    }
}
