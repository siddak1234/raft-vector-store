//! The event queue: future events ordered by time.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::SimTime;

/// Future events, earliest first. Events scheduled for the same time come out in the order
/// they were scheduled, which keeps runs deterministic.
pub(crate) struct EventQueue<E> {
    heap: BinaryHeap<Scheduled<E>>,
    next_seq: u64,
}

struct Scheduled<E> {
    time: SimTime,
    seq: u64,
    event: E,
}

impl<E> PartialEq for Scheduled<E> {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.seq == other.seq
    }
}

impl<E> Eq for Scheduled<E> {}

impl<E> PartialOrd for Scheduled<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> Ord for Scheduled<E> {
    /// `BinaryHeap` pops the largest item, so the order is reversed to pop the earliest.
    fn cmp(&self, other: &Self) -> Ordering {
        (other.time, other.seq).cmp(&(self.time, self.seq))
    }
}

impl<E> Default for EventQueue<E> {
    fn default() -> Self {
        EventQueue {
            heap: BinaryHeap::new(),
            next_seq: 0,
        }
    }
}

impl<E> EventQueue<E> {
    pub(crate) fn push(&mut self, time: SimTime, event: E) {
        self.heap.push(Scheduled {
            time,
            seq: self.next_seq,
            event,
        });
        self.next_seq += 1;
    }

    pub(crate) fn pop(&mut self) -> Option<(SimTime, E)> {
        self.heap.pop().map(|s| (s.time, s.event))
    }

    pub(crate) fn peek_time(&self) -> Option<SimTime> {
        self.heap.peek().map(|s| s.time)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::EventQueue;
    use crate::SimTime;

    fn at(ms: u64) -> SimTime {
        SimTime::from_duration(Duration::from_millis(ms))
    }

    #[test]
    fn earliest_first_then_insertion_order() {
        let mut queue = EventQueue::default();
        queue.push(at(5), "c");
        queue.push(at(1), "a");
        queue.push(at(5), "d");
        queue.push(at(3), "b");
        let order: Vec<&str> = std::iter::from_fn(|| queue.pop().map(|(_, e)| e)).collect();
        assert_eq!(order, ["a", "b", "c", "d"]);
    }
}
