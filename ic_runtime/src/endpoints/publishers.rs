use core::cell::UnsafeCell;
use core::mem;
use core::pin::Pin;
use core::sync::atomic::AtomicU32;
use core::sync::atomic::Ordering::Relaxed;
use core::task::{Context, Poll, Waker};

#[derive(Debug)]
enum PublisherState<T> {
    Unused,
    Acquired,
    Waiting(Waker),
    Ready(Option<T>),
}

impl<T> PublisherState<T> {
    #[inline]
    fn discriminant_eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

struct PublisherData<T> {
    next: AtomicU32,
    cycle: AtomicU32,
    state: UnsafeCell<PublisherState<T>>,
    guard: parking_lot::Mutex<()>,
}

unsafe impl<T: Send> Sync for PublisherData<T> {}

impl<T> PublisherData<T> {
    const fn new(next: u32) -> Self {
        Self {
            next: AtomicU32::new(next),
            cycle: AtomicU32::new(0),
            state: UnsafeCell::new(PublisherState::Unused),
            guard: parking_lot::Mutex::new(()),
        }
    }
}

/// Publisher is released on drop.
pub(crate) struct Publisher<'a, T> {
    pub(crate) id: u64,
    pub_data: &'a PublisherData<T>,
    publishers: &'a Publishers<T>,
}

impl<'a, T> Drop for Publisher<'a, T> {
    fn drop(&mut self) {
        self.publishers.release(self.id, self.pub_data);
    }
}

impl<'a, T> Publisher<'a, T> {
    #[inline(always)]
    const fn new(id: u64, pub_data: &'a PublisherData<T>, publishers: &'a Publishers<T>) -> Self {
        Self {
            id,
            pub_data,
            publishers,
        }
    }

    #[inline(always)]
    pub(crate) const fn wait(&'a self) -> PublishingFuture<'a, T> {
        PublishingFuture { publisher: self }
    }
}

pub(crate) struct PublishingFuture<'a, T> {
    publisher: &'a Publisher<'a, T>,
}

impl<'a, T> Future for PublishingFuture<'a, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _self = self.get_mut();

        let publisher = &_self.publisher.pub_data;

        // Safety: Access synchronized.
        let _access_lock = publisher.guard.lock();

        let state = unsafe { &mut *publisher.state.get() };

        match state {
            PublisherState::Acquired => {
                *state = PublisherState::Waiting(cx.waker().clone());
                Poll::Pending
            }
            PublisherState::Ready(value) => Poll::Ready(value.take()),
            PublisherState::Waiting(prev_waker) => {
                if !prev_waker.will_wake(cx.waker()) {
                    *prev_waker = cx.waker().clone();
                }
                Poll::Pending
            }
            _ => unreachable!("Polling publisher in an invalid state"),
        }
    }
}

/// A thread-safe component for tracking in-flight issues.
///
/// It blocks per-publisher only when releasing and publishing simultaneously.
pub(crate) struct Publishers<T> {
    // Practically, it is static, but const N will force full type annotation.
    publishers: Box<[PublisherData<T>]>,
    free: parking_lot::Mutex<u32>,
}

impl<T> Publishers<T> {
    const INVALID_INDEX: u32 = u32::MAX;

    pub(crate) fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0 && capacity <= u32::MAX as usize,
            "Capacity must be > 0 and <= u32::MAX"
        );

        let mut publishers = Vec::with_capacity(capacity);

        let cap_u32 = capacity as u32;

        for i in 0..cap_u32 {
            publishers.push(PublisherData::new(i + 1));
        }

        publishers[capacity - 1]
            .next
            .store(Self::INVALID_INDEX, Relaxed);

        Self {
            publishers: publishers.into_boxed_slice(),
            free: parking_lot::Mutex::new(0),
        }
    }

    const fn split(id: u64) -> (u32, u32) {
        (id as u32, (id >> 32) as u32)
    }

    const fn combine(index: u32, tag: u32) -> u64 {
        ((tag as u64) << 32) | index as u64
    }

    pub(crate) fn acquire(&self) -> Option<Publisher<'_, T>> {
        let mut current_free = self.free.lock();

        let current_index = *current_free;

        if current_index == Self::INVALID_INDEX {
            return None;
        }

        let current_pub = &self.publishers[current_index as usize];

        let acquired_state = unsafe { &mut *current_pub.state.get() };
        debug_assert!(acquired_state.discriminant_eq(&PublisherState::Unused));

        *acquired_state = PublisherState::Acquired;

        *current_free = current_pub.next.load(Relaxed);

        // Bind the current cycle to the index for cycle-detection when publishing.
        let acquired = Self::combine(current_index, current_pub.cycle.load(Relaxed));

        Some(Publisher::new(acquired, current_pub, self))
    }

    /// Tries to published the value to an identified publisher.
    ///
    /// If the ID is invalid, the value is dropped.
    pub(crate) fn publish(&self, id: u64, value: T) {
        let (index, prev_cycle) = Self::split(id);

        let index_usize = index as usize;

        // TODO: Maybe some error?
        // RT_ASSERT.
        if index_usize >= self.publishers.len() {
            return;
        }

        let publisher = &self.publishers[index_usize];

        // Safety: Access synchronized.
        let _access_lock = publisher.guard.lock();

        let current_cycle = publisher.cycle.load(Relaxed);

        if current_cycle == prev_cycle {
            let state = unsafe { &mut *publisher.state.get() };
            let current_state = mem::replace(state, PublisherState::Ready(Some(value)));
            if let PublisherState::Waiting(waker) = current_state {
                waker.wake();
            }
        }
    }

    /// Releases the publisher at the provided index by making it available for ownership.
    ///
    /// Publisher's cycle will be incremented and its state will be set to wait again.
    fn release(&self, id: u64, publisher: &PublisherData<T>) {
        // Safety: Access synchronized.
        let _value_lock = publisher.guard.lock();

        let (index, cycle) = Self::split(id);

        let new_cycle = cycle.wrapping_add(1);

        // Update cycle and reset state.
        publisher.cycle.store(new_cycle, Relaxed);

        let current_state = unsafe { &mut *publisher.state.get() };
        drop(mem::replace(current_state, PublisherState::Unused));

        // Update the free index.
        let mut current_free = self.free.lock();

        let next_free = *current_free;

        publisher.next.store(next_free, Relaxed);

        *current_free = index;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{sync::Arc, thread};

    fn debug_integrity<T>(publishers: &Publishers<T>) -> usize {
        let mut seen = vec![false; publishers.publishers.len()];

        let current_free = publishers.free.lock();
        let mut index = *current_free;

        let mut count = 0;

        while index != Publishers::<T>::INVALID_INDEX {
            if seen[index as usize] {
                panic!("Cycle detected");
            }

            seen[index as usize] = true;

            let publisher = &publishers.publishers[index as usize];

            let pub_state = unsafe { &*publisher.state.get() };

            if !pub_state.discriminant_eq(&PublisherState::Unused) {
                panic!("Reserved publisher detected");
            }

            count += 1;
            index = publisher.next.load(Relaxed);
        }

        count
    }

    #[test]
    fn test_publishers_acquire_drop() {
        let publishers = Publishers::<u8>::new(4);
        assert_eq!(self::debug_integrity(&publishers), 4);

        let publisher = publishers.acquire().expect("Must get free publisher");

        assert_eq!(self::debug_integrity(&publishers), 3);

        drop(publisher);

        assert_eq!(self::debug_integrity(&publishers), 4);
    }

    #[test]
    fn test_publishers_cycles_acquire_free() {
        let publishers = Arc::new(Publishers::<u8>::new(1000));
        let mut threads = Vec::with_capacity(8);

        for _ in 0..8 {
            let mu_clone = publishers.clone();
            threads.push(thread::spawn(move || {
                for _ in 0..1000 {
                    loop {
                        if let Some(_) = mu_clone.acquire() {
                            break;
                        }
                    }
                }
            }));
        }

        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(self::debug_integrity(&publishers), 1000);
    }
}
