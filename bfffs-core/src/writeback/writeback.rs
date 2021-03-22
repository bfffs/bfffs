// vim: tw=80
use cfg_if::cfg_if;
use futures::{
    Future,
    FutureExt,
    channel::oneshot,
    future
};
use serde_derive::Serialize;
#[cfg(test)]
use serde_derive::Deserialize;
use std::{
    cmp::Ordering,
    collections::VecDeque,
    mem,
    ops::Add,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicIsize, AtomicUsize, Ordering::*}
    }
};
#[cfg(debug_assertions)]
use std::convert::TryInto;
#[cfg(test)]
use std::fmt::Debug;

/// WriteBack cache credit
// Deliberately does not implement Clone.  We wouldn't want users to counterfeit
// credit!
// The inner value is equal to 2x the amount of credit, in bytes.
#[derive(Debug)]
#[derive(Serialize)]
#[cfg_attr(test, derive(Deserialize))]
pub struct Credit(AtomicUsize);

impl Credit {
    /// Like [`split`], but slower since it uses atomic instructions.  Does not
    /// require mutable access.
    pub fn atomic_split(&self, credit: usize) -> Credit {
        // Saturate the subtraction, because we don't want any credit to ever be
        // negative.
        let old = self.0.fetch_update(Relaxed, Relaxed, |old| {
            let new = old.saturating_sub(credit << 1usize);
            Some(new)
        }).unwrap();
        Credit(AtomicUsize::new((credit << 1).min(old)))
    }

    pub fn extend(&mut self, mut other: Credit) {
        *self.0.get_mut() += *other.0.get_mut();
        mem::forget(other);
    }

    /// Forge credit not associated with a WriteBack.
    ///
    /// For use in unit tests only!
    #[cfg(test)]
    pub fn forge(size: usize) -> Credit {
        Credit(AtomicUsize::new(size << 1usize))
    }

    /// Split this Credit into two nearly equal pieces
    pub fn halve(&mut self) -> Credit {
        let old = *self.0.get_mut();
        self.split(old >> 2)
    }

    #[cfg(debug_assertions)]
    pub fn is_null(&self) -> bool {
        self.0.load(Relaxed) == 0
    }

    /// Generate Credit for zero bytes.
    ///
    // This is basically like `Option::None`, but it can add and subtract with
    // real `Credit`.  Using this instead of `Option<Credit>` lets the `Tree`
    // be more generic.
    pub fn null() -> Self {
        Credit(AtomicUsize::new(0))
    }

    pub fn split(&mut self, credit: usize) -> Credit {
        // Saturate the subtraction, because we don't want any credit to ever be
        // negative.
        let old = *self.0.get_mut();
        let new = old.saturating_sub(credit << 1usize);
        self.0 = AtomicUsize::new(new);
        Credit(AtomicUsize::new(old - new))
    }

    /// Takes all of this `Credit`'s credit out into a new `Credit`.
    ///
    /// Analagous to `Option::take`.
    pub fn take(&mut self) -> Credit {
        let old = *self.0.get_mut();
        self.split(old >> 1)
    }
}

// Doesn't actually mutate Self, but needs mutable access to avoid atomic
// instructions.
impl Add<usize> for &mut Credit {
    type Output = usize;

    fn add(self, other: usize) -> Self::Output {
        (*self.0.get_mut() >> 1) + other
    }
}

impl Drop for Credit {
    fn drop(&mut self) {
        if *self.0.get_mut() != 0 && !std::thread::panicking() {
            // Dropping Credit amounts to leaking it.  To continue the monetary
            // analogy, this is like destroying currency.
            panic!("Can't drop credit! ({:?})", self.0);
        }
    }
}

impl PartialEq<usize> for Credit {
    /// Is this credit correct for `other` bytes' worth of data?
    fn eq(&self, other: &usize) -> bool {
        (self.0.load(Relaxed) >> 1) == *other
    }
}

impl PartialOrd<usize> for Credit {
    fn partial_cmp(&self, other: &usize) -> Option<Ordering> {
        Some((self.0.load(Relaxed) >> 1).cmp(other))
    }
}

#[derive(Debug)]
struct Sleeper {
    tx: oneshot::Sender::<()>,
    /// The amount of bytes the caller requested, left shifted by one bit.
    wants: isize
}

/// A WriteBack cache usage tracker.
///
/// It doesn't actually own the cached data; it just tracks how much there is.
#[derive(Debug)]
pub struct WriteBack {
    #[cfg(debug_assertions)]
    capacity: isize,
    // Use isize instead of usize because it might temporarily go negative due
    // to the Relaxed ordering in the atomic fetch_sub
    // The lowest bit is a flag meaning "there are non-zero sleepers"
    // The other bits are the supply of credit available.
    supply: AtomicIsize,
    sleepers: Mutex<VecDeque<Sleeper>>
}

impl WriteBack {
    /// Awaken as many sleepers as possible
    fn awaken(&self) {
        let mut guard = self.sleepers.lock().unwrap();
        loop {
            if let Some(sleeper) = guard.front() {
                let wants = sleeper.wants;
                if self.supply.fetch_sub(wants, Relaxed) < wants {
                    // Not enough supply to wake this sleeper
                    self.supply.fetch_add(wants, Relaxed);
                    break;
                } else {
                    let sleeper = guard.pop_front().unwrap();
                    let tx = sleeper.tx;
                    if tx.send(()).is_err() {
                        // The borrower abandoned his loan application
                        self.supply.fetch_add(wants, Relaxed);
                    }
                }
            } else {
                // Out of sleepers
                self.supply.fetch_and(!0x1, Release);
                break;
            }
        }
    }

    pub fn borrow(&self, size: usize)
        -> Pin<Box<dyn Future<Output=Credit> + Send>>
    {
        let wants = size << 1;
        debug_assert!(self.capacity >= wants.try_into().unwrap());
        let iwants = wants as isize;

        loop {
            // NB: To be strictly correct, we ought to use Acquire here in order
            // to pick up any prior stores.  Using Relaxed means that we might
            // overdraw the bank's credit supply.  And that's not so bad!
            // Slightly exceeding the credit limit is permissible.
            let old_supply = self.supply.fetch_sub(iwants, Relaxed);
            if old_supply < iwants || (old_supply & 0x1 == 0x1) {
                // We need to sleep if the supply is depleted.  For fairness'
                // sake, we must also sleep if there are already any other
                // sleepers, even if there is sufficient credit left for us.
                // Otherwise a large write could be delayed indefinitely by
                // smaller ones.
                let mut guard = self.sleepers.lock().unwrap();
                // NB: to be strictly correct, we ought to use Acquire here in
                // order to pick up any credit that's just been repayed.  But
                // since the total supply will always be much greater than the
                // maximum credit used by any one operation, the consequence of
                // using Relaxed here is just that we may fail to use all of the
                // memory we're allowed.  That's not so bad!  We won't sleep
                // unboundedly.
                let locked_supply = self.supply.fetch_add(iwants, Relaxed);
                if locked_supply != old_supply - iwants {
                    // Race! New credit became available before we locked the
                    // sleepers.  We must try again.
                    continue;
                } else {
                    let (tx, rx) = oneshot::channel::<()>();
                    let sleeper = Sleeper { tx, wants: iwants };
                    guard.push_back(sleeper);
                    self.supply.fetch_or(1, Release);
                    break rx.map(move |_| Credit(AtomicUsize::new(wants)))
                        .boxed();
                }
            } else {
                break future::ready(Credit(AtomicUsize::new(wants))).boxed();
            }
        }
    }

    /// Construct a nearly unlimited WriteBack cache.
    ///
    /// TODO: remove this method once the actual constructor is hooked up.
    pub fn limitless() -> Self {
        Self::with_capacity(isize::max_value() >> 1)
    }

    pub fn repay(&self, mut credit: Credit) {
        if *credit.0.get_mut() == 0 {
            // Short-circuit this common case
            return;
        }
        let iwants = *credit.0.get_mut() as isize;
        let old_supply = self.supply.fetch_add(iwants, Acquire);
        debug_assert!(self.capacity >= (old_supply & !0x1) + iwants);

        if old_supply & 0x1 == 0x1 {
            // There must be somebody to wake up
            self.awaken();
        }

        mem::forget(credit);
    }

    pub fn with_capacity(capacity: isize) -> Self {
        cfg_if! {
            if #[cfg(debug_assertions)] {
                WriteBack {
                    capacity: capacity << 1,
                    supply: AtomicIsize::new(capacity << 1),
                    sleepers: Default::default()
                }
            } else {
                WriteBack {
                    supply: AtomicIsize::new(capacity << 1),
                    sleepers: Default::default()
                }
            }
        }
    }
}

#[cfg(test)]
mod t {
use super::*;
use futures_test::task::noop_context;

/// A borrow must sleep, but abandon his loan application (drops his future)
/// before being awakened.  This might happen in production if we ever support
/// I/O cancellation.
#[test]
fn abandoned_loan_application() {
    let mut ctx = noop_context();
    let writeback = WriteBack::with_capacity(10);

    let credit0 = writeback.borrow(5).now_or_never().unwrap();
    let mut fut1 = Box::pin(writeback.borrow(6));
    assert!(fut1.as_mut().poll(&mut ctx).is_pending());

    drop(fut1);
    writeback.repay(credit0);
    assert_eq!(writeback.supply.load(Relaxed), 20);
}

/// Atomically split one Credit into two
#[test]
fn atomic_split() {
    let writeback = WriteBack::with_capacity(10);
    let credit0 = writeback.borrow(10).now_or_never().unwrap();
    let credit1 = credit0.atomic_split(1);
    assert_eq!(credit1.0.load(Relaxed), 2);
    writeback.repay(credit0);
    writeback.repay(credit1);
    assert_eq!(writeback.supply.load(Relaxed), writeback.capacity)
}

/// Insufficient credit is available for the requested atomic split
#[test]
fn atomic_split_saturate() {
    let writeback = WriteBack::with_capacity(10);
    let credit0 = writeback.borrow(10).now_or_never().unwrap();
    let credit1 = credit0.atomic_split(11);
    assert_eq!(credit1.0.load(Relaxed), 20);
    writeback.repay(credit0);
    writeback.repay(credit1);
    assert_eq!(writeback.supply.load(Relaxed), writeback.capacity)
}

/// Merge two Credits into one
#[test]
fn extend() {
    let writeback = WriteBack::with_capacity(10);
    let mut credit0 = writeback.borrow(5).now_or_never().unwrap();
    let credit1 = writeback.borrow(5).now_or_never().unwrap();
    credit0.extend(credit1);
    writeback.repay(credit0);
    assert_eq!(writeback.supply.load(Relaxed), writeback.capacity)
}

#[test]
fn halve_even() {
    let writeback = WriteBack::with_capacity(10);
    let mut credit0 = writeback.borrow(10).now_or_never().unwrap();
    let credit1 = credit0.halve();
    assert_eq!(credit0.0.load(Relaxed), 10);
    assert_eq!(credit1.0.load(Relaxed), 10);
    writeback.repay(credit0);
    writeback.repay(credit1);
}

#[test]
fn halve_odd() {
    let writeback = WriteBack::with_capacity(10);
    let mut credit0 = writeback.borrow(9).now_or_never().unwrap();
    let credit1 = credit0.halve();
    assert!((credit0.0.load(Relaxed) == 10 && credit1.0.load(Relaxed) == 8) ||
            (credit1.0.load(Relaxed) == 10 && credit0.0.load(Relaxed) == 8));
    writeback.repay(credit0);
    writeback.repay(credit1);
}

/// repay() tries to awaken a borrower, but doesn't have enough credit to awaken
/// the first one
#[test]
fn heavy_sleeper() {
    let mut ctx = noop_context();
    let writeback = WriteBack::with_capacity(10);

    let credit0 = writeback.borrow(5).now_or_never().unwrap();
    let credit1 = writeback.borrow(5).now_or_never().unwrap();
    let mut fut2 = Box::pin(writeback.borrow(6));
    assert!(fut2.as_mut().poll(&mut ctx).is_pending());
    writeback.repay(credit0);
    // Repaying credit0 is not sufficient to wake the sleeper
    assert!(fut2.as_mut().poll(&mut ctx).is_pending());
    // Repaying credit2 should be
    writeback.repay(credit1);
    let credit2 = fut2.now_or_never().unwrap();
    writeback.repay(credit2);
}

#[test]
fn immediately_available() {
    let writeback = WriteBack::limitless();
    let credit = writeback.borrow(1).now_or_never().unwrap();
    writeback.repay(credit);
}

#[test]
#[should_panic(expected="Can't drop credit! (20)")]
fn leak() {
    let writeback = WriteBack::with_capacity(10);
    let credit = writeback.borrow(10).now_or_never().unwrap();
    drop(credit);
}

/// Leaking a 0-valued Credit is explicitly allowed.
#[test]
fn leak_0() {
    let writeback = WriteBack::with_capacity(10);
    let mut credit1 = writeback.borrow(10).now_or_never().unwrap();
    let credit2 = credit1.split(10);
    drop(credit1);
    writeback.repay(credit2);
}

#[test]
fn not_enough_supply() {
    let mut ctx = noop_context();
    let writeback = WriteBack::with_capacity(10);

    let credit0 = writeback.borrow(5).now_or_never().unwrap();
    let mut fut1 = Box::pin(writeback.borrow(6));
    assert!(fut1.as_mut().poll(&mut ctx).is_pending());
    writeback.repay(credit0);
    let credit1 = fut1.now_or_never().unwrap();
    writeback.repay(credit1);
}

/// Nothing funny should happen if we borrow 0 bytes
#[test]
fn null_credit() {
    let writeback = WriteBack::with_capacity(10);

    let credit = writeback.borrow(0).now_or_never().unwrap();
    writeback.repay(credit);
}

/// A race condition: insufficient credit is initially available during borrow,
/// but becomes available before the borrower locks the sleepers mutex.
#[test]
fn repay_during_borrow_race() {
    let writeback = std::sync::Arc::new(WriteBack::with_capacity(10));
    let wb2 = writeback.clone();

    let credit0 = writeback.borrow(5).now_or_never().unwrap();

    // Lock sleepers to facilitate creating a race
    let guard = writeback.sleepers.lock().unwrap();

    let join_handle = std::thread::spawn(move || {
        let credit1 = wb2.borrow(6).now_or_never().unwrap();
        wb2.repay(credit1);
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    writeback.repay(credit0);
    drop(guard);
    join_handle.join().unwrap();
}

/// Split one Credit into two
#[test]
fn split() {
    let writeback = WriteBack::with_capacity(10);
    let mut credit0 = writeback.borrow(10).now_or_never().unwrap();
    let credit1 = credit0.split(1);
    assert_eq!(credit1.0.load(Relaxed), 2);
    writeback.repay(credit0);
    writeback.repay(credit1);
    assert_eq!(writeback.supply.load(Relaxed), writeback.capacity)
}

/// Insufficient credit is available for the requested split
#[test]
fn split_saturate() {
    let writeback = WriteBack::with_capacity(10);
    let mut credit0 = writeback.borrow(10).now_or_never().unwrap();
    let credit1 = credit0.split(11);
    assert_eq!(credit1.0.load(Relaxed), 20);
    writeback.repay(credit0);
    writeback.repay(credit1);
    assert_eq!(writeback.supply.load(Relaxed), writeback.capacity)
}

#[test]
fn take() {
    let writeback = WriteBack::with_capacity(10);
    let mut credit0 = writeback.borrow(10).now_or_never().unwrap();
    let credit1 = credit0.take();
    assert!(credit0.is_null());
    assert_eq!(credit1.0.load(Relaxed), 20);
    writeback.repay(credit1);
    writeback.repay(credit0);
}

/// As soon as one borrower must sleep, all borrowers must sleep, even those
/// whose credit needs are small enough to be satisfied.  That prevents any
/// borrower from sleeping indefinitely.
#[test]
fn strict_ordering() {
    let mut ctx = noop_context();
    let writeback = WriteBack::with_capacity(10);

    let credit0 = writeback.borrow(5).now_or_never().unwrap();
    let mut fut1 = Box::pin(writeback.borrow(6));
    assert!(fut1.as_mut().poll(&mut ctx).is_pending());
    let mut fut2 = Box::pin(writeback.borrow(1));
    assert!(fut2.as_mut().poll(&mut ctx).is_pending());
    writeback.repay(credit0);
}

}
