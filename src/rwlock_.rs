use core::{
    cell::UnsafeCell,
    marker::PhantomData,
    mem::ManuallyDrop,
    pin::Pin,
};

use abs_sync::async_lock::{TrAsyncRwLock, TrAcquire};
use atomex::{
    CmpxchResult, PhantomAtomicPtr, StrictOrderings,
    TrAtomicData, TrAtomicFlags, TrCmpxchOrderings,
};
use pincol::x_deps::{abs_sync, atomex};

use super::{
    acquire_::Acquire,
    contexts_::{CtxType, WakeList},
};

/// An asynchronous reader-writer lock for `no_std` and without heap memory
/// allocation along with its usage.
#[repr(C)]
pub struct RwLock<T, O = StrictOrderings>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    lock_stat_: RwLockState<O>,
    wake_list_: WakeList<O>,
    data_cell_: UnsafeCell<T>,
}

impl<T, O> RwLock<T, O>
where
    T: Sized,
    O: TrCmpxchOrderings,
{
    /// Creates a new reader-writer lock.
    ///
    /// # Examples
    ///
    /// ```
    /// use atomex::StrictOrderings;
    /// use asyncex::{rwlock::RwLock, x_deps::atomex};
    ///
    /// let lock = RwLock::<usize, StrictOrderings>::new(42);
    /// assert!(!lock.is_acquired())
    /// ```
    pub const fn new(data: T) -> RwLock<T, O> {
        RwLock {
            lock_stat_: RwLockState::new(),
            wake_list_: WakeList::new(),
            data_cell_: UnsafeCell::new(data),
        }
    }

    /// Unwraps the lock and returns the inner value.
    ///
    /// # Examples
    ///
    /// ```
    /// use atomex::StrictOrderings;
    /// use asyncex::{rwlock::RwLock, x_deps::atomex};
    ///
    /// const ANSWER: usize = 5;
    /// let lock = RwLock::<usize, StrictOrderings>::new(ANSWER);
    /// assert_eq!(lock.into_inner(), 5);
    /// ```
    #[must_use]
    #[inline]
    pub fn into_inner(self) -> T {
        let mut m = ManuallyDrop::new(self);
        m.clear_wake_list_on_drop_();
        unsafe { m.data_cell_.get().read() }
    }
}

impl<T, O> RwLock<T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    /// Tests if the rwlock is acquired by any numbers of readers or a writer.
    ///
    /// # Examples
    ///
    /// ```
    /// use pin_utils::pin_mut;
    /// use atomex::StrictOrderings;
    /// use asyncex::{rwlock::RwLock, x_deps::{atomex, pin_utils}};
    ///
    /// let rwlock = RwLock::<(), StrictOrderings>::new(());
    /// assert!(!rwlock.is_acquired());
    ///
    /// let acq = rwlock.acquire();
    /// pin_mut!(acq);
    /// assert!(!rwlock.is_acquired());
    ///
    /// let reader = acq.try_read().unwrap();
    /// assert!(rwlock.is_acquired());
    /// drop(reader);
    /// assert!(!rwlock.is_acquired());
    /// ```
    pub fn is_acquired(&self) -> bool {
        self.lock_stat_.is_acquired()
    }

    /// Return the number of readers that currently hold the lock (including
    /// upgradable readers).
    ///
    /// # Safety
    ///
    /// This function provides no synchronization guarantees and so its result
    /// should be considered 'out of date' the instant it is called. Do not use
    /// it for synchronization purposes. However, it may be useful as a
    /// heuristic.
    #[inline(always)]
    pub fn reader_count(&self) -> usize {
        self.lock_stat_.reader_count()
    }

    /// Return the `Acquire` instance that prepares the context for acquiring
    /// the lock.
    #[inline(always)]
    pub fn acquire(&self) -> Acquire<'_, T, O> {
        Acquire::new(self)
    }

    pub(super) const fn state(&self) -> &RwLockState<O> {
        &self.lock_stat_
    }

    pub(super) const fn queue(&self) -> &WakeList<O> {
        &self.wake_list_
    }

    pub(super) const fn data_cell(&self) -> &UnsafeCell<T> {
        &self.data_cell_
    }

    pub(super) fn wake_next_(mut list: Pin<&mut WakeList<O>>) {
        let mut curr = list.as_mut().head_mut();
        loop {
            let Option::Some(ctx) = curr.current_pinned()
            else {
                debug_assert!(list.is_empty());
                let s = Self::state_ref_from_pin_wake_list_(list.as_ref());
                debug_assert!(s.is_queue_empty());
                let _ = s.try_set_queue_empty();
                return;
            };
            let ctx_type = ctx.context_type();
            #[cfg(test)]
            if matches!(ctx_type, CtxType::Uninit) {
                log::warn!("[RwLock::wake_next_] unexpected {ctx_type}");
                unreachable!();
            };
            if !ctx.try_signal() {
                return;
            }
            if matches!(ctx_type, CtxType::ReadOnly) {
                curr.move_next();
            }
        }
    }

    /// By declaration, `lock_stat_` is the 1st field and `wake_list_` is 2nd.
    /// 
    /// This function calculates the address of `lock_stat_` with a pin pointer
    /// to the `wake_list_` of `RwLock`.
    fn state_ref_from_pin_wake_list_(
        p: Pin<&WakeList<O>>,
    ) -> &RwLockState<O> {
        let offset = {
            let a = core::mem::offset_of!(Self, lock_stat_);
            let b = core::mem::offset_of!(Self, wake_list_);
            b - a
        };
        unsafe {
            let p = p.get_ref() as *const WakeList<O> as usize;
            let s = (p - offset) as *const RwLockState<O>;
            &*s
        }
    }

    fn clear_wake_list_on_drop_(&mut self) {
        let mut list = unsafe {
            Pin::new_unchecked(&mut self.wake_list_)
        };
        loop {
            let Option::Some(head) = list.as_mut().pop_head() else {
                break;
            };
            let _ = head.data().try_invalidate();
        }
    }
}

impl<T, O> Drop for RwLock<T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    fn drop(&mut self) {
        self.clear_wake_list_on_drop_();
    }
}

impl<T, O> TrAsyncRwLock for RwLock<T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type Target = T;

    #[inline(always)]
    fn acquire(&self) -> impl TrAcquire<'_, Self::Target> {
        RwLock::acquire(self)
    }
}

unsafe impl<T, O> Send for RwLock<T, O>
where
    T: Send + ?Sized,
    O: TrCmpxchOrderings,
{}

unsafe impl<T, O> Sync for RwLock<T, O>
where
    T: Send + Sync + ?Sized,
    O: TrCmpxchOrderings,
{}

/// RwLock State Value `usize`
pub(super) type RwStVal = usize;

/// RwLock State Cell `AtomicUsize`
type RwStCell = <RwStVal as TrAtomicData>::AtomicCell;

/// An atomic state to accelerate the information report of RwLock wait queue.
///
/// With the most significant 3 bits reserved for flags,
/// the rest bits are for non-queued readers count.
#[derive(Debug)]
pub(super) struct RwLockState<O: TrCmpxchOrderings>(
    RwStCell,
    PhantomAtomicPtr<O>,
);

const K_NOT_LOCKED: RwStVal = 0;
const K_QUEUE_NOT_EMPTY: RwStVal = 1 << (RwStVal::BITS - 1);

/// Indicating an `WriterGuard` is alive even though the queue is empty.
const K_WRITER_ACQUIRED: RwStVal = K_QUEUE_NOT_EMPTY >> 1;

/// Indicating an `UpgradableGuard` is alive even though the queue is empty.
const K_UPGRADE_ACTIVE: RwStVal = K_WRITER_ACQUIRED >> 1;

/// The max count of concurrent readers.
const K_MAX_READER_COUNT: RwStVal = K_UPGRADE_ACTIVE - 1;

impl<O: TrCmpxchOrderings> RwLockState<O> {
    pub const fn new() -> Self {
        RwLockState(RwStCell::new(K_NOT_LOCKED), PhantomData)
    }

    #[inline]
    pub const fn expect_lock_acquired(s: RwStVal) -> bool {
        s != K_NOT_LOCKED
    }
    #[allow(dead_code)]
    #[inline]
    pub const fn expect_lock_released(s: RwStVal) -> bool {
        s == K_NOT_LOCKED
    }

    // K_QUEUE_NOT_EMPTY
    #[inline]
    pub const fn expect_queue_empty(s: RwStVal) -> bool {
        !Self::expect_queue_not_empty(s)
    }
    #[inline]
    pub const fn expect_queue_not_empty(s: RwStVal) -> bool {
        s & K_QUEUE_NOT_EMPTY == K_QUEUE_NOT_EMPTY
    }
    #[inline]
    pub const fn desire_queue_empty(s: RwStVal) -> RwStVal {
        s & (!K_QUEUE_NOT_EMPTY)
    }
    #[inline]
    pub const fn desire_queue_not_empty(s: RwStVal) -> RwStVal {
        s | K_QUEUE_NOT_EMPTY
    }

    // K_WRITER_ACQUIRED
    #[inline]
    pub const fn expect_writer_acquired(s: RwStVal) -> bool {
        s & K_WRITER_ACQUIRED == K_WRITER_ACQUIRED
    }
    #[inline]
    pub const fn expect_writer_not_acquired(s: RwStVal) -> bool {
        !Self::expect_writer_acquired(s)
    }
    #[inline]
    pub const fn desire_writer_acquired(s: RwStVal) -> RwStVal {
        s | K_WRITER_ACQUIRED
    }
    #[inline]
    pub const fn desire_writer_not_acquired(s: RwStVal) -> RwStVal {
        s & (!K_WRITER_ACQUIRED)
    }
    #[allow(dead_code)]
    pub fn try_set_writer_acquired(&self, being: bool) -> bool {
        if being {
            self.try_spin_compare_exchange_weak(
                    Self::expect_writer_not_acquired,
                    Self::desire_writer_acquired)
                .is_succ()
        } else {
            self.try_spin_compare_exchange_weak(
                    Self::expect_writer_acquired,
                    Self::desire_writer_not_acquired)
                .is_succ()
        }
    }

    // K_UPGREAD_ACTIVE
    #[inline]
    pub const fn expect_upgread_active(s: RwStVal) -> bool {
        s & K_UPGRADE_ACTIVE == K_UPGRADE_ACTIVE
    }
    #[inline]
    pub const fn expect_upgrade_inactive(s: RwStVal) -> bool {
        !Self::expect_upgread_active(s)
    }
    pub const fn desire_upgrade_active(s: RwStVal) -> RwStVal {
        s | K_UPGRADE_ACTIVE
    }
    pub const fn desire_upgrade_inactive(s: RwStVal) -> RwStVal {
        s & (!K_UPGRADE_ACTIVE)
    }
    pub fn try_downgrade_upgradable_to_readonly(&self) -> bool {
        self.try_spin_compare_exchange_weak(
                Self::expect_upgread_active,
                Self::desire_upgrade_inactive)
            .is_succ()
    }

    // K_MAX_READER_COUNT
    #[inline]
    pub const fn get_reader_count(s: RwStVal) -> RwStVal {
        s & K_MAX_READER_COUNT
    }
    #[inline]
    pub const fn expect_inc_reader_count_valid(s: RwStVal) -> bool {
        Self::get_reader_count(s) < K_MAX_READER_COUNT
    }
    #[inline]
    pub const fn expect_dec_reader_count_valid(s: RwStVal) -> bool {
        Self::get_reader_count(s) > 0
    }
    #[inline]
    pub const fn desire_reader_count_incr(s: RwStVal) -> RwStVal {
        s + 1
    }
    #[inline]
    pub const fn desire_reader_count_decr(s: RwStVal) -> RwStVal {
        s - 1
    }

    #[inline]
    pub fn is_acquired(&self) -> bool {
        Self::expect_lock_acquired(self.value())
    }

    #[inline]
    pub fn is_queue_empty(&self) -> bool {
        Self::expect_queue_empty(self.value())
    }
    #[inline]
    pub fn try_set_queue_empty(&self) -> CmpxchResult<RwStVal> {
        self.try_spin_compare_exchange_weak(
            Self::expect_queue_not_empty,
            Self::desire_queue_empty)
    }
    #[inline]
    pub fn try_set_queue_not_empty(&self) -> CmpxchResult<RwStVal> {
        self.try_spin_compare_exchange_weak(
            Self::expect_queue_empty,
            Self::desire_queue_not_empty)
    }

    #[cfg(test)]
    #[inline]
    pub fn is_writer_acquired(&self) -> bool {
        Self::expect_writer_acquired(self.value())
    }

    #[inline]
    pub fn reader_count(&self) -> usize {
        Self::get_reader_count(self.value())
    }

    pub fn decrease_reader_count(&self) -> Result<usize, StateReport> {
        let r = self.try_spin_compare_exchange_weak(
            Self::expect_dec_reader_count_valid, 
            Self::desire_reader_count_decr,
        );
        match r.into() {
            Result::Ok(1usize) => Result::Err(StateReport::LastReader),
            Result::Err(0usize) => Result::Err(StateReport::ReaderCountOverflow),
            Result::Ok(c) => Result::Ok(c),
            _ => unreachable!("[RwLockState::decrease_reader_count]"),
        }
    }

    /// Note: Call this function only when an reader guard or upgradable reader
    /// guard is to returned to the caller.
    #[allow(dead_code)]
    pub fn increase_reader_count(&self) -> Result<usize, StateReport> {
        let r = self.try_spin_compare_exchange_weak(
            Self::expect_inc_reader_count_valid,
            Self::desire_reader_count_incr
        );
        match r.into() {
            Result::Err(K_MAX_READER_COUNT)
                => Result::Err(StateReport::ReaderCountOverflow),
            Result::Ok(c) => Result::Ok(c),
            _ => unreachable!("[RwLockState::increase_reader_count]"),
        }
    }
}

impl<O: TrCmpxchOrderings> AsRef<RwStCell> for RwLockState<O> {
    fn as_ref(&self) -> &RwStCell {
        &self.0
    }
}

impl<O: TrCmpxchOrderings> TrAtomicFlags<RwStVal, O> for RwLockState<O> {}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum StateReport {
    LastReader,
    ReaderCountOverflow,
}

#[cfg(test)]
mod tests_ {
    use core::ptr;
    use atomex::StrictOrderings;
    use pincol::x_deps::atomex;
    use super::*;

    fn state_ref_from_lock_smoke_<T, O>(x: &RwLock<T, O>)
    where
        T: ?Sized,
        O: TrCmpxchOrderings,
    {
        let a = &x.lock_stat_;
        let b = unsafe { Pin::new_unchecked(&x.wake_list_) };
        let c = RwLock::<T, O>::state_ref_from_pin_wake_list_(b);
        assert!(ptr::eq(a, c))
    }

    /// Verify if `RwLock::state_ref_from_pin_wake_list_` works as expected.
    #[test]
    fn state_ref_from_pin_wake_list_smoke() {
        let x = RwLock::<(), StrictOrderings>::new(());
        state_ref_from_lock_smoke_(&x);

        let x = std::boxed::Box::new(x);
        state_ref_from_lock_smoke_(&*x);
    }
}