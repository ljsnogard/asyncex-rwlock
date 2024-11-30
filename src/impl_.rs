use core::{
    cell::UnsafeCell,
    marker::PhantomData,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut, Try},
    pin::Pin,
    ptr::{self, NonNull},
};

use abs_sync::{
    async_lock::{TrAsyncRwLock, TrAcquire},
    cancellation::TrIntoFutureMayCancel,
};
use atomex::{
    CmpxchResult, PhantomAtomicPtr, StrictOrderings,
    TrAtomicData, TrAtomicFlags, TrCmpxchOrderings,
};
use spmv_oneshot::x_deps::{abs_sync, atomex};

use super::{
    contexts_::{CtxType, Message, WaitCtx, WakeList, WakeListGuard, WakeSlot},
    reader_::{ReadAsync, ReaderGuard},
    upgrade_::{UpgradableReadAsync, UpgradableReaderGuard},
    writer_::{WriteAsync, WriterGuard},
};

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

    fn wake_next_(mut list: Pin<&mut WakeList<O>>) {
        let mut curr = list.as_mut().head_mut();
        loop {
            let Option::Some(ctx) = curr.current_pinned() else {
                debug_assert!(list.is_empty());
                let s = Self::state_ref_from_pin_wake_list_(list.as_ref());
                debug_assert!(s.is_queue_empty());
                let _ = s.try_set_queue_empty();
                return;
            };
            let ctx_type = ctx.context_type();
            if matches!(ctx_type, CtxType::Uninit) {
                unreachable!()
            };
            if !ctx.can_signal() {
                return;
            }
            let r = ctx.channel().send(Message::Ready).wait();
            if r.is_err() {
                unreachable!()
            }
            if matches!(ctx_type, CtxType::ReadOnly) {
                curr.move_next();
            }
        }
    }

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
            let _ = head.data().try_set_ctx_cancelled();
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

/// The isolated context for concurrent attempts of acquiring the rwlock.
///
/// 提供一个异步上下文的栈上存储空间。每一个 `Acquire` 实例能同时容纳不多于一个异步请求，即使
/// 是可并发的读锁请求，也必须使用不同的实例。
pub struct Acquire<'a, T, O>
where
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    src_lock_: &'a RwLock<T, O>,
    ctx_slot_: WakeSlot<O>,
}

impl<'a, T, O> Acquire<'a, T, O>
where
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    #[inline]
    pub const fn new(lock: &'a RwLock<T, O>) -> Self {
        Acquire {
            src_lock_: lock,
            ctx_slot_: WakeSlot::new(WaitCtx::new()),
        }
    }

    #[inline]
    pub const fn rwlock(&self) -> &RwLock<T, O> {
        self.src_lock_
    }

    #[inline]
    pub fn try_read(
        self: Pin<&mut Self>,
    ) -> Option<ReaderGuard<'a, '_, T, O>> {
        unsafe { self.get_unchecked_mut().try_read_() }
    }

    #[inline]
    pub fn try_write(
        self: Pin<&mut Self>,
    ) -> Option<WriterGuard<'a, '_, T, O>> {
        unsafe { self.get_unchecked_mut().try_write_() }
    }

    #[inline]
    pub fn try_upgradable_read(
        self: Pin<&mut Self>,
    ) -> Option<UpgradableReaderGuard<'a, '_, T, O>> {
        unsafe { self.get_unchecked_mut().try_upgradable_read_() }
    }

    pub fn read_async(
        mut self: Pin<&mut Self>,
    ) -> ReadAsync<'a, '_, T, O> {
        self.as_mut().init_slot_(CtxType::ReadOnly);
        ReadAsync::new(self)
    }

    pub fn write_async(
        mut self: Pin<&mut Self>,
    ) -> WriteAsync<'a, '_, T, O> {
        self.as_mut().init_slot_(CtxType::Exclusive);
        WriteAsync::new(self)
    }

    pub fn upgradable_read_async(
        mut self: Pin<&mut Self>,
    ) -> UpgradableReadAsync<'a, '_, T, O> {
        self.as_mut().init_slot_(CtxType::Upgradable);
        UpgradableReadAsync::new(self)
    }
}

impl<'a, T, O> Acquire<'a, T, O>
where
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    pub(super) fn slot_(&self) -> &WakeSlot<O> {
        &self.ctx_slot_
    }

    pub(super) fn slot_pinned_(
        self: Pin<&mut Self>,
    ) -> Pin<&mut WakeSlot<O>> {
        unsafe {
            let slot = &mut self.get_unchecked_mut().ctx_slot_;
            Pin::new_unchecked(slot)
        }
    }

    fn init_slot_(
        mut self: Pin<&mut Self>,
        ctx_type: CtxType,
    ) {
        let mut this_ptr = unsafe {
            NonNull::new_unchecked(self.as_mut().get_unchecked_mut())
        };
        let this = self.as_ref().get_ref();
        let _ = this.swap_slot_ctx_type(ctx_type);
        let slot = this.slot_();
        let Option::Some(q) = slot.attached_list() else {
            return;
        };
        let mutex = q.mutex();
        let slot = unsafe {
            let this_pin = Pin::new_unchecked(this_ptr.as_mut());
            this_pin.slot_pinned_()
        };
        let mut g = mutex.acquire().wait();
        let Option::Some(mut cursor) = (*g).as_mut().find(slot) else {
            unreachable!()
        };
        let x = cursor.try_detach();
        assert!(x)
    }

    pub(super) fn deref_impl(&self) -> &T {
        unsafe { &*self.src_lock_.data_cell_.get() }
    }

    pub(super) fn deref_mut_impl(self: Pin<&mut Self>) -> &mut T {
        unsafe {
            let this = self.get_unchecked_mut();
            &mut *this.src_lock_.data_cell_.get()
        }
    }

    /// Try to get a reader guard within the fast path (no queue operation)
    pub(super) fn try_read_(&mut self) -> Option<ReaderGuard<'a, '_, T, O>> {
        let expect = |s| 
            RwLockState::<O>::expect_inc_reader_count_valid(s) &&
            RwLockState::<O>::expect_writer_not_acquired(s) &&
            RwLockState::<O>::expect_queue_empty(s);
        let desire = RwLockState::<O>::desire_reader_count_incr;
        let r = self
            .rwlock()
            .state()
            .try_spin_compare_exchange_weak(expect, desire);
        if r.is_succ() {
            let _ = self.slot_().data().try_validate();
            let acquire = unsafe { Pin::new_unchecked(self) };
            Option::Some(ReaderGuard::new(acquire))
        } else {
            Option::None
        }
    }

    /// Try to get a writer guard within the fast path (no queue operation)
    pub(super) fn try_write_(&mut self) -> Option<WriterGuard<'a, '_, T, O>> {
        let expect = |s|
            RwLockState::<O>::get_reader_count(s) == 0 &&
            RwLockState::<O>::expect_upgrade_inactive(s) &&
            RwLockState::<O>::expect_writer_not_acquired(s) &&
            RwLockState::<O>::expect_queue_empty(s);
        let desire = RwLockState::<O>::desire_writer_acquired;
        let r = self
            .rwlock()
            .state()
            .try_spin_compare_exchange_weak(expect, desire);
        if r.is_succ() {
            let _ = self.slot_().data().try_validate();
            let acquire = unsafe { Pin::new_unchecked(self) };
            Option::Some(WriterGuard::from_acquire(acquire))
        } else {
            Option::None
        }
    }

    pub(super) fn try_upgradable_read_(
        &mut self,
    ) -> Option<UpgradableReaderGuard<'a, '_, T, O>> {
        let expect = |s|
            RwLockState::<O>::expect_inc_reader_count_valid(s) &&
            RwLockState::<O>::expect_writer_not_acquired(s) &&
            RwLockState::<O>::expect_upgrade_inactive(s) &&
            RwLockState::<O>::expect_queue_empty(s);
        let desire = |s| {
            let s1 = RwLockState::<O>::desire_upgrade_active(s);
            RwLockState::<O>::desire_reader_count_incr(s1)
        };
        let r = self
            .rwlock()
            .state()
            .try_spin_compare_exchange_weak(expect, desire);
        if r.is_succ() {
            let _ = self.slot_().data().try_validate();
            let acquire = unsafe { Pin::new_unchecked(self) };
            Option::Some(UpgradableReaderGuard::new(acquire))
        } else {
            Option::None
        }
    }

    pub(super) fn try_upgrade_(&mut self) -> Option<WriterGuard<'a, '_, T, O>> {
        let expect = |s|
            RwLockState::<O>::expect_upgread_active(s) &&
            RwLockState::<O>::get_reader_count(s) == 1;
        let desire = |s| {
            let s1 = RwLockState::<O>::desire_writer_acquired(s);
            let s2 = RwLockState::<O>::desire_upgrade_inactive(s1);
            RwLockState::<O>::desire_reader_count_decr(s2)
        };
        let r = self
            .rwlock()
            .state()
            .try_spin_compare_exchange_weak(expect, desire);
        if r.is_succ() {
            let _ = self.slot_().data().try_validate();
            let acquire = unsafe { Pin::new_unchecked(self) };
            Option::Some(WriterGuard::from_acquire(acquire))
        } else {
            Option::None
        }
    }

    pub(super) fn try_spin_update_rwstate_from_exclusive_to_readonly_(
        &self,
    ) -> CmpxchResult<RwStVal> {
        let expect = |s|
            RwLockState::<O>::expect_writer_acquired(s) &&
            RwLockState::<O>::expect_inc_reader_count_valid(s) &&
            RwLockState::<O>::expect_upgrade_inactive(s) &&
            RwLockState::<O>::expect_queue_empty(s);
        let desire = |s| {
            let s1 = RwLockState::<O>::desire_writer_not_acquired(s);
            RwLockState::<O>::desire_reader_count_incr(s1)
        };
        self.rwlock().state().try_spin_compare_exchange_weak(expect, desire)
    }

    pub(super) fn try_spin_update_rwstate_from_exclusive_to_upgradable_(
        &self,
    ) -> CmpxchResult<RwStVal> {
        let expect = |s|
            RwLockState::<O>::expect_writer_acquired(s) &&
            RwLockState::<O>::expect_inc_reader_count_valid(s) &&
            RwLockState::<O>::expect_upgrade_inactive(s) &&
            RwLockState::<O>::expect_queue_empty(s);
        let desire = |s| {
            let s1 = RwLockState::<O>::desire_writer_not_acquired(s);
            let s2 = RwLockState::<O>::desire_upgrade_active(s1);
            RwLockState::<O>::desire_reader_count_incr(s2)
        };
        self.rwlock().state().try_spin_compare_exchange_weak(expect, desire)
    }

    #[inline]
    pub(super) fn swap_slot_ctx_type(&self, ctx_type: CtxType) -> CtxType {
        self.slot_().data().swap_slot_ctx_type(ctx_type)
    }

    #[allow(dead_code)]
    #[inline]
    pub(super) fn try_init_slot_ctx_type_(&self, ctx_type: CtxType) -> bool {
        self.slot_().data().try_init_context_type(ctx_type)
    }

    #[allow(dead_code)]
    #[inline]
    pub(super) fn try_reset_slot_ctx_type_(&self) -> bool {
        self.slot_().data().try_reset_context_type()
    }

    pub(super) fn invalidate_slot_ctx_(slot: &WakeSlot<O>) {
        let ctx = slot.data();
        let x = ctx.try_invalidate();
        assert!(x.is_succ(), "[Acquire::invalidate_slot_ctx_] {ctx:?}");
    }

    pub(super) fn on_reader_guard_drop_(mut self: Pin<&mut Self>) {
        let slot = self.as_mut().slot_pinned_();
        if slot.attached_list().is_some() {
            Self::invalidate_slot_ctx_(slot.deref());
        }
        loop {
            // A reader slot may be detached by other reader once invalidated.
            // So we need to keep watching if current slot is already detached.
            let Option::Some(q) = slot.attached_list() else {
                break;
            };
            let mutex = q.mutex();
            let Option::Some(mut g) = mutex.try_acquire() else {
                continue;
            };
            loop {
                let mut head = (*g).as_mut().head_mut();
                let Option::Some(cx) = head.current_pinned() else {
                    return;
                };
                let ctx_type = cx.context_type();
                if !matches!(ctx_type, CtxType::ReadOnly) {
                    return;
                }
                if cx.is_invalidated() {
                    let r = head.try_detach();
                    debug_assert!(r); 
                    continue;
                } else {
                    RwLock::<T, O>::wake_next_((*g).as_mut());
                    return;
                }
            }
        };
        // Deal with the case that the slot is not enqueued.
        let r = self
            .rwlock()
            .state()
            .decrease_reader_count();
        let Result::Err(StateReport::LastReader) = r else {
            return;
        };
        let mutex = self.rwlock().queue().mutex();
        let mut g = mutex.acquire().wait();
        RwLock::<T, O>::wake_next_((*g).as_mut());
    }

    pub(super) fn on_writer_guard_drop_with_list_guard_<'g>(
        mut self: Pin<&mut Self>,
        g: &mut WakeListGuard<'a, 'g, O>,
    ) {
        let slot = self.as_mut().slot_pinned_();
        let q_pin = g.deref_mut();
        let front = q_pin.as_mut().head_mut();
        if let Option::Some(head) = front.pinned_slot() {
            if ptr::eq(slot.deref(), head.deref()) {
                #[cfg(test)]
                log::warn!("[Acquire::on_writer_guard_drop_with_list_guard_] not head");
                return
            }
            let head_ctx_type = head.data().context_type();
            if !matches!(head_ctx_type, CtxType::Exclusive) {
                return;
            }
        } else {
            #[cfg(test)]
            log::warn!("[Acquire::on_writer_guard_drop_with_list_guard_] empty head.");
            return;
        }
        if q_pin.as_mut().pop_head().is_none() {
            unreachable!("[Acquire::on_writer_guard_drop_attached_] pop head failed")
        };
        RwLock::<T, O>::wake_next_((*g).as_mut());
        Self::invalidate_slot_ctx_(slot.deref());
    }

    pub(super) fn on_writer_guard_drop_(mut self: Pin<&mut Self>) {
        let mut acq_ptr = unsafe {
            NonNull::new_unchecked(self.as_mut().get_unchecked_mut())
        };
        let slot = self.as_mut().slot_pinned_();
        if let Option::Some(q) = slot.attached_list() {
            let mutex = q.mutex();
            let mut g = mutex.acquire().wait();

            let acq_pin = unsafe {
                Pin::new_unchecked(acq_ptr.as_mut())
            };
            acq_pin.on_writer_guard_drop_with_list_guard_(&mut g);
        } else {
            // Deal with the case that the slot is not enqueued.
            let expect = RwLockState::<O>::expect_writer_acquired;
            let desire = RwLockState::<O>::desire_writer_not_acquired;
            let acq_pin = self.as_ref();
            let rwlock = acq_pin.get_ref().rwlock();
            let r = rwlock
                .state()
                .try_spin_compare_exchange_weak(expect, desire);
            debug_assert!(r.is_succ());

            let mutex = rwlock.queue().mutex();
            let mut g = mutex.acquire().wait();
            RwLock::<T, O>::wake_next_((*g).as_mut()); 
        }
    }

    pub(super) fn on_upgradable_guard_drop_(mut self: Pin<&mut Self>) {
        let mut slot = self.as_mut().slot_pinned_();
        let ctx = slot.as_mut().data_pinned();
        let x = ctx.try_invalidate();
        assert!(
            x.is_succ(),
            "[Acquire::on_upgradable_guard_drop_] try_invalidate {ctx:?}",
        );
        if let Option::Some(q) = slot.attached_list() {
            let mutex = q.mutex();
            let mut g = mutex.acquire().wait();
            let opt_head = (*g).as_mut().pop_head();
            let Option::Some(head) = opt_head else {
                unreachable!("[Acquire::on_upgradable_guard_drop_]")
            };
            assert!(
                ptr::eq(head.deref(), slot.deref()),
                "[Acquire::on_upgradable_guard_drop_] head eq",
            );
            RwLock::<T, O>::wake_next_((*g).as_mut());
        } else {
            // Deal with the case that the slot is not enqueued.
            let expect = |s| {
                RwLockState::<O>::expect_upgread_active(s)
                && RwLockState::<O>::expect_dec_reader_count_valid(s)
            };
            let desire = |s| {
                let s1 = RwLockState::<O>::desire_upgrade_inactive(s);
                RwLockState::<O>::desire_reader_count_decr(s1)
            };
            let r = self
                .rwlock()
                .state()
                .try_spin_compare_exchange_weak(expect, desire);
            debug_assert!(r.is_succ());

            let mutex = self.rwlock().queue().mutex();
            let mut g = mutex.acquire().wait();
            RwLock::<T, O>::wake_next_((*g).as_mut());
        }
    }

    /// Update the context within the instance of `Acquire`, changing from
    /// writer to reader.
    pub(super) fn downgrade_writer_to_reader_(self: Pin<&mut Self>) {
        let mut acq = unsafe {
            NonNull::new_unchecked(self.get_unchecked_mut())
        };
        let mut acq_pin = unsafe { Pin::new_unchecked(acq.as_mut()) };
        let slot = acq_pin.as_mut().slot_pinned_();
        if let Option::Some(list) = slot.attached_list() {
            debug_assert!({
                let mutex = list.mutex();
                let mut g = mutex.acquire().wait();
                let head = (*g).as_mut().head_mut();
                let head_slot = head.pinned_slot().unwrap();
                ptr::eq(head_slot.get_ref(), slot.as_ref().get_ref())
            });
            let head_cx = slot.data();
            let r = head_cx.try_downgrade_exclusive_to_readonly();
            debug_assert!(r);
        } else {
            let mutex = unsafe { acq.as_ref().rwlock().queue().mutex() };
            let mut g = mutex.acquire().wait();
            let r = (*g).as_mut().push_head(slot);
            debug_assert!(r.is_ok());
            let mut acq_pin = unsafe { Pin::new_unchecked(acq.as_mut()) };
            let r = acq_pin
                .as_mut()
                .try_spin_update_rwstate_from_exclusive_to_readonly_();
            assert!(r.is_succ());
            acq_pin.on_writer_guard_drop_with_list_guard_(&mut g)
        };
    }

    pub(super) fn downgrade_writer_to_upgradable_(self: Pin<&mut Self>) {
        let mut acq = unsafe {
            NonNull::new_unchecked(self.get_unchecked_mut())
        };
        let mut acq_pin = unsafe { Pin::new_unchecked(acq.as_mut()) };
        let slot = acq_pin.as_mut().slot_pinned_();
        if let Option::Some(list) = slot.attached_list() {
            // If the writer context has enqueued, it is by-design that the
            // the head slot of the queue is the writer context of `self`
            debug_assert!({
                let mutex = list.mutex();
                let mut g = mutex.acquire().wait();
                let head = (*g).as_mut().head_mut();
                let head_slot = head.pinned_slot().unwrap();
                ptr::eq(head_slot.get_ref(), slot.as_ref().get_ref())
            });
            let head_cx = slot.data();
            let r = head_cx.try_downgrade_exclusive_to_upgradable();
            debug_assert!(r);
            return;
        };
        // If the writer context has not yet enqueued, while other contenders 
        // may be trying to acquire the rwlock, we first try not to enqueue
        // another context.
        let acq_pin = unsafe { Pin::new_unchecked(acq.as_mut()) };
        let try_st = acq_pin.try_spin_update_rwstate_from_exclusive_to_upgradable_();
        let CmpxchResult::Unexpected(s) = try_st else {
            return;
        };
        debug_assert!(RwLockState::<O>::expect_queue_not_empty(s));
        // Since the quick path failed, we now must push a upgradable context
        // into the front of the queue.
        let rwlock =  unsafe { acq.as_ref().rwlock() };
        let mutex = rwlock.queue().mutex();
        let mut g = mutex.acquire().wait();
        let r = (*g).as_mut().push_head(slot);
        debug_assert!(r.is_ok());
        acq_pin.on_writer_guard_drop_with_list_guard_(&mut g);

        let expect = |_| true;
        let desire = |s| {
            // This is neccessary to tell other contenders.
            let s1 = RwLockState::<O>::desire_writer_not_acquired(s);
            // This is neccessary because we have decided to enqueue the ctx.
            let s2 = RwLockState::<O>::desire_upgrade_inactive(s1);
            RwLockState::<O>::desire_queue_not_empty(s2)
        };
        #[allow(unused)]
        let r = rwlock
            .state()
            .try_spin_compare_exchange_weak(expect, desire);
        #[cfg(test)]
        if !r.is_succ() {
            let v = r.into_inner();
            log::warn!("[Acquire::downgrade_writer_to_upgradable_] {v:x}");
        }
    }
}

impl<'a, T, O> Clone for Acquire<'a, T, O>
where
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    fn clone(&self) -> Self {
        Acquire::new(self.src_lock_)
    }
}

impl<'a, T, O> TrAcquire<'a, T> for Acquire<'a, T, O>
where
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    type ReaderGuard<'g> = ReaderGuard<'a, 'g, T, O> where 'a: 'g;
    type WriterGuard<'g> = WriterGuard<'a, 'g, T, O> where 'a: 'g;
    type UpgradableGuard<'g> = UpgradableReaderGuard<'a, 'g, T, O> where 'a: 'g;

    #[inline(always)]
    fn try_read<'g>(
        self: Pin<&'g mut Self>,
    ) -> impl Try<Output = Self::ReaderGuard<'g>>
    where
        'a: 'g,
    {
        Acquire::try_read(self)
    }

    #[inline(always)]
    fn try_write<'g>(
        self: Pin<&'g mut Self>,
    ) -> impl Try<Output = Self::WriterGuard<'g>>
    where
        'a: 'g,
    {
        Acquire::try_write(self)
    }

    #[inline(always)]
    fn try_upgradable_read<'g>(
        self: Pin<&'g mut Self>,
    ) -> impl Try<Output = Self::UpgradableGuard<'g>>
    where
        'a: 'g,
    {
        Acquire::try_upgradable_read(self)
    }

    #[inline(always)]
    fn read_async<'g>(
        self: Pin<&'g mut Self>,
    ) -> impl TrIntoFutureMayCancel<'g,
            MayCancelOutput: Try<Output = Self::ReaderGuard<'g>>>
    where
        'a: 'g,
    {
        Acquire::read_async(self)
    }

    #[inline(always)]
    fn write_async<'g>(
        self: Pin<&'g mut Self>,
    ) -> impl TrIntoFutureMayCancel<'g,
            MayCancelOutput: Try<Output = Self::WriterGuard<'g>>>
    where
        'a: 'g,
    {
        Acquire::write_async(self)
    }

    #[inline(always)]
    fn upgradable_read_async<'g>(
        self: Pin<&'g mut Self>,
    ) -> impl TrIntoFutureMayCancel<'g,
            MayCancelOutput: Try<Output = Self::UpgradableGuard<'g>>>
    where
        'a: 'g,
    {
        Acquire::upgradable_read_async(self)
    }

}

/// RwLock State Value `usize`
type RwStVal = usize;

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
    const fn expect_lock_acquired(s: RwStVal) -> bool {
        s != K_NOT_LOCKED
    }
    #[allow(dead_code)]
    #[inline]
    const fn expect_lock_released(s: RwStVal) -> bool {
        s == K_NOT_LOCKED
    }

    // K_QUEUE_NOT_EMPTY
    #[inline]
    const fn expect_queue_empty(s: RwStVal) -> bool {
        !Self::expect_queue_not_empty(s)
    }
    #[inline]
    const fn expect_queue_not_empty(s: RwStVal) -> bool {
        s & K_QUEUE_NOT_EMPTY == K_QUEUE_NOT_EMPTY
    }
    #[inline]
    const fn desire_queue_empty(s: RwStVal) -> RwStVal {
        s & (!K_QUEUE_NOT_EMPTY)
    }
    #[inline]
    const fn desire_queue_not_empty(s: RwStVal) -> RwStVal {
        s | K_QUEUE_NOT_EMPTY
    }

    // K_WRITER_ACQUIRED
    #[inline]
    const fn expect_writer_acquired(s: RwStVal) -> bool {
        s & K_WRITER_ACQUIRED == K_WRITER_ACQUIRED
    }
    #[inline]
    const fn expect_writer_not_acquired(s: RwStVal) -> bool {
        !Self::expect_writer_acquired(s)
    }
    #[inline]
    const fn desire_writer_acquired(s: RwStVal) -> RwStVal {
        s | K_WRITER_ACQUIRED
    }
    #[inline]
    const fn desire_writer_not_acquired(s: RwStVal) -> RwStVal {
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
    const fn expect_upgread_active(s: RwStVal) -> bool {
        s & K_UPGRADE_ACTIVE == K_UPGRADE_ACTIVE
    }
    #[inline]
    const fn expect_upgrade_inactive(s: RwStVal) -> bool {
        !Self::expect_upgread_active(s)
    }
    const fn desire_upgrade_active(s: RwStVal) -> RwStVal {
        s | K_UPGRADE_ACTIVE
    }
    const fn desire_upgrade_inactive(s: RwStVal) -> RwStVal {
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
    const fn get_reader_count(s: RwStVal) -> RwStVal {
        s & K_MAX_READER_COUNT
    }
    #[inline]
    const fn expect_inc_reader_count_valid(s: RwStVal) -> bool {
        Self::get_reader_count(s) < K_MAX_READER_COUNT
    }
    #[inline]
    const fn expect_dec_reader_count_valid(s: RwStVal) -> bool {
        Self::get_reader_count(s) > 0
    }
    #[inline]
    const fn desire_reader_count_incr(s: RwStVal) -> RwStVal {
        s + 1
    }
    #[inline]
    const fn desire_reader_count_decr(s: RwStVal) -> RwStVal {
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
    use atomex::StrictOrderings;
    use spmv_oneshot::x_deps::atomex;
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

    #[test]
    fn state_ref_from_pin_wake_list_smoke() {
        let x = RwLock::<(), StrictOrderings>::new(());
        state_ref_from_lock_smoke_(&x);

        let x = std::boxed::Box::new(x);
        state_ref_from_lock_smoke_(&*x);
    }
}