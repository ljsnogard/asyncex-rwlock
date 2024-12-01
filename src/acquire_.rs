use core::{
    ops::{Deref, DerefMut, Try},
    pin::Pin,
    ptr::{self, NonNull},
};

use abs_sync::{
    async_lock::TrAcquire,
    cancellation::TrIntoFutureMayCancel,
};
use atomex::{CmpxchResult, TrAtomicFlags, TrCmpxchOrderings};
use pincol::x_deps::{abs_sync, atomex};

use super::{
    contexts_::{CtxType, AsPinnedMut, WaitCtx, WakeListGuard, WakeSlot},
    rwlock_::{RwLock, RwLockState, RwStVal, StateReport},
    reader_::{ReadAsync, ReaderGuard},
    upgradable_::{UpgradableReadAsync, UpgradableReaderGuard},
    upgrade_::Upgrade,
    writer_::{WriteAsync, WriterGuard},
};

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
        self: Pin<&mut Self>,
    ) -> ReadAsync<'a, '_, T, O> {
        ReadAsync::new(self)
    }

    pub fn write_async(
        self: Pin<&mut Self>,
    ) -> WriteAsync<'a, '_, T, O> {
        WriteAsync::new(self)
    }

    pub fn upgradable_read_async(
        self: Pin<&mut Self>,
    ) -> UpgradableReadAsync<'a, '_, T, O> {
        UpgradableReadAsync::new(self)
    }
}

impl<'a, T, O> AsPinnedMut<WakeSlot<O>> for Acquire<'a, T, O>
where
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    fn as_pinned_mut(self: Pin<&mut Self>) -> Pin<&mut WakeSlot<O>> {
        Self::slot_pinned_(self)
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

    pub(super) fn deref_impl(&self) -> &T {
        unsafe { &*self.src_lock_.data_cell().get() }
    }

    pub(super) fn deref_mut_impl(self: Pin<&mut Self>) -> &mut T {
        unsafe {
            let this = self.get_unchecked_mut();
            &mut *this.src_lock_.data_cell().get()
        }
    }

    /// Try to get a reader guard within the fast path (no enqueue)
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

    /// Try to get a writer guard within the fast path (no enqueue)
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

    pub(super) fn try_upgrade_<'g, 'u>(
        &'g mut self,
        upgrade: Pin<&'u mut Upgrade<'a, 'g, T, O>>,
    ) -> Option<WriterGuard<'a, 'g, T, O>>
    where
        'a: 'g,
    {
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
            Option::Some(WriterGuard::from_upgrade(upgrade))
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
            unreachable!("[Acquire::on_writer_guard_drop_with_list_guard_] pop head failed")
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
            let Option::Some(head) = opt_head
            else {
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
