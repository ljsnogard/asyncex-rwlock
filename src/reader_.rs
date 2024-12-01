﻿use core::{
    fmt,
    future::{Future, IntoFuture},
    ops::Deref,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

use pin_project::pin_project;
use pin_utils::pin_mut;

use atomex::TrCmpxchOrderings; 
use abs_sync::{
    async_lock::TrReaderGuard,
    cancellation::{TrCancellationToken, TrIntoFutureMayCancel},
    never_cancel::FutureForTaskNeverCancel as FutNonCancel,
};
use pincol::x_deps::{abs_sync, atomex, pin_utils};

use super::{
    acquire_::Acquire,
    contexts_::{init_slot, CtxType},
    rwlock_::RwLock,
};

/// A guard that releases the read lock when dropped.
///
/// ## Developer notice
/// A new `RwLockReadGuard` will may or may not cause the increment of reader
/// count in `RwLock::state_`
pub struct ReaderGuard<'a, 'g, T, O>(Pin<&'g mut Acquire<'a, T, O>>)
where
    T: ?Sized,
    O: TrCmpxchOrderings;

impl<'a, 'g, T, O> ReaderGuard<'a, 'g, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(acquire: Pin<&'g mut Acquire<'a, T, O>>) -> Self {
        #[cfg(test)]
        unsafe {
            let a = acquire.as_ref().get_ref()
                as *const _ as *mut Acquire<'a, T, O>;
            let s = (*a).slot_().data();
            log::trace!("[ReaderGuard::new] acq({a:?}) WaitCtx({s:p})");
        }
        ReaderGuard(acquire)
    }

    #[allow(dead_code)]
    pub(super) fn acq_ptr(&self) -> *mut Acquire<'a, T, O> {
        self.0.as_ref().get_ref() as *const _ as *mut _
    }

    #[inline]
    fn rwlock(&self) -> &RwLock<T, O> {
        self.0.rwlock()
    }
}

impl<'a, 'g, T, O> TrReaderGuard<'a, 'g, T> for ReaderGuard<'a, 'g, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type Acquire = Acquire<'a, T, O>;
}

impl<T, O> Drop for ReaderGuard<'_, '_, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    fn drop(&mut self) {
        #[cfg(test)]
        unsafe {
            let a = self.acq_ptr();
            let s = (*a).slot_().data();
            log::trace!("[ReaderGuard::drop] acq({a:?}) WaitCtx({s:p})");
        }
        self.0.as_mut().on_reader_guard_drop_()
    }
}

impl<T, O> Deref for ReaderGuard<'_, '_, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type Target = T;

    fn deref(&self) -> &T {
        self.0.deref_impl()
    }
}

impl<T, O> fmt::Debug for ReaderGuard<'_, '_, T, O>
where
    T: fmt::Debug + ?Sized,
    O: TrCmpxchOrderings,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReaderGuard(lock: {:p}, value: {:?})",
            self.rwlock(),
            self.deref(),
        )
    }
}

impl<T, O> fmt::Display for ReaderGuard<'_, '_, T, O>
where
    T: fmt::Display + ?Sized,
    O: TrCmpxchOrderings,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReaderGuard(lock: {:p}, value: {})",
            self.rwlock(),
            self.deref(),
        )
    }
}

pub struct ReadAsync<'l, 'a, T, O>(Pin<&'a mut Acquire<'l, T, O>>)
where
    T: ?Sized,
    O: TrCmpxchOrderings;

impl<'l, 'a, T, O> ReadAsync<'l, 'a, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    pub fn new(mut acquire: Pin<&'a mut Acquire<'l, T, O>>) -> Self {
        init_slot(acquire.as_mut(), CtxType::ReadOnly);
        ReadAsync(acquire)
    }

    pub fn may_cancel_with<C>(
        self,
        cancel: Pin<&'a mut C>,
    ) -> ReadFuture<'l, 'a, C, T, O>
    where
        C: TrCancellationToken,
    {
        ReadFuture::new(self.0, cancel)
    }
}

impl<'l, 'a, T, O> TrIntoFutureMayCancel<'a> for ReadAsync<'l, 'a, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type MayCancelOutput = Option<ReaderGuard<'l, 'a, T, O>>;

    #[inline]
    fn may_cancel_with<C>(
        self,
        cancel: Pin<&'a mut C>,
    ) -> impl Future<Output = Self::MayCancelOutput>
    where
        C: TrCancellationToken,
    {
        ReadFuture::new(self.0, cancel)
    }
}

impl<'a, T, O> IntoFuture for ReadAsync<'_, 'a, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type IntoFuture = FutNonCancel<'a, Self>;
    type Output = <Self::IntoFuture as Future>::Output;

    fn into_future(self) -> Self::IntoFuture {
        FutNonCancel::new(self)
    }
}

/// Future for RwLock::read operation.
#[pin_project]
pub struct ReadFuture<'l, 'a, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    acquire_: Pin<&'a mut Acquire<'l, T, O>>,
    cancel_: Pin<&'a mut C>,
}

impl<'l, 'a, C, T, O> ReadFuture<'l, 'a, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(
        acquire: Pin<&'a mut Acquire<'l, T, O>>,
        cancel: Pin<&'a mut C>,
    ) -> Self {
        ReadFuture {
            acquire_: acquire,
            cancel_: cancel,
        }
    }
}

impl<'l, 'a, C, T, O> Future for ReadFuture<'l, 'a, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type Output = Option<ReaderGuard<'l, 'a, T, O>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut acquire = unsafe {
            let ptr = this.acquire_.as_mut().get_unchecked_mut();
            NonNull::new_unchecked(ptr)
        };
        let mut cancel = this.cancel_.as_mut();
        let mut slot_ptr = unsafe {
            let slot_pin = this.acquire_.as_mut().slot_pinned_();
            NonNull::new_unchecked(slot_pin.get_unchecked_mut())
        };
        loop {
            let slot_ref = unsafe { slot_ptr.as_ref() };
            if let Option::Some(q) = slot_ref.attached_list() {
                let fut_can = cancel.as_mut().cancellation().into_future();
                pin_mut!(fut_can);
                if fut_can.poll(cx).is_ready() {
                    let mutex = q.mutex();
                    let mut g = mutex.acquire().wait();
                    let mut q = (*g).as_mut();
                    let slot_pin = unsafe {
                        Pin::new_unchecked(slot_ptr.as_mut())
                    };
                    let Option::Some(mut cursor) = q.as_mut().find(slot_pin)
                    else {
                        unreachable!();
                    };
                    let detach_succ = cursor.try_detach();
                    if detach_succ && q.is_empty() {
                        let acq_ref = unsafe { acquire.as_ref() };
                        let _ = acq_ref.rwlock().state().try_set_queue_empty();
                    }
                    drop(g);
                    return Poll::Ready(Option::None);
                }
                return Poll::Pending;
            } else {
                // First we try the fast path that will not enqueue the slot.
                let try_read = unsafe { acquire.as_mut().try_read_() };
                if try_read.is_some() {
                    return Poll::Ready(try_read);
                };
                // Since fast path failed, we have to enqueue the slot
                let mutex = unsafe {
                    // Safe because the rwlock queue mutex is thread-safe
                    acquire.as_ref().rwlock().queue().mutex()
                };
                let opt_q_guard = mutex
                    .acquire()
                    .may_cancel_with(cancel.as_mut());
                let Option::Some(mut g) = opt_q_guard
                else {
                    return Poll::Ready(Option::None);
                };
                let mut queue = (*g).as_mut();
                let mut tail = queue.as_mut().tail_mut();
                let mut slot = unsafe {
                    Pin::new_unchecked(slot_ptr.as_mut())
                };
                if let Option::Some(tail_cx) = tail.current_pinned() {
                    let ctx_type = tail_cx.context_type();
                    // Special treatment: readonly guard contenders should queue
                    // prior to the upgradable guard.
                    if matches!(ctx_type, CtxType::Upgradable) {
                        let r = tail.insert_prev(slot.as_mut());
                        debug_assert!(r.is_ok());
                        debug_assert!(!slot.is_detached());
                    }
                    // If the tail context is not the slot of upgradable read, 
                    // and thus this slot is not enqueued at the moment.
                    if slot.is_detached() {
                        let r = tail.insert_next(slot.as_mut());
                        debug_assert!(r.is_ok());
                        debug_assert!(!slot.is_detached());
                    }
                } else {
                    // The queue is empty.
                    let r = queue.as_mut().push_tail(slot.as_mut());
                    debug_assert!(r.is_ok());
                    debug_assert!(!slot.is_detached());
                }
                let s = unsafe { acquire.as_ref().rwlock().state() };
                let _ = s.try_set_queue_not_empty();
                drop(g);
            }
        }
    }
}
