use core::{
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
use spmv_oneshot::x_deps::{abs_sync, atomex, pin_utils};

use super::{
    contexts_::{CtxType, Message},
    impl_::{Acquire, RwLock},
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
    pub fn new(acquire: Pin<&'a mut Acquire<'l, T, O>>) -> Self {
        let _ = acquire.slot_().data().try_reset_context_type();
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

    async fn read_async_(self: Pin<&mut Self>) -> <Self as Future>::Output {
        let this = self.project();
        if true {
            let acq_ref = this.acquire_.as_ref();
            let init = acq_ref.try_init_slot_ctx_(CtxType::ReadOnly);
            assert!(init);
        }
        let mut acquire = unsafe {
            let ptr = this.acquire_.as_mut().get_unchecked_mut();
            NonNull::new_unchecked(ptr)
        };
        let try_read = unsafe { acquire.as_mut().try_read_() };
        if try_read.is_some() {
            return try_read;
        };
        let mut cancel = this.cancel_.as_mut();
        let mut slot = this.acquire_.as_mut().slot_pinned_();
        debug_assert!(slot.is_detached());

        let mutex = unsafe {
            // Safe because the rwlock queue mutex is thread-safe
            acquire.as_ref().rwlock().queue().mutex()
        };
        let mut g = mutex.acquire().may_cancel_with(cancel.as_mut())?;
        let mut queue = (*g).as_mut();
        let mut tail = queue.as_mut().tail_mut();
        let Option::Some(tail_cx) = tail.current_pinned() else {
            unreachable!("[rwlock::ReadFuture::read_async_]")
        };
        let ctx_type = tail_cx.context_type();
        if matches!(ctx_type, CtxType::Upgradable) {
            let r = tail.insert_prev(slot.as_mut());
            debug_assert!(r.is_ok());
            debug_assert!(!slot.is_detached());
        }
        if slot.is_detached() {
            let r = tail.insert_next(slot.as_mut());
            debug_assert!(r.is_ok());
            debug_assert!(!slot.is_detached());
        }
        let s = unsafe { acquire.as_ref().rwlock().state() };
        let _ = s.try_set_queue_not_empty();
        drop(g);

        let curr_cx = slot.data();
        let peeker = curr_cx.channel().peeker();
        pin_mut!(peeker);
        let msg = peeker
            .peek_async()
            .may_cancel_with(cancel)
            .await;
        if let Result::Ok(Message::Ready) = msg {
            let acquire = unsafe {
                // To get a `ReaderGuard` with proper lifetime
                Pin::new_unchecked(acquire.as_mut())
            };
            Option::Some(ReaderGuard::new(acquire))
        } else {
            Option::None
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
        let f= self.read_async_();
        pin_mut!(f);
        f.poll(cx)
    }
}
