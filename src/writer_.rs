use core::{
    fmt,
    future::{Future, IntoFuture},
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

use pin_project::pin_project;
use pin_utils::pin_mut;

use abs_sync::{
    async_lock::{TrAcquire, TrReaderGuard, TrWriterGuard},
    cancellation::{TrCancellationToken, TrIntoFutureMayCancel},
    never_cancel::FutureForTaskNeverCancel as FutNonCancel,
};
use atomex::TrCmpxchOrderings; 
use spmv_oneshot::x_deps::{abs_sync, atomex, pin_utils};

use super::{
    contexts_::{CtxType, Message},
    impl_::*,
    reader_::ReaderGuard,
    upgrade_::{Upgrade, UpgradableReaderGuard},
};

enum WGuardCtx<'a, 'g, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    Acq(Pin<&'g mut Acquire<'a, T, O>>),
    Upg(NonNull<Upgrade<'a, 'g, T, O>>),
}

impl<'a, 'g, T, O> WGuardCtx<'a, 'g, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    pub fn acquire(&self) -> &Acquire<'a, T, O> {
        match self {
            WGuardCtx::Acq(a) => unsafe {
                let a = a.as_ref().get_ref() as *const Acquire<'a, T, O>;
                a.as_ref().unwrap()
            }
            WGuardCtx::Upg(u) => unsafe {
                let u = Pin::new_unchecked(u.as_ref());
                let a = u.acquire_().as_ref().get_ref() as *const Acquire<'a, T, O>;
                a.as_ref().unwrap()
            }
        }
    }

    fn acquire_pinned_(&mut self) -> Pin<&mut Acquire<'a, T, O>> {
        match self {
            WGuardCtx::Acq(a) => a.as_mut(),
            WGuardCtx::Upg(u) => unsafe {
                let upg = Pin::new_unchecked(u.as_mut());
                upg.acquire_pinned_()
            }
        }
    }

    #[allow(unused)]
    fn upgrade_pinned_(
        &mut self,
    ) -> Option<Pin<&mut Upgrade<'a, 'g, T, O>>> {
        if let WGuardCtx::Upg(u) = self {
            let p = unsafe { Pin::new_unchecked(u.as_mut()) };
            Option::Some(p)
        } else {
            Option::None
        }
    }

    pub fn rwlock(&self) -> &RwLock<T, O> {
        self.acquire().rwlock()
    }
}

unsafe impl<T, O> Send for WGuardCtx<'_, '_, T, O>
where
    T: Send + ?Sized,
    O: TrCmpxchOrderings,
{}

unsafe impl<T, O> Sync for WGuardCtx<'_, '_, T, O>
where
    T: Send + Sync + ?Sized,
    O: TrCmpxchOrderings,
{}

/// A guard that releases the write lock when dropped.
pub struct WriterGuard<'a, 'g, T, O>(WGuardCtx<'a, 'g, T, O>)
where
    T: ?Sized,
    O: TrCmpxchOrderings;

impl<'a, 'g, T, O> WriterGuard<'a, 'g, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    #[inline(always)]
    pub(super) fn from_acquire(
        acquire: Pin<&'g mut Acquire<'a, T, O>>,
    ) -> Self {
        WriterGuard(WGuardCtx::Acq(acquire))
    }

    #[inline(always)]
    pub(super) fn from_upgrade<'u>(
        upgrade: Pin<&'u mut Upgrade<'a, 'g, T, O>>,
    ) -> Self {
        let upgrade = unsafe {
            NonNull::new_unchecked(upgrade.get_unchecked_mut())
        };
        WriterGuard(WGuardCtx::Upg(upgrade))
    }

    /// Downgrades into a regular reader guard.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures_lite::future::block_on(async {
    /// use atomex::StrictOrderings;
    /// use asyncex::{
    ///     rwlock::RwLock,
    ///     x_deps::{atomex, pin_utils},
    /// };
    /// use pin_utils::pin_mut;
    ///
    /// let lock = RwLock::<usize, StrictOrderings>::new(1);
    /// let acq1 = lock.acquire();
    /// pin_mut!(acq1);
    /// let mut writer = acq1.as_mut().write_async().await;
    /// *writer += 1;
    ///
    /// let acq2 = lock.acquire();
    /// pin_mut!(acq2);
    /// assert!(acq2.as_mut().try_read().is_none());
    /// let reader = writer.downgrade();
    /// assert_eq!(*reader, 2);
    /// assert!(acq2.as_mut().try_read().is_some());
    /// # })
    /// ```
    pub fn downgrade(self) -> ReaderGuard<'a, 'g, T, O> {
        let mut m = ManuallyDrop::new(self);
        return match &mut m.0 {
            WGuardCtx::Acq(acq) => from_acq_(acq),
            WGuardCtx::Upg(upg) => from_upg_(upg),
        };

        fn from_acq_<'acq, 'guard, TRes, TOrd>(
            acq: &mut Pin<&mut Acquire<'acq, TRes, TOrd>>,
        ) -> ReaderGuard<'acq, 'guard, TRes, TOrd>
        where
            TRes: ?Sized,
            TOrd: TrCmpxchOrderings,
        {
            let mut acq_ptr: NonNull<Acquire<'acq, TRes, TOrd>> = unsafe {
                let ptr = acq.as_mut().get_unchecked_mut();
                NonNull::new_unchecked(ptr)
            };
            let opt_g = unsafe {
                let acq = Pin::new_unchecked(acq_ptr.as_mut());
                acq.try_read()
            };
            if let Option::Some(g) = opt_g {
                return g;
            } else {
                let acq = unsafe { Pin::new_unchecked(acq_ptr.as_mut()) };
                acq.downgrade_writer_to_reader_();
            }
            ReaderGuard::new(unsafe {
                Pin::new_unchecked(acq_ptr.as_mut())
            })
        }

        fn from_upg_<'acq, 'guard, TRes, TOrd>(
            upg: &mut NonNull<Upgrade<'acq, 'guard, TRes, TOrd>>
        ) -> ReaderGuard<'acq, 'guard, TRes, TOrd>
        where
            TRes: ?Sized,
            TOrd: TrCmpxchOrderings,
        {
            let mut acquire = unsafe {
                let pinned = Pin::new_unchecked(upg.as_mut());
                pinned.acquire_pinned_()
            };
            debug_assert!({
                let slot = acquire.slot_();
                let ctx_type = slot.data().context_type();
                slot.is_element_of(acquire.rwlock().queue())
                    && matches!(ctx_type, CtxType::ReadOnly)
            });
            acquire.as_mut().downgrade_writer_to_reader_();
            ReaderGuard::new(acquire)
        }
    }

    /// Downgrades into an upgradable reader guard.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures_lite::future::block_on(async {
    /// use atomex::StrictOrderings;
    /// use asyncex::{
    ///     rwlock::RwLock,
    ///     x_deps::{atomex, pin_utils},
    /// };
    /// use pin_utils::pin_mut;
    ///
    /// let lock = RwLock::<usize, StrictOrderings>::new(1);
    /// let acq = lock.acquire();
    /// pin_mut!(acq);
    /// let mut writer = acq.write_async().await;
    /// *writer += 1;
    /// 
    /// let mut acq2 = lock.acquire();
    /// pin_mut!(acq2);
    /// assert!(acq2.as_mut().try_read().is_none());
    /// 
    /// let reader = writer.downgrade_to_upgradable();
    /// assert_eq!(*reader, 2);
    ///
    /// assert!(acq2.as_mut().try_write().is_none());
    /// assert!(acq2.as_mut().try_read().is_some());
    ///
    /// let upgrade = reader.upgrade();
    /// pin_mut!(upgrade);
    /// assert!(upgrade.as_mut().try_upgrade().is_some())
    /// # })
    /// ```
    pub fn downgrade_to_upgradable(self) -> UpgradableReaderGuard<'a, 'g, T, O> {
        let mut m = ManuallyDrop::new(self);
        return match &mut m.0 {
            WGuardCtx::Acq(acq) => from_acq_(acq),
            WGuardCtx::Upg(upg) => from_upg_(upg),
        };

        fn from_acq_<'acq, 'guard, TRes, TOrd>(
            acq: &mut Pin<&mut Acquire<'acq, TRes, TOrd>>,
        ) -> UpgradableReaderGuard<'acq, 'guard, TRes, TOrd>
        where
            TRes: ?Sized,
            TOrd: TrCmpxchOrderings,
        {
            let mut acq_ptr: NonNull<Acquire<'acq, TRes, TOrd>> = unsafe {
                let ptr = acq.as_mut().get_unchecked_mut();
                NonNull::new_unchecked(ptr)
            };
            let opt_g = unsafe {
                let acq = Pin::new_unchecked(acq_ptr.as_mut());
                acq.try_upgradable_read()
            };
            if let Option::Some(g) = opt_g {
                return g;
            } else {
                let acq = unsafe { Pin::new_unchecked(acq_ptr.as_mut()) };
                acq.downgrade_writer_to_upgradable_();
            }
            UpgradableReaderGuard::new(unsafe {
                Pin::new_unchecked(acq_ptr.as_mut())
            })
        }

        fn from_upg_<'acq, 'guard, TRes, TOrd>(
            upg: &mut NonNull<Upgrade<'acq, 'guard, TRes, TOrd>>
        ) -> UpgradableReaderGuard<'acq, 'guard, TRes, TOrd>
        where
            TRes: ?Sized,
            TOrd: TrCmpxchOrderings,
        {
            let mut acquire = unsafe {
                let pinned = Pin::new_unchecked(upg.as_mut());
                pinned.acquire_pinned_()
            };
            debug_assert!({
                let slot = acquire.slot_();
                let ctx_type = slot.data().context_type();
                slot.is_element_of(acquire.rwlock().queue())
                    && matches!(ctx_type, CtxType::Upgradable)
            });
            acquire.as_mut().downgrade_writer_to_upgradable_();
            UpgradableReaderGuard::new(acquire)
        }
    }

    #[inline]
    fn rwlock(&self) -> &RwLock<T, O> {
        self.0.rwlock()
    }
}

impl<T, O> Drop for WriterGuard<'_, '_, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    fn drop(&mut self) {
        self.0.acquire_pinned_().on_writer_guard_drop_()
    }
}

impl<T, O> Deref for WriterGuard<'_, '_, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type Target = T;

    fn deref(&self) -> &T {
        self.0.acquire().deref_impl()
    }
}

impl<T, O> DerefMut for WriterGuard<'_, '_, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    fn deref_mut(&mut self) -> &mut T {
        self.0.acquire_pinned_().deref_mut_impl()
    }
}

impl<'a, 'g, T, O> TrReaderGuard<'a, 'g, T> for WriterGuard<'a, 'g, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type Acquire = Acquire<'a, T, O>;
}

impl<'a, 'g, T, O> TrWriterGuard<'a, 'g, T>
for WriterGuard<'a, 'g, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    #[inline]
    fn downgrade(self) -> <Self::Acquire as TrAcquire<'a, T>>::ReaderGuard<'g> {
        WriterGuard::downgrade(self)
    }

    #[inline]
    fn downgrade_to_upgradable(
        self,
    ) -> <Self::Acquire as TrAcquire<'a, T>>::UpgradableGuard<'g> {
        WriterGuard::downgrade_to_upgradable(self)
    }
}

impl<T, O> fmt::Debug for WriterGuard<'_, '_, T, O>
where
    T: fmt::Debug + ?Sized,
    O: TrCmpxchOrderings,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WriterGuard(lock: {:p}, value: {:?})",
            self.0.rwlock(),
            self.deref(),
        )
    }
}

impl<T, O> fmt::Display for WriterGuard<'_, '_, T, O>
where
    T: fmt::Display + ?Sized,
    O: TrCmpxchOrderings,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WriterGuard(lock: {:p}, value: {})",
            self.rwlock(),
            self.deref(),
        )
    }
}

pub struct WriteAsync<'l, 'a, T, O>(Pin<&'a mut Acquire<'l, T, O>>)
where
    T: ?Sized,
    O: TrCmpxchOrderings;

impl<'l, 'a, T, O> WriteAsync<'l, 'a, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    pub fn new(acquire: Pin<&'a mut Acquire<'l, T, O>>) -> Self {
        let _ = acquire.slot_().data().try_reset_context_type();
        WriteAsync(acquire)
    }

    #[inline(always)]
    pub fn may_cancel_with<C>(
        self,
        cancel: Pin<&'a mut C>,
    ) -> WriteFuture<'l, 'a, C, T, O>
    where
        C: TrCancellationToken,
    {
        WriteFuture::new(self.0, cancel)
    }
}

impl<'l, 'a, T, O> TrIntoFutureMayCancel<'a> for WriteAsync<'l, 'a, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type MayCancelOutput = Option<WriterGuard<'l, 'a, T, O>>;

    #[inline(always)]
    fn may_cancel_with<C>(
        self,
        cancel: Pin<&'a mut C>,
    ) -> impl Future<Output = Self::MayCancelOutput>
    where
        C: TrCancellationToken,
    {
        WriteAsync::may_cancel_with(self, cancel)
    }
}

impl<'a, T, O> IntoFuture for WriteAsync<'_, 'a, T, O>
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

/// Future for RwLock::write operation.
#[pin_project]
pub struct WriteFuture<'l, 'a, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    acquire_: Pin<&'a mut Acquire<'l, T, O>>,
    cancel_: Pin<&'a mut C>,
}

impl<'l, 'a, C, T, O> WriteFuture<'l, 'a, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(
        acquire: Pin<&'a mut Acquire<'l, T, O>>,
        cancel: Pin<&'a mut C>,
    ) -> Self {
        WriteFuture {
            acquire_: acquire,
            cancel_: cancel,
        }
    }

    async fn write_async_(self: Pin<&mut Self>) -> <Self as Future>::Output {
        let this = self.project();
        let mut acquire = unsafe {
            let ptr = this.acquire_.as_mut().get_unchecked_mut();
            NonNull::new_unchecked(ptr)
        };
        let try_write = unsafe { acquire.as_mut().try_write_() };
        if try_write.is_some() {
            return try_write;
        };
        let mut cancel = this.cancel_.as_mut();
        let mut slot = this.acquire_.as_mut().slot_pinned_();
        debug_assert!(slot.is_detached());

        let mutex = unsafe {
            // Safe because the rwlock queue mutex is thread-safe
            acquire.as_ref().rwlock().queue().mutex()
        };
        let mut g = mutex.acquire().may_cancel_with(cancel.as_mut())?;
        let queue = (*g).as_mut();
        let r = queue.push_tail(slot.as_mut());
        debug_assert!(r.is_ok());

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
                // Use unsafe here to get a `WriterGuard` with proper lifetime
                Pin::new_unchecked(acquire.as_mut())
            };
            Option::Some(WriterGuard::from_acquire(acquire))
        } else {
            Option::None
        }
    }
}

impl<'l, 'a, C, T, O> Future for WriteFuture<'l, 'a, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type Output = Option<WriterGuard<'l, 'a, T, O>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let f = self.write_async_();
        pin_mut!(f);
        f.poll(cx)
    }
}
