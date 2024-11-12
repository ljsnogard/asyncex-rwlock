use core::{
    fmt,
    future::{Future, IntoFuture},
    mem::ManuallyDrop,
    ops::{Deref, Try},
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

use pin_project::pin_project;
use pin_utils::pin_mut;

use abs_sync::{
    async_lock::{TrAcquire, TrReaderGuard, TrUpgradableReaderGuard, TrUpgrade},
    cancellation::{
        NonCancellableToken, TrCancellationToken, TrIntoFutureMayCancel,
    },
    never_cancel::FutureForTaskNeverCancel as FutNonCancel,
};
use asyncex_channel::x_deps::{abs_sync, atomex, pin_utils};
use atomex::TrCmpxchOrderings; 

use super::{
    contexts_::*,
    impl_::*,
    reader_::ReaderGuard,
    writer_::WriterGuard,
};

pub struct Upgrade<'a, 'g, T, O>
where
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    acquire_: Pin<&'g mut Acquire<'a, T, O>>,
    upg_ctx_: WakeSlot<O>,
}

impl<'a, 'g, T, O> Upgrade<'a, 'g, T, O>
where
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    #[inline]
    pub(super) fn new(acquire: Pin<&'g mut Acquire<'a, T, O>>) -> Self {
        Upgrade {
            acquire_: acquire,
            upg_ctx_: WakeSlot::new(WaitCtx::new()),
        }
    }

    #[inline]
    pub fn try_upgrade<'u>(
        self: Pin<&'u mut Self>,
    ) -> Option<WriterGuard<'a, 'u, T, O>> {
        unsafe { self.acquire_pinned_().get_unchecked_mut().try_upgrade_() }
    }

    pub fn upgrade_async<'u>(
        self: Pin<&'u mut Self>,
    ) -> UpgradeAsync<'a, 'g, 'u, T, O>
    where
        'g: 'u,
    {
        UpgradeAsync::new(self)
    }

    pub fn into_guard(self) -> UpgradableReaderGuard<'a, 'g, T, O> {
        UpgradableReaderGuard::new(self.acquire_)
    }

    pub(super) fn acquire_(self: Pin<&Self>) -> Pin<&Acquire<'a, T, O>> {
        let a = self.acquire_.as_ref().get_ref() as *const Acquire<'a, T, O>;
        unsafe { Pin::new_unchecked(a.as_ref().unwrap()) }
    }

    pub(super) fn acquire_pinned_(
        self: Pin<&mut Self>,
    ) -> Pin<&mut Acquire<'a, T, O>> {
        unsafe { self.get_unchecked_mut().acquire_.as_mut() }
    }
 
    pub(super) fn slot_pinned_(self: Pin<&mut Self>) -> Pin<&mut WakeSlot<O>> {
        unsafe {
            let slot = &mut self.get_unchecked_mut().upg_ctx_;
            Pin::new_unchecked(slot)
        }
    }
}

impl<'a, 'g, T, O> TrUpgrade<'a, 'g, T> for Upgrade<'a, 'g, T, O>
where
    'a: 'g,
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    type Acquire = Acquire<'a, T, O>;

    #[inline(always)]
    fn try_upgrade<'u>(
        self: Pin<&'u mut Self>,
    ) -> impl Try<Output = <Self::Acquire as TrAcquire<'a, T>>::WriterGuard<'u>>
    where
        'g: 'u,
    {
        Upgrade::try_upgrade(self)
    }

    #[inline(always)]
    fn upgrade_async<'u>(
        self: Pin<&'u mut Self>,
    ) -> impl TrIntoFutureMayCancel<'u, MayCancelOutput: Try<Output =
            <Self::Acquire as TrAcquire<'a, T>>::WriterGuard<'u>>>
    where
        'g: 'u,
    {
        Upgrade::upgrade_async(self)
    }

    #[inline(always)]
    fn into_guard(
        self,
    ) -> <Self::Acquire as TrAcquire<'a, T>>::UpgradableGuard<'g> {
        Upgrade::into_guard(self)
    }
}

/// A guard that releases the upgradable read lock when dropped.
pub struct UpgradableReaderGuard<'a, 'g, T, O>(Pin<&'g mut Acquire<'a, T, O>>)
where
    'a: 'g,
    T: 'a + ?Sized,
    O: TrCmpxchOrderings;

impl<'a, 'g, T, O> UpgradableReaderGuard<'a, 'g, T, O>
where
    'a: 'g,
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(acquire: Pin<&'g mut Acquire<'a, T, O>>) -> Self {
        UpgradableReaderGuard(acquire)
    }

    #[allow(dead_code)]
    pub(super) fn acq_ptr(&self) -> *mut Acquire<'a, T, O> {
        self.0.as_ref().get_ref() as *const _ as *mut _
    }

    /// Extracts the inner pin pointer from the guard without invoking `drop`.
    /// 
    /// This is safe because the target of the `Pin` pointer doesn't move 
    /// and the life time is not changed either.
    fn into_inner_no_drop_(self) -> Pin<&'g mut Acquire<'a, T, O>> {
        let mut m = ManuallyDrop::new(self);
        unsafe {
            let ptr = m.0.as_mut().get_unchecked_mut();
            let mut ptr = NonNull::new_unchecked(ptr);
            Pin::new_unchecked(ptr.as_mut())
        }
    }

    /// Downgrades into a regular reader guard.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures_lite::future::block_on(async {
    /// use pin_utils::pin_mut;
    /// use atomex::StrictOrderings;
    /// use asyncex::{rwlock::RwLock, x_deps::{atomex, pin_utils}};
    ///
    /// let rwlock = RwLock::<usize, StrictOrderings>::new(42);
    /// let acq1 = rwlock.acquire();
    /// pin_mut!(acq1);
    /// let reader1 = acq1.as_mut().upgradable_read_async().await;
    /// assert_eq!(*reader1, 1);
    ///
    /// let acq2 = rwlock.acquire();
    /// pin_mut!(acq2);
    /// assert!(acq2.as_mut().try_read().is_some());
    /// assert!(acq2.as_mut().try_upgradable_read().is_none());
    ///
    /// let reader = reader1.downgrade();
    /// assert!(acq2.as_mut().try_upgradable_read().is_some());
    /// assert!(acq2.as_mut().try_read().is_some());
    /// # })
    /// ```
    pub fn downgrade(self) -> ReaderGuard<'a, 'g, T, O> {
        let slot = self.0.slot_();
        if slot.is_detached() {
            let s = self.0.rwlock().state();
            assert!(
                s.try_downgrade_upgradable_to_readonly(),
                "[UpgradableReaderGuard::downgrade] unexpected rwlock state",
            );
        } else {
            let ctx = slot.data();
            assert!(
                ctx.try_downgrade_upgradable_to_readonly(),
                "[UpgradableReaderGuard::downgrade] unexpected slot context",
            );
        }
        ReaderGuard::new(Self::into_inner_no_drop_(self))
    }

    pub fn upgrade(self) -> Upgrade<'a, 'g, T, O> {
        Upgrade::new(Self::into_inner_no_drop_(self))
    }

    #[inline]
    fn rwlock(&self) -> &RwLock<T, O> {
        self.0.rwlock()
    }
}

impl<'a, 'g, T, O> TrReaderGuard<'a, 'g, T>
for UpgradableReaderGuard<'a, 'g, T, O>
where
    'a: 'g,
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    type Acquire = Acquire<'a, T, O>;
}

impl<'a, 'g, T, O> TrUpgradableReaderGuard<'a, 'g, T>
for UpgradableReaderGuard<'a, 'g, T, O>
where
    'a: 'g,
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    #[inline(always)]
    fn downgrade(self) -> <Self::Acquire as TrAcquire<'a, T>>::ReaderGuard<'g> {
        UpgradableReaderGuard::downgrade(self)
    }

    #[inline(always)]
    fn upgrade(self) -> impl TrUpgrade<'a, 'g, T, Acquire = Self::Acquire> {
        UpgradableReaderGuard::upgrade(self)
    }
}

impl<T, O> Drop for UpgradableReaderGuard<'_, '_, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    fn drop(&mut self) {
        self.0.as_mut().on_upgradable_guard_drop_();
    }
}

impl<T, O> Deref for UpgradableReaderGuard<'_, '_, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type Target = T;

    fn deref(&self) -> &T {
        self.0.deref_impl()
    }
}

impl<T, O> fmt::Debug for UpgradableReaderGuard<'_, '_, T, O>
where
    T: fmt::Debug + ?Sized,
    O: TrCmpxchOrderings,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "UpgradableReaderGuard(lock: {:p}, value: {:?})",
            self.rwlock(),
            self.deref(),
        )
    }
}

impl<T, O> fmt::Display for UpgradableReaderGuard<'_, '_, T, O>
where
    T: fmt::Display + ?Sized,
    O: TrCmpxchOrderings,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RwLockUpgradableReadGuard(lock: {:p}, value: {})",
            self.rwlock(),
            self.deref(),
        )
    }
}

pub struct UpgradableReadAsync<'l, 'a, T, O>(Pin<&'a mut Acquire<'l, T, O>>)
where
    T: ?Sized,
    O: TrCmpxchOrderings;

impl<'l, 'a, T, O> UpgradableReadAsync<'l, 'a, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    pub fn new(acquire: Pin<&'a mut Acquire<'l, T, O>>) -> Self {
        let _ = acquire.slot_().data().try_reset_context_type();
        UpgradableReadAsync(acquire)
    }

    pub fn may_cancel_with<C>(
        self,
        cancel: Pin<&'a mut C>,
    ) -> UpgradableReadFuture<'l, 'a, C, T, O>
    where
        C: TrCancellationToken,
    {
        UpgradableReadFuture::new(self.0, cancel)
    }
}

impl<'l, 'a, T, O> TrIntoFutureMayCancel<'a>
for UpgradableReadAsync<'l, 'a, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type MayCancelOutput = Option<UpgradableReaderGuard<'l, 'a, T, O>>;

    #[inline]
    fn may_cancel_with<C>(
        self,
        cancel: Pin<&'a mut C>,
    ) -> impl Future<Output = Self::MayCancelOutput>
    where
        C: TrCancellationToken,
    {
        UpgradableReadAsync::may_cancel_with(self, cancel)
    }
}

impl<'a, T, O> IntoFuture for UpgradableReadAsync<'_, 'a, T, O>
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

#[pin_project]
/// Future for `RwLock::upgradable_read`.
pub struct UpgradableReadFuture<'l, 'a, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    acquire_: Pin<&'a mut Acquire<'l, T, O>>,
    cancel_: Pin<&'a mut C>,
}

impl<'l, 'a, C, T, O> UpgradableReadFuture<'l, 'a, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(
        acquire: Pin<&'a mut Acquire<'l, T, O>>,
        cancel: Pin<&'a mut C>,
    ) -> Self {
        UpgradableReadFuture {
            acquire_: acquire,
            cancel_: cancel,
        }
    }

    async fn upgradable_read_async_(
        self: Pin<&mut Self>,
    ) -> Option<UpgradableReaderGuard<'l, 'a, T, O>> {
        let this = self.project();
        let mut acquire = unsafe {
            let ptr = this.acquire_.as_mut().get_unchecked_mut();
            NonNull::new_unchecked(ptr)
        };
        let try_upg_read = unsafe { acquire.as_mut().try_upgradable_read_() };
        if try_upg_read.is_some() {
            return try_upg_read;
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
            Option::Some(UpgradableReaderGuard::new(acquire))
        } else {
            Option::None
        }
    }
}

impl<'l, 'a, C, T, O> Future for UpgradableReadFuture<'l, 'a, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type Output = Option<UpgradableReaderGuard<'l, 'a, T, O>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let f = self.upgradable_read_async_();
        pin_mut!(f);
        f.poll(cx)
    }
}

pub struct UpgradeAsync<'a, 'g, 'u, T, O>(Pin<&'u mut Upgrade<'a, 'g, T, O>>)
where
    T: ?Sized,
    O: TrCmpxchOrderings;

impl<'a, 'g, 'u, T, O> UpgradeAsync<'a, 'g, 'u, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(upgrade: Pin<&'u mut Upgrade<'a, 'g, T, O>>) -> Self {
        let _ = upgrade.upg_ctx_.data().try_reset_context_type();
        UpgradeAsync(upgrade)
    }

    #[inline(always)]
    pub fn may_cancel_with<C>(
        self,
        cancel: Pin<&'u mut C>,
    ) -> UpgradeFuture<'a, 'g, 'u, C, T, O>
    where
        C: TrCancellationToken,
    {
        UpgradeFuture::new(self.0, cancel)
    }
}

impl<'a, 'g, 'u, T, O> IntoFuture for UpgradeAsync<'a, 'g, 'u, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type IntoFuture = UpgradeFuture<'a, 'g, 'u, NonCancellableToken, T, O>;
    type Output = <Self::IntoFuture as Future>::Output;

    #[inline(always)]
    fn into_future(self) -> Self::IntoFuture {
        let cancel = NonCancellableToken::pinned();
        UpgradeFuture::new(self.0, cancel)
    }
}

impl<'a, 'u, T, O> TrIntoFutureMayCancel<'u> for UpgradeAsync<'a, '_, 'u, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type MayCancelOutput = Option<WriterGuard<'a, 'u, T, O>>;

    #[inline]
    fn may_cancel_with<C>(
        self,
        cancel: Pin<&'u mut C>,
    ) -> impl Future<Output = Self::MayCancelOutput>
    where
        C: TrCancellationToken,
    {
        UpgradeAsync::may_cancel_with(self, cancel)
    }
}

#[pin_project]
pub struct UpgradeFuture<'a, 'g, 'u, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    upgrade_: Pin<&'u mut Upgrade<'a, 'g, T, O>>,
    cancel_: Pin<&'u mut C>,
}

impl<'a, 'g, 'u, C, T, O> UpgradeFuture<'a, 'g, 'u, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(
        upgrade: Pin<&'u mut Upgrade<'a, 'g, T, O>>,
        cancel: Pin<&'u mut C>,
    ) -> Self {
        UpgradeFuture {
            upgrade_: upgrade,
            cancel_: cancel,
        }
    }

    async fn upgrade_async_(
        self: Pin<&mut Self>,
    ) -> Option<WriterGuard<'a, 'u, T, O>> {
        let this = self.project();
        let mut upgrade = unsafe {
            let p = this.upgrade_.as_mut().get_unchecked_mut();
            NonNull::new_unchecked(p)
        };
        let mut acquire = unsafe {
            let acq = this.upgrade_.as_mut().acquire_pinned_();
            NonNull::new_unchecked(acq.get_unchecked_mut())
        };
        let try_upgrade = unsafe { acquire.as_mut().try_upgrade_() };
        if try_upgrade.is_some() {
            return try_upgrade;
        };
        let mut cancel = this.cancel_.as_mut();
        let mut slot = this.upgrade_.as_mut().slot_pinned_();
        debug_assert!(slot.is_detached());

        let mutex = unsafe {
            // Safe because the rwlock queue mutex is thread-safe
            acquire.as_ref().rwlock().queue().mutex()
        };
        let mut g = mutex.acquire().may_cancel_with(cancel.as_mut())?;
        let queue = (*g).as_mut();
        let hint = unsafe {
            let upg = Pin::new_unchecked(upgrade.as_mut());
            upg.acquire_pinned_().slot_pinned_()
        };
        if let Option::Some(mut hint) = queue.find(hint) {
            let r = hint.insert_next(slot.as_mut());
            assert!(r.is_ok())
        } else {
            let r = (*g).as_mut().push_head(slot.as_mut());
            assert!(r.is_ok());
        }
        drop(g);

        let curr_cx = slot.data();
        let peeker = curr_cx.channel().peeker();
        pin_mut!(peeker);
        let msg = peeker
            .peek_async()
            .may_cancel_with(cancel)
            .await;
        if let Result::Ok(Message::Ready) = msg {
            let r = curr_cx.try_mark_ctx_upgraded();
            assert!(
                r.is_succ(),
                "[rwlock::UpgradeFuture::upgrade_async_] try_set_ctx_upgraded",
            );
            let upgrade_pin = unsafe {
                Pin::new_unchecked(upgrade.as_mut())
            };
            Option::Some(WriterGuard::from_upgrade(upgrade_pin))
        } else {
            Option::None
        }
    }
}

impl<'a, 'u, C, T, O> Future for UpgradeFuture<'a, '_, 'u, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type Output = Option<WriterGuard<'a, 'u, T, O>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let f = self.upgrade_async_();
        pin_mut!(f);
        f.poll(cx)
    }
}
