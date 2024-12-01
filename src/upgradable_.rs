use core::{
    fmt,
    future::{Future, IntoFuture},
    mem::ManuallyDrop,
    ops::Deref,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

use pin_project::pin_project;
use pin_utils::pin_mut;

use abs_sync::{
    async_lock::{TrAcquire, TrReaderGuard, TrUpgradableReaderGuard, TrUpgrade},
    cancellation::{TrCancellationToken, TrIntoFutureMayCancel},
    never_cancel::FutureForTaskNeverCancel as FutNonCancel,
};
use atomex::TrCmpxchOrderings; 
use pincol::x_deps::{abs_sync, atomex, pin_utils};

use super::{
    acquire_::Acquire,
    contexts_::*,
    rwlock_::RwLock,
    reader_::ReaderGuard,
    upgrade_::Upgrade,
};

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
    pub fn new(mut acquire: Pin<&'a mut Acquire<'l, T, O>>) -> Self {
        init_slot(acquire.as_mut(), CtxType::Upgradable);
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
}

impl<'l, 'a, C, T, O> Future for UpgradableReadFuture<'l, 'a, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type Output = Option<UpgradableReaderGuard<'l, 'a, T, O>>;

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
                let try_upg_read = unsafe { acquire.as_mut().try_upgradable_read_() };
                if try_upg_read.is_some() {
                    return Poll::Ready(try_upg_read);
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
                let queue = (*g).as_mut();
                let mut slot = unsafe {
                    Pin::new_unchecked(slot_ptr.as_mut())
                };
                let r = queue.push_tail(slot.as_mut());
                debug_assert!(r.is_ok());

                let s = unsafe { acquire.as_ref().rwlock().state() };
                let _ = s.try_set_queue_not_empty();
                drop(g);
            }
        }
    }
}
