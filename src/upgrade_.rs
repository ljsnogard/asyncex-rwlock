use core::{
    future::{Future, IntoFuture},
    ops::Try,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

use pin_project::pin_project;
use pin_utils::pin_mut;

use abs_sync::{
    async_lock::{TrAcquire, TrUpgrade},
    cancellation::{
        NonCancellableToken, TrCancellationToken, TrIntoFutureMayCancel,
    },
};
use atomex::TrCmpxchOrderings; 
use pincol::x_deps::{abs_sync, atomex, pin_utils};

use super::{
    acquire_::Acquire,
    contexts_::*,
    writer_::WriterGuard,
    upgradable_::UpgradableReaderGuard,
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
        unsafe {
            let mut this_mut = NonNull::new_unchecked(self.get_unchecked_mut());
            let mut upg_pin = Pin::new_unchecked(this_mut.as_mut());
            let mut acq_ptr = NonNull::new_unchecked(upg_pin.as_mut().acquire_pinned_().get_unchecked_mut());
            acq_ptr.as_mut().try_upgrade_(upg_pin)
        }
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

impl<'a, T, O> AsPinnedMut<WakeSlot<O>> for Upgrade<'a, '_, T, O>
where
    T: 'a + ?Sized,
    O: TrCmpxchOrderings,
{
    fn as_pinned_mut(self: Pin<&mut Self>) -> Pin<&mut WakeSlot<O>> {
        Self::slot_pinned_(self)
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

pub struct UpgradeAsync<'a, 'g, 'u, T, O>(Pin<&'u mut Upgrade<'a, 'g, T, O>>)
where
    T: ?Sized,
    O: TrCmpxchOrderings;

impl<'a, 'g, 'u, T, O> UpgradeAsync<'a, 'g, 'u, T, O>
where
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(mut upgrade: Pin<&'u mut Upgrade<'a, 'g, T, O>>) -> Self {
        init_slot(upgrade.as_mut(), CtxType::Exclusive);
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
}

impl<'a, 'u, C, T, O> Future for UpgradeFuture<'a, '_, 'u, C, T, O>
where
    C: TrCancellationToken,
    T: ?Sized,
    O: TrCmpxchOrderings,
{
    type Output = Option<WriterGuard<'a, 'u, T, O>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut upgrade = unsafe {
            let p = this.upgrade_.as_mut().get_unchecked_mut();
            NonNull::new_unchecked(p)
        };
        let mut acquire = unsafe {
            let acq = this.upgrade_.as_mut().acquire_pinned_();
            NonNull::new_unchecked(acq.get_unchecked_mut())
        };
        let mut cancel = this.cancel_.as_mut();
        let mut slot_ptr = unsafe {
            let slot_pin = this.upgrade_.as_mut().slot_pinned_();
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
                let try_upgrade = unsafe {
                    let upg = Pin::new_unchecked(upgrade.as_mut());
                    acquire.as_mut().try_upgrade_(upg)
                };
                if try_upgrade.is_some() {
                    return Poll::Ready(try_upgrade);
                };
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
            }
        }
    }
}
