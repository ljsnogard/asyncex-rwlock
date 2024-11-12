use core::{
    fmt,
    marker::{PhantomData, PhantomPinned},
    sync::atomic::AtomicUsize,
};

use atomex::{CmpxchResult, StrictOrderings, TrAtomicFlags, TrCmpxchOrderings};
use pincol::{
    linked_list::{PinnedList, PinnedListGuard, PinnedSlot},
    x_deps::atomex,
};

use asyncex_channel::{
    oneshot::Oneshot,
    x_deps::{atomex, pincol},
};

type StVal = usize;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) enum CtxType {
    Uninit     = 0,
    ReadOnly   = 1,
    Exclusive  = 2,
    Upgradable = 3,
}

impl CtxType {
    const fn into_st_val(self) -> StVal {
        self as StVal
    }

    const fn try_from_st_val(u: StVal) -> Result<Self, StVal> {
        match u {
            0 => Result::Ok(CtxType::Uninit),
            1 => Result::Ok(CtxType::ReadOnly),
            2 => Result::Ok(CtxType::Exclusive),
            3 => Result::Ok(CtxType::Upgradable),
            _ => Result::Err(u),
        }
    }
}

impl From<CtxType> for StVal {
    fn from(value: CtxType) -> Self {
        value.into_st_val()
    }
}

impl TryFrom<StVal> for CtxType {
    type Error = StVal;

    fn try_from(value: StVal) -> Result<Self, Self::Error> {
        CtxType::try_from_st_val(value)
    }
}

impl fmt::Display for CtxType {
    fn fmt(&self,  f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let x = match self {
            CtxType::Uninit => "Uninit",
            CtxType::ReadOnly => "ReadOnly",
            CtxType::Exclusive => "Exclusive",
            CtxType::Upgradable => "Upgradable",
        };
        write!(f, "CtxType::{x}")
    }
}

pub(super) type Channel<O> = Oneshot<Message, O>;
pub(super) type WakeSlot<O> = PinnedSlot<WaitCtx<O>, O>;
pub(super) type WakeList<O> = PinnedList<WaitCtx<O>, O>;
pub(super) type WakeListGuard<'a, 'g, O> = PinnedListGuard<'a, 'g, WaitCtx<O>, O>;

#[derive(Debug, Clone, Copy)]
pub(super) enum Message {
    Ready,
    Cancel,
}

pub(super) struct WaitCtx<O>
where
    O: TrCmpxchOrderings,
{
    _marker_: PhantomPinned,
    /// The state of the wait context, including `CtxType`
    cx_stat_: CtxState<O>,
    /// An CtxType::Exclusive ctx should have only 1 signal.
    channel_: Channel<O>,
}

impl<O> WaitCtx<O>
where
    O: TrCmpxchOrderings,
{
    pub const fn new() -> Self {
        WaitCtx {
            _marker_: PhantomPinned,
            cx_stat_: CtxState::new(),
            channel_: Channel::new(),
        }
    }

    pub fn channel(&self) -> &Channel<O> {
        &self.channel_
    }

    #[inline]
    pub fn context_type(&self) -> CtxType {
        self.cx_stat_.context_type()
    }

    #[inline]
    pub fn can_signal(&self) -> bool {
        let x = self.channel_.try_peek();
        matches!(x, Result::Ok(Option::None))
    }

    #[inline]
    pub fn try_init_context_type(&self, ctx_type: CtxType) -> bool {
        self.cx_stat_.try_init_context_type(ctx_type)
    }

    #[inline]
    pub fn try_reset_context_type(&self) -> bool {
        self.cx_stat_.try_reset_context_type()
    }

    pub fn try_invalidate(&self) -> CmpxchResult<StVal> {
        #[cfg(test)]log::trace!("{self:?}");
        self.cx_stat_.try_invalidate()
    }
    pub fn try_validate(&self) -> CmpxchResult<StVal> {
        #[cfg(test)]log::trace!("{self:?}");
        self.cx_stat_.try_validate()
    }

    #[inline]
    pub fn is_invalidated(&self) -> bool {
        self.cx_stat_.is_invalidated()
    }

    #[inline]
    pub fn try_mark_ctx_upgraded(&self) -> CmpxchResult<StVal> {
        self.cx_stat_.try_mark_ctx_upgraded()
    }
    #[inline]
    pub fn is_upgraded_writer_ctx(&self) -> bool {
        let s = self.cx_stat_.value();
        let u = CtxStConf::load_flag_ctx_type(s);
        let Result::Ok(t) = CtxType::try_from_st_val(u) else {
            return false;
        };
        CtxStConf::expect_ctx_upgraded(s) && matches!(t, CtxType::Exclusive)
    }

    #[inline]
    pub fn try_set_ctx_cancelled(&self) -> bool {
        self.channel_.try_send(Message::Cancel).is_ok()
    }

    /// Update the context state from `ReadOnly` to `Upgradable`
    #[allow(dead_code)]
    #[inline]
    pub fn try_upgrade_to_upgradable(&self) -> bool {
        self.cx_stat_.try_upgrade_to_upgradable()
    }

    #[inline]
    pub fn try_downgrade_upgradable_to_readonly(&self) -> bool {
        self.cx_stat_.try_downgrade_upgradable_to_readonly()
    }

    #[inline]
    pub fn try_downgrade_exclusive_to_readonly(&self) -> bool {
        self.cx_stat_.try_downgrade_exclusive_to_readonly()
    }

    #[inline]
    pub fn try_downgrade_exclusive_to_upgradable(&self) -> bool {
        self.cx_stat_.try_downgrade_exclusive_to_upgradable()
    }
}

impl<O> fmt::Debug for WaitCtx<O>
where
    O: TrCmpxchOrderings,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.cx_stat_.value();
        let v = CtxStConf::load_flag_ctx_type(s);
        let t = CtxType::try_from_st_val(v).unwrap();
        let c = CtxStConf::load_reader_count(s);
        let i = CtxStConf::expect_invalid(s);
        write!(f,
            "[WaitCtx({self:p}):\
            ctx_type({t:?}), \
            invalid({i}), \
            use_count({c})]")
    }
}

/// Waker Context State Config
#[derive(Debug)]
struct CtxStConf(core::convert::Infallible);

impl CtxStConf {
    /// The waker ctx type mask is 2 bits wide
    const K_CTX_SHIFT_BIT: u32 = StVal::BITS - 2;
    const K_CTX_TYPE_MASK: StVal = 0b0011 << Self::K_CTX_SHIFT_BIT;

    // The WaitCtx is invalidated and pending for detach
    const K_CTX_INVALID_FLAG: StVal = 1 << (StVal::BITS - 3);

    // The WaitCtx is Exclusive and was upgrgaded from Upgradable
    const K_CTX_UPGRADE_FLAG: StVal = Self::K_CTX_INVALID_FLAG >> 1;

    const K_USE_COUNT_MASK: StVal = Self::K_CTX_UPGRADE_FLAG - 1;

    const fn expect_ctx_type_uninit(s: StVal) -> bool {
        let r = CtxType::try_from_st_val(Self::load_flag_ctx_type(s));
        let Result::Ok(t) = r else { return false; };
        matches!(t, CtxType::Uninit)
    }
    const fn expect_ctx_type_initialized(s: StVal) -> bool {
        !Self::expect_ctx_type_uninit(s)
    }
    const fn expect_ctx_type_readonly(s: StVal) -> bool {
        let r = CtxType::try_from_st_val(Self::load_flag_ctx_type(s));
        let Result::Ok(t) = r else { return false; };
        matches!(t, CtxType::ReadOnly)
    }
    const fn expect_ctx_type_upgradable(s: StVal) -> bool {
        let r = CtxType::try_from_st_val(Self::load_flag_ctx_type(s));
        let Result::Ok(t) = r else { return false; };
        matches!(t, CtxType::Upgradable)
    }
    const fn expect_ctx_type_exclusive(s: StVal) -> bool {
        let r = CtxType::try_from_st_val(Self::load_flag_ctx_type(s));
        let Result::Ok(t) = r else { return false; };
        matches!(t, CtxType::Exclusive)
    }
    const fn desire_ctx_type_upgradable(s: StVal) -> StVal {
        const UPGRADABLE_FLAG: StVal =
            CtxStConf::make_flag_ctx_type(CtxType::Upgradable);
        s & (!Self::K_CTX_TYPE_MASK) | UPGRADABLE_FLAG
    }
    const fn desire_ctx_type_readonly(s: StVal) -> StVal {
        const READONLY_FLAG: StVal =
            CtxStConf::make_flag_ctx_type(CtxType::ReadOnly);
        s & (!Self::K_CTX_TYPE_MASK) | READONLY_FLAG
    }
    const fn desire_ctx_type_uninit(s: StVal) -> StVal {
        const UNINIT_FLAG: StVal =
            CtxStConf::make_flag_ctx_type(CtxType::Uninit);
        s & (!Self::K_CTX_TYPE_MASK) | UNINIT_FLAG
    }
    const fn load_flag_ctx_type(s: StVal) -> StVal {
        (s & Self::K_CTX_TYPE_MASK) >> Self::K_CTX_SHIFT_BIT
    }
    const fn make_flag_ctx_type(ctx_type: CtxType) -> StVal {
        ctx_type.into_st_val() << Self::K_CTX_SHIFT_BIT
    }

    const fn expect_invalid(s: StVal) -> bool {
        s & Self::K_CTX_INVALID_FLAG == Self::K_CTX_INVALID_FLAG
    }
    const fn expect_valid(s: StVal) -> bool {
        s & Self::K_CTX_INVALID_FLAG == 0
    }
    const fn desire_invalid(s: StVal) -> StVal {
        s | Self::K_CTX_INVALID_FLAG
    }
    const fn desire_valid(s: StVal) -> StVal {
        s & (!Self::K_CTX_INVALID_FLAG)
    }

    const fn expect_ctx_upgraded(s: StVal) -> bool {
        s & Self::K_CTX_UPGRADE_FLAG == Self::K_CTX_UPGRADE_FLAG
    }
    const fn expect_ctx_non_upgraded(s: StVal) -> bool {
        !Self::expect_ctx_upgraded(s)
    }
    const fn desire_ctx_upgraded(s: StVal) -> StVal {
        s | Self::K_CTX_UPGRADE_FLAG
    }

    const fn load_reader_count(s: StVal) -> StVal {
        s & Self::K_USE_COUNT_MASK
    }
    const fn expect_inc_count_valid(s: StVal) -> bool {
        Self::load_reader_count(s) < Self::K_USE_COUNT_MASK
    }
    const fn expect_dec_count_valid(s: StVal) -> bool {
        Self::load_reader_count(s) > 0
    }
    const fn desire_inc_reader_count(s: StVal) -> StVal {
        s + 1
    }
    const fn desire_dec_reader_count(s: StVal) -> StVal {
        s - 1
    }
}

#[derive(Debug)]
struct CtxState<O = StrictOrderings>(AtomicUsize, PhantomData<O>)
where
    O: TrCmpxchOrderings;

impl<O: TrCmpxchOrderings> AsRef<AtomicUsize> for CtxState<O> {
    fn as_ref(&self) -> &AtomicUsize {
        &self.0
    }
}

impl<O: TrCmpxchOrderings> TrAtomicFlags<usize, O> for CtxState<O>
{}

impl<O: TrCmpxchOrderings> CtxState<O> {
    pub const fn new() -> Self {
        let init = CtxStConf::make_flag_ctx_type(CtxType::Uninit);
        Self(AtomicUsize::new(init), PhantomData)
    }

    #[inline]
    pub fn try_init_context_type(&self, ctx_type: CtxType) -> bool {
        let partial_flag: StVal = CtxStConf::make_flag_ctx_type(ctx_type);
        let desire = |s| s & (!CtxStConf::K_CTX_TYPE_MASK) | partial_flag;
        self.try_spin_compare_exchange_weak(
                CtxStConf::expect_ctx_type_uninit,
                desire)
            .is_succ()
    }

    #[inline]
    pub fn try_reset_context_type(&self) -> bool {
        self.try_spin_compare_exchange_weak(
                CtxStConf::expect_ctx_type_initialized,
                CtxStConf::desire_ctx_type_uninit)
            .is_succ()
    }

    #[inline]
    pub fn context_type(&self) -> CtxType {
        let u = CtxStConf::load_flag_ctx_type(self.value());
        let Result::Ok(t) = CtxType::try_from_st_val(u) else {
            unreachable!()
        };
        t
    }

    #[inline]
    pub fn is_invalidated(&self) -> bool {
        CtxStConf::expect_invalid(self.value())
    }

    #[inline]
    pub fn try_invalidate(&self) -> CmpxchResult<StVal> {
        let expect = CtxStConf::expect_valid;
        let desire = CtxStConf::desire_invalid;
        self.try_spin_compare_exchange_weak(expect, desire)
    }
    #[inline]
    pub fn try_validate(&self) -> CmpxchResult<StVal> {
        let expect = CtxStConf::expect_inc_count_valid;
        let desire = CtxStConf::desire_valid;
        self.try_spin_compare_exchange_weak(expect, desire)
    }

    #[inline]
    pub fn try_mark_ctx_upgraded(&self) -> CmpxchResult<StVal> {
        let expect = CtxStConf::expect_ctx_non_upgraded;
        let desire = CtxStConf::desire_ctx_upgraded;
        self.try_spin_compare_exchange_weak(expect, desire)
    }

    #[inline]
    pub fn try_upgrade_to_upgradable(&self) -> bool {
        self.try_spin_compare_exchange_weak(
                CtxStConf::expect_ctx_type_readonly,
                CtxStConf::desire_ctx_type_upgradable)
            .is_succ()
    }

    #[inline]
    pub fn try_downgrade_upgradable_to_readonly(&self) -> bool {
        self.try_spin_compare_exchange_weak(
                CtxStConf::expect_ctx_type_upgradable,
                CtxStConf::desire_ctx_type_readonly)
            .is_succ()
    }

    #[inline]
    pub fn try_downgrade_exclusive_to_readonly(&self) -> bool {
        self.try_spin_compare_exchange_weak(
                CtxStConf::expect_ctx_type_exclusive,
                CtxStConf::desire_ctx_type_readonly)
            .is_succ()
    }

    #[inline]
    pub fn try_downgrade_exclusive_to_upgradable(&self) -> bool {
        self.try_spin_compare_exchange_weak(
                CtxStConf::expect_ctx_type_exclusive,
                CtxStConf::desire_ctx_type_upgradable)
            .is_succ()
    }

    #[allow(dead_code)]
    #[deprecated(note = "Algo updated: Readers now have individual contexts")]
    #[inline]
    pub fn reader_count(&self) -> usize {
        CtxStConf::load_reader_count(self.value())
    }

    #[allow(dead_code)]
    #[deprecated(note = "Algo updated: Readers now have individual contexts")]
    pub fn increase_reader_count(&self) -> usize {
        let f = self.try_spin_compare_exchange_weak(
            CtxStConf::expect_inc_count_valid,
            CtxStConf::desire_inc_reader_count,
        );
        let CmpxchResult::Succ(f) = f else {
            unreachable!("[WakerCtxState::increase_reader_count]")
        };
        CtxStConf::load_reader_count(f)
    }

    #[allow(dead_code)]
    #[deprecated(note = "Algo updated: Readers now have individual contexts")]
    pub fn decrease_reader_count(&self) -> usize {
        let f = self.try_spin_compare_exchange_weak(
            CtxStConf::expect_dec_count_valid,
            CtxStConf::desire_dec_reader_count,
        );
        let CmpxchResult::Succ(f) = f else {
            unreachable!("[WakerCtxState::decrease_reader_count]");
        };
        CtxStConf::load_reader_count(f)
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;

    fn assure_send<T: Send>(t: T) -> T { t }
    fn assure_sync<T: Sync>(t: T) -> T { t }

    #[test]
    fn ctx_conf_flag_smoke() {
        let readonly_flags = CtxStConf::make_flag_ctx_type(CtxType::ReadOnly);
        let readonly = CtxType
            ::try_from_st_val(CtxStConf::load_flag_ctx_type(readonly_flags))
            .expect("");
        assert_eq!(CtxType::ReadOnly, readonly);

        let upgradable_flags = CtxStConf::make_flag_ctx_type(CtxType::Upgradable);
        let upgradable = CtxType
            ::try_from_st_val(CtxStConf::load_flag_ctx_type(upgradable_flags))
            .expect("");
        assert_eq!(CtxType::Upgradable, upgradable);

        assert!(CtxStConf::expect_ctx_type_readonly(
            CtxStConf::desire_ctx_type_readonly(upgradable_flags)
        ));
        assert!(CtxStConf::expect_ctx_type_upgradable(
            CtxStConf::desire_ctx_type_upgradable(readonly_flags)
        ));
    }

    #[test]
    fn ctx_type_smoke() {
        let s = CtxState::<StrictOrderings>::new();
        assert!(!s.is_invalidated());
        assert_eq!(s.context_type(), CtxType::Uninit);
        assert!(!s.try_reset_context_type());

        assert!(s.try_init_context_type(CtxType::ReadOnly));
        assert!(!s.try_init_context_type(CtxType::Uninit));

        assert!(s.try_reset_context_type());
        assert!(!s.try_reset_context_type());

        assert_eq!(s.context_type(), CtxType::Uninit);
    }

    #[test]
    fn ctx_smoke() {
        let ctx = CtxState::<StrictOrderings>::new();
        let ctx = assure_send(ctx);
        let ctx = assure_sync(ctx);
        assert_eq!(ctx.context_type(), CtxType::ReadOnly);

        // assert_eq!(ctx.reader_count(), 0);
        // assert_eq!(ctx.increase_reader_count(), 0);
        // assert_eq!(ctx.increase_reader_count(), 1);
        // assert_eq!(ctx.decrease_reader_count(), 2);

        assert!(ctx.try_upgrade_to_upgradable());
        assert_eq!(ctx.context_type(), CtxType::Upgradable);

        assert!(ctx.try_downgrade_upgradable_to_readonly());
        assert_eq!(ctx.context_type(), CtxType::ReadOnly);
    }
}
