use std::{
    borrow::BorrowMut,
    pin::Pin,
    ptr,
    sync::Arc,
};

use pin_utils::pin_mut;

use async_channel::{Receiver, Sender};

use abs_sync::cancellation::{NonCancellableToken, TrCancellationToken};
use asyncex_cancel::CancellationTokenSource;
use atomex::StrictOrderings;
use core_malloc::CoreAlloc;
use mm_ptr::{Shared, XtMallocShared};
use spmv_oneshot::{
    self,
    x_deps::{abs_sync, atomex, pin_utils}, Oneshot,
};

use super::impl_::*;

type TestAlloc = CoreAlloc;

fn init_env_logger_() {
    let r = env_logger::builder().is_test(true).try_init();
    assert!(r.is_ok())
}

#[test]
fn rwlock_state_default_smoke() {
    let state = RwLockState::<StrictOrderings>::new();
    assert!(!state.is_acquired());

    let incr_res = state.increase_reader_count();
    assert_eq!(incr_res.unwrap(), 0usize);

    let decr_res = state.decrease_reader_count();
    assert!(decr_res.is_err());
    assert_eq!(decr_res.unwrap_err(), StateReport::LastReader);

    assert!(state.try_set_writer_acquired(true));
    assert!(state.is_writer_acquired());

    assert!(state.try_set_writer_acquired(false));
    assert!(!state.is_writer_acquired());
}

/// Smoke test reader lock and upgradable reader lock without enqueued context
#[tokio::test]
async fn rwlock_no_queue_read_upg_smoke() {
    init_env_logger_();

    log::trace!("[rwlock_light_weight_read_upg_smoke]");

    let rwlock = RwLock::<_, StrictOrderings>::new(1usize);
    let state = rwlock.state();

    assert!(!rwlock.is_acquired());
    assert!(state.is_queue_empty());

    let acq1 = rwlock.acquire();
    pin_mut!(acq1);
    let reader1 = acq1.as_mut().read_async().await;

    assert_eq!(state.reader_count(), 1usize);

    assert!(rwlock.is_acquired());
    assert!(state.is_queue_empty());
    assert_eq!(*reader1, 1usize);

    // cannot borrow `acq1` as mutable more than once at a time
    // let _ = acq1.as_mut().try_upgradable_read().unwrap();

    let acq2 = rwlock.acquire();
    pin_mut!(acq2);
    assert!(acq2.as_mut().try_read().is_some());
    assert!(acq2.as_mut().try_write().is_none());
    assert!(!state.is_writer_acquired()); // try_write failure shall not affect state

    let upgradable_reader = acq2.try_upgradable_read().unwrap();
    assert_eq!(state.reader_count(), 2usize);
    assert!(state.is_queue_empty());

    let reader2 = upgradable_reader.downgrade();
    assert_eq!(state.reader_count(), 2usize);
    assert!(state.is_queue_empty());

    assert_eq!(*reader1, *reader2);
    unsafe {
        let a1 = reader1.acq_ptr();
        let a2 = reader2.acq_ptr();
        assert_ne!(a1, a2);

        let slot1 = (*a1).slot_();
        let slot2 = (*a2).slot_();
        assert!(!ptr::eq(slot1, slot2));

        let ctx1 = slot1.data();
        let ctx2 = slot2.data();
        assert!(!ptr::eq(ctx1, ctx2));
    }
    drop(reader2);
    assert_eq!(state.reader_count(), 1usize);
    assert!(rwlock.is_acquired());

    drop(reader1);
    assert_eq!(state.reader_count(), 0usize);
    assert!(!rwlock.is_acquired());
}

async fn read_<Tc>(
    step: usize,
    count: usize,
    rwlock: Shared<RwLock<usize>, TestAlloc>,
    sender: Sender<usize>,
    mut cancel: Pin<&mut Tc>)
where
    Tc: TrCancellationToken,
{
    let mut i = 0usize;
    let acq = rwlock.acquire();
    pin_mut!(acq);
    // log::info!("read_ starts loop");
    loop {
        let Option::Some(reader) = acq
            .as_mut()
            .read_async()
            .may_cancel_with(cancel.as_mut())
            .await
        else {
            break;
        };
        let c = *reader;
        if c >= step {
            assert!(sender.send(step).await.is_ok());
            i += 1;
        }
        if i >= count {
            break;
        }
    }
}

async fn upgradable_read_<Tc>(
    rwlock: Shared<RwLock<usize>, TestAlloc>,
    recver: Receiver<usize>,
    mut cancel: Pin<&mut Tc>)
where
    Tc: TrCancellationToken,
{
    // log::info!("upgradable_read_ starts loop");
    let acq = rwlock.acquire();
    pin_mut!(acq);
    let tok_cloned = cancel.clone();
    pin_mut!(tok_cloned);
    loop {
        if recver.is_closed() {
            break;
        }
        let step = recver.recv().await.unwrap();
        let Option::Some(upg_reader) = acq
            .as_mut()
            .upgradable_read_async()
            .may_cancel_with(tok_cloned.as_mut())
            .await
        else {
            break;
        };
        let c = *upg_reader;
        if c >= step {
            let upg = upg_reader.upgrade();
            pin_mut!(upg);
            let Option::Some(mut writer) = upg
                .upgrade_async()
                .may_cancel_with(cancel.as_mut())
                .await
            else {
                break;
            };
            assert_eq!(*writer, c);
            *writer = c - step;
        }
    }
}

async fn write_<Tc>(
    count: usize,
    rwlock: Shared<RwLock<usize>, TestAlloc>,
    mut cancel: Pin<&mut Tc>)
where
    Tc: TrCancellationToken,
{
    let mut i = 0usize;
    let acq = rwlock.acquire();
    pin_mut!(acq);
    // log::info!("write_ starts loop");
    loop {
        let Option::Some(mut writer) = acq
            .as_mut()
            .write_async()
            .may_cancel_with(cancel.as_mut())
            .await
        else {
            break;
        };
        // log::info!("#{}, {}", i, *writer);
        *writer += 1;
        i += 1;
        if i >= count {
            break;
        }
    }
}

#[tokio::test]
async fn rwlock_read_write_upgrade_async_smoke() {
    use tokio::task::spawn as spawn;

    init_env_logger_();

    let rwlock = TestAlloc::default()
        .shared(RwLock::<_, StrictOrderings>::new(1usize));
    let (tx, rx) = async_channel::unbounded::<usize>();

    const STEP: usize = 5;
    const READER_LOOP_COUNT: usize = 100;
    const WRITER_LOOP_COUNT: usize = 1024;

    let writer_task = spawn(write_(
        WRITER_LOOP_COUNT,
        rwlock.clone(),
        NonCancellableToken::pinned(),
    ));
    let reader_task = spawn(read_(
        STEP,
        READER_LOOP_COUNT,
        rwlock.clone(),
        tx,
        NonCancellableToken::pinned(),
    ));
    let upg_reader_task = spawn(upgradable_read_(
        rwlock,
        rx,
        NonCancellableToken::pinned(),
    ));

    assert!(writer_task.await.is_ok());
    assert!(reader_task.await.is_ok());
    assert!(upg_reader_task.await.is_ok());
}

#[tokio::test]
async fn upgradable_guard_downgrade_should_work() {
    let lock = RwLock::<_, StrictOrderings>::new(1);
    let acq1 = lock.acquire();
    pin_mut!(acq1);
    let reader1 = acq1.upgradable_read_async().await;
    assert_eq!(*reader1, 1);

    let acq2 = lock.acquire();
    pin_mut!(acq2);
    assert!(acq2.as_mut().try_read().is_some());
    assert!(acq2.as_mut().try_upgradable_read().is_none());

    let reader = reader1.downgrade();
    assert!(acq2.as_mut().try_upgradable_read().is_some());
    assert!(acq2.as_mut().try_read().is_some());
    assert!(acq2.as_mut().try_write().is_none());
    drop(reader)
}

#[tokio::test]
async fn writer_guard_downgrade_to_reader_guard_should_work() {
    let rwlock = RwLock::<_, StrictOrderings>::new(1);
    let acq1 = rwlock.acquire();
    pin_mut!(acq1);

    let mut writer = acq1.write_async().await;
    *writer += 1;

    let acq2 = rwlock.acquire();
    pin_mut!(acq2);
    assert!(acq2.as_mut().try_read().is_none());
    assert!(acq2.as_mut().try_write().is_none());

    let reader = writer.downgrade();
    assert_eq!(*reader, 2);
    assert!(acq2.as_mut().try_read().is_some());
    assert!(acq2.as_mut().try_write().is_none());
    drop(reader)
}

#[tokio::test]
async fn writer_guard_downgrade_to_upgradable_should_work() {
    let rwlock = RwLock::<_, StrictOrderings>::new(1);
    let acq1 = rwlock.acquire();
    pin_mut!(acq1);
    // Acquire a WriterGuard and increase the number by 1
    let mut writer = acq1.write_async().await;
    *writer += 1;

    let acq2 = rwlock.acquire();
    pin_mut!(acq2);

    // The WriterGuard is not released, `try_read` SHOULD return `None`
    assert!(acq2.as_mut().try_read().is_none());

    let upgradable = writer.downgrade_to_upgradable();

    // Make sure the upgradable can read the correct value
    assert_eq!(*upgradable, 2);
    let upg = upgradable.upgrade();
    pin_mut!(upg);

    // The UpgradableReaderGuard still alive, `try_write` SHOULD return `None``
    assert!(acq2.as_mut().try_write().is_none());

    // The UpgradableReaderGuard still alive, with no other enqueued contenders,
    // `try_read` SHOULD return `Some`
    assert!(acq2.as_mut().try_read().is_some());

    // 上一行获取读锁成功后马上释放，因此可升级锁升级为写锁的尝试，应该成功
    assert!(upg.try_upgrade().is_some());
}

/// ## 测试目标
/// `CancellationToken` 触发时，能否顺利取消已排队的读锁请求，并顺利启动后续的写锁请求
/// 
/// ## 测试步骤
/// 1. 启动一个获取写锁并一直持有该写锁的任务，该任务会在收到一个释放信号后才结束;
/// 2. 启动一个尝试获取读锁的任务，该任务在获得读锁或者从 `CancellationToken` 接收到取消
/// 信号后才结束;
/// 3. 启动一个尝试获得写锁的任务，用于验证后续请求是否会在步骤2 所述任务被取消后顺利启动;
/// 4. 向步骤2 的任务发送取消信号;
/// 5. 向步骤1 的任务发送继续信号，并验证步骤3 的任务是否顺利获得写锁。
#[tokio::test]
async fn cancelling_enqueued_reader_should_wake_following_queued_writer() {
    let _ = env_logger::builder().is_test(true).try_init();

    const ACQUIRE_FAIL: usize = 0usize;
    const ACQUIRE_SUCC: usize = 1usize;

    async fn writer1_blocker_(
        start_signal_send: Sender<()>,
        hold_signal_send: Sender<usize>,
        exit_signal_recv: Receiver<usize>,
        rwlock: Arc<RwLock<()>>,
    ) {
        log::trace!("[writer1_blocker_] start");
        let x = start_signal_send.send(()).await;
        log::trace!("[writer1_blocker_] start_signal_send result: {x:?}");
        assert!(x.is_ok());

        let acq = rwlock.acquire();
        pin_mut!(acq);
        let g = acq.write_async().await;
        log::trace!("[writer1_blocker_] rwlock.write_async result: {g:?}");
        let x = hold_signal_send.send(0usize).await;
        assert!(x.is_ok());
        log::trace!("[writer1_blocker_] hold_signal_send result: {x:?}");

        let x = exit_signal_recv.recv().await;
        assert!(x.is_ok());
        drop(g);
        log::trace!("[writer1_blocker_] exit");
    }

    async fn reader2_cancel_<Bc, Tc>(
        start_signal_send: Sender<()>,
        exit_signal_send: Sender<usize>,
        rwlock: Arc<RwLock<()>>,
        mut cancel: Bc)
    where
        Bc: BorrowMut<Tc>,
        Tc: TrCancellationToken,
    {
        log::trace!("[reader2_cancel_] before start_signal_send");
        let x = start_signal_send.send(()).await;
        log::trace!("[reader2_cancel_] start_signal_send result: {x:?}");
        assert!(x.is_ok());

        let mut tok = unsafe { Pin::new_unchecked(cancel.borrow_mut()) };
        let acq = rwlock.acquire();
        pin_mut!(acq);
        let o = acq
            .read_async()
            .may_cancel_with(tok.as_mut())
            .await;
        log::trace!("[reader2_cancel_] after read_async {o:?}");
        let s = if o.is_some() {
            ACQUIRE_SUCC
        } else {
            ACQUIRE_FAIL
        };
        let x = exit_signal_send.send(s).await;
        assert!(x.is_ok());
    }

    async fn writer3_follower_<Bc, Tc>(
        start_signal_send: Sender<()>,
        exit_signal3_send: Sender<usize>,
        rwlock: Arc<RwLock<()>>,
        mut cancel: Bc)
    where
        Bc: BorrowMut<Tc>,
        Tc: TrCancellationToken,
    {
        let x = start_signal_send.send(()).await;
        assert!(x.is_ok());

        let mut tok = unsafe { Pin::new_unchecked(cancel.borrow_mut()) };
        let acq = rwlock.acquire();
        pin_mut!(acq);
        let r = acq.write_async().may_cancel_with(tok.as_mut()).await;
        let s = if r.is_some() {
            ACQUIRE_SUCC
        } else {
            ACQUIRE_FAIL
        };
        let x = exit_signal3_send.send(s).await;
        assert!(x.is_ok());
    }

    let rwlock = Arc::new(RwLock::new(()));

    // 1
    let rwlock_cloned = rwlock.clone();
    let (start_signal_send, start_signal_recv) = async_channel::bounded::<()>(1);
    let (hold_signal1_send, hold_signal1_recv) = async_channel::bounded::<usize>(1);
    let (exit_signal1_send, exit_signal1_recv) = async_channel::bounded::<usize>(1);
    let writer1 = tokio::task::spawn(writer1_blocker_(
        start_signal_send, 
        hold_signal1_send,
        exit_signal1_recv, rwlock_cloned,
    ));
    // make sure the writer task is started.
    log::trace!("[writer1] before start_signal_recv");
    let started = start_signal_recv.recv().await;
    log::trace!("[writer1] started result {started:?}");
    assert!(started.is_ok());

    // make sure the writer guard of the rwlock is acquired
    log::trace!("[writer1] before hold_signal1_recv");
    let held_signal = hold_signal1_recv.recv().await.unwrap();
    log::trace!("[writer1] held_signal {held_signal}");
    assert_eq!(held_signal, 0usize);

    // 2
    let reader_cts = CancellationTokenSource::try_new(
        Shared::new(
            Oneshot::<(), StrictOrderings>::new(),
            TestAlloc::new(),
        ),
        Shared::strong_count,
        Shared::weak_count,
    ).unwrap();
    let reader_cancel = reader_cts.child_token();
    assert!(!reader_cancel.is_cancelled());
    let rwlock_cloned = rwlock.clone();
    let (start_signal_send, start_signal_recv) = async_channel::bounded::<()>(1);
    let (exit_signal2_send, exit_signal2_recv) = async_channel::bounded::<usize>(1);
    let reader2 = tokio::task::spawn(reader2_cancel_(
        start_signal_send, 
        exit_signal2_send,
        rwlock_cloned,
        reader_cancel,
    ));
    // make sure the reader task is started.
    log::trace!("[reader2] before start_signal_recv");
    let start_signal = start_signal_recv.recv().await;
    assert!(start_signal.is_ok());
    // to validate if the reader guard is acquired (it should not be acquired)
    assert!(exit_signal2_recv.try_recv().is_err());

    // 3
    let follower_cts = CancellationTokenSource::try_new(
        Shared::new(
            Oneshot::<(), StrictOrderings>::new(),
            TestAlloc::new(),
        ),
        Shared::strong_count,
        Shared::weak_count,
    ).unwrap();
    let follower_cancel = follower_cts.child_token();
    let rwlock_cloned = rwlock.clone();
    let (start_signal_send, start_signal_recv) = async_channel::bounded::<()>(1);
    let (exit_signal3_send, exit_signal3_recv) = async_channel::bounded::<usize>(1);
    let writer3 = tokio::task::spawn(writer3_follower_(
        start_signal_send, exit_signal3_send, rwlock_cloned, follower_cancel,
    ));
    // make sure the follower task is started.
    log::trace!("[writer3] before start_signal_recv");
    let start_signal = start_signal_recv.recv().await;
    assert!(start_signal.is_ok());
    // make sure the writer guard of the rwlock is NOT acquired
    assert!(exit_signal3_recv.try_recv().is_err());

    // 4
    assert!(reader_cts.try_cancel());
    log::trace!("[reader2] after reader_cts.try_cancel");
    let r = exit_signal2_recv.recv().await.unwrap();
    assert_eq!(r, ACQUIRE_FAIL);
    log::trace!("[reader2] before reader2.await");
    let _ = reader2.await;

    // 5
    let x = exit_signal1_send.send(0usize).await;
    assert!(x.is_ok());
    assert!(writer1.await.is_ok());
    let recv = exit_signal3_recv.recv().await;
    let Result::Ok(r) = recv else {
        log::trace!("{}", recv.err().unwrap());
        panic!()
    };
    assert_eq!(r, ACQUIRE_SUCC);
    assert!(writer3.await.is_ok());
}
