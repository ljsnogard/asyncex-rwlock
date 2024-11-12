//! An asynchronous reader-writer lock with no heap memory allocation.

#![no_std]
#![feature(type_alias_impl_trait)]
#![feature(try_trait_v2)]

mod contexts_;
mod impl_;
mod reader_;
mod writer_;
mod upgrade_;

pub use impl_::{Acquire, RwLock};
pub use reader_::{ReaderGuard, ReadAsync, ReadFuture};
pub use upgrade_::{
    UpgradableReaderGuard, UpgradableReadAsync, UpgradableReadFuture,
    Upgrade, UpgradeAsync, UpgradeFuture,
};
pub use writer_::{WriterGuard, WriteAsync, WriteFuture};

#[cfg(test)]mod tests_;

pub mod x_deps {
    pub use async_channel;

    pub use async_channel::x_deps::{
        abs_sync, atomex, atomic_sync, mm_ptr, pincol,
    };
}
