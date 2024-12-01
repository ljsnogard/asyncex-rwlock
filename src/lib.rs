//! An asynchronous reader-writer lock with no heap memory allocation.

#![no_std]
#![feature(type_alias_impl_trait)]
#![feature(try_trait_v2)]

#[cfg(test)]extern crate std;

mod acquire_;
mod contexts_;
mod rwlock_;
mod reader_;
mod writer_;
mod upgradable_;
mod upgrade_;

pub use acquire_::Acquire;
pub use rwlock_::RwLock;
pub use reader_::{ReaderGuard, ReadAsync, ReadFuture};
pub use upgradable_::{
    UpgradableReaderGuard, UpgradableReadAsync, UpgradableReadFuture,
};
pub use upgrade_::{Upgrade, UpgradeAsync, UpgradeFuture};
pub use writer_::{WriterGuard, WriteAsync, WriteFuture};

#[cfg(test)]mod tests_;

pub mod x_deps {
    pub use pincol;

    pub use pincol::x_deps::{abs_sync, atomex, atomic_sync};
}
