# asyncex_rwlock

A queue based asynchronous reader-writer lock with no heap memory allocation 
(yet the asynchronous-runtime it runs within, say `tokio`, may use heap memory), 
working within `no_std` environment and on arbitrary asynchronous runtime
(`tokio`, `smol`, `async_std`), implementing traits in `abs_sync`.

