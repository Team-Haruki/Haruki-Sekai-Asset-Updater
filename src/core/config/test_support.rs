//! Scaffolding shared by the config modules' test suites.
//!
//! `env_lock` has to be one lock for the whole crate. These tests mutate
//! process environment variables, and a module that kept its own mutex would
//! not exclude the tests in the module next door -- which is exactly the kind
//! of failure that only shows up on a loaded machine.

use std::sync::{Mutex, MutexGuard, OnceLock};

pub(super) fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

pub(super) fn restore_env(name: &str, value: Option<String>) {
    match value {
        Some(value) => std::env::set_var(name, value),
        None => std::env::remove_var(name),
    }
}
