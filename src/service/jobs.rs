//! Async job management: submission, planning, execution, progress.
//!
//! This file is the jobs module entry point; the submodules below hold the implementation.

mod failure;
mod haruki_3d;
mod manager;
mod progress;
mod runner;
mod state;
mod store;

pub use manager::JobManager;
pub use store::{JobListEntry, JobListSummary};

#[cfg(test)]
mod test_support;
