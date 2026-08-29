//! Configuration: shape, loading, environment overrides, validation, tuning.
//!
//! This file is the facade. Every type is still reachable as
//! `crate::core::config::Thing`; the submodules below hold the definitions.

mod env;
mod load;
mod pipeline;
mod schema;
mod tuning;
mod validate;

pub use pipeline::pipeline_options;
pub use schema::*;

#[cfg(test)]
mod test_support;
