//! Extensions and utility layers for OpenDAL.
//!
//! This crate builds on top of `opendal`, providing additional layers,
//! configuration helpers, and virtual filesystem functionality while
//! re-exporting the entire OpenDAL API.

#![warn(missing_docs)]

/// Configuration types and helpers for constructing OpenDAL operators.
pub mod config;

/// Write quota layer for limiting the amount of data written through an operator.
pub mod quota;

/// Virtual filesystem layer that mounts multiple operators into a unified namespace.
pub mod vfs;

mod macros;
mod retry;
mod throttle;
mod util;

#[cfg(feature = "services-rustic")]
mod rustic_be;

#[cfg(feature = "services-rustic")]
mod rustic_config;

/// Configuration for retry behavior used by extension layers.
pub use retry::RetrySetting;

/// Layer for throttling OpenDAL operations.
pub use throttle::Throttle;

/// Re-export of the entire OpenDAL crate.
pub use opendal::*;

#[cfg(feature = "services-rustic")]
pub use rustic_config::*;