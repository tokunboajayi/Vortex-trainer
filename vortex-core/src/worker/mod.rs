//! Worker-side logic
//!
//! Worker state machine and coordinator client.

pub mod client;
pub mod state;

pub use client::CoordinatorClient;
pub use state::WorkerState;
