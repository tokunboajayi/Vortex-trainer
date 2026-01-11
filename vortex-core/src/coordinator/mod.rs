//! Coordinator for worker management
//!
//! Handles worker registration, heartbeats, shard assignment, and epoch sync.

pub mod leader;
pub mod membership;
pub mod epoch;
pub mod shard_assigner;

pub use leader::{Coordinator, CoordinatorConfig};
pub use membership::{MembershipManager, WorkerInfo, WorkerStatus};
pub use epoch::EpochTracker;
pub use shard_assigner::ShardAssigner;
