#![doc = include_str!("../README.md")]

mod actor;
pub mod manager;
pub use actor::{Actor, ActorContext, ActorHandle, CancelToken};
