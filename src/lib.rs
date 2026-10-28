#![no_std]
//! WS53 vendor firmware cooperation protocols; no peer ownership is implied.

#[cfg(test)]
extern crate std;

/// Channel number encoded in the vendor cooperation protocol. The platform
/// adapter must explicitly map it to the physical HAL doorbell channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Channel {
    Channel0,
    Channel1,
}

pub use protocol::sdk_163d9150::{
    MAX_MESSAGE, MAX_PAYLOAD, Message, MessageError, MessageId, Priority, layout, mailbox,
};
pub use services::random_buffer;
pub use transport::spsc;

#[cfg(test)]
mod message_tests;

/// Chip-neutral bounded message interface.
pub mod endpoint;
/// Vendor firmware protocol profiles. Enforcement by a live session is pending.
pub mod protocol;
/// Control-core services over explicitly authorized resources.
pub mod services;
/// Explicit local endpoint lifecycle.
pub mod session;
/// WS53 platform transport implementations.
pub mod transport;
