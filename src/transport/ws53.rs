//! Polling-only adapter for the audited SDK cursor protocol.
//!
//! This does not signal a doorbell or register a waker. Use only where the
//! selected peer polls or platform code separately performs qualified notification.
//! A short output buffer leaves the complete message in endpoint-owned staging;
//! the shared queue slot has already been released. Do not discard an endpoint
//! with a staged message unless the platform explicitly accepts that loss.
//! The send bound is the smaller of the protocol frame limit and the queue
//! capacity minus its reserved byte. An oversized frame is rejected even when
//! the queue is empty; `Full` instead denotes temporary backpressure.
use crate::{
    MAX_MESSAGE, Message, MessageError,
    endpoint::{Endpoint, ReceiveError, SendError},
    mailbox::MailboxError,
    protocol::FirmwareProtocol,
    session::Session,
    spsc::{Error, Receive, SharedMailbox, Transmit},
};

/// Platform policy that checks each message against actual peer authority.
///
/// # Safety
/// Returning true must establish that the selected peer may perform that
/// service, and that all encoded remote addresses, extents, permissions and
/// lifetimes remain valid until the peer can no longer use them. The policy
/// must not confuse mere format validity with authority. An implementation may
/// instead reject every message. Hardware ownership and cache handoff remain
/// separate prerequisites for the supplied queues.
pub unsafe trait FrameAuthority {
    fn permits(&self, message: Message<'_>) -> bool;
}

/// Exclusive local producer and consumer, with at most one received frame staged.
/// Invalidating a surrounding `Session` does not stop a peer or release memory.
pub struct PollingEndpoint<T: SharedMailbox, R: SharedMailbox, A: FrameAuthority> {
    tx: Transmit<T>,
    rx: Receive<R>,
    authority: A,
    pending: [u8; MAX_MESSAGE],
    pending_len: Option<usize>,
}
impl<T: SharedMailbox, R: SharedMailbox, A: FrameAuthority> PollingEndpoint<T, R, A> {
    /// Join already prepared queues after selecting the actual firmware profile.
    ///
    /// # Safety
    /// The peer must really implement the selected SDK cursor protocol, and
    /// have sole complementary producer/consumer roles. The existing queue
    /// mapping, ordering, memory lifetime and quiescence contracts continue to
    /// apply. Platform notification, peer resets and cache maintenance are not
    /// supplied here. Construct only after the explicit platform handoff.
    pub unsafe fn from_prepared(
        tx: Transmit<T>,
        rx: Receive<R>,
        authority: A,
        profile: FirmwareProtocol,
    ) -> Self {
        match profile {
            FirmwareProtocol::Sdk163d9150 => {}
        }
        Self {
            tx,
            rx,
            authority,
            pending: [0; MAX_MESSAGE],
            pending_len: None,
        }
    }

    /// Recover both retained mailbox backends and the authority policy.
    /// Any staged received message is discarded; this does not acknowledge it
    /// to the peer or restore queue metadata.
    ///
    /// # Safety
    /// Both peers and all other users must be quiescent; no notification,
    /// outstanding payload reference, cache operation or memory access may
    /// remain. Reconcile ownership and cache state before reusing either
    /// mapping. The caller must explicitly accept loss of any staged message.
    /// This does not make stale queues safe to attach again.
    pub unsafe fn reclaim_after_peer_quiescent(self) -> (T, R, A) {
        // SAFETY: the caller supplies the quiescence and ownership proof for
        // both queues, including any references retained by the peer.
        let tx = unsafe { self.tx.reclaim_after_peer_quiescent() };
        // SAFETY: same proof applies independently to the receive queue.
        let rx = unsafe { self.rx.reclaim_after_peer_quiescent() };
        (tx, rx, self.authority)
    }
}

impl<T: SharedMailbox, R: SharedMailbox, A: FrameAuthority> Session<PollingEndpoint<T, R, A>> {
    /// Consume this session and reclaim both queues without reopening it.
    /// This is also available after explicit or corruption-driven invalidation.
    ///
    /// # Safety
    /// The full quiescence, cache, reference and staged-message conditions of
    /// [`PollingEndpoint::reclaim_after_peer_quiescent`] must hold. Invalidating
    /// a session does not itself stop the peer or establish these conditions.
    pub unsafe fn reclaim_after_peer_quiescent(self) -> (T, R, A) {
        let endpoint = self.into_reclaim_endpoint();
        // SAFETY: caller supplies the endpoint's complete reclaim contract.
        unsafe { endpoint.reclaim_after_peer_quiescent() }
    }
}
impl<T: SharedMailbox, R: SharedMailbox, A: FrameAuthority> Endpoint for PollingEndpoint<T, R, A> {
    fn try_send(&mut self, bytes: &[u8]) -> Result<(), SendError> {
        let maximum = self.tx.maximum_frame_len();
        if bytes.len() > maximum {
            return Err(SendError::TooLong { maximum });
        }
        let message = Message::decode(bytes).map_err(|error| match error {
            MessageError::PayloadTooLong => SendError::TooLong { maximum },
            _ => SendError::InvalidMessage,
        })?;
        if !self.authority.permits(message) {
            return Err(SendError::Unauthorized);
        }
        // SAFETY: FrameAuthority's unsafe contract supplies the per-message
        // peer authorization and retained address/lifetime guarantees.
        unsafe { self.tx.try_push(message) }.map_err(|error| match error {
            Error::Mailbox(MailboxError::Full) => SendError::Full,
            _ => SendError::Corrupt,
        })
    }
    fn try_receive(&mut self, output: &mut [u8]) -> Result<Option<usize>, ReceiveError> {
        if self.pending_len.is_none() {
            let mut frame = [0; MAX_MESSAGE];
            let Some(message) = self
                .rx
                .try_pop_into(&mut frame)
                .map_err(|error| match error {
                    Error::Mailbox(MailboxError::Message(_)) => ReceiveError::InvalidMessage,
                    _ => ReceiveError::Corrupt,
                })?
            else {
                return Ok(None);
            };
            self.pending_len = Some(message.encode(&mut self.pending));
        }
        let n = self.pending_len.expect("pending length set above");
        if output.len() < n {
            return Err(ReceiveError::BufferTooSmall { required: n });
        }
        output[..n].copy_from_slice(&self.pending[..n]);
        self.pending_len = None;
        Ok(Some(n))
    }
}
