//! Publish-before-notify coordination without assuming a particular doorbell.
//!
//! The platform owns notification resources, cache handoff, ordering, and the
//! peer's drain/recheck protocol. A pending notification is not proof that a
//! newly published message will be observed. This module does not establish
//! peer liveness, wakeup delivery, or ownership transfer.

use crate::endpoint::{Endpoint, SendError};

/// A message was published, followed by one notification attempt.
/// Neither variant promises that the peer received or processed the message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Published<N> {
    /// The platform notification operation returned success.
    Notified,
    /// The operation failed or reported an outstanding notification. The
    /// message is already published; do not retry the message blindly.
    NotificationFailed(N),
}

/// Publish one complete message and then make exactly one notification attempt.
///
/// `Err` comes solely from the endpoint; the callback is not called on error.
/// In particular, `SendError::Indeterminate` can still mean that publication
/// occurred, so the platform must reconcile that case without blindly resending.
/// `Ok(Published::NotificationFailed(..))` means publication succeeded even
/// though notification did not: callers may retry the notification separately
/// only under their platform/peer protocol, never republish automatically.
///
/// The callback may use the platform's explicitly handed-over HAL doorbell.
/// A successful callback or an already-pending indication does not resolve the
/// race between a peer's ACK and queue inspection. The platform must establish a
/// proven drain/recheck or equivalent protocol, including barriers and cache
/// maintenance; this helper does not install an ISR or provide async waits.
pub fn try_send_and_notify<E: Endpoint, N>(
    endpoint: &mut E,
    message: &[u8],
    notify: impl FnOnce() -> Result<(), N>,
) -> Result<Published<N>, SendError> {
    endpoint.try_send(message)?;
    Ok(match notify() {
        Ok(()) => Published::Notified,
        Err(error) => Published::NotificationFailed(error),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::ReceiveError;
    use core::cell::Cell;

    struct Fake<'a> {
        published: &'a Cell<usize>,
        result: Result<(), SendError>,
    }
    impl Endpoint for Fake<'_> {
        fn try_send(&mut self, _: &[u8]) -> Result<(), SendError> {
            // Simulate the "published but not known to caller" branch of an
            // indeterminate outcome; the helper must not reinterpret it.
            if self.result.is_ok() || self.result == Err(SendError::Indeterminate) {
                self.published.set(self.published.get() + 1);
            }
            self.result
        }
        fn try_receive(&mut self, _: &mut [u8]) -> Result<Option<usize>, ReceiveError> {
            Ok(None)
        }
    }

    #[test]
    fn publish_precedes_notification_and_failure_does_not_republish() {
        let published = Cell::new(0);
        let attempts = Cell::new(0);
        let mut endpoint = Fake {
            published: &published,
            result: Ok(()),
        };
        let result = try_send_and_notify(&mut endpoint, b"message", || {
            assert_eq!(published.get(), 1);
            attempts.set(attempts.get() + 1);
            Err("pending")
        });
        assert_eq!(result, Ok(Published::NotificationFailed("pending")));
        assert_eq!(published.get(), 1);
        assert_eq!(attempts.get(), 1);
        let result = try_send_and_notify(&mut endpoint, b"next", || {
            assert_eq!(published.get(), 2);
            attempts.set(attempts.get() + 1);
            Ok::<_, &str>(())
        });
        assert_eq!(result, Ok(Published::Notified));
        assert_eq!(published.get(), 2);
        assert_eq!(attempts.get(), 2);
    }

    #[test]
    fn failed_or_indeterminate_publication_never_invokes_notification() {
        for error in [
            SendError::Full,
            SendError::Corrupt,
            SendError::Invalidated,
            SendError::Indeterminate,
        ] {
            let published = Cell::new(0);
            let mut endpoint = Fake {
                published: &published,
                result: Err(error),
            };
            let result = try_send_and_notify(&mut endpoint, b"message", || -> Result<(), ()> {
                panic!("notification must not run on publication error")
            });
            assert_eq!(result, Err(error));
            assert_eq!(
                published.get(),
                usize::from(error == SendError::Indeterminate)
            );
        }
    }
}
