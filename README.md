# WS53 control-core cooperation prototype

This experimental, unpublished crate contains vendor-firmware message framing,
mailbox layout, SPSC storage and IPC18 random-buffer transaction logic. It does
not own or initialize the hardware doorbell, negotiate ownership, map shared
memory, maintain caches or prove peer quiescence. A platform adapter must map
the protocol channel to a HAL doorbell and establish those contracts explicitly.

The entropy callback accepts a platform-controlled source without coupling this
protocol crate to a specific HAL or random-number driver. All destination updates
remain transactional on an entropy failure. HAL retains the physical doorbell;
the firmware protocol and service callers use this component. This migration
does not imply new on-silicon validation.

The experimental polling adapter combines the existing shared queues with the
bounded endpoint contract, an explicitly selected SDK profile and a platform
authorization policy. It does not send notifications or wait asynchronously;
the peer must poll or the platform must separately issue qualified notifications.
After a short receive buffer the frame is retained locally even though the queue
slot has been released. Explicit session invalidation prevents later endpoint
operations; detected queue corruption also latches invalidation. A malformed
complete message is retained and reported separately. Neither path resets a
queue, detects an unobservable peer reset or automatically reclaims peer
memory. Once the platform proves that both peers and all users are quiescent,
the endpoint or its session can explicitly reclaim both backends through
`reclaim_after_peer_quiescent`; its safety contract also requires reconciliation
of caches and acceptance that a locally staged receive will be discarded.
Invalidation alone does not establish quiescence.

`transport::notification::try_send_and_notify` publishes first and then invokes
one platform-supplied doorbell operation. A notification failure is returned as
`Published::NotificationFailed`, so the already-published message is not retried
by mistake. The platform must still qualify the peer's ACK/drain/recheck and
cache ordering; a pending or successful doorbell is not proof of delivery.
This helper does not implement interrupt handling or asynchronous wakeup.

Run independent host tests with `cargo test --locked --target
x86_64-unknown-linux-gnu --features defmt`; the RISC-V check uses the project's
pinned nightly and `-Zbuild-std=core,alloc`.
