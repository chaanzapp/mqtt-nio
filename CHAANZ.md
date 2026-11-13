# Chaanz integration branch

Based on upstream `c93d67fbad5b827a30ab356cd8b7c64f85c63763` (the 3.x development line, newer than 3.0.0-rc.1).

The upstream version already preserves negative PUBACK properties and enforces outgoing packet limits. Those v2 patches are removed.

Remaining changes:

- Handle zero and short keepalive intervals without unsigned underflow; reschedule negotiated pings and invalidate callbacks from an older schedule.
- Enforce the CONNECT Maximum Packet Size at the incoming fixed header, before waiting for its body; reject variable-length integers longer than four bytes. Oversize input closes the connection.
- Expose the successful CONNACK so a caller-owned queue can honour Receive Maximum and inspect Session Present.

`DeliveryLimitsTests` covers the decoder boundaries, keepalive values and CONNACK access. The Analytics sender/receiver interoperability suite additionally exercises real TLS, negotiated one-second pings and one outstanding publish, packet limits and negative PUBACK metadata.

This branch is for the Chaanz integration. It is not an upstream submission.
