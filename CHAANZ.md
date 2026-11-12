# Chaanz MQTTNIO fork

Branch `chaanz/2.13-fixes` is based on upstream **2.13.0**. It retains that release's
Swift toolchain requirements and adds two focused fixes:

- Negative PUBACK, PUBREC and PUBCOMP responses throw
  `MQTTError.publishError(MQTTAckV5)`, preserving the reason code, Reason String and
  all User Properties (including repetitions). Connect failures continue to use
  `MQTTError.reasonError(MQTTReasonCode)`. Callers handling publish failures must
  handle the new error case; no application-specific retry policy is added here.
- Automatic keepalive handles zero and short intervals without unsigned underflow.
  A broker's Server Keep Alive replaces the pending timer immediately. Zero stops
  automatic pings, and stale completions cannot restart an old timer.

`MQTTAcknowledgementTests` uses a local TCP peer to cover error metadata, positive
acknowledgements, unknown reason handling, and client/server keepalive negotiation.
It runs without an external broker:

```sh
swift test --filter MQTTAcknowledgementTests
```

The full upstream suite additionally needs the broker configuration documented in
`CONTRIBUTING.md`. The certificates shipped with 2.13.0 expire in October 2026;
regenerate test certificates locally before TLS tests when necessary. Do not use
those test credentials outside the test broker.

This fork does not change the b.sure contract or receiver. Integrating it into
Analytics still requires the sender/receiver interoperability checks.

Validated on Linux with Swift 6.0.3 and Mosquitto 2.0.18: all 76 tests pass,
including 10 new regressions. The three negative-acknowledgement tests fail against
the original implementation and pass with the patch.
