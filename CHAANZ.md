# Chaanz MQTTNIO integration

The `chaanz/3-fixes` branch follows upstream `0247a08` with two changes:

- [#1](https://github.com/chaanzapp/mqtt-nio/pull/1): convert Server Keep Alive to
  `Int64` before subtraction, preventing unsigned underflow.
- [#3](https://github.com/chaanzapp/mqtt-nio/pull/3): expose successful CONNACK
  properties through `MQTTConnection.connackProperties`.

The b.sure sender uses the properties to respect Receive Maximum. Its contract
requires Server Keep Alive to be omitted or at least 30 seconds. The crash fix
does not change upstream timer behavior or support shorter negotiated intervals.
The former custom timer and inbound packet-limit changes have been removed.
