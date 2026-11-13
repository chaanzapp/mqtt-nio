// Copyright (c) 2026 the MQTTNIO authors
// SPDX-License-Identifier: Apache-2.0

import NIOCore
import Testing
@testable import MQTTNIO

struct DeliveryLimitsTests {
    @Test(arguments: [UInt16(0), 1, 4, 5, 30])
    func negotiatedKeepAliveAndReceiveMaximum(seconds: UInt16) async throws {
        try await withTestMQTTServer(
            configuration: .init(versionConfiguration: .v5_0()),
            connackProperties: [.serverKeepAlive(seconds), .receiveMaximum(1)]
        ) { connection in
            let ack = try #require(await connection.connectAcknowledgement)
            #expect(ack.properties.contains { if case .receiveMaximum(1) = $0 { true } else { false } })
        } server: { _ in }
    }

    @Test(arguments: [0, 1, 4, 5, 30])
    func initialKeepAlive(seconds: Int) {
        let config = MQTTChannelHandler.Configuration(.init(keepAliveInterval: .seconds(seconds)))
        if seconds == 0 {
            #expect(config.pingInterval == nil)
        } else {
            #expect(config.pingInterval == .seconds(Int64(max(seconds - 5, 1))))
        }
    }

    @Test func oversizedPacketRejectedFromHeader() {
        var decoder = ByteToMQTTMessageDecoder(version: .v5_0, maximumPacketSize: 4096)
        // CONNACK declaring a 4096-byte body is already too big with its header.
        var input = ByteBuffer(bytes: [0x20, 0x80, 0x20])
        #expect(throws: MQTTError.packetTooLarge) { try decoder.decode(buffer: &input) }
    }

    @Test func boundedPacketCanArriveInFragments() throws {
        var decoder = ByteToMQTTMessageDecoder(version: .v5_0, maximumPacketSize: 5)
        var input = ByteBuffer(bytes: [0x20, 0x03])
        #expect(try decoder.decode(buffer: &input) == nil)
        #expect(input.readerIndex == 0)
        input.writeBytes([0x00, 0x00, 0x00])
        #expect(try decoder.decode(buffer: &input) is MQTTConnAckPacket)
        #expect(input.readableBytes == 0)
    }

    @Test func oversizedVariableLengthIntegerIsRejected() {
        var input = ByteBuffer(bytes: [0x80, 0x80, 0x80, 0x80])
        #expect(throws: MQTTError.decodeError) { try MQTTSerializer.readVariableLengthInteger(from: &input) }
    }

    @Test func configuredIncomingLimitReachesDecoderConfiguration() {
        let config = MQTTChannelHandler.Configuration(.init(
            versionConfiguration: .v5_0(connectProperties: [.maximumPacketSize(4096)])))
        #expect(config.maximumIncomingPacketSize == 4096)
    }
}
