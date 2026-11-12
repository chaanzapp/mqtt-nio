// SPDX-License-Identifier: Apache-2.0

import NIO
import XCTest

@testable import MQTTNIO

/// A local MQTT peer exercises the public client without requiring broker configuration.
final class MQTTAcknowledgementTests: XCTestCase {
    func testNegativePUBACKPreservesProperties() throws {
        try self.checkPublishError(stage: .PUBACK, qos: .atLeastOnce, reason: .implementationSpecificError)
    }

    func testNegativePUBRECPreservesProperties() throws {
        try self.checkPublishError(stage: .PUBREC, qos: .exactlyOnce, reason: .quotaExceeeded)
    }

    func testNegativePUBCOMPPreservesProperties() throws {
        try self.checkPublishError(stage: .PUBCOMP, qos: .exactlyOnce, reason: .packetIdentifierNotFound)
    }

    func testUnknownPUBACKReasonDoesNotCrash() throws {
        try self.checkPublishError(stage: .PUBACK, qos: .atLeastOnce, reason: .unrecognisedReason)
    }

    func testNonErrorPUBACKStillReturnsAcknowledgement() throws {
        try self.withPeer(stage: .PUBACK, reason: .noMatchingSubscriber) { client in
            _ = try client.v5.connect().wait()
            let ack = try XCTUnwrap(client.v5.publish(to: "test", payload: ByteBuffer(string: "{}"), qos: .atLeastOnce).wait())
            XCTAssertEqual(ack.reason, .noMatchingSubscriber)
            XCTAssertEqual(
                ack.properties.compactMap { property -> String? in
                    if case .reasonString(let value) = property { return value }
                    return nil
                },
                ["receiver_detail"]
            )
        }
    }

    func testOneSecondClientKeepAlive() throws {
        try self.checkKeepAlive(clientSeconds: 1, serverSeconds: nil)
    }

    func testServerShortensKeepAliveAndReschedulesPing() throws {
        try self.checkKeepAlive(clientSeconds: 30, serverSeconds: 1)
    }

    func testServerEnablesKeepAliveWhenClientRequestedZero() throws {
        try self.checkKeepAlive(clientSeconds: 0, serverSeconds: 1)
    }

    func testServerZeroDisablesPendingPing() throws {
        try self.checkKeepAlive(clientSeconds: 1, serverSeconds: 0)
    }

    func testZeroClientKeepAliveDoesNotSchedulePings() throws {
        try self.checkKeepAlive(clientSeconds: 0, serverSeconds: nil)
    }

    private func checkPublishError(stage: MQTTPacketType, qos: MQTTQoS, reason: MQTTReasonCode) throws {
        try self.withPeer(stage: stage, reason: reason) { client in
            _ = try client.v5.connect().wait()
            XCTAssertThrowsError(try client.v5.publish(to: "test", payload: ByteBuffer(string: "{}"), qos: qos).wait()) { error in
                guard case MQTTError.publishError(let ack) = error else {
                    return XCTFail("Expected publishError with acknowledgement, got \(error)")
                }
                XCTAssertEqual(ack.reason, reason)
                XCTAssertEqual(
                    ack.properties.compactMap { property -> String? in
                        if case .reasonString(let value) = property { return value }
                        return nil
                    },
                    ["receiver_detail"]
                )
                let hints = ack.properties.compactMap { property -> String? in
                    if case .userProperty("retry-after", let value) = property { return value }
                    return nil
                }
                // Preserve repeated properties too; interpretation belongs to the caller.
                XCTAssertEqual(hints, ["7", "9"])
            }
        }
    }

    private func checkKeepAlive(clientSeconds: Int64, serverSeconds: UInt16?) throws {
        let ping = self.expectation(description: "PINGREQ")
        let disabled = (serverSeconds.map(Int64.init) ?? clientSeconds) == 0
        ping.isInverted = disabled
        try self.withPeer(
            keepAlive: clientSeconds,
            serverKeepAlive: serverSeconds,
            onPing: { ping.fulfill() },
            body: { client in
                _ = try client.v5.connect().wait()
                // Above the 1s interval, below the broker's 1.5s idle-disconnect threshold.
                self.wait(for: [ping], timeout: 1.4)
            }
        )
    }

    private func withPeer(
        stage: MQTTPacketType = .PUBACK,
        reason: MQTTReasonCode = .success,
        keepAlive: Int64 = 30,
        serverKeepAlive: UInt16? = nil,
        onPing: @escaping () -> Void = {},
        body: (MQTTClient) throws -> Void
    ) throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try group.syncShutdownGracefully()) }
        let server = try ServerBootstrap(group: group)
            .childChannelInitializer { channel in
                channel.pipeline.addHandler(Peer(stage: stage, reason: reason, keepAlive: serverKeepAlive, onPing: onPing))
            }
            .bind(host: "127.0.0.1", port: 0).wait()
        defer { XCTAssertNoThrow(try server.close().wait()) }
        let client = MQTTClient(
            host: "127.0.0.1",
            port: server.localAddress!.port!,
            identifier: "regression",
            eventLoopGroupProvider: .shared(group),
            configuration: .init(version: .v5_0, keepAliveInterval: .seconds(keepAlive), timeout: .seconds(2))
        )
        defer { XCTAssertNoThrow(try client.syncShutdownGracefully()) }
        try body(client)
    }

    private final class Peer: ChannelInboundHandler {
        typealias InboundIn = ByteBuffer
        typealias OutboundOut = ByteBuffer
        let stage: MQTTPacketType
        let reason: MQTTReasonCode
        let keepAlive: UInt16?
        let onPing: () -> Void
        var input = ByteBuffer()

        init(stage: MQTTPacketType, reason: MQTTReasonCode, keepAlive: UInt16?, onPing: @escaping () -> Void) {
            self.stage = stage
            self.reason = reason
            self.keepAlive = keepAlive
            self.onPing = onPing
        }

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            var bytes = self.unwrapInboundIn(data)
            self.input.writeBuffer(&bytes)
            while self.input.readableBytes > 0 {
                var remaining = self.input
                do {
                    let packet = try MQTTIncomingPacket.read(from: &remaining)
                    self.input = remaining
                    switch packet.type {
                    case .CONNECT:
                        // Independent minimal CONNACK; the client library does not encode CONNACK.
                        var ack = ByteBuffer(bytes: [0x20, self.keepAlive == nil ? 3 : 6, 0, 0, self.keepAlive == nil ? 0 : 3])
                        if let keepAlive = self.keepAlive {
                            ack.writeInteger(UInt8(0x13))
                            ack.writeInteger(keepAlive)
                        }
                        context.writeAndFlush(self.wrapOutboundOut(ack), promise: nil)
                    case .PUBLISH:
                        let publish = try MQTTPublishPacket.read(version: .v5_0, from: packet)
                        let type: MQTTPacketType = publish.publish.qos == .atLeastOnce ? .PUBACK : .PUBREC
                        try self.ack(type, id: publish.packetId, context: context)
                    case .PUBREL:
                        let release = try MQTTPubAckPacket.read(version: .v5_0, from: packet)
                        try self.ack(.PUBCOMP, id: release.packetId, context: context)
                    case .PINGREQ:
                        self.onPing()
                        context.writeAndFlush(self.wrapOutboundOut(ByteBuffer(bytes: [0xD0, 0])), promise: nil)
                    case .DISCONNECT:
                        context.close(promise: nil)
                    default:
                        XCTFail("Unexpected packet \(packet.type)")
                    }
                } catch InternalError.incompletePacket {
                    return
                } catch {
                    XCTFail("Peer failed: \(error)")
                    context.close(promise: nil)
                    return
                }
            }
        }

        private func ack(_ type: MQTTPacketType, id: UInt16, context: ChannelHandlerContext) throws {
            let properties: MQTTProperties =
                type == self.stage
                ? [.reasonString("receiver_detail"), .userProperty("retry-after", "7"), .userProperty("retry-after", "9")] : []
            let ack = MQTTPubAckPacket(type: type, packetId: id, reason: type == self.stage ? self.reason : .success, properties: properties)
            var bytes = ByteBuffer()
            try ack.write(version: .v5_0, to: &bytes)
            context.writeAndFlush(self.wrapOutboundOut(bytes), promise: nil)
        }
    }
}
