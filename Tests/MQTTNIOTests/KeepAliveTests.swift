// Copyright (c) 2026 the MQTTNIO authors
// SPDX-License-Identifier: Apache-2.0

import Logging
import NIOCore
import NIOEmbedded
import Testing

@testable import MQTTNIO

struct KeepAliveTests {
    @Test(arguments: [false, true])
    func negotiatedAutomaticPings(enabled: Bool) async throws {
        let channel = NIOAsyncTestingChannel()
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: .init(versionConfiguration: .v5_0()),
            session: .init(clientID: "negotiated-timer", logger: Logger(label: "negotiated-timer"))
        )
        async let connect = connection.sendConnect(clientID: "negotiated-timer", cleanSession: true)
        _ = try await channel.waitForOutboundPacket()
        try await channel.writeInboundPacket(
            MQTTConnAckPacket(returnCode: 0, acknowledgeFlags: 0, properties: [.serverKeepAlive(enabled ? 1 : 0)]),
            version: .v5_0
        )
        _ = try await connect
        await channel.testingEventLoop.advanceTime(by: .seconds(enabled ? 1 : 60))
        if enabled {
            var bytes = try #require(try await channel.readOutbound(as: ByteBuffer.self))
            #expect(try MQTTIncomingPacket.read(from: &bytes).type == .PINGREQ)
            try await channel.writeInboundPacket(MQTTPingrespPacket(), version: .v5_0)
        } else {
            #expect(try await channel.readOutbound(as: ByteBuffer.self) == nil)
        }
        _ = await connection.closeAndCleanup(sendDisconnect: false)
    }

    @Test(arguments: [0, 1, 4, 5, 30])
    func initialInterval(seconds: Int) {
        let config = MQTTChannelHandler.Configuration(.init(keepAliveInterval: .seconds(seconds)))
        #expect(config.pingInterval == (seconds == 0 ? nil : .seconds(Int64(max(seconds - 5, 1)))))
    }

    @Test(arguments: [UInt16(0), 1, 4, 5, 30])
    func negotiatedInterval(seconds: UInt16) async throws {
        try await withTestMQTTServer(
            configuration: .init(versionConfiguration: .v5_0()),
            connackProperties: [.serverKeepAlive(seconds)]
        ) { connection in
            // Explicit pings remain available even when automatic pings are disabled.
            try await connection.ping()
        } server: { channel in
            #expect(try await channel.waitForOutboundPacket().type == .PINGREQ)
            try await channel.writeInboundPacket(MQTTPingrespPacket(), version: .v5_0)
        }
    }

    private func setup() throws -> (EmbeddedChannel, MQTTChannelHandler) {
        let loop = EmbeddedEventLoop()
        let handler = MQTTChannelHandler(
            configuration: .init(.init(keepAliveInterval: .seconds(30))),
            eventLoop: loop,
            session: .init(clientID: "keepalive-test", logger: Logger(label: "keepalive-test")),
            logger: Logger(label: "keepalive-test")
        )
        let channel = EmbeddedChannel(handler: handler, loop: loop)
        try channel.connect(to: .init(ipAddress: "127.0.0.1", port: 1883)).wait()
        return (channel, handler)
    }

    private func read(_ channel: EmbeddedChannel) throws -> MQTTPacketType? {
        guard var bytes = try channel.readOutbound(as: ByteBuffer.self) else { return nil }
        return try MQTTIncomingPacket.read(from: &bytes).type
    }

    private func respond(_ channel: EmbeddedChannel) throws {
        var bytes = ByteBuffer()
        try MQTTPingrespPacket().write(version: .v3_1_1, to: &bytes)
        try channel.writeInbound(bytes)
    }

    @Test func replacingTimerUsesNewInterval() throws {
        let (channel, handler) = try setup()
        defer { _ = try? channel.finish() }
        handler.updatePingreqTimeout(.seconds(1))
        channel.embeddedEventLoop.advanceTime(by: .milliseconds(999))
        #expect(try read(channel) == nil)
        channel.embeddedEventLoop.advanceTime(by: .milliseconds(1))
        #expect(try read(channel) == .PINGREQ)
        try respond(channel)
        handler.updatePingreqTimeout(.seconds(3))
        channel.embeddedEventLoop.advanceTime(by: .seconds(2))
        #expect(try read(channel) == nil)
        channel.embeddedEventLoop.advanceTime(by: .seconds(1))
        #expect(try read(channel) == .PINGREQ)
        try respond(channel)
    }

    @Test func delayedResponseDoesNotPostponeNextPing() throws {
        let (channel, handler) = try setup()
        defer { _ = try? channel.finish() }
        handler.updatePingreqTimeout(.seconds(1))
        channel.embeddedEventLoop.advanceTime(by: .seconds(1))
        #expect(try read(channel) == .PINGREQ)
        channel.embeddedEventLoop.advanceTime(by: .milliseconds(600))
        try respond(channel)
        channel.embeddedEventLoop.advanceTime(by: .milliseconds(400))
        #expect(try read(channel) == .PINGREQ)
        try respond(channel)
    }

    @Test(arguments: [false, true])
    func disablingCancelsPendingTimer(withOutstandingPing: Bool) throws {
        let (channel, handler) = try setup()
        defer { _ = try? channel.finish() }
        handler.updatePingreqTimeout(.seconds(1))
        if withOutstandingPing {
            channel.embeddedEventLoop.advanceTime(by: .seconds(1))
            #expect(try read(channel) == .PINGREQ)
        }
        handler.updatePingreqTimeout(nil)
        if withOutstandingPing { try respond(channel) }
        channel.embeddedEventLoop.advanceTime(by: .seconds(60))
        #expect(try read(channel) == nil)
        #expect(channel.isActive)
    }

    @Test func stalePingCompletionDoesNotReplaceNewSchedule() throws {
        let (channel, handler) = try setup()
        defer { _ = try? channel.finish() }
        handler.updatePingreqTimeout(.seconds(1))
        channel.embeddedEventLoop.advanceTime(by: .seconds(1))
        #expect(try read(channel) == .PINGREQ)
        handler.updatePingreqTimeout(.seconds(3))
        channel.embeddedEventLoop.advanceTime(by: .seconds(1))
        try respond(channel)
        channel.embeddedEventLoop.advanceTime(by: .seconds(2))
        #expect(try read(channel) == .PINGREQ)
        try respond(channel)
        #expect(try read(channel) == nil)
    }

    @Test func closingWithOutstandingPingDoesNotReschedule() throws {
        let (channel, handler) = try setup()
        handler.updatePingreqTimeout(.seconds(1))
        channel.embeddedEventLoop.advanceTime(by: .seconds(1))
        #expect(try read(channel) == .PINGREQ)
        try channel.close().wait()
        channel.embeddedEventLoop.advanceTime(by: .seconds(60))
        #expect(try read(channel) == nil)
        #expect(!channel.isActive)
        _ = try channel.finish(acceptAlreadyClosed: true)
    }

    @Test func cancelledCallbackCannotSendAfterReplacement() throws {
        let (channel, handler) = try setup()
        defer { _ = try? channel.finish() }
        let obsolete = MQTTChannelHandler.MQTTPingreqSchedule(
            channelHandler: .init(handler, eventLoop: channel.eventLoop),
            pingreqTimeout: .seconds(1),
            generation: 0
        )
        handler.updatePingreqTimeout(nil)
        channel.embeddedEventLoop.advanceTime(by: .seconds(60))
        obsolete.handleScheduledCallback(eventLoop: channel.eventLoop)
        #expect(try read(channel) == nil)
    }
}
