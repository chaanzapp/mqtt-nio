// Copyright (c) 2026 the MQTTNIO authors
// SPDX-License-Identifier: Apache-2.0

import Logging
import NIOCore
import NIOEmbedded
import Testing

@testable import MQTTNIO

struct KeepAliveTests {
    @Test(arguments: [UInt16(0), 1], [false, true])
    func negotiationPreservesPingPolicy(serverSeconds: UInt16, explicitInterval: Bool) async throws {
        let channel = NIOAsyncTestingChannel()
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: .init(
                versionConfiguration: .v5_0(),
                pingConfiguration: explicitInterval ? .pingInterval(.seconds(2)) : .disable
            ),
            session: .init(clientID: "ping-policy", logger: Logger.current)
        )
        async let connect = connection.sendConnect(clientID: "ping-policy", cleanSession: true)
        #expect(try await channel.waitForOutboundPacket().type == .CONNECT)
        try await channel.writeInboundPacket(
            MQTTConnAckPacket(returnCode: 0, acknowledgeFlags: 0, properties: [.serverKeepAlive(serverSeconds)]),
            version: .v5_0
        )
        _ = try await connect

        await channel.testingEventLoop.advanceTime(by: .milliseconds(1999))
        #expect(try await channel.readOutbound(as: ByteBuffer.self) == nil)
        await channel.testingEventLoop.advanceTime(by: .milliseconds(1))
        if explicitInterval {
            var bytes = try #require(try await channel.readOutbound(as: ByteBuffer.self))
            #expect(try MQTTIncomingPacket.read(from: &bytes).type == .PINGREQ)
            try await channel.writeInboundPacket(MQTTPingrespPacket(), version: .v5_0)
        } else {
            await channel.testingEventLoop.advanceTime(by: .seconds(100))
            #expect(try await channel.readOutbound(as: ByteBuffer.self) == nil)
        }
        _ = await connection.closeAndCleanup(sendDisconnect: false)
    }

    @Test(arguments: [UInt16?.none, 0, 1, 2, 3, 4, 5, 10, 29, 30, UInt16.max])
    func negotiatedInterval(seconds: UInt16?) async throws {
        let channel = NIOAsyncTestingChannel()
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: .init(versionConfiguration: .v5_0(), keepAliveInterval: .seconds(30)),
            session: .init(clientID: "negotiated-timer", logger: Logger(label: "negotiated-timer"))
        )
        async let connect = connection.sendConnect(clientID: "negotiated-timer", cleanSession: true)
        _ = try await channel.waitForOutboundPacket()
        try await channel.writeInboundPacket(
            MQTTConnAckPacket(returnCode: 0, acknowledgeFlags: 0, properties: seconds.map { [.serverKeepAlive($0)] } ?? []),
            version: .v5_0
        )
        _ = try await connect
        if seconds == 0 {
            // The original 25-second timer must also have been cancelled.
            await channel.testingEventLoop.advanceTime(by: .seconds(120))
            #expect(try await channel.readOutbound(as: ByteBuffer.self) == nil)
            // Explicit pings remain available when automatic pings are disabled.
            async let ping = connection.ping()
            #expect(try await channel.waitForOutboundPacket().type == .PINGREQ)
            try await channel.writeInboundPacket(MQTTPingrespPacket(), version: .v5_0)
            try await ping
        } else {
            let interval = Int64(max(Int(seconds ?? 30) - 5, 1))
            await channel.testingEventLoop.advanceTime(by: .milliseconds(interval * 1000 - 1))
            #expect(try await channel.readOutbound(as: ByteBuffer.self) == nil)
            await channel.testingEventLoop.advanceTime(by: .milliseconds(1))
            var bytes = try #require(try await channel.readOutbound(as: ByteBuffer.self))
            #expect(try MQTTIncomingPacket.read(from: &bytes).type == .PINGREQ)
            try await channel.writeInboundPacket(MQTTPingrespPacket(), version: .v5_0)
        }
        _ = await connection.closeAndCleanup(sendDisconnect: false)
    }

    private func setup(active: Bool = true) throws -> (EmbeddedChannel, MQTTChannelHandler) {
        let loop = EmbeddedEventLoop()
        let handler = MQTTChannelHandler(
            configuration: .init(.init(keepAliveInterval: .seconds(30))),
            eventLoop: loop,
            session: .init(clientID: "keepalive-test", logger: Logger(label: "keepalive-test")),
            logger: Logger(label: "keepalive-test")
        )
        let channel = EmbeddedChannel(handler: handler, loop: loop)
        if active {
            try channel.connect(to: .init(ipAddress: "127.0.0.1", port: 1883)).wait()
        }
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

    @Test(arguments: [-1, 0, 1, 3])
    func dequeuedCallbackUsesCurrentInterval(replacementSeconds: Int64) throws {
        let (channel, handler) = try setup()
        defer { _ = try? channel.finish() }
        handler.updatePingreqTimeout(.seconds(2))
        // Model a callback dequeued into an event-loop batch before cancellation.
        let obsolete = MQTTChannelHandler.MQTTPingreqSchedule(
            channelHandler: .init(handler, eventLoop: channel.eventLoop)
        )
        channel.embeddedEventLoop.advanceTime(by: .milliseconds(500))
        handler.updatePingreqTimeout(replacementSeconds < 0 ? nil : .seconds(replacementSeconds))
        obsolete.handleScheduledCallback(eventLoop: channel.eventLoop)
        obsolete.handleScheduledCallback(eventLoop: channel.eventLoop)
        #expect(try read(channel) == nil)
        if replacementSeconds > 0 {
            channel.embeddedEventLoop.advanceTime(by: .milliseconds(replacementSeconds * 1000 - 501))
            #expect(try read(channel) == nil)
            channel.embeddedEventLoop.advanceTime(by: .milliseconds(1))
            #expect(try read(channel) == .PINGREQ)
            obsolete.handleScheduledCallback(eventLoop: channel.eventLoop)
            channel.embeddedEventLoop.advanceTime(by: .seconds(replacementSeconds))
            #expect(try read(channel) == nil)
            handler.updatePingreqTimeout(nil)
            try respond(channel)
        }
        channel.embeddedEventLoop.advanceTime(by: .seconds(60))
        obsolete.handleScheduledCallback(eventLoop: channel.eventLoop)
        #expect(try read(channel) == nil)
        #expect(channel.isActive)
    }

    @Test(arguments: [1, 3], [false, true])
    func updatedIntervalWaitsForOutstandingPing(seconds: Int64, responseLate: Bool) throws {
        let (channel, handler) = try setup()
        defer { _ = try? channel.finish() }
        handler.updatePingreqTimeout(.seconds(2))
        channel.embeddedEventLoop.advanceTime(by: .seconds(2))
        #expect(try read(channel) == .PINGREQ)
        if responseLate { handler.updatePingreqTimeout(nil) }
        handler.updatePingreqTimeout(.seconds(seconds))
        channel.embeddedEventLoop.advanceTime(by: .milliseconds(responseLate ? (seconds + 1) * 1000 : 500))
        #expect(try read(channel) == nil)
        try respond(channel)
        if !responseLate {
            channel.embeddedEventLoop.advanceTime(by: .milliseconds(seconds * 1000 - 501))
            #expect(try read(channel) == nil)
            channel.embeddedEventLoop.advanceTime(by: .milliseconds(1))
        }
        channel.embeddedEventLoop.run()
        #expect(try read(channel) == .PINGREQ)
        #expect(try read(channel) == nil)
        try respond(channel)
    }

    @Test(arguments: [0, 1, 3])
    func failedPingClosesChannelAndFailsPendingTasks(replacementSeconds: Int64) throws {
        let (channel, handler) = try setup()
        defer { _ = try? channel.finish(acceptAlreadyClosed: true) }
        let (_, continuation) = MQTTSubscription.makeStream()
        _ = try handler.session.subscriptions.addSubscription(
            id: nil,
            continuation: continuation,
            subscriptions: [.init(topicFilter: "subscribed", qos: .atMostOnce)],
            usesSubscriptionIdentifiers: false
        )
        #expect(!handler.session.subscriptions.subscriptionIDMap.isEmpty)
        handler.updatePingreqTimeout(.seconds(1))
        channel.embeddedEventLoop.advanceTime(by: .seconds(1))
        #expect(try read(channel) == .PINGREQ)
        if replacementSeconds != 1 {
            handler.updatePingreqTimeout(replacementSeconds == 0 ? nil : .seconds(replacementSeconds))
        }
        channel.embeddedEventLoop.advanceTime(by: .milliseconds(500))
        let pending = handler.sendMessage(
            MQTTPublishPacket(
                publish: .init(qos: .atLeastOnce, retain: false, topicName: "pending", payload: .init(), properties: []),
                packetId: 1
            ),
            requestID: MQTTConnection.requestIDGenerator.next()
        ) { $0.type == .PUBACK }
        var pendingError: (any Error)?
        pending.assumeIsolated().whenFailure { pendingError = $0 }
        #expect(try read(channel) == .PUBLISH)
        // Only the ping has reached its 30-second response deadline.
        channel.embeddedEventLoop.advanceTime(by: .milliseconds(29_500))
        #expect(throws: MQTTError.timeout) { try channel.throwIfErrorCaught() }
        #expect(pendingError as? MQTTError == .timeout)
        #expect(handler.session.subscriptions.subscriptionIDMap.isEmpty)
        #expect(handler.deadlineCallback == nil)
        #expect(!channel.isActive)
        channel.embeddedEventLoop.advanceTime(by: .seconds(60))
        #expect(try read(channel) == nil)
    }

    @Test func updatingBeforeActivationWaitsForInitialization() throws {
        let (channel, handler) = try setup(active: false)
        defer { _ = try? channel.finish() }
        handler.updatePingreqTimeout(.seconds(1))
        channel.embeddedEventLoop.advanceTime(by: .seconds(2))
        #expect(try read(channel) == nil)
        #expect(!channel.isActive)
        try channel.connect(to: .init(ipAddress: "127.0.0.1", port: 1883)).wait()
        channel.embeddedEventLoop.advanceTime(by: .seconds(1))
        #expect(try read(channel) == .PINGREQ)
        #expect(try read(channel) == nil)
        try respond(channel)
    }
}
