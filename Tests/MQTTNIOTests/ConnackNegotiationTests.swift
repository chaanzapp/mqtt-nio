// Copyright (c) 2026 the MQTTNIO authors
// SPDX-License-Identifier: Apache-2.0

import Logging
import NIOCore
import NIOEmbedded
import Testing

@testable import MQTTNIO

struct ConnackNegotiationTests {
    @Test(arguments: [500, 6500])
    func initialPingUsesEncodedKeepAlive(milliseconds: Int) async throws {
        let configuration = MQTTConnectionConfiguration(
            versionConfiguration: .v5_0(),
            keepAliveInterval: .milliseconds(milliseconds)
        )
        #expect(MQTTChannelHandler.Configuration(configuration).pingInterval == (milliseconds < 1000 ? nil : .seconds(1)))
        let channel = NIOAsyncTestingChannel()
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: configuration,
            session: .init(clientID: "fractional-keepalive", logger: Logger.current)
        )
        async let connect = connection.sendConnect(clientID: "fractional-keepalive", cleanSession: true)
        let packet = try MQTTConnectPacket.read(version: .v5_0, from: try await channel.waitForOutboundPacket())
        #expect(packet.keepAliveSeconds == UInt16(milliseconds / 1000))
        try await channel.writeInboundPacket(
            MQTTConnAckPacket(returnCode: 0, acknowledgeFlags: 0, properties: []),
            version: .v5_0
        )
        _ = try await connect
        await channel.testingEventLoop.advanceTime(by: .milliseconds(999))
        #expect(try await channel.readOutbound(as: ByteBuffer.self) == nil)
        await channel.testingEventLoop.advanceTime(by: .milliseconds(1))
        if milliseconds >= 1000 {
            var bytes = try #require(try await channel.readOutbound(as: ByteBuffer.self))
            #expect(try MQTTIncomingPacket.read(from: &bytes).type == .PINGREQ)
            try await channel.writeInboundPacket(MQTTPingrespPacket(), version: .v5_0)
        } else {
            await channel.testingEventLoop.advanceTime(by: .seconds(60))
            #expect(try await channel.readOutbound(as: ByteBuffer.self) == nil)
        }
        _ = await connection.closeAndCleanup(sendDisconnect: false)
    }

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

    @Test func enhancedAuthAppliesNegotiatedProperties() async throws {
        let channel = NIOAsyncTestingChannel()
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: .init(versionConfiguration: .v5_0(authWorkflow: SimpleAuthWorkflow())),
            session: .init(clientID: "", logger: Logger.current)
        )
        async let connect = connection.sendConnect(clientID: "", cleanSession: true)
        try await authenticate(channel)
        try await channel.writeInboundPacket(
            MQTTConnAckPacket(
                returnCode: 0,
                acknowledgeFlags: 0,
                properties: [
                    .serverKeepAlive(10), .assignedClientIdentifier("assigned"), .maximumPacketSize(50),
                    .maximumQoS(.atMostOnce), .retainAvailable(0), .topicAliasMaximum(1),
                ]
            ),
            version: .v5_0
        )
        #expect(try await connect == false)

        await #expect(throws: MQTTError.packetTooLarge) {
            try await connection.publish(to: "foo", payload: ByteBuffer(repeating: 0xFF, count: 100), qos: .atMostOnce)
        }
        await #expect(throws: MQTTPacketError.qosInvalid) {
            try await connection.publish(to: "foo", payload: ByteBuffer(), qos: .atLeastOnce)
        }
        await #expect(throws: MQTTPacketError.retainUnavailable) {
            try await connection.publish(to: "foo", payload: ByteBuffer(), qos: .atMostOnce, retain: true)
        }
        await #expect(throws: MQTTPacketError.topicAliasOutOfRange) {
            try await connection.v5.publish(to: "foo", payload: ByteBuffer(), qos: .atMostOnce, properties: [.topicAlias(2)])
        }

        await channel.testingEventLoop.advanceTime(by: .milliseconds(4999))
        #expect(try await channel.readOutbound(as: ByteBuffer.self) == nil)
        await channel.testingEventLoop.advanceTime(by: .milliseconds(1))
        var bytes = try #require(try await channel.readOutbound(as: ByteBuffer.self))
        #expect(try MQTTIncomingPacket.read(from: &bytes).type == .PINGREQ)
        try await channel.writeInboundPacket(MQTTPingrespPacket(), version: .v5_0)
        let session = await connection.closeAndCleanup(sendDisconnect: false)
        #expect(session.clientID == "assigned")
    }

    @Test func enhancedAuthRejectsFailedConnack() async throws {
        let channel = NIOAsyncTestingChannel()
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: .init(versionConfiguration: .v5_0(authWorkflow: SimpleAuthWorkflow())),
            session: .init(clientID: "auth-failure", logger: Logger.current)
        )
        async let connect = connection.sendConnect(clientID: "auth-failure", cleanSession: true)
        try await authenticate(channel)
        let properties: MQTTProperties = [.reasonString("denied")]
        try await channel.writeInboundPacket(
            MQTTConnAckPacket(returnCode: MQTTReasonCode.notAuthorized.rawValue, acknowledgeFlags: 0, properties: properties),
            version: .v5_0
        )
        do {
            _ = try await connect
            Issue.record("Failed CONNACK was accepted after enhanced authentication")
        } catch MQTTError.reasonError(let ack) {
            #expect(ack.reason == .notAuthorized)
            #expect(ack.properties == properties)
        }
        _ = await connection.closeAndCleanup(sendDisconnect: false)
    }

    @Test(arguments: [false, true])
    func enhancedAuthRestoresSession(sessionPresent: Bool) async throws {
        let channel = NIOAsyncTestingChannel()
        var session = MQTTSessionStorage(clientID: "resume", logger: Logger.current)
        session.inflight.add(packet: MQTTPubAckPacket(type: .PUBREL, packetId: 42))
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: .init(versionConfiguration: .v5_0(authWorkflow: SimpleAuthWorkflow())),
            session: session
        )
        async let connect = connection.sendConnect(clientID: "resume", cleanSession: false)
        try await authenticate(channel)
        try await channel.writeInboundPacket(
            MQTTConnAckPacket(returnCode: 0, acknowledgeFlags: sessionPresent ? 1 : 0, properties: []),
            version: .v5_0
        )
        if sessionPresent {
            let packet = try await channel.waitForOutboundPacket()
            let pubrel = try MQTTPubAckPacket.read(version: .v5_0, from: packet)
            #expect(pubrel.type == .PUBREL)
            #expect(pubrel.packetId == 42)
            try await channel.writeInboundPacket(MQTTPubAckPacket(type: .PUBCOMP, packetId: 42), version: .v5_0)
        }
        #expect(try await connect == sessionPresent)
        #expect(try await channel.readOutbound(as: ByteBuffer.self) == nil)
        let restoredSession = await connection.closeAndCleanup(sendDisconnect: false)
        #expect(restoredSession.inflight.packets.isEmpty)
    }

    private func authenticate(_ channel: NIOAsyncTestingChannel) async throws {
        #expect(try await channel.waitForOutboundPacket().type == .CONNECT)
        try await channel.writeInboundPacket(
            MQTTAuthPacket(reason: .continueAuthentication, properties: [.authenticationData(.init(string: "User"))]),
            version: .v5_0
        )
        #expect(try await channel.waitForOutboundPacket().type == .AUTH)
    }
}
