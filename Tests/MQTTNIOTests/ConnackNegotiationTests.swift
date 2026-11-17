// Copyright (c) 2026 the MQTTNIO authors
// SPDX-License-Identifier: Apache-2.0

import Logging
import NIOCore
import NIOEmbedded
import Testing

@testable import MQTTNIO

struct ConnackNegotiationTests {
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

    @Test(arguments: [0, 1, 2], [false, true])
    func resumedSessionSkipsRejectedPacketAndSendsRemaining(limit: Int, enhancedAuth: Bool) async throws {
        let properties: MQTTProperties =
            switch limit {
            case 0: [.maximumQoS(.atLeastOnce)]
            case 1: [.retainAvailable(0)]
            default: [.maximumPacketSize(50)]
            }
        let rejected = MQTTPublishPacket(
            publish: .init(qos: .exactlyOnce, retain: true, topicName: "queued", payload: ByteBuffer(repeating: 1, count: 100), properties: []),
            packetId: 41
        )
        var session = MQTTSessionStorage(clientID: "replay", logger: Logger.current)
        session.inflight.add(packet: rejected)
        session.inflight.add(packet: MQTTPubAckPacket(type: .PUBREL, packetId: 42))
        session.inflight.add(
            packet: MQTTPublishPacket(
                publish: .init(qos: .atLeastOnce, retain: false, topicName: "allowed", payload: ByteBuffer(), properties: []),
                packetId: 43
            )
        )
        let channel = NIOAsyncTestingChannel()
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: .init(versionConfiguration: .v5_0(authWorkflow: enhancedAuth ? SimpleAuthWorkflow() : nil)),
            session: session
        )
        async let connect = connectClosingOnFailure(connection)
        if enhancedAuth {
            try await authenticate(channel)
        } else {
            #expect(try await channel.waitForOutboundPacket().type == .CONNECT)
        }
        try await channel.writeInboundPacket(
            MQTTConnAckPacket(returnCode: 0, acknowledgeFlags: 1, properties: properties),
            version: .v5_0
        )
        let pubrel = try MQTTPubAckPacket.read(version: .v5_0, from: try await channel.waitForOutboundPacket())
        #expect(pubrel.type == .PUBREL)
        #expect(pubrel.packetId == 42)
        try await channel.writeInboundPacket(MQTTPubAckPacket(type: .PUBCOMP, packetId: 42), version: .v5_0)
        let publish = try MQTTPublishPacket.read(version: .v5_0, from: try await channel.waitForOutboundPacket())
        #expect(publish.packetId == 43)
        #expect(publish.publish.dup)
        #expect(publish.publish.topicName == "allowed")
        try await channel.writeInboundPacket(MQTTPubAckPacket(type: .PUBACK, packetId: 43), version: .v5_0)
        #expect(try await connect)
        #expect(channel.isActive)
        let restored = await connection.closeAndCleanup(sendDisconnect: false)
        #expect(restored.inflight.packets.isEmpty)
    }

    @Test(arguments: [false, true])
    func interruptedReplayPreservesUnattemptedPackets(inboundPacketTooLarge: Bool) async throws {
        var session = MQTTSessionStorage(clientID: "replay", logger: Logger.current)
        for id in [UInt16(41), 42, 43] {
            session.inflight.add(packet: MQTTPubAckPacket(type: .PUBREL, packetId: id))
        }
        let channel = NIOAsyncTestingChannel()
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: .init(versionConfiguration: .v5_0()),
            session: session
        )
        async let connect = connection.sendConnect(clientID: "replay", cleanSession: false)
        #expect(try await channel.waitForOutboundPacket().type == .CONNECT)
        try await channel.writeInboundPacket(MQTTConnAckPacket(returnCode: 0, acknowledgeFlags: 1, properties: []), version: .v5_0)
        let packet = try MQTTPubAckPacket.read(version: .v5_0, from: try await channel.waitForOutboundPacket())
        #expect(packet.packetId == 41)
        if inboundPacketTooLarge {
            try await channel.testingEventLoop.executeInContext {
                channel.pipeline.fireErrorCaught(MQTTError.packetTooLarge)
            }
        } else {
            try await channel.close().get()
        }
        do {
            _ = try await connect
            Issue.record("Replay succeeded after a terminal connection error")
        } catch {
            #expect(error as? MQTTError == (inboundPacketTooLarge ? .packetTooLarge : .serverClosedConnection))
        }
        let restored = await connection.closeAndCleanup(sendDisconnect: false)
        #expect(restored.inflight.packets.map { $0.packetId } == [41, 42, 43])
    }

    @Test func invalidReplayAliasPreservesCurrentAndRemainingPackets() async throws {
        var session = MQTTSessionStorage(clientID: "replay", logger: Logger.current)
        session.inflight.add(
            packet: MQTTPublishPacket(
                publish: .init(qos: .atLeastOnce, retain: false, topicName: "queued", payload: ByteBuffer(), properties: [.topicAlias(2)]),
                packetId: 41
            )
        )
        session.inflight.add(packet: MQTTPubAckPacket(type: .PUBREL, packetId: 42))
        let channel = NIOAsyncTestingChannel()
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: .init(versionConfiguration: .v5_0()),
            session: session
        )
        async let connect = connection.sendConnect(clientID: "replay", cleanSession: false)
        #expect(try await channel.waitForOutboundPacket().type == .CONNECT)
        try await channel.writeInboundPacket(
            MQTTConnAckPacket(returnCode: 0, acknowledgeFlags: 1, properties: [.topicAliasMaximum(1)]),
            version: .v5_0
        )
        do {
            _ = try await connect
            Issue.record("Replay accepted an invalid topic alias")
        } catch MQTTPacketError.topicAliasOutOfRange {
        }
        let restored = await connection.closeAndCleanup(sendDisconnect: false)
        #expect(restored.inflight.packets.map { $0.packetId } == [41, 42])
    }

    private func connectClosingOnFailure(_ connection: MQTTConnection) async throws -> Bool {
        do {
            return try await connection.sendConnect(clientID: "replay", cleanSession: false)
        } catch {
            // Unblock the peer's next read if replay fails before writing anything.
            _ = await connection.closeAndCleanup(sendDisconnect: false)
            throw error
        }
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
