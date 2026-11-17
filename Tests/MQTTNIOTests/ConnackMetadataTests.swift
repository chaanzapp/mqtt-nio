// Copyright (c) 2026 the MQTTNIO authors
// SPDX-License-Identifier: Apache-2.0

import Logging
import NIOCore
import NIOEmbedded
import Testing

@testable import MQTTNIO

struct ConnackMetadataTests {
    @Test func negotiatedLimitsApplyBeforeSessionReplay() async throws {
        let channel = NIOAsyncTestingChannel()
        var session = MQTTSessionStorage(clientID: "metadata-replay", logger: Logger(label: "metadata-replay"))
        session.inflight.add(
            packet: MQTTPublishPacket(
                publish: .init(
                    qos: .atLeastOnce,
                    retain: false,
                    topicName: "replay",
                    payload: ByteBuffer(repeating: 0, count: 100),
                    properties: []
                ),
                packetId: 1
            )
        )
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: .init(versionConfiguration: .v5_0()),
            session: session
        )
        let connect = Task { try await connection.sendConnect(clientID: "metadata-replay", cleanSession: false) }
        _ = try await channel.waitForOutboundPacket()
        try await channel.writeInboundPacket(
            MQTTConnAckPacket(returnCode: 0, acknowledgeFlags: 1, properties: [.maximumPacketSize(32)]),
            version: .v5_0
        )
        await #expect(throws: MQTTError.packetTooLarge) { try await connect.value }
        #expect(try await channel.readOutbound(as: ByteBuffer.self) == nil)
        let acknowledgement = try #require(await connection.connectAcknowledgement)
        #expect(acknowledgement.sessionPresent)
        #expect(acknowledgement.properties.contains(.maximumPacketSize(32)))
        _ = await connection.closeAndCleanup(sendDisconnect: false)
    }

    @Test func successfulPropertiesAvailableInConnectionClosure() async throws {
        let properties: MQTTProperties = [.receiveMaximum(1), .maximumPacketSize(4096), .userProperty("region", "test")]
        try await withTestMQTTServer(configuration: .init(versionConfiguration: .v5_0()), connackProperties: properties) { connection in
            let acknowledgement = try #require(await connection.connectAcknowledgement)
            #expect(acknowledgement.sessionPresent)
            #expect(acknowledgement.reason == .success)
            #expect(acknowledgement.properties == properties)
        } server: { _ in
        }
    }

    @Test func mqtt311HasNoProperties() async throws {
        try await withTestMQTTServer { connection in
            let acknowledgement = try #require(await connection.connectAcknowledgement)
            #expect(acknowledgement.sessionPresent)
            #expect(acknowledgement.reason == .success)
            #expect(acknowledgement.properties.count == 0)
        } server: { _ in
        }
    }

    @Test(arguments: [false, true])
    func acknowledgementOnlySetAfterSuccess(refused: Bool) async throws {
        let channel = NIOAsyncTestingChannel()
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: .init(versionConfiguration: .v5_0()),
            session: .init(clientID: "metadata-test", logger: Logger(label: "metadata-test"))
        )
        #expect(await connection.connectAcknowledgement == nil)
        let connect = Task { try await connection.sendConnect(clientID: "metadata-test", cleanSession: true) }
        _ = try await channel.waitForOutboundPacket()
        try await channel.writeInboundPacket(
            MQTTConnAckPacket(
                returnCode: refused ? MQTTReasonCode.notAuthorized.rawValue : 0,
                acknowledgeFlags: 0,
                properties: []
            ),
            version: .v5_0
        )
        if refused {
            await #expect(throws: MQTTError.reasonError(.init(reason: .notAuthorized))) { try await connect.value }
            #expect(await connection.connectAcknowledgement == nil)
        } else {
            #expect(try await connect.value == false)
            let acknowledgement = try #require(await connection.connectAcknowledgement)
            #expect(!acknowledgement.sessionPresent)
        }
        _ = await connection.closeAndCleanup(sendDisconnect: false)
    }

    @Test(arguments: [false, true])
    func enhancedAuthenticationStoresOnlySuccessfulConnack(refused: Bool) async throws {
        let channel = NIOAsyncTestingChannel()
        let connection = try await MQTTConnection.setupChannelAndConnect(
            channel,
            configuration: .init(versionConfiguration: .v5_0(authWorkflow: SimpleAuthWorkflow())),
            session: .init(clientID: "metadata-auth", logger: Logger(label: "metadata-auth"))
        )
        let connect = Task { try await connection.sendConnect(clientID: "metadata-auth", cleanSession: true) }
        _ = try await channel.waitForOutboundPacket()
        try await channel.writeInboundPacket(
            MQTTAuthPacket(reason: .continueAuthentication, properties: [.authenticationData(.init(string: "User"))]),
            version: .v5_0
        )
        #expect(try await channel.waitForOutboundPacket().type == .AUTH)
        #expect(await connection.connectAcknowledgement == nil)
        try await channel.writeInboundPacket(
            MQTTConnAckPacket(returnCode: refused ? MQTTReasonCode.notAuthorized.rawValue : 0, acknowledgeFlags: 0, properties: [.receiveMaximum(2)]),
            version: .v5_0
        )
        if refused {
            await #expect(throws: MQTTError.reasonError(.init(reason: .notAuthorized, properties: [.receiveMaximum(2)]))) {
                try await connect.value
            }
            #expect(await connection.connectAcknowledgement == nil)
        } else {
            _ = try await connect.value
            let acknowledgement = try #require(await connection.connectAcknowledgement)
            #expect(acknowledgement.properties.contains(.receiveMaximum(2)))
        }
        _ = await connection.closeAndCleanup(sendDisconnect: false)
    }
}
