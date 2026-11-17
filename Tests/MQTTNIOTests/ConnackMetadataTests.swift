// Copyright (c) 2026 the MQTTNIO authors
// SPDX-License-Identifier: Apache-2.0

import Logging
import NIOCore
import NIOEmbedded
import Testing

@testable import MQTTNIO

struct ConnackMetadataTests {
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
}
