// Copyright (c) 2026 the MQTTNIO authors
// SPDX-License-Identifier: Apache-2.0

import Logging
import NIOCore
import NIOEmbedded
import Testing

@testable import MQTTNIO

struct IncomingPacketLimitTests {
    private final class ReentrantInputOnError: ChannelInboundHandler {
        typealias InboundIn = ByteBuffer
        var errorCount = 0

        func errorCaught(context: ChannelHandlerContext, error: any Error) {
            self.errorCount += 1
            // Error notification happens before close; new input must already be ignored.
            context.channel.pipeline.fireChannelRead(ByteBuffer(bytes: [0xD0, 0x00]))
            context.fireErrorCaught(error)
        }
    }

    @Test func oversizedHeaderRejectedWithoutBody() {
        var decoder = ByteToMQTTMessageDecoder(version: .v5_0, maximumPacketSize: 4096)
        var input = ByteBuffer(bytes: [0x20, 0x80, 0x20])
        #expect(throws: MQTTError.packetTooLarge) { try decoder.decode(buffer: &input) }
    }

    @Test func exactLimitAndFragments() throws {
        var decoder = ByteToMQTTMessageDecoder(version: .v5_0, maximumPacketSize: 5)
        var input = ByteBuffer(bytes: [0x20, 0x03])
        #expect(try decoder.decode(buffer: &input) == nil)
        #expect(input.readerIndex == 0)
        input.writeBytes([0, 0, 0])
        #expect(try decoder.decode(buffer: &input) is MQTTConnAckPacket)
        #expect(input.readableBytes == 0)
    }

    @Test(arguments: [0, 127, 128, 16383, 16384, 2_097_151, 2_097_152, 268_435_455])
    func legalLengthWidths(value: Int) throws {
        var input = ByteBuffer()
        MQTTSerializer.writeVariableLengthInteger(value, to: &input)
        #expect(try MQTTSerializer.readVariableLengthInteger(from: &input) == value)
        #expect(input.readableBytes == 0)
    }

    @Test(arguments: [1, 2, 3])
    func partialLengthCanReceiveMoreBytes(count: Int) throws {
        var decoder = ByteToMQTTMessageDecoder(version: .v5_0, maximumPacketSize: 4096)
        var input = ByteBuffer(bytes: [0x20] + Array(repeating: UInt8(0x80), count: count))
        #expect(try decoder.decode(buffer: &input) == nil)
        #expect(input.readerIndex == 0)
    }

    @Test(arguments: [false, true])
    func fourthContinuationByteRejected(hasFifthByte: Bool) {
        var input = ByteBuffer(bytes: [0x80, 0x80, 0x80, 0x80] + (hasFifthByte ? [0] : []))
        #expect(throws: MQTTError.decodeError) { try MQTTSerializer.readVariableLengthInteger(from: &input) }
        #expect(input.readerIndex == 4)
    }

    @Test(arguments: [0, 128, 16384, 2_097_152])
    func completePacketLimitCountsActualHeader(bodyLength: Int) {
        var input = ByteBuffer(bytes: [0x30])
        MQTTSerializer.writeVariableLengthInteger(bodyLength, to: &input)
        let fullSize = input.readableBytes + bodyLength
        #expect(throws: MQTTError.packetTooLarge) {
            try MQTTIncomingPacket.read(from: &input, maximumPacketSize: fullSize - 1)
        }
    }

    @Test func configurationUsesConnectLimitOnly() throws {
        let configured = try MQTTChannelHandler.Configuration(
            .init(
                versionConfiguration: .v5_0(
                    connectProperties: [.userProperty("test", "value"), .maximumPacketSize(4096)])
            )
        )
        #expect(configured.maximumIncomingPacketSize == 4096)
        let largest = try MQTTChannelHandler.Configuration(.init(versionConfiguration: .v5_0(connectProperties: [.maximumPacketSize(.max)])))
        #expect(largest.maximumIncomingPacketSize == Int(clamping: UInt32.max))
        #expect(try MQTTChannelHandler.Configuration(.init(versionConfiguration: .v5_0())).maximumIncomingPacketSize == nil)
        #expect(try MQTTChannelHandler.Configuration(.init()).maximumIncomingPacketSize == nil)
    }

    @Test(arguments: [[UInt32(0)], [16, 16], [16, 32]])
    func invalidConnectLimitRejectedBeforeSending(sizes: [UInt32]) async throws {
        let configuration = MQTTConnectionConfiguration(
            versionConfiguration: .v5_0(connectProperties: .init(sizes.map { .maximumPacketSize($0) }))
        )
        #expect(throws: MQTTPacketError.badParameter) {
            try MQTTChannelHandler.Configuration(configuration)
        }
        let channel = NIOAsyncTestingChannel()
        await #expect(throws: MQTTPacketError.badParameter) {
            try await MQTTConnection.setupChannelAndConnect(
                channel,
                configuration: configuration,
                session: .init(clientID: "invalid-limit", logger: Logger(label: "invalid-limit"))
            )
        }
        #expect(!channel.isActive)
        #expect(try await channel.readOutbound(as: ByteBuffer.self) == nil)
        try await channel.close().get()
    }

    @Test(arguments: [false, true])
    func invalidConnectLimitRejectedBeforeOpeningTransport(webSocket: Bool) async {
        await #expect(throws: MQTTPacketError.badParameter) {
            try await MQTTConnection.withConnection(
                address: .unixDomainSocket(path: "/tmp/mqtt-pr2-nonexistent.sock"),
                configuration: .init(
                    versionConfiguration: .v5_0(connectProperties: [.maximumPacketSize(0)]),
                    transport: webSocket ? .webSocket(.init()) : .tcp()
                )
            ) { _ in
                Issue.record("Invalid configuration reached the connection operation")
            }
        }
    }

    @Test(arguments: [false, true])
    func invalidHeaderClosesChannelWithoutDisconnect(malformedLength: Bool) throws {
        let loop = EmbeddedEventLoop()
        let handler = MQTTChannelHandler(
            configuration: try .init(.init(versionConfiguration: .v5_0(connectProperties: [.maximumPacketSize(16)]))),
            eventLoop: loop,
            session: .init(clientID: "packet-limit", logger: Logger(label: "packet-limit")),
            logger: Logger(label: "packet-limit")
        )
        let reentrantInput = ReentrantInputOnError()
        let channel = EmbeddedChannel(handlers: [handler, reentrantInput], loop: loop)
        try channel.connect(to: .init(ipAddress: "127.0.0.1", port: 1883)).wait()
        let bytes: [UInt8] = malformedLength ? [0x20, 0x80, 0x80, 0x80, 0x80] : [0x20, 0x10]
        let error: MQTTError = malformedLength ? .decodeError : .packetTooLarge
        #expect(throws: error) { try channel.writeInbound(ByteBuffer(bytes: bytes)) }
        #expect(reentrantInput.errorCount == 1)
        #expect(!channel.isActive)
        #expect(try channel.readOutbound(as: ByteBuffer.self) == nil)
        _ = try channel.finish(acceptAlreadyClosed: true)
    }
}
