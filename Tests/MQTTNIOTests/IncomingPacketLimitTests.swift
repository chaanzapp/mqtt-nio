// Copyright (c) 2026 the MQTTNIO authors
// SPDX-License-Identifier: Apache-2.0

import Logging
import NIOCore
import NIOEmbedded
import Testing

@testable import MQTTNIO

struct IncomingPacketLimitTests {
    private final class WithheldWrite: ChannelOutboundHandler {
        typealias OutboundIn = ByteBuffer
        var writes = 0
        var pending: EventLoopPromise<Void>?

        func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
            self.writes += 1
            self.pending = promise
        }
    }

    @Test(arguments: [false, true])
    func noFurtherInputWhileDisconnectFlushIsPending(completeWrite: Bool) throws {
        let loop = EmbeddedEventLoop()
        let handler = MQTTChannelHandler(
            configuration: .init(.init(versionConfiguration: .v5_0(connectProperties: [.maximumPacketSize(16)]), timeout: .seconds(1))),
            eventLoop: loop,
            session: .init(clientID: "pending-close", logger: Logger(label: "pending-close")),
            logger: Logger(label: "pending-close")
        )
        let withheld = WithheldWrite()
        let channel = EmbeddedChannel(handlers: [withheld, handler], loop: loop)
        try channel.connect(to: .init(ipAddress: "127.0.0.1", port: 1883)).wait()
        #expect(throws: MQTTError.packetTooLarge) { try channel.writeInbound(ByteBuffer(bytes: [0x20, 0x10])) }
        #expect(channel.isActive)
        try channel.writeInbound(ByteBuffer(bytes: [0xD0, 0x00]))
        try channel.writeInbound(ByteBuffer(bytes: [0x20, 0x10]))
        #expect(withheld.writes == 1)
        let pending = try #require(withheld.pending)
        if completeWrite {
            pending.succeed(())
        } else {
            loop.advanceTime(by: .seconds(1))
            pending.fail(ChannelError.ioOnClosedChannel)
        }
        #expect(!channel.isActive)
        _ = try channel.finish(acceptAlreadyClosed: true)
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

    @Test func configurationUsesConnectLimitOnly() {
        let configured = MQTTChannelHandler.Configuration(
            .init(
                versionConfiguration: .v5_0(
                    connectProperties: [.userProperty("test", "value"), .maximumPacketSize(4096)])
            )
        )
        #expect(configured.maximumIncomingPacketSize == 4096)
        let largest = MQTTChannelHandler.Configuration(.init(versionConfiguration: .v5_0(connectProperties: [.maximumPacketSize(.max)])))
        #expect(largest.maximumIncomingPacketSize == Int(clamping: UInt32.max))
        #expect(MQTTChannelHandler.Configuration(.init(versionConfiguration: .v5_0())).maximumIncomingPacketSize == nil)
        #expect(MQTTChannelHandler.Configuration(.init()).maximumIncomingPacketSize == nil)
    }

    @Test(arguments: [false, true])
    func configuredLimitClosesChannelWithDisconnect(responseFits: Bool) throws {
        let loop = EmbeddedEventLoop()
        let handler = MQTTChannelHandler(
            configuration: .init(.init(versionConfiguration: .v5_0(connectProperties: [.maximumPacketSize(16)]))),
            eventLoop: loop,
            session: .init(clientID: "packet-limit", logger: Logger(label: "packet-limit")),
            logger: Logger(label: "packet-limit")
        )
        handler.maxPacketSize = responseFits ? .max : 1
        let channel = EmbeddedChannel(handler: handler, loop: loop)
        try channel.connect(to: .init(ipAddress: "127.0.0.1", port: 1883)).wait()
        #expect(throws: MQTTError.packetTooLarge) { try channel.writeInbound(ByteBuffer(bytes: [0x20, 0x10])) }
        if responseFits {
            var bytes = try #require(try channel.readOutbound(as: ByteBuffer.self))
            let packet = try MQTTIncomingPacket.read(from: &bytes)
            #expect(packet.type == .DISCONNECT)
            #expect(try MQTTDisconnectPacket.read(version: .v5_0, from: packet).reason == .packetTooLarge)
        } else {
            #expect(try channel.readOutbound(as: ByteBuffer.self) == nil)
        }
        #expect(!channel.isActive)
        _ = try channel.finish(acceptAlreadyClosed: true)
    }
}
