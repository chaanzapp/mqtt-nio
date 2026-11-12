// SPDX-License-Identifier: Apache-2.0

import NIO
import XCTest

@testable import MQTTNIO

final class MQTTPacketLimitTests: XCTestCase {
    func testOversizedPacketRejectedBeforeItsBodyArrives() throws {
        var decoder = ByteToMQTTMessageDecoder(version: .v5_0, maximumPacketSize: 4096)
        var bytes = ByteBuffer(bytes: [0x20, 0xFF, 0x1F])  // Remaining Length=4095; total=4098.
        XCTAssertThrowsError(try decoder.decode(buffer: &bytes)) { error in
            guard case MQTTError.packetTooLarge = error else { return XCTFail("Unexpected error: \(error)") }
        }
    }

    func testExactLimitAndFragmentedHeader() throws {
        var decoder = ByteToMQTTMessageDecoder(version: .v5_0, maximumPacketSize: 5)
        var bytes = ByteBuffer(bytes: [0x20])
        XCTAssertNil(try decoder.decode(buffer: &bytes))
        bytes.writeBytes([0x03, 0x00, 0x00, 0x00] as [UInt8])
        XCTAssertNotNil(try decoder.decode(buffer: &bytes))
        XCTAssertEqual(bytes.readableBytes, 0)
    }

    func testOverlongRemainingLengthRejectedWithoutWaitingForFifthByte() throws {
        var decoder = ByteToMQTTMessageDecoder(version: .v5_0)
        var bytes = ByteBuffer(bytes: [0x20, 0x80, 0x80, 0x80, 0x80])
        XCTAssertThrowsError(try decoder.decode(buffer: &bytes)) { error in
            guard case MQTTError.decodeError = error else { return XCTFail("Unexpected error: \(error)") }
        }
    }

    func testLargestValidVariableLengthInteger() throws {
        var bytes = ByteBuffer(bytes: [0xFF, 0xFF, 0xFF, 0x7F])
        XCTAssertEqual(try MQTTSerializer.readVariableLengthInteger(from: &bytes), 268_435_455)
    }
}
