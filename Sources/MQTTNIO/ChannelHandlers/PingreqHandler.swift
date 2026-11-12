//===----------------------------------------------------------------------===//
//
// This source file is part of the MQTTNIO project
//
// Copyright (c) 2020-2021 Adam Fowler
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIO

/// Channel handler for sending PINGREQ messages to keep connect alive
final class PingreqHandler {
    typealias OutboundIn = MQTTPacket
    typealias OutboundOut = MQTTPacket
    typealias InboundIn = MQTTPacket
    typealias InboundOut = MQTTPacket

    let client: MQTTClient
    var timeout: TimeAmount
    var lastEventTime: NIODeadline
    var task: Scheduled<Void>?
    private var generation: UInt64 = 0

    init(client: MQTTClient, timeout: TimeAmount) {
        self.client = client
        self.timeout = timeout
        self.lastEventTime = .now()
        self.task = nil
    }

    func updateTimeout(_ timeout: TimeAmount, context: ChannelHandlerContext) {
        self.stop()
        self.timeout = timeout
        self.start(context: context)
    }

    func start(context: ChannelHandlerContext) {
        guard self.task == nil else { return }
        self.scheduleTask(context)
    }

    func stop() {
        self.generation &+= 1
        self.task?.cancel()
        self.task = nil
    }

    func write() {
        self.lastEventTime = .now()
    }

    func scheduleTask(_ context: ChannelHandlerContext) {
        guard context.channel.isActive, self.timeout.nanoseconds > 0 else { return }
        let generation = self.generation

        self.task = context.eventLoop.scheduleTask(deadline: self.lastEventTime + self.timeout) {
            guard generation == self.generation else { return }
            // if lastEventTime plus the timeout is less than now send PINGREQ
            // otherwise reschedule task
            if self.lastEventTime + self.timeout <= .now() {
                guard context.channel.isActive else { return }

                self.client.ping().whenComplete { result in
                    guard generation == self.generation else { return }
                    switch result {
                    case .failure(let error):
                        context.fireErrorCaught(error)
                    case .success:
                        break
                    }
                    self.lastEventTime = .now()
                    self.scheduleTask(context)
                }
            } else {
                self.scheduleTask(context)
            }
        }
    }
}
