/*
 * This file is a part of the Raknetify project, licensed under MIT.
 *
 * Copyright (c) 2022-2025 ishland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.ishland.raknetify.common.connection;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.CorruptedFrameException;

final class CausalFenceProtocol {

    static final int ORDER_CHANNEL_COUNT = 8;

    private static final int MAGIC = 0x524b4631; // "RKF1"
    private static final int VERSION = 1;
    private static final int REQUEST = 1;
    private static final int ACK = 2;

    private CausalFenceProtocol() {
    }

    static ByteBuf encodeRequest(
            ByteBufAllocator allocator,
            long fenceId,
            int epoch,
            int channelMask
    ) {
        validateFence(fenceId, epoch);
        if (channelMask <= 0 || (channelMask >>> ORDER_CHANNEL_COUNT) != 0) {
            throw new IllegalArgumentException("Invalid fence channel mask: " + channelMask);
        }
        return allocator.buffer(22, 22)
                .writeInt(MAGIC)
                .writeByte(VERSION)
                .writeByte(REQUEST)
                .writeLong(fenceId)
                .writeInt(epoch)
                .writeInt(channelMask);
    }

    static ByteBuf encodeAck(ByteBufAllocator allocator, long fenceId, int epoch) {
        validateFence(fenceId, epoch);
        return allocator.buffer(18, 18)
                .writeInt(MAGIC)
                .writeByte(VERSION)
                .writeByte(ACK)
                .writeLong(fenceId)
                .writeInt(epoch);
    }

    static Message decode(ByteBuf payload) {
        if (payload.readableBytes() < 18) {
            throw new CorruptedFrameException("Truncated causal fence message");
        }
        if (payload.readInt() != MAGIC) {
            throw new CorruptedFrameException("Invalid causal fence magic");
        }
        final int version = payload.readUnsignedByte();
        if (version != VERSION) {
            throw new CorruptedFrameException("Unsupported causal fence version: " + version);
        }
        final int type = payload.readUnsignedByte();
        final long fenceId = payload.readLong();
        final int epoch = payload.readInt();
        if (fenceId <= 0 || epoch < 0) {
            throw new CorruptedFrameException("Invalid causal fence id or epoch");
        }
        if (type == ACK) {
            if (payload.isReadable()) {
                throw new CorruptedFrameException("Trailing bytes after causal fence ACK");
            }
            return new Ack(fenceId, epoch);
        }
        if (type == REQUEST) {
            if (payload.readableBytes() != 4) {
                throw new CorruptedFrameException("Invalid causal fence request length");
            }
            final int channelMask = payload.readInt();
            if (channelMask <= 0 || (channelMask >>> ORDER_CHANNEL_COUNT) != 0) {
                throw new CorruptedFrameException("Invalid causal fence channel mask: "
                        + channelMask);
            }
            return new Request(fenceId, epoch, channelMask);
        }
        throw new CorruptedFrameException("Unknown causal fence message type: " + type);
    }

    private static void validateFence(long fenceId, int epoch) {
        if (fenceId <= 0 || epoch < 0) {
            throw new IllegalArgumentException("Invalid causal fence id or epoch");
        }
    }

    sealed interface Message permits Request, Ack {
        long fenceId();

        int epoch();
    }

    record Request(long fenceId, int epoch, int channelMask) implements Message {
    }

    record Ack(long fenceId, int epoch) implements Message {
    }

}
