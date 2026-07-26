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
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InboundGameplayEpochGateTest {

    @Test
    void futureFramesReleaseInOriginalOrderAtEpochCommit() {
        final InboundGameplayEpochGate gate =
                new InboundGameplayEpochGate(8, 1024);
        final EmbeddedChannel channel = channel(gate);
        final ByteBuf first = frame(channel, 1, 1, 10);
        final ByteBuf second = frame(channel, 1, 2, 20);

        assertFalse(channel.writeInbound(first));
        assertFalse(channel.writeInbound(second));
        assertEquals(1, first.refCnt());
        assertEquals(1, second.refCnt());

        gate.advance(channel.pipeline().firstContext(), 1);
        assertEquals(1, gate.currentEpoch());
        assertPacket(channel.readInbound(), 1, 10);
        assertPacket(channel.readInbound(), 2, 20);
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void frameCountOverflowReleasesRejectedAndQueuedPayloads() {
        final InboundGameplayEpochGate gate =
                new InboundGameplayEpochGate(2, 1024);
        final EmbeddedChannel channel = channel(gate);
        final ByteBuf first = frame(channel, 1, 1);
        final ByteBuf second = frame(channel, 1, 2);
        final ByteBuf overflow = frame(channel, 1, 3);

        assertFalse(channel.writeInbound(first));
        assertFalse(channel.writeInbound(second));
        assertThrows(
                CorruptedFrameException.class,
                () -> channel.writeInbound(overflow)
        );
        assertEquals(0, overflow.refCnt());

        assertFalse(channel.finishAndReleaseAll());
        assertEquals(0, first.refCnt());
        assertEquals(0, second.refCnt());
    }

    @Test
    void byteOverflowReleasesRejectedAndQueuedPayloads() {
        final ByteBuf first = frame(1, 1, 2, 3);
        final ByteBuf second = frame(1, 4, 5, 6);
        final int byteLimit = first.readableBytes() + second.readableBytes();
        final InboundGameplayEpochGate gate =
                new InboundGameplayEpochGate(8, byteLimit);
        final EmbeddedChannel channel = channel(gate);
        final ByteBuf overflow = frame(channel, 1, 7);

        assertFalse(channel.writeInbound(first));
        assertFalse(channel.writeInbound(second));
        assertThrows(
                CorruptedFrameException.class,
                () -> channel.writeInbound(overflow)
        );
        assertEquals(0, overflow.refCnt());

        assertFalse(channel.finishAndReleaseAll());
        assertEquals(0, first.refCnt());
        assertEquals(0, second.refCnt());
    }

    private static EmbeddedChannel channel(InboundGameplayEpochGate gate) {
        return new EmbeddedChannel(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                gate.handle(ctx, (ByteBuf) msg);
            }

            @Override
            public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
                gate.close(ctx);
                super.handlerRemoved(ctx);
            }
        });
    }

    private static ByteBuf frame(
            EmbeddedChannel channel,
            int epoch,
            int... bytes
    ) {
        final ByteBuf packet = Unpooled.buffer(bytes.length);
        for (int value : bytes) {
            packet.writeByte(value);
        }
        try {
            return CausalTransportProtocol.encodeGameplayFrame(
                    channel.alloc(),
                    epoch,
                    packet
            );
        } finally {
            packet.release();
        }
    }

    private static ByteBuf frame(int epoch, int... bytes) {
        final ByteBuf packet = Unpooled.buffer(bytes.length);
        for (int value : bytes) {
            packet.writeByte(value);
        }
        try {
            return CausalTransportProtocol.encodeGameplayFrame(
                    UnpooledByteBufAllocator.DEFAULT,
                    epoch,
                    packet
            );
        } finally {
            packet.release();
        }
    }

    private static void assertPacket(ByteBuf packet, int... expected) {
        try {
            assertEquals(expected.length, packet.readableBytes());
            for (int value : expected) {
                assertEquals(value, packet.readUnsignedByte());
            }
        } finally {
            packet.release();
        }
    }

}
