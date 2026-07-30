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

import com.ishland.raknetify.common.connection.multichannel.MultichannelPolicy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InboundBulkDependencyGateTest {

    @Test
    void strictFrameWaitsForItsBulkWatermarkWithoutBlockingIndependentData() {
        final InboundBulkDependencyGate gate =
                new InboundBulkDependencyGate(8, 1024);
        final EmbeddedChannel channel = channel(gate);

        assertFalse(channel.writeInbound(strict(1, 20)));
        assertEquals(1, gate.pendingFrames());
        assertTrue(channel.writeInbound(independent(30)));
        assertPacket(channel.readInbound(), 30);

        // The bulk frame itself and the strict frame it unblocks are both
        // forwarded during this write, so EmbeddedChannel reports readable
        // inbound output.
        assertTrue(channel.writeInbound(bulk(1, 10)));
        assertPacket(channel.readInbound(), 10);
        assertPacket(channel.readInbound(), 20);
        assertEquals(0, gate.pendingFrames());
        assertEquals(1, gate.deliveredBulkSequence());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void skippedBulkSequenceFailsClosedAndReleasesPayload() {
        final InboundBulkDependencyGate gate =
                new InboundBulkDependencyGate(8, 1024);
        final EmbeddedChannel channel = channel(gate);
        final ByteBuf packet = Unpooled.buffer(1).writeByte(10);
        final Inbound skipped = new Inbound(
                frame(
                        CausalTransportProtocol.DependencyKind.GUARDED_BULK,
                        2,
                        packet
                ),
                true,
                MultichannelPolicy.GUARDED_BULK_CHANNEL
        );

        assertThrows(
                CorruptedFrameException.class,
                () -> channel.writeInbound(skipped)
        );
        assertEquals(0, packet.refCnt());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void strictQueueOverflowReleasesQueuedAndRejectedPackets() {
        final InboundBulkDependencyGate gate =
                new InboundBulkDependencyGate(1, 1024);
        final EmbeddedChannel channel = channel(gate);
        final ByteBuf firstPacket = Unpooled.buffer(1).writeByte(1);
        final ByteBuf rejectedPacket = Unpooled.buffer(1).writeByte(2);

        assertFalse(channel.writeInbound(new Inbound(
                frame(
                        CausalTransportProtocol.DependencyKind.STRICT,
                        1,
                        firstPacket
                ),
                true,
                MultichannelPolicy.STRICT_GAME_CHANNEL
        )));
        assertThrows(
                CorruptedFrameException.class,
                () -> channel.writeInbound(new Inbound(
                        frame(
                                CausalTransportProtocol.DependencyKind.STRICT,
                                1,
                                rejectedPacket
                        ),
                        true,
                        MultichannelPolicy.STRICT_GAME_CHANNEL
                ))
        );
        assertEquals(0, firstPacket.refCnt());
        assertEquals(0, rejectedPacket.refCnt());
        assertFalse(channel.finishAndReleaseAll());
    }

    private static EmbeddedChannel channel(InboundBulkDependencyGate gate) {
        return new EmbeddedChannel(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                final Inbound inbound = (Inbound) msg;
                gate.handle(
                        ctx,
                        inbound.frame(),
                        inbound.reliableOrdered(),
                        inbound.orderChannel()
                );
            }

            @Override
            public void handlerRemoved(ChannelHandlerContext ctx)
                    throws Exception {
                gate.close(ctx);
                super.handlerRemoved(ctx);
            }
        });
    }

    private static Inbound strict(int sequence, int value) {
        return new Inbound(
                frame(
                        CausalTransportProtocol.DependencyKind.STRICT,
                        sequence,
                        Unpooled.buffer(1).writeByte(value)
                ),
                true,
                MultichannelPolicy.STRICT_GAME_CHANNEL
        );
    }

    private static Inbound bulk(int sequence, int value) {
        return new Inbound(
                frame(
                        CausalTransportProtocol.DependencyKind.GUARDED_BULK,
                        sequence,
                        Unpooled.buffer(1).writeByte(value)
                ),
                true,
                MultichannelPolicy.GUARDED_BULK_CHANNEL
        );
    }

    private static Inbound independent(int value) {
        return new Inbound(
                frame(
                        CausalTransportProtocol.DependencyKind.NONE,
                        0,
                        Unpooled.buffer(1).writeByte(value)
                ),
                true,
                1
        );
    }

    private static CausalTransportProtocol.GameplayFrame frame(
            CausalTransportProtocol.DependencyKind dependencyKind,
            int sequence,
            ByteBuf packet
    ) {
        final List<ByteBuf> packets = new ArrayList<>(1);
        packets.add(packet);
        return new CausalTransportProtocol.GameplayFrame(
                0,
                false,
                dependencyKind,
                sequence,
                packets
        );
    }

    private static void assertPacket(ByteBuf packet, int expected) {
        try {
            assertEquals(expected, packet.readUnsignedByte());
        } finally {
            packet.release();
        }
    }

    private record Inbound(
            CausalTransportProtocol.GameplayFrame frame,
            boolean reliableOrdered,
            int orderChannel
    ) {
    }
}
