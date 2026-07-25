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

import com.ishland.raknetify.common.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import network.ycc.raknet.frame.FrameData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SynchronizationLayerTest {

    @Test
    void preFencePeerFallbackNeverDeletesOrCompletesAQueuedFrameEarly() {
        final EmbeddedChannel channel = new EmbeddedChannel(new SynchronizationLayer());
        final ChannelPromise syncPromise = channel.newPromise();
        channel.pipeline().write(SynchronizationLayer.SYNC_REQUEST_OBJECT, syncPromise);

        final ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{12});
        final FrameData frameData;
        try {
            frameData = FrameData.create(channel.alloc(), 0xfd, payload);
        } finally {
            payload.release();
        }
        final ChannelPromise framePromise = channel.newPromise();
        channel.pipeline().write(frameData, framePromise);
        channel.pipeline().flush();

        assertTrue(syncPromise.isSuccess());
        assertTrue(framePromise.isSuccess());
        final FrameData outbound = channel.readOutbound();
        assertEquals(0xfd, outbound.getPacketId());
        outbound.release();
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void fenceWaitsForEveryChannelAndPreservesLaterWrites() {
        final EventCapture senderEvents = new EventCapture();
        final EventCapture receiverEvents = new EventCapture();
        final EmbeddedChannel sender = channel(senderEvents);
        final EmbeddedChannel receiver = channel(receiverEvents);

        final ChannelPromise fencePromise = sender.newPromise();
        sender.pipeline().write(SynchronizationLayer.SYNC_REQUEST_OBJECT, fencePromise);
        sender.pipeline().flush();
        assertFalse(fencePromise.isDone());

        final ByteBuf laterPayload = Unpooled.wrappedBuffer(new byte[]{11});
        final FrameData laterFrame;
        try {
            laterFrame = FrameData.create(sender.alloc(), 0xfd, laterPayload);
            laterFrame.setOrderChannel(7);
        } finally {
            laterPayload.release();
        }
        final ChannelPromise laterPromise = sender.newPromise();
        sender.pipeline().write(laterFrame, laterPromise);
        sender.pipeline().flush();
        assertFalse(laterPromise.isDone());

        final List<FrameData> requests = new ArrayList<>();
        FrameData outbound;
        while ((outbound = sender.readOutbound()) != null) {
            assertEquals(Constants.RAKNET_SYNC_PACKET_ID, outbound.getPacketId());
            requests.add(outbound);
        }
        assertEquals(CausalFenceProtocol.ORDER_CHANNEL_COUNT, requests.size());
        Collections.reverse(requests);
        for (int i = 0; i < requests.size() - 1; i++) {
            assertFalse(receiver.writeInbound(requests.get(i)));
            assertFalse(receiverEvents.hasInboundEpoch());
        }
        assertFalse(receiver.writeInbound(requests.get(requests.size() - 1)));
        assertTrue(receiverEvents.hasInboundEpoch());

        receiver.flushOutbound();
        final FrameData ack = receiver.readOutbound();
        assertEquals(Constants.RAKNET_SYNC_PACKET_ID, ack.getPacketId());
        assertFalse(sender.writeInbound(ack));
        sender.flushOutbound();
        assertTrue(fencePromise.isSuccess());
        assertTrue(laterPromise.isSuccess());
        assertTrue(senderEvents.hasOutboundEpoch());

        final FrameData releasedLaterFrame = sender.readOutbound();
        assertEquals(0xfd, releasedLaterFrame.getPacketId());
        releasedLaterFrame.release();
        assertFalse(sender.finishAndReleaseAll());
        assertFalse(receiver.finishAndReleaseAll());
    }

    @Test
    void overflowingFenceQueueFailsFenceAndReleasesEveryWrite() {
        final SynchronizationLayer synchronizationLayer =
                new SynchronizationLayer(new int[0], 4, 4);
        final EmbeddedChannel channel = new EmbeddedChannel(
                synchronizationLayer
        );
        CausalTransportProtocol.setNegotiatedCapabilities(
                channel,
                CausalTransportProtocol.LOCAL_CAPABILITIES
        );

        final ChannelPromise fencePromise = channel.newPromise();
        channel.pipeline().write(
                SynchronizationLayer.SYNC_REQUEST_OBJECT,
                fencePromise
        );
        final ByteBuf first = Unpooled.buffer(2).writeZero(2);
        final ByteBuf second = Unpooled.buffer(2).writeZero(2);
        final ByteBuf overflow = Unpooled.buffer(1).writeZero(1);
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        final ChannelPromise overflowPromise = channel.newPromise();
        channel.pipeline().write(first, firstPromise);
        channel.pipeline().write(second, secondPromise);
        channel.pipeline().write(overflow, overflowPromise);

        assertThrows(CorruptedFrameException.class, channel::checkException);
        assertTrue(fencePromise.isDone());
        assertFalse(fencePromise.isSuccess());
        assertTrue(firstPromise.isDone());
        assertFalse(firstPromise.isSuccess());
        assertTrue(secondPromise.isDone());
        assertFalse(secondPromise.isSuccess());
        assertTrue(overflowPromise.isDone());
        assertFalse(overflowPromise.isSuccess());
        assertEquals(0, first.refCnt());
        assertEquals(0, second.refCnt());
        assertEquals(0, overflow.refCnt());
        Object outbound;
        while ((outbound = channel.readOutbound()) != null) {
            ReferenceCountUtil.safeRelease(outbound);
        }
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void inboundFenceMustUseTheExactLocalChannelMask() {
        final EmbeddedChannel channel = channel(new EventCapture());

        assertThrows(
                CorruptedFrameException.class,
                () -> channel.writeInbound(fenceRequest(
                        channel,
                        1,
                        1,
                        0x7f,
                        0
                ))
        );
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void inboundFenceIdsCannotInterleaveOrBeReusedForAnotherEpoch() {
        final EmbeddedChannel interleaved = channel(new EventCapture());
        assertFalse(interleaved.writeInbound(fenceRequest(
                interleaved,
                1,
                1,
                0xff,
                0
        )));
        assertThrows(
                CorruptedFrameException.class,
                () -> interleaved.writeInbound(fenceRequest(
                        interleaved,
                        2,
                        1,
                        0xff,
                        1
                ))
        );
        assertFalse(interleaved.finishAndReleaseAll());

        final EmbeddedChannel reused = channel(new EventCapture());
        for (int orderChannel = 0;
             orderChannel < CausalFenceProtocol.ORDER_CHANNEL_COUNT;
             orderChannel++) {
            assertFalse(reused.writeInbound(fenceRequest(
                    reused,
                    1,
                    1,
                    0xff,
                    orderChannel
            )));
        }
        ReferenceCountUtil.safeRelease(reused.readOutbound());
        assertThrows(
                CorruptedFrameException.class,
                () -> reused.writeInbound(fenceRequest(
                        reused,
                        1,
                        2,
                        0xff,
                        0
                ))
        );
        assertFalse(reused.finishAndReleaseAll());
    }

    private static FrameData fenceRequest(
            EmbeddedChannel channel,
            long fenceId,
            int epoch,
            int channelMask,
            int orderChannel
    ) {
        final ByteBuf payload = CausalFenceProtocol.encodeRequest(
                channel.alloc(),
                fenceId,
                epoch,
                channelMask
        );
        final FrameData request;
        try {
            request = FrameData.create(
                    channel.alloc(),
                    Constants.RAKNET_SYNC_PACKET_ID,
                    payload
            );
            request.setOrderChannel(orderChannel);
        } finally {
            payload.release();
        }
        return request;
    }

    private static EmbeddedChannel channel(EventCapture capture) {
        final EmbeddedChannel channel = new EmbeddedChannel(
                new SynchronizationLayer(),
                capture
        );
        CausalTransportProtocol.setNegotiatedCapabilities(
                channel,
                CausalTransportProtocol.LOCAL_CAPABILITIES
        );
        return channel;
    }

    private static final class EventCapture extends ChannelInboundHandlerAdapter {
        private final List<Object> events = new ArrayList<>();

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            events.add(evt);
            ctx.fireUserEventTriggered(evt);
        }

        private boolean hasInboundEpoch() {
            return events.stream().anyMatch(SynchronizationLayer.InboundEpochAdvanced.class::isInstance);
        }

        private boolean hasOutboundEpoch() {
            return events.stream().anyMatch(SynchronizationLayer.OutboundEpochAdvanced.class::isInstance);
        }
    }

}
