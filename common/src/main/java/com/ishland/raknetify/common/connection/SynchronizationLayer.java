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
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import network.ycc.raknet.frame.FrameData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Lossless cross-channel drain fence.
 *
 * <p>A request marker is appended to every active ordered channel. The peer
 * acknowledges only after every marker reaches this handler, proving that all
 * earlier ordered frames and their fragments were delivered. Later writes stay
 * queued until that acknowledgement. No reliability queue, order index or
 * packet promise is deleted or rewritten.</p>
 */
public class SynchronizationLayer extends ChannelDuplexHandler {

    public static final Object SYNC_REQUEST_OBJECT = new Object();

    private static final int MAX_INBOUND_FENCES = 32;

    private final IntSet channelsToIgnore = new IntOpenHashSet();
    private final Queue<PendingWrite> queuedWrites = new LinkedList<>();
    private final List<ChannelPromise> activeFencePromises = new ArrayList<>();
    private final Map<Long, InboundFence> inboundFences = new HashMap<>();

    private boolean waitingForAck;
    private long nextFenceId = 1L;
    private long activeFenceId;
    private int activeFenceEpoch;
    private int outboundEpoch;
    private int inboundEpoch;
    private long lastCompletedInboundFenceId;

    public SynchronizationLayer(int... channelsToIgnore) {
        for (int channel : channelsToIgnore) {
            if (channel < 0 || channel >= CausalFenceProtocol.ORDER_CHANNEL_COUNT) {
                throw new IllegalArgumentException("Invalid ignored order channel: " + channel);
            }
            this.channelsToIgnore.add(channel);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg == SYNC_REQUEST_OBJECT) {
            if (!CausalTransportProtocol.hasCapability(
                    ctx.channel(),
                    CausalTransportProtocol.CAPABILITY_LOSSLESS_FENCE
            )) {
                // A pre-fence peer cannot acknowledge the new protocol. Preserve
                // every queued frame and let per-channel ordering continue.
                promise.trySuccess();
                return;
            }
            if (waitingForAck) {
                activeFencePromises.add(promise);
                return;
            }
            beginFence(ctx, promise);
            return;
        }
        if (waitingForAck) {
            queuedWrites.add(new PendingWrite(msg, promise));
            return;
        }
        super.write(ctx, msg, promise);
    }

    private void beginFence(ChannelHandlerContext ctx, ChannelPromise promise) {
        if (outboundEpoch == Integer.MAX_VALUE) {
            final IllegalStateException exception =
                    new IllegalStateException("Gameplay epoch exhausted");
            promise.tryFailure(exception);
            ctx.fireExceptionCaught(exception);
            return;
        }

        int channelMask = 0;
        for (int channel = 0; channel < CausalFenceProtocol.ORDER_CHANNEL_COUNT; channel++) {
            if (!channelsToIgnore.contains(channel)) {
                channelMask |= 1 << channel;
            }
        }
        if (channelMask == 0) {
            final IllegalStateException exception =
                    new IllegalStateException("Causal fence has no active channels");
            promise.tryFailure(exception);
            ctx.fireExceptionCaught(exception);
            return;
        }

        waitingForAck = true;
        activeFenceId = nextFenceId++;
        activeFenceEpoch = outboundEpoch + 1;
        activeFencePromises.add(promise);

        for (int channel = 0; channel < CausalFenceProtocol.ORDER_CHANNEL_COUNT; channel++) {
            if ((channelMask & 1 << channel) == 0) {
                continue;
            }
            writeFenceRequest(ctx, activeFenceId, activeFenceEpoch, channelMask, channel);
        }
    }

    private void writeFenceRequest(
            ChannelHandlerContext ctx,
            long fenceId,
            int epoch,
            int channelMask,
            int channel
    ) {
        final ByteBuf payload = CausalFenceProtocol.encodeRequest(
                ctx.alloc(),
                fenceId,
                epoch,
                channelMask
        );
        FrameData frameData = null;
        try {
            frameData = FrameData.create(ctx.alloc(), Constants.RAKNET_SYNC_PACKET_ID, payload);
            frameData.setOrderChannel(channel);
            ctx.write(frameData).addListener(future -> {
                if (!future.isSuccess()) {
                    failActiveFence(ctx, future.cause());
                }
            });
            frameData = null;
        } finally {
            payload.release();
            ReferenceCountUtil.safeRelease(frameData);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof FrameData packet)
                || packet.isFragment()
                || packet.getDataSize() <= 0
                || packet.getPacketId() != Constants.RAKNET_SYNC_PACKET_ID) {
            super.channelRead(ctx, msg);
            return;
        }

        try {
            if (!CausalTransportProtocol.hasCapability(
                    ctx.channel(),
                    CausalTransportProtocol.CAPABILITY_LOSSLESS_FENCE
            )) {
                return;
            }
            final ByteBuf payload = packet.createData().skipBytes(1);
            try {
                final CausalFenceProtocol.Message message = CausalFenceProtocol.decode(payload);
                if (message instanceof CausalFenceProtocol.Request request) {
                    handleFenceRequest(ctx, packet, request);
                } else if (message instanceof CausalFenceProtocol.Ack ack) {
                    handleFenceAck(ctx, packet, ack);
                }
            } finally {
                payload.release();
            }
        } finally {
            packet.release();
        }
    }

    private void handleFenceRequest(
            ChannelHandlerContext ctx,
            FrameData packet,
            CausalFenceProtocol.Request request
    ) {
        requireReliableOrdered(packet, "request");
        final int channel = packet.getOrderChannel();
        if ((request.channelMask() & 1 << channel) == 0) {
            throw new CorruptedFrameException("Fence request arrived on an unexpected channel");
        }

        if (request.fenceId() == lastCompletedInboundFenceId
                && request.epoch() == inboundEpoch) {
            writeFenceAck(ctx, request.fenceId(), request.epoch());
            return;
        }
        if (request.fenceId() < lastCompletedInboundFenceId) {
            return;
        }

        InboundFence fence = inboundFences.get(request.fenceId());
        if (fence == null) {
            if (inboundFences.size() >= MAX_INBOUND_FENCES) {
                throw new CorruptedFrameException("Too many incomplete causal fences");
            }
            if (request.epoch() != inboundEpoch + 1) {
                throw new CorruptedFrameException("Unexpected inbound gameplay epoch "
                        + request.epoch() + ", expected " + (inboundEpoch + 1));
            }
            fence = new InboundFence(request.epoch(), request.channelMask());
            inboundFences.put(request.fenceId(), fence);
        } else if (fence.epoch != request.epoch()
                || fence.channelMask != request.channelMask()) {
            throw new CorruptedFrameException("Inconsistent causal fence request");
        }

        fence.seenMask |= 1 << channel;
        if (fence.seenMask == fence.channelMask) {
            inboundFences.remove(request.fenceId());
            inboundEpoch = fence.epoch;
            lastCompletedInboundFenceId = request.fenceId();
            ctx.fireUserEventTriggered(new InboundEpochAdvanced(inboundEpoch));
            writeFenceAck(ctx, request.fenceId(), inboundEpoch);
        }
    }

    private void writeFenceAck(ChannelHandlerContext ctx, long fenceId, int epoch) {
        final ByteBuf payload = CausalFenceProtocol.encodeAck(ctx.alloc(), fenceId, epoch);
        FrameData frameData = null;
        try {
            frameData = FrameData.create(ctx.alloc(), Constants.RAKNET_SYNC_PACKET_ID, payload);
            frameData.setOrderChannel(7);
            ctx.write(frameData).addListener(future -> {
                if (!future.isSuccess()) {
                    ctx.fireExceptionCaught(future.cause());
                }
            });
            frameData = null;
        } finally {
            payload.release();
            ReferenceCountUtil.safeRelease(frameData);
        }
    }

    private void handleFenceAck(
            ChannelHandlerContext ctx,
            FrameData packet,
            CausalFenceProtocol.Ack ack
    ) {
        requireReliableOrdered(packet, "ack");
        if (packet.getOrderChannel() != 7) {
            throw new CorruptedFrameException("Causal fence ACK must use channel 7");
        }
        if (!waitingForAck || ack.fenceId() < activeFenceId) {
            return;
        }
        if (ack.fenceId() != activeFenceId || ack.epoch() != activeFenceEpoch) {
            throw new CorruptedFrameException("Unexpected causal fence ACK");
        }

        waitingForAck = false;
        outboundEpoch = activeFenceEpoch;
        flushQueuedWrites(ctx);
        ctx.fireUserEventTriggered(new OutboundEpochAdvanced(outboundEpoch));
        activeFencePromises.forEach(ChannelPromise::trySuccess);
        activeFencePromises.clear();
        activeFenceId = 0L;
        activeFenceEpoch = 0;
    }

    private static void requireReliableOrdered(FrameData packet, String description) {
        if (!packet.getReliability().isReliable
                || !packet.getReliability().isOrdered
                || packet.getReliability().isSequenced) {
            throw new CorruptedFrameException("Causal fence " + description
                    + " is not reliable ordered");
        }
    }

    private void flushQueuedWrites(ChannelHandlerContext ctx) {
        PendingWrite pendingWrite;
        while ((pendingWrite = queuedWrites.poll()) != null) {
            try {
                ctx.write(pendingWrite.message, pendingWrite.promise);
            } catch (Throwable throwable) {
                pendingWrite.promise.tryFailure(throwable);
                ReferenceCountUtil.safeRelease(pendingWrite.message);
                failQueuedWrites(throwable);
                ctx.fireExceptionCaught(throwable);
                return;
            }
        }
    }

    private void failActiveFence(ChannelHandlerContext ctx, Throwable cause) {
        if (!ctx.channel().eventLoop().inEventLoop()) {
            ctx.channel().eventLoop().execute(() -> failActiveFence(ctx, cause));
            return;
        }
        if (!waitingForAck) {
            return;
        }
        waitingForAck = false;
        activeFencePromises.forEach(promise -> promise.tryFailure(cause));
        activeFencePromises.clear();
        failQueuedWrites(cause);
        ctx.fireExceptionCaught(cause);
        ctx.close();
    }

    private void failQueuedWrites(Throwable cause) {
        PendingWrite pendingWrite;
        while ((pendingWrite = queuedWrites.poll()) != null) {
            pendingWrite.promise.tryFailure(cause);
            ReferenceCountUtil.safeRelease(pendingWrite.message);
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        final IllegalStateException cause = new IllegalStateException("Channel closed");
        activeFencePromises.forEach(promise -> promise.tryFailure(cause));
        activeFencePromises.clear();
        failQueuedWrites(cause);
        inboundFences.clear();
        super.handlerRemoved(ctx);
    }

    public record InboundEpochAdvanced(int epoch) {
    }

    public record OutboundEpochAdvanced(int epoch) {
    }

    private record PendingWrite(Object message, ChannelPromise promise) {
    }

    private static final class InboundFence {
        private final int epoch;
        private final int channelMask;
        private int seenMask;

        private InboundFence(int epoch, int channelMask) {
            this.epoch = epoch;
            this.channelMask = channelMask;
        }
    }

}
