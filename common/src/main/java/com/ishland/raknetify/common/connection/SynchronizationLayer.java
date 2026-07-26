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
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.RakNet;

import java.util.ArrayList;
import java.util.List;

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

    private final int activeChannelMask;
    private final BoundedPendingWriteQueue queuedWrites;
    private final List<ChannelPromise> activeFencePromises = new ArrayList<>();

    private boolean waitingForAck;
    private long nextFenceId = 1L;
    private long activeFenceId;
    private int activeFenceEpoch;
    private int outboundEpoch;
    private final InboundCausalFenceTracker inboundFenceTracker;

    public SynchronizationLayer(int... channelsToIgnore) {
        this(
                channelsToIgnore,
                CausalTransportProtocol.MAX_PENDING_CAUSAL_WRITES,
                Constants.MAX_QUEUED_SIZE
        );
    }

    SynchronizationLayer(
            int[] channelsToIgnore,
            int maxPendingWrites,
            int maxPendingWriteBytes
    ) {
        this.queuedWrites = new BoundedPendingWriteQueue(
                maxPendingWrites,
                maxPendingWriteBytes
        );
        int ignoredChannelMask = 0;
        for (int channel : channelsToIgnore) {
            if (channel < 0 || channel >= CausalFenceProtocol.ORDER_CHANNEL_COUNT) {
                throw new IllegalArgumentException("Invalid ignored order channel: " + channel);
            }
            ignoredChannelMask |= 1 << channel;
        }
        this.activeChannelMask =
                ((1 << CausalFenceProtocol.ORDER_CHANNEL_COUNT) - 1)
                        & ~ignoredChannelMask;
        if (activeChannelMask == 0) {
            throw new IllegalArgumentException(
                    "Causal fence has no active channels"
            );
        }
        this.inboundFenceTracker =
                new InboundCausalFenceTracker(activeChannelMask);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg == SYNC_REQUEST_OBJECT) {
            if (!CausalTransportProtocol.hasOutboundCapability(
                    ctx.channel(),
                    CausalTransportProtocol.CAPABILITY_LOSSLESS_FENCE
            )) {
                // A pre-fence peer cannot acknowledge the new protocol. Preserve
                // every queued frame and let per-channel ordering continue.
                promise.trySuccess();
                return;
            }
            if (waitingForAck) {
                queuePendingWrite(ctx, msg, promise);
                return;
            }
            beginFence(ctx, promise);
            return;
        }
        if (waitingForAck) {
            queuePendingWrite(ctx, msg, promise);
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

        waitingForAck = true;
        activeFenceId = nextFenceId++;
        activeFenceEpoch = outboundEpoch + 1;
        activeFencePromises.add(promise);
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalFenceStarted(activeFenceEpoch);
        }

        for (int channel = 0; channel < CausalFenceProtocol.ORDER_CHANNEL_COUNT; channel++) {
            if ((activeChannelMask & 1 << channel) == 0) {
                continue;
            }
            writeFenceRequest(
                    ctx,
                    activeFenceId,
                    activeFenceEpoch,
                    activeChannelMask,
                    channel
            );
        }
        ctx.flush();
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
            if (!CausalTransportProtocol.hasInboundCapability(
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
        final InboundCausalFenceTracker.Result result =
                inboundFenceTracker.accept(request, channel);
        if (result.action()
                == InboundCausalFenceTracker.Action.COMMIT_AND_ACKNOWLEDGE) {
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalInboundFenceCompleted(result.epoch());
            }
            ctx.fireUserEventTriggered(new InboundEpochAdvanced(result.epoch()));
            writeFenceAck(ctx, request.fenceId(), result.epoch());
        } else if (result.action()
                == InboundCausalFenceTracker.Action.ACKNOWLEDGE) {
            writeFenceAck(ctx, request.fenceId(), result.epoch());
        }
    }

    private void writeFenceAck(ChannelHandlerContext ctx, long fenceId, int epoch) {
        final ByteBuf payload = CausalFenceProtocol.encodeAck(ctx.alloc(), fenceId, epoch);
        FrameData frameData = null;
        try {
            frameData = FrameData.create(ctx.alloc(), Constants.RAKNET_SYNC_PACKET_ID, payload);
            frameData.setOrderChannel(7);
            ctx.writeAndFlush(frameData).addListener(future -> {
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

        final int completedEpoch = activeFenceEpoch;
        final List<ChannelPromise> completedPromises =
                new ArrayList<>(activeFencePromises);
        activeFencePromises.clear();
        waitingForAck = false;
        outboundEpoch = completedEpoch;
        activeFenceId = 0L;
        activeFenceEpoch = 0;
        final Throwable replayFailure = flushQueuedWrites(ctx);
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (replayFailure != null) {
            if (metrics != null) {
                metrics.causalFenceFailed();
            }
            completedPromises.forEach(promise ->
                    promise.tryFailure(replayFailure)
            );
            return;
        }
        if (metrics != null) {
            metrics.causalFenceCompleted(outboundEpoch);
        }
        ctx.fireUserEventTriggered(new OutboundEpochAdvanced(completedEpoch));
        completedPromises.forEach(ChannelPromise::trySuccess);
    }

    private static void requireReliableOrdered(FrameData packet, String description) {
        if (!packet.getReliability().isReliable
                || !packet.getReliability().isOrdered
                || packet.getReliability().isSequenced) {
            throw new CorruptedFrameException("Causal fence " + description
                    + " is not reliable ordered");
        }
    }

    private Throwable flushQueuedWrites(ChannelHandlerContext ctx) {
        BoundedPendingWriteQueue.PendingWrite pendingWrite;
        boolean replayedWrite = false;
        while (!waitingForAck && (pendingWrite = queuedWrites.poll()) != null) {
            recordPendingWriteQueueState(ctx);
            try {
                write(ctx, pendingWrite.message(), pendingWrite.promise());
                replayedWrite = true;
            } catch (Throwable throwable) {
                pendingWrite.promise().tryFailure(throwable);
                ReferenceCountUtil.safeRelease(pendingWrite.message());
                failQueuedWrites(ctx, throwable);
                ctx.fireExceptionCaught(throwable);
                ctx.close();
                return throwable;
            }
        }
        if (replayedWrite) {
            ctx.flush();
        }
        return null;
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
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalFenceFailed();
        }
        activeFencePromises.forEach(promise -> promise.tryFailure(cause));
        activeFencePromises.clear();
        failQueuedWrites(ctx, cause);
        ctx.fireExceptionCaught(cause);
        ctx.close();
    }

    private void queuePendingWrite(
            ChannelHandlerContext ctx,
            Object message,
            ChannelPromise promise
    ) {
        if (!queuedWrites.tryAdd(message, promise)) {
            final CorruptedFrameException exception = new CorruptedFrameException(
                    "Pending causal fence queue exceeded its bound "
                            + "(frames=" + queuedWrites.size()
                            + ", bytes=" + queuedWrites.bytes() + ")"
            );
            promise.tryFailure(exception);
            ReferenceCountUtil.safeRelease(message);
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalOutboundQueueOverflow(
                        SimpleMetricsLogger.CausalOutboundQueue.FENCE
                );
            }
            failActiveFence(ctx, exception);
            return;
        }
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalOutboundFrameQueued(
                    SimpleMetricsLogger.CausalOutboundQueue.FENCE,
                    queuedWrites.size(),
                    queuedWrites.bytes()
            );
        }
    }

    private void failQueuedWrites(
            ChannelHandlerContext ctx,
            Throwable cause
    ) {
        queuedWrites.failAll(cause);
        recordPendingWriteQueueState(ctx);
    }

    private void recordPendingWriteQueueState(ChannelHandlerContext ctx) {
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalOutboundQueueState(
                    SimpleMetricsLogger.CausalOutboundQueue.FENCE,
                    queuedWrites.size(),
                    queuedWrites.bytes()
            );
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        final IllegalStateException cause = new IllegalStateException("Channel closed");
        if (waitingForAck) {
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalFenceFailed();
            }
        }
        activeFencePromises.forEach(promise -> promise.tryFailure(cause));
        activeFencePromises.clear();
        failQueuedWrites(ctx, cause);
        inboundFenceTracker.clearActive();
        super.handlerRemoved(ctx);
    }

    private static SimpleMetricsLogger metrics(ChannelHandlerContext ctx) {
        if (ctx.channel().config() instanceof RakNet.Config config
                && config.getMetrics() instanceof SimpleMetricsLogger logger) {
            return logger;
        }
        return null;
    }

    public record InboundEpochAdvanced(int epoch) {
    }

    public record OutboundEpochAdvanced(int epoch) {
    }

}
