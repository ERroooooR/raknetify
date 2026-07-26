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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import network.ycc.raknet.RakNet;

import java.util.ArrayDeque;
import java.util.List;

/**
 * Holds gameplay frames for the next inbound epoch until the matching causal
 * fence commits that epoch.
 *
 * <p>This class owns every queued payload. A call to {@link #handle} either
 * releases the payload, queues it, or transfers decoded packet ownership to the
 * inbound pipeline.</p>
 */
final class InboundGameplayEpochGate {

    private final int maxPendingFrames;
    private final int maxPendingBytes;
    private final ArrayDeque<PendingFrame> pendingFrames = new ArrayDeque<>();
    private int pendingBytes;
    private int currentEpoch;

    InboundGameplayEpochGate() {
        this(
                CausalTransportProtocol.MAX_ATOMIC_BUNDLE_PACKETS,
                Constants.MAX_QUEUED_SIZE
        );
    }

    InboundGameplayEpochGate(int maxPendingFrames, int maxPendingBytes) {
        if (maxPendingFrames <= 0 || maxPendingBytes <= 0) {
            throw new IllegalArgumentException(
                    "Inbound gameplay epoch queue limits must be positive"
            );
        }
        this.maxPendingFrames = maxPendingFrames;
        this.maxPendingBytes = maxPendingBytes;
    }

    int currentEpoch() {
        return currentEpoch;
    }

    void handle(ChannelHandlerContext ctx, ByteBuf payload) {
        final int epoch;
        try {
            epoch = CausalTransportProtocol.peekGameplayEpoch(payload);
        } catch (RuntimeException exception) {
            payload.release();
            throw exception;
        }

        if (epoch < currentEpoch) {
            recordStaleFrame(ctx);
            payload.release();
            return;
        }
        if (epoch == currentEpoch) {
            fireCurrentFrame(ctx, payload);
            return;
        }
        if (currentEpoch == Integer.MAX_VALUE || epoch != currentEpoch + 1) {
            payload.release();
            throw new CorruptedFrameException(
                    "Inbound gameplay epoch skipped from "
                            + currentEpoch + " to " + epoch
            );
        }

        final int bytes = payload.readableBytes();
        if (pendingFrames.size() >= maxPendingFrames
                || bytes > maxPendingBytes - pendingBytes) {
            payload.release();
            throw new CorruptedFrameException(
                    "Pending gameplay epoch queue exceeded its bound"
            );
        }
        pendingFrames.addLast(new PendingFrame(epoch, payload, bytes));
        pendingBytes += bytes;
        recordQueueState(ctx, true);
    }

    void advance(ChannelHandlerContext ctx, int epoch) {
        if (epoch <= currentEpoch) {
            return;
        }
        if (epoch != currentEpoch + 1) {
            throw new CorruptedFrameException(
                    "Inbound gameplay epoch skipped from "
                            + currentEpoch + " to " + epoch
            );
        }

        currentEpoch = epoch;
        final int queued = pendingFrames.size();
        for (int i = 0; i < queued; i++) {
            final PendingFrame pending = pendingFrames.removeFirst();
            pendingBytes -= pending.bytes;
            if (pending.epoch < currentEpoch) {
                pending.payload.release();
            } else if (pending.epoch == currentEpoch) {
                fireCurrentFrame(ctx, pending.payload);
            } else {
                pendingFrames.addLast(pending);
                pendingBytes += pending.bytes;
            }
        }
        recordQueueState(ctx, false);
    }

    void close(ChannelHandlerContext ctx) {
        PendingFrame pending;
        while ((pending = pendingFrames.pollFirst()) != null) {
            pending.payload.release();
        }
        pendingBytes = 0;
        recordQueueState(ctx, false);
    }

    private void fireCurrentFrame(ChannelHandlerContext ctx, ByteBuf payload) {
        final CausalTransportProtocol.GameplayFrame gameplayFrame;
        try {
            gameplayFrame = CausalTransportProtocol.decodeGameplayFrame(payload);
        } finally {
            payload.release();
        }
        if (gameplayFrame.epoch() != currentEpoch) {
            gameplayFrame.packets().forEach(ReferenceCountUtil::safeRelease);
            throw new CorruptedFrameException(
                    "Gameplay epoch changed while decoding"
            );
        }
        if (gameplayFrame.atomicBundle()) {
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalAtomicBundleInbound(
                        gameplayFrame.packets().size()
                );
            }
        }
        firePackets(ctx, gameplayFrame.packets());
    }

    static void firePackets(ChannelHandlerContext ctx, List<ByteBuf> packets) {
        try {
            for (int i = 0; i < packets.size(); i++) {
                final ByteBuf packet = packets.set(i, null);
                ctx.fireChannelRead(packet);
            }
        } finally {
            packets.forEach(ReferenceCountUtil::safeRelease);
        }
    }

    private static void recordStaleFrame(ChannelHandlerContext ctx) {
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalStaleFrameDropped();
        }
    }

    private void recordQueueState(
            ChannelHandlerContext ctx,
            boolean queued
    ) {
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics == null) {
            return;
        }
        if (queued) {
            metrics.causalFutureFrameQueued(
                    pendingFrames.size(),
                    pendingBytes
            );
        } else {
            metrics.causalFutureQueueState(
                    pendingFrames.size(),
                    pendingBytes
            );
        }
    }

    private static SimpleMetricsLogger metrics(ChannelHandlerContext ctx) {
        if (ctx.channel().config() instanceof RakNet.Config config
                && config.getMetrics() instanceof SimpleMetricsLogger logger) {
            return logger;
        }
        return null;
    }

    private record PendingFrame(int epoch, ByteBuf payload, int bytes) {
    }

}
