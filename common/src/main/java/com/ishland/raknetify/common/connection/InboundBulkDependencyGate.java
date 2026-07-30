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
import com.ishland.raknetify.common.connection.multichannel.MultichannelPolicy;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import network.ycc.raknet.RakNet;

import java.util.ArrayDeque;

/**
 * Preserves the one dependency that guarded bulk needs after moving to its own
 * order channel: a strict frame may not reach Minecraft before all earlier
 * guarded-bulk bodies have been delivered.
 *
 * <p>The sender numbers bulk frames and places the latest number in every
 * following strict frame. This gate releases strict frames when that watermark
 * is satisfied without adding an RTT or globally serializing independent
 * control/effect traffic.</p>
 */
final class InboundBulkDependencyGate {

    private final int maxPendingFrames;
    private final int maxPendingBytes;
    private final ArrayDeque<PendingStrictFrame> pendingStrictFrames =
            new ArrayDeque<>();
    private int pendingBytes;
    private int deliveredBulkSequence;
    private int lastStrictRequirement;

    InboundBulkDependencyGate() {
        this(
                CausalTransportProtocol.MAX_PENDING_CAUSAL_WRITES,
                Constants.MAX_QUEUED_SIZE
        );
    }

    InboundBulkDependencyGate(int maxPendingFrames, int maxPendingBytes) {
        if (maxPendingFrames <= 0 || maxPendingBytes <= 0) {
            throw new IllegalArgumentException(
                    "Inbound guarded-bulk queue limits must be positive"
            );
        }
        this.maxPendingFrames = maxPendingFrames;
        this.maxPendingBytes = maxPendingBytes;
    }

    void handle(
            ChannelHandlerContext ctx,
            CausalTransportProtocol.GameplayFrame frame,
            boolean reliableOrdered,
            int orderChannel
    ) {
        switch (frame.dependencyKind()) {
            case NONE -> InboundGameplayEpochGate.firePackets(
                    ctx,
                    frame.packets()
            );
            case GUARDED_BULK -> handleBulk(
                    ctx,
                    frame,
                    reliableOrdered,
                    orderChannel
            );
            case STRICT -> handleStrict(
                    ctx,
                    frame,
                    reliableOrdered,
                    orderChannel
            );
        }
    }

    void close(ChannelHandlerContext ctx) {
        PendingStrictFrame pending;
        while ((pending = pendingStrictFrames.pollFirst()) != null) {
            release(pending.frame());
        }
        pendingBytes = 0;
        recordQueueState(ctx);
    }

    int pendingFrames() {
        return pendingStrictFrames.size();
    }

    int pendingBytes() {
        return pendingBytes;
    }

    int deliveredBulkSequence() {
        return deliveredBulkSequence;
    }

    private void handleBulk(
            ChannelHandlerContext ctx,
            CausalTransportProtocol.GameplayFrame frame,
            boolean reliableOrdered,
            int orderChannel
    ) {
        try {
            requireChannel(
                    reliableOrdered,
                    orderChannel,
                    MultichannelPolicy.GUARDED_BULK_CHANNEL,
                    "Guarded-bulk"
            );
        } catch (RuntimeException exception) {
            release(frame);
            throw exception;
        }
        if (frame.atomicBundle()) {
            release(frame);
            throw new CorruptedFrameException(
                    "Atomic bundle cannot use the guarded-bulk dependency kind"
            );
        }
        final int expected = nextSequence(deliveredBulkSequence);
        if (frame.dependencySequence() != expected) {
            release(frame);
            throw new CorruptedFrameException(
                    "Guarded-bulk sequence skipped from "
                            + deliveredBulkSequence + " to "
                            + frame.dependencySequence()
            );
        }

        final int bytes = packetBytes(frame);
        InboundGameplayEpochGate.firePackets(ctx, frame.packets());
        deliveredBulkSequence = expected;
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalBulkFrameInbound(bytes);
        }
        releaseReadyStrictFrames(ctx);
    }

    private void handleStrict(
            ChannelHandlerContext ctx,
            CausalTransportProtocol.GameplayFrame frame,
            boolean reliableOrdered,
            int orderChannel
    ) {
        try {
            requireChannel(
                    reliableOrdered,
                    orderChannel,
                    MultichannelPolicy.STRICT_GAME_CHANNEL,
                    "Strict dependency"
            );
        } catch (RuntimeException exception) {
            release(frame);
            throw exception;
        }
        final int requirement = frame.dependencySequence();
        if (requirement < lastStrictRequirement) {
            release(frame);
            throw new CorruptedFrameException(
                    "Strict guarded-bulk watermark moved backwards from "
                            + lastStrictRequirement + " to " + requirement
            );
        }
        lastStrictRequirement = requirement;
        if (requirement <= deliveredBulkSequence) {
            InboundGameplayEpochGate.firePackets(ctx, frame.packets());
            return;
        }

        final int bytes = packetBytes(frame);
        if (pendingStrictFrames.size() >= maxPendingFrames
                || bytes > maxPendingBytes - pendingBytes) {
            release(frame);
            close(ctx);
            throw new CorruptedFrameException(
                    "Pending strict guarded-bulk dependency queue exceeded its bound"
            );
        }
        pendingStrictFrames.addLast(new PendingStrictFrame(
                frame,
                requirement,
                bytes,
                System.nanoTime()
        ));
        pendingBytes += bytes;
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalStrictFrameBlocked(
                    pendingStrictFrames.size(),
                    pendingBytes
            );
        }
    }

    private void releaseReadyStrictFrames(ChannelHandlerContext ctx) {
        PendingStrictFrame pending;
        while ((pending = pendingStrictFrames.peekFirst()) != null
                && pending.requiredBulkSequence() <= deliveredBulkSequence) {
            pendingStrictFrames.removeFirst();
            pendingBytes -= pending.bytes();
            final long waitNanos = Math.max(
                    0L,
                    System.nanoTime() - pending.queuedNanos()
            );
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalStrictFrameReleased(
                        pendingStrictFrames.size(),
                        pendingBytes,
                        waitNanos
                );
            }
            InboundGameplayEpochGate.firePackets(
                    ctx,
                    pending.frame().packets()
            );
        }
        recordQueueState(ctx);
    }

    private void recordQueueState(ChannelHandlerContext ctx) {
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalStrictQueueState(
                    pendingStrictFrames.size(),
                    pendingBytes
            );
        }
    }

    private static int packetBytes(
            CausalTransportProtocol.GameplayFrame frame
    ) {
        int bytes = 0;
        for (var packet : frame.packets()) {
            bytes = Math.addExact(bytes, packet.readableBytes());
        }
        return bytes;
    }

    private static int nextSequence(int sequence) {
        if (sequence == Integer.MAX_VALUE) {
            throw new CorruptedFrameException(
                    "Guarded-bulk sequence exhausted"
            );
        }
        return sequence + 1;
    }

    private static void requireChannel(
            boolean reliableOrdered,
            int actualChannel,
            int expectedChannel,
            String description
    ) {
        if (!reliableOrdered || actualChannel != expectedChannel) {
            throw new CorruptedFrameException(
                    description + " frame was not delivered on reliable ordered channel "
                            + expectedChannel
            );
        }
    }

    private static void release(
            CausalTransportProtocol.GameplayFrame frame
    ) {
        frame.packets().forEach(ReferenceCountUtil::safeRelease);
    }

    private static SimpleMetricsLogger metrics(ChannelHandlerContext ctx) {
        if (ctx.channel().config() instanceof RakNet.Config config
                && config.getMetrics() instanceof SimpleMetricsLogger logger) {
            return logger;
        }
        return null;
    }

    private record PendingStrictFrame(
            CausalTransportProtocol.GameplayFrame frame,
            int requiredBulkSequence,
            int bytes,
            long queuedNanos
    ) {
    }
}
