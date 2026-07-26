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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import network.ycc.raknet.RakNet;

/**
 * Owns application writes while an outbound start barrier or gameplay epoch
 * transition is unresolved.
 */
final class OutboundGameplayEpochGate {

    private final BoundedPendingWriteQueue pendingWrites;
    private boolean holding;
    private long nextGeneration = 1L;
    private long activeGeneration;
    private int currentEpoch;

    OutboundGameplayEpochGate(int maxPendingWrites, long maxPendingBytes) {
        this.pendingWrites = new BoundedPendingWriteQueue(
                maxPendingWrites,
                maxPendingBytes
        );
    }

    boolean isHolding() {
        return holding;
    }

    int currentEpoch() {
        return currentEpoch;
    }

    Hold beginHold() {
        if (holding) {
            throw new IllegalStateException(
                    "Outbound gameplay gate is already closed"
            );
        }
        holding = true;
        activeGeneration = nextGeneration++;
        return new Hold(activeGeneration);
    }

    boolean open(Hold hold) {
        if (!holding || activeGeneration != hold.generation()) {
            return false;
        }
        holding = false;
        activeGeneration = 0L;
        return true;
    }

    void advance(int epoch) {
        if (epoch != currentEpoch + 1) {
            throw new CorruptedFrameException(
                    "Outbound gameplay epoch skipped from "
                            + currentEpoch + " to " + epoch
            );
        }
        currentEpoch = epoch;
        holding = false;
        activeGeneration = 0L;
    }

    CorruptedFrameException hold(
            ChannelHandlerContext ctx,
            Object message,
            ChannelPromise promise
    ) {
        if (!holding) {
            throw new IllegalStateException(
                    "Cannot retain a write while the gameplay gate is open"
            );
        }
        if (pendingWrites.tryAdd(message, promise)) {
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalOutboundFrameQueued(
                        SimpleMetricsLogger.CausalOutboundQueue.APPLICATION,
                        pendingWrites.size(),
                        pendingWrites.bytes()
                );
            }
            return null;
        }

        final CorruptedFrameException exception = new CorruptedFrameException(
                "Pending causal application queue exceeded its bound "
                        + "(frames=" + pendingWrites.size()
                        + ", bytes=" + pendingWrites.bytes() + ")"
        );
        promise.tryFailure(exception);
        ReferenceCountUtil.safeRelease(message);
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalOutboundQueueOverflow(
                    SimpleMetricsLogger.CausalOutboundQueue.APPLICATION
            );
        }
        failOwnedWrites(ctx, exception);
        return exception;
    }

    Throwable replay(
            ChannelHandlerContext ctx,
            WriteReplayer replayer
    ) {
        BoundedPendingWriteQueue.PendingWrite pendingWrite;
        boolean replayedWrite = false;
        while (!holding
                && (pendingWrite = pendingWrites.poll()) != null) {
            recordQueueState(ctx);
            try {
                replayer.replay(
                        pendingWrite.message(),
                        pendingWrite.promise()
                );
                replayedWrite = true;
            } catch (Throwable throwable) {
                pendingWrite.promise().tryFailure(throwable);
                ReferenceCountUtil.safeRelease(pendingWrite.message());
                failOwnedWrites(ctx, throwable);
                return throwable;
            }
        }
        if (replayedWrite) {
            ctx.flush();
        }
        return null;
    }

    void fail(
            ChannelHandlerContext ctx,
            Hold hold,
            Throwable cause
    ) {
        if (!holding || activeGeneration != hold.generation()) {
            return;
        }
        failOwnedWrites(ctx, cause);
    }

    void close(ChannelHandlerContext ctx, Throwable cause) {
        failOwnedWrites(ctx, cause);
    }

    int pendingWriteCount() {
        return pendingWrites.size();
    }

    long pendingBytes() {
        return pendingWrites.bytes();
    }

    private void failOwnedWrites(
            ChannelHandlerContext ctx,
            Throwable cause
    ) {
        holding = false;
        activeGeneration = 0L;
        pendingWrites.failAll(cause);
        recordQueueState(ctx);
    }

    private void recordQueueState(ChannelHandlerContext ctx) {
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalOutboundQueueState(
                    SimpleMetricsLogger.CausalOutboundQueue.APPLICATION,
                    pendingWrites.size(),
                    pendingWrites.bytes()
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

    record Hold(long generation) {
    }

    @FunctionalInterface
    interface WriteReplayer {

        void replay(Object message, ChannelPromise promise) throws Exception;
    }
}
