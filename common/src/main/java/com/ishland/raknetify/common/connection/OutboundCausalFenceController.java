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

import java.util.ArrayList;
import java.util.List;

/**
 * Owns one outbound causal fence and every write retained behind its ACK.
 *
 * <p>ACK completion detaches the completed fence before replay begins. A
 * replayed synchronization request may therefore start the next fence
 * re-entrantly without inheriting or completing the previous fence's
 * promises.</p>
 */
final class OutboundCausalFenceController {

    private final BoundedPendingWriteQueue queuedWrites;
    private final List<ChannelPromise> activeFencePromises = new ArrayList<>();

    private boolean waitingForAck;
    private long nextFenceId = 1L;
    private long activeFenceId;
    private int activeFenceEpoch;
    private int outboundEpoch;

    OutboundCausalFenceController(int maxPendingWrites, long maxPendingBytes) {
        this.queuedWrites = new BoundedPendingWriteQueue(
                maxPendingWrites,
                maxPendingBytes
        );
    }

    boolean waitingForAck() {
        return waitingForAck;
    }

    Fence begin(ChannelHandlerContext ctx, ChannelPromise promise) {
        if (waitingForAck) {
            throw new IllegalStateException(
                    "Cannot begin a causal fence while another fence is active"
            );
        }
        if (outboundEpoch == Integer.MAX_VALUE) {
            final IllegalStateException exception =
                    new IllegalStateException("Gameplay epoch exhausted");
            promise.tryFailure(exception);
            ctx.fireExceptionCaught(exception);
            return null;
        }

        waitingForAck = true;
        activeFenceId = nextFenceId++;
        activeFenceEpoch = outboundEpoch + 1;
        activeFencePromises.add(promise);
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalFenceStarted(activeFenceEpoch);
        }
        return new Fence(activeFenceId, activeFenceEpoch);
    }

    void hold(
            ChannelHandlerContext ctx,
            Object message,
            ChannelPromise promise
    ) {
        if (!waitingForAck) {
            throw new IllegalStateException(
                    "Cannot retain a write without an active causal fence"
            );
        }
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
            fail(
                    ctx,
                    new Fence(activeFenceId, activeFenceEpoch),
                    exception
            );
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

    Completion acknowledge(
            ChannelHandlerContext ctx,
            CausalFenceProtocol.Ack acknowledgement,
            WriteReplayer replayer
    ) {
        if (!waitingForAck || acknowledgement.fenceId() < activeFenceId) {
            return null;
        }
        if (acknowledgement.fenceId() != activeFenceId
                || acknowledgement.epoch() != activeFenceEpoch) {
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

        final Throwable replayFailure = replayQueuedWrites(ctx, replayer);
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (replayFailure != null) {
            if (metrics != null) {
                metrics.causalFenceFailed();
            }
            completedPromises.forEach(promise ->
                    promise.tryFailure(replayFailure)
            );
            return null;
        }
        if (metrics != null) {
            metrics.causalFenceCompleted(outboundEpoch);
        }
        return new Completion(completedEpoch, completedPromises);
    }

    void fail(
            ChannelHandlerContext ctx,
            Fence fence,
            Throwable cause
    ) {
        final Throwable failure = CausalFutureUtil.nonNullCause(
                cause,
                "Causal fence marker write"
        );
        if (!ctx.channel().eventLoop().inEventLoop()) {
            ctx.channel().eventLoop().execute(() ->
                    fail(ctx, fence, failure)
            );
            return;
        }
        if (!waitingForAck
                || activeFenceId != fence.id()
                || activeFenceEpoch != fence.epoch()) {
            return;
        }
        waitingForAck = false;
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalFenceFailed();
        }
        failActivePromises(failure);
        failQueuedWrites(ctx, failure);
        ctx.fireExceptionCaught(failure);
        ctx.close();
    }

    void close(ChannelHandlerContext ctx, Throwable cause) {
        if (waitingForAck) {
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalFenceFailed();
            }
        }
        waitingForAck = false;
        failActivePromises(cause);
        failQueuedWrites(ctx, cause);
    }

    int queuedWriteCount() {
        return queuedWrites.size();
    }

    long queuedBytes() {
        return queuedWrites.bytes();
    }

    private Throwable replayQueuedWrites(
            ChannelHandlerContext ctx,
            WriteReplayer replayer
    ) {
        BoundedPendingWriteQueue.PendingWrite pendingWrite;
        boolean replayedWrite = false;
        while (!waitingForAck
                && (pendingWrite = queuedWrites.poll()) != null) {
            recordPendingWriteQueueState(ctx);
            try {
                replayer.replay(
                        pendingWrite.message(),
                        pendingWrite.promise()
                );
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

    private void failActivePromises(Throwable cause) {
        activeFencePromises.forEach(promise -> promise.tryFailure(cause));
        activeFencePromises.clear();
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

    private static SimpleMetricsLogger metrics(ChannelHandlerContext ctx) {
        if (ctx.channel().config() instanceof RakNet.Config config
                && config.getMetrics() instanceof SimpleMetricsLogger logger) {
            return logger;
        }
        return null;
    }

    record Fence(long id, int epoch) {
    }

    record Completion(
            int epoch,
            List<ChannelPromise> promises
    ) {

        void succeed() {
            promises.forEach(ChannelPromise::trySuccess);
        }
    }

    @FunctionalInterface
    interface WriteReplayer {

        void replay(Object message, ChannelPromise promise) throws Exception;
    }
}
