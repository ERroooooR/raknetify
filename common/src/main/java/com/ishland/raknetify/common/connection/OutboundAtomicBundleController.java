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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import network.ycc.raknet.RakNet;

import java.util.List;

/**
 * Owns an outbound atomic bundle and control writes that must not overtake it.
 */
final class OutboundAtomicBundleController {

    private final AtomicBundleAssembler assembler = new AtomicBundleAssembler();
    private final BoundedPendingWriteQueue pendingControlWrites;

    OutboundAtomicBundleController(
            int maxPendingControlWrites,
            long maxPendingControlBytes
    ) {
        this.pendingControlWrites = new BoundedPendingWriteQueue(
                maxPendingControlWrites,
                maxPendingControlBytes
        );
    }

    boolean isOpen() {
        return assembler.isOpen();
    }

    CompletedBundle accept(
            ChannelHandlerContext ctx,
            ByteBuf packet,
            ChannelPromise promise,
            boolean delimiter,
            int epoch,
            boolean epochFraming
    ) {
        final AtomicBundleAssembler.CompletedBundle completed;
        try {
            completed = assembler.accept(
                    ctx.alloc(),
                    packet,
                    promise,
                    delimiter,
                    epoch,
                    epochFraming
            );
        } catch (RuntimeException | Error throwable) {
            abort(ctx, throwable);
            throw throwable;
        }
        if (completed == null) {
            return null;
        }
        return new CompletedBundle(
                completed.payload(),
                completed.promises()
        );
    }

    CorruptedFrameException holdControl(
            ChannelHandlerContext ctx,
            Object message,
            ChannelPromise promise
    ) {
        if (!assembler.isOpen()) {
            throw new IllegalStateException(
                    "Cannot retain bundle control while no bundle is open"
            );
        }
        if (pendingControlWrites.tryAdd(message, promise)) {
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalOutboundFrameQueued(
                        SimpleMetricsLogger.CausalOutboundQueue.BUNDLE_CONTROL,
                        pendingControlWrites.size(),
                        pendingControlWrites.bytes()
                );
            }
            return null;
        }

        final CorruptedFrameException exception = new CorruptedFrameException(
                "Pending causal bundle control queue exceeded its bound "
                        + "(frames=" + pendingControlWrites.size()
                        + ", bytes=" + pendingControlWrites.bytes() + ")"
        );
        promise.tryFailure(exception);
        ReferenceCountUtil.safeRelease(message);
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalOutboundQueueOverflow(
                    SimpleMetricsLogger.CausalOutboundQueue.BUNDLE_CONTROL
            );
        }
        abort(ctx, exception);
        return exception;
    }

    Throwable replayControls(
            ChannelHandlerContext ctx,
            ControlReplayer replayer
    ) {
        BoundedPendingWriteQueue.PendingWrite pendingControlWrite;
        while ((pendingControlWrite = pendingControlWrites.poll()) != null) {
            recordControlQueueState(ctx);
            try {
                replayer.replay(
                        pendingControlWrite.message(),
                        pendingControlWrite.promise()
                );
            } catch (Throwable throwable) {
                pendingControlWrite.promise().tryFailure(throwable);
                ReferenceCountUtil.safeRelease(
                        pendingControlWrite.message()
                );
                abort(ctx, throwable);
                return throwable;
            }
        }
        return null;
    }

    void abort(ChannelHandlerContext ctx, Throwable cause) {
        assembler.abort(cause);
        pendingControlWrites.failAll(cause);
        recordControlQueueState(ctx);
    }

    int pendingControlCount() {
        return pendingControlWrites.size();
    }

    long pendingControlBytes() {
        return pendingControlWrites.bytes();
    }

    private void recordControlQueueState(ChannelHandlerContext ctx) {
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalOutboundQueueState(
                    SimpleMetricsLogger.CausalOutboundQueue.BUNDLE_CONTROL,
                    pendingControlWrites.size(),
                    pendingControlWrites.bytes()
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

    record CompletedBundle(
            ByteBuf payload,
            List<ChannelPromise> promises
    ) {
    }

    @FunctionalInterface
    interface ControlReplayer {

        void replay(Object message, ChannelPromise promise) throws Exception;
    }
}
