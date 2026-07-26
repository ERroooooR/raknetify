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
 * Owns an outbound atomic bundle, transition controls that wait for its
 * envelope, and every later write that must remain behind those controls.
 */
final class OutboundAtomicBundleController {

    private final AtomicBundleAssembler assembler = new AtomicBundleAssembler();
    private final BoundedPendingWriteQueue pendingBarrierWrites;

    OutboundAtomicBundleController(
            int maxPendingControlWrites,
            long maxPendingControlBytes
    ) {
        this.pendingBarrierWrites = new BoundedPendingWriteQueue(
                maxPendingControlWrites,
                maxPendingControlBytes
        );
    }

    boolean isOpen() {
        return assembler.isOpen();
    }

    boolean hasPendingWrites() {
        return pendingBarrierWrites.size() != 0;
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

    CorruptedFrameException holdBehindBundleBarrier(
            ChannelHandlerContext ctx,
            Object message,
            ChannelPromise promise
    ) {
        if (!assembler.isOpen() && pendingBarrierWrites.size() == 0) {
            throw new IllegalStateException(
                    "Cannot retain a write without an open or pending bundle barrier"
            );
        }
        if (pendingBarrierWrites.tryAdd(message, promise)) {
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalOutboundFrameQueued(
                        SimpleMetricsLogger.CausalOutboundQueue.BUNDLE_CONTROL,
                        pendingBarrierWrites.size(),
                        pendingBarrierWrites.bytes()
                );
            }
            return null;
        }

        final CorruptedFrameException exception = new CorruptedFrameException(
                "Pending causal bundle barrier queue exceeded its bound "
                        + "(frames=" + pendingBarrierWrites.size()
                        + ", bytes=" + pendingBarrierWrites.bytes() + ")"
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

    Throwable replayBarrierWrites(
            ChannelHandlerContext ctx,
            WriteReplayer replayer
    ) {
        BoundedPendingWriteQueue.PendingWrite pendingWrite;
        while ((pendingWrite = pendingBarrierWrites.poll()) != null) {
            recordBarrierQueueState(ctx);
            try {
                replayer.replay(
                        pendingWrite.message(),
                        pendingWrite.promise()
                );
            } catch (Throwable throwable) {
                pendingWrite.promise().tryFailure(throwable);
                ReferenceCountUtil.safeRelease(
                        pendingWrite.message()
                );
                abort(ctx, throwable);
                return throwable;
            }
        }
        return null;
    }

    void abort(ChannelHandlerContext ctx, Throwable cause) {
        assembler.abort(cause);
        pendingBarrierWrites.failAll(cause);
        recordBarrierQueueState(ctx);
    }

    int pendingBarrierWriteCount() {
        return pendingBarrierWrites.size();
    }

    long pendingBarrierBytes() {
        return pendingBarrierWrites.bytes();
    }

    private void recordBarrierQueueState(ChannelHandlerContext ctx) {
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalOutboundQueueState(
                    SimpleMetricsLogger.CausalOutboundQueue.BUNDLE_CONTROL,
                    pendingBarrierWrites.size(),
                    pendingBarrierWrites.bytes()
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
    interface WriteReplayer {

        void replay(Object message, ChannelPromise promise) throws Exception;
    }
}
