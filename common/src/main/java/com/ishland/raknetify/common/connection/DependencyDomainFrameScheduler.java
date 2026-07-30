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

import com.ishland.raknetify.common.connection.multichannel.DependencyDomain;
import com.ishland.raknetify.common.connection.multichannel.DependencyDomainScheduler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.FrameData;

import java.util.Objects;

/**
 * Connects the pure dependency-domain scheduling policy to Netty ownership.
 *
 * <p>A successful {@link #schedule} transfers ownership of both the frame and
 * promise to this adapter until the frame is written or failed. A rejected
 * frame is released here as part of the same fail-closed operation.</p>
 */
final class DependencyDomainFrameScheduler {

    private static final int MAX_DRAIN_FRAMES = 64;
    private static final long MAX_DRAIN_BYTES = 64L * 1024L;
    private final DependencyDomainScheduler<PendingFrame> scheduler;
    private final DrainTaskSubmitter drainTaskSubmitter;
    private boolean drainScheduled;
    private boolean backpressureBypassUsed;

    DependencyDomainFrameScheduler(int maxFrames, long maxBytes) {
        this(
                maxFrames,
                maxBytes,
                (ctx, task) -> ctx.executor().execute(task)
        );
    }

    DependencyDomainFrameScheduler(
            int maxFrames,
            long maxBytes,
            DrainTaskSubmitter drainTaskSubmitter
    ) {
        this.scheduler = new DependencyDomainScheduler<>(maxFrames, maxBytes);
        this.drainTaskSubmitter = Objects.requireNonNull(
                drainTaskSubmitter,
                "drainTaskSubmitter"
        );
    }

    void schedule(
            ChannelHandlerContext ctx,
            DependencyDomain domain,
            FrameData frame,
            ChannelPromise promise
    ) {
        schedule(ctx, domain, domain, frame, promise);
    }

    void schedule(
            ChannelHandlerContext ctx,
            DependencyDomain metricDomain,
            DependencyDomain schedulingDomain,
            FrameData frame,
            ChannelPromise promise
    ) {
        final int bytes = frame.getDataSize();
        if (!scheduler.offer(
                schedulingDomain,
                new PendingFrame(frame, promise, bytes, metricDomain),
                bytes
        )) {
            final CorruptedFrameException exception = new CorruptedFrameException(
                    "Dependency-domain scheduler exceeded its bound "
                            + "(frames=" + scheduler.size()
                            + ", bytes=" + scheduler.bytes() + ")"
            );
            promise.tryFailure(exception);
            ReferenceCountUtil.safeRelease(frame);
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalOutboundQueueOverflow(
                        SimpleMetricsLogger.CausalOutboundQueue.DOMAIN_SCHEDULER
                );
            }
            fail(ctx, exception);
            ctx.fireExceptionCaught(exception);
            ctx.close();
            return;
        }

        recordQueued(ctx, metricDomain, bytes);
        try {
            scheduleDrain(ctx);
        } catch (Throwable throwable) {
            fail(ctx, throwable);
            ctx.fireExceptionCaught(throwable);
            ctx.close();
        }
    }

    void drain(ChannelHandlerContext ctx) {
        drain(ctx, Integer.MAX_VALUE, Long.MAX_VALUE);
    }

    /**
     * Admits one bounded batch to RakNet's fragmentation/reliability pipeline.
     *
     * <p>Keeping the remainder here is important: RakNet's own writability
     * signal reflects its reliable frame queue, while Minecraft commonly
     * continues writing after that signal turns false. Without this boundary
     * a chunk burst can fill the transport queue before a later independent
     * control frame has a chance to preempt it.</p>
     */
    void drainAvailable(ChannelHandlerContext ctx) {
        final boolean writable = isTransportWritable(ctx);
        if (writable) {
            backpressureBypassUsed = false;
        } else if (backpressureBypassUsed) {
            return;
        }
        final long bytesBefore = scheduler.bytes();
        drain(
                ctx,
                MAX_DRAIN_FRAMES,
                MAX_DRAIN_BYTES,
                writable
                        ? domain -> true
                        : DependencyDomainFrameScheduler::mayBypassBackpressure
        );
        if (!writable && scheduler.bytes() != bytesBefore) {
            // One bounded high-priority escape batch is enough to put control
            // fragments into RakNet's priority queue. Waiting for the next
            // writable edge prevents a particle/control burst from bypassing
            // transport backpressure without limit.
            backpressureBypassUsed = true;
        }
    }

    void resume(ChannelHandlerContext ctx) {
        final boolean writable = isTransportWritable(ctx);
        if (writable) {
            backpressureBypassUsed = false;
        }
        if (!scheduler.isEmpty() && (writable
                || (!backpressureBypassUsed
                && hasBackpressureBypassFrame()))) {
            scheduleDrain(ctx);
        }
    }

    private void drain(
            ChannelHandlerContext ctx,
            int maxFrames,
            long maxBytes
    ) {
        drain(ctx, maxFrames, maxBytes, domain -> true);
    }

    private void drain(
            ChannelHandlerContext ctx,
            int maxFrames,
            long maxBytes,
            java.util.function.Predicate<DependencyDomain> eligible
    ) {
        DependencyDomainScheduler.Scheduled<PendingFrame> scheduled;
        int frames = 0;
        long bytes = 0L;
        while (frames < maxFrames
                && bytes < maxBytes
                && (scheduled = scheduler.poll(eligible)) != null) {
            final PendingFrame pending = scheduled.value();
            try {
                ctx.write(pending.frame(), pending.promise());
                recordSent(ctx, pending.metricDomain(), pending.bytes());
                frames++;
                bytes += pending.bytes();
            } catch (Throwable throwable) {
                recordDiscarded(
                        ctx,
                        pending.metricDomain(),
                        pending.bytes()
                );
                ReferenceCountUtil.safeRelease(pending.frame());
                pending.promise().tryFailure(throwable);
                fail(ctx, throwable);
                ctx.fireExceptionCaught(throwable);
                ctx.close();
                return;
            }
        }
    }

    void fail(ChannelHandlerContext ctx, Throwable cause) {
        DependencyDomainScheduler.Scheduled<PendingFrame> scheduled;
        while ((scheduled = scheduler.poll()) != null) {
            final PendingFrame pending = scheduled.value();
            recordDiscarded(ctx, pending.metricDomain(), pending.bytes());
            ReferenceCountUtil.safeRelease(pending.frame());
            pending.promise().tryFailure(cause);
        }
    }

    int size() {
        return scheduler.size();
    }

    long bytes() {
        return scheduler.bytes();
    }

    private void scheduleDrain(ChannelHandlerContext ctx) {
        if (drainScheduled) {
            return;
        }
        drainScheduled = true;
        try {
            drainTaskSubmitter.submit(ctx, () -> {
                drainScheduled = false;
                if (!scheduler.isEmpty()) {
                    final long bytesBefore = scheduler.bytes();
                    drainAvailable(ctx);
                    if (scheduler.bytes() != bytesBefore) {
                        ctx.flush();
                    }
                    resume(ctx);
                }
            });
        } catch (Throwable throwable) {
            drainScheduled = false;
            throw throwable;
        }
    }

    private static void recordQueued(
            ChannelHandlerContext ctx,
            DependencyDomain domain,
            int bytes
    ) {
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.dependencyDomainQueued(domain, bytes);
        }
    }

    private static void recordSent(
            ChannelHandlerContext ctx,
            DependencyDomain domain,
            int bytes
    ) {
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.dependencyDomainSent(domain, bytes);
        }
    }

    private static void recordDiscarded(
            ChannelHandlerContext ctx,
            DependencyDomain domain,
            int bytes
    ) {
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.dependencyDomainDiscarded(domain, bytes);
        }
    }

    private static SimpleMetricsLogger metrics(ChannelHandlerContext ctx) {
        if (ctx.channel().config() instanceof RakNet.Config config
                && config.getMetrics() instanceof SimpleMetricsLogger logger) {
            return logger;
        }
        return null;
    }

    private static boolean isTransportWritable(ChannelHandlerContext ctx) {
        return !Boolean.FALSE.equals(ctx.channel().attr(RakNet.WRITABLE).get());
    }

    private boolean hasBackpressureBypassFrame() {
        return scheduler.has(DependencyDomainFrameScheduler::mayBypassBackpressure);
    }

    private static boolean mayBypassBackpressure(DependencyDomain domain) {
        return domain == DependencyDomain.INDEPENDENT_CONTROL
                || domain == DependencyDomain.EPHEMERAL_EFFECT;
    }

    private record PendingFrame(
            FrameData frame,
            ChannelPromise promise,
            int bytes,
            DependencyDomain metricDomain
    ) {
    }

    @FunctionalInterface
    interface DrainTaskSubmitter {

        void submit(ChannelHandlerContext ctx, Runnable task);
    }
}
