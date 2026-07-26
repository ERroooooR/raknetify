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
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import network.ycc.raknet.frame.FrameData;
import org.junit.jupiter.api.Test;

import java.util.concurrent.RejectedExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DependencyDomainFrameSchedulerTest {

    @Test
    void explicitDrainPreservesDomainOrderAndTransfersOwnership() {
        final ContextCapture capture = new ContextCapture();
        final FlushCounter flushCounter = new FlushCounter();
        final EmbeddedChannel channel = new EmbeddedChannel(flushCounter, capture);
        final DependencyDomainFrameScheduler scheduler =
                new DependencyDomainFrameScheduler(16, 1024);

        final ChannelPromise strictFirst = channel.newPromise();
        final ChannelPromise strictSecond = channel.newPromise();
        final ChannelPromise control = channel.newPromise();
        final ChannelPromise effect = channel.newPromise();
        scheduler.schedule(
                capture.context,
                DependencyDomain.STRICT_WORLD,
                frame(channel, 1),
                strictFirst
        );
        scheduler.schedule(
                capture.context,
                DependencyDomain.GUARDED_BULK,
                frame(channel, 2),
                strictSecond
        );
        scheduler.schedule(
                capture.context,
                DependencyDomain.INDEPENDENT_CONTROL,
                frame(channel, 3),
                control
        );
        scheduler.schedule(
                capture.context,
                DependencyDomain.EPHEMERAL_EFFECT,
                frame(channel, 4),
                effect
        );

        assertEquals(4, scheduler.size());
        scheduler.drain(capture.context);

        assertEquals(0, scheduler.size());
        assertEquals(0, scheduler.bytes());
        assertTrue(strictFirst.isSuccess());
        assertTrue(strictSecond.isSuccess());
        assertTrue(control.isSuccess());
        assertTrue(effect.isSuccess());
        assertFrame(channel, 1);
        assertFrame(channel, 3);
        assertFrame(channel, 4);
        assertFrame(channel, 2);

        // The already queued event-loop task observes an empty scheduler and
        // must not create a redundant transport flush after an explicit fence
        // drain.
        final int flushesAfterExplicitDrain = flushCounter.flushes;
        channel.runPendingTasks();
        assertEquals(flushesAfterExplicitDrain, flushCounter.flushes);
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void eventLoopDrainCoalescesWritesIntoOneFlush() {
        final ContextCapture capture = new ContextCapture();
        final FlushCounter flushCounter = new FlushCounter();
        final EmbeddedChannel channel = new EmbeddedChannel(flushCounter, capture);
        final DependencyDomainFrameScheduler scheduler =
                new DependencyDomainFrameScheduler(16, 1024);

        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        scheduler.schedule(
                capture.context,
                DependencyDomain.INDEPENDENT_CONTROL,
                frame(channel, 1),
                firstPromise
        );
        scheduler.schedule(
                capture.context,
                DependencyDomain.EPHEMERAL_EFFECT,
                frame(channel, 2),
                secondPromise
        );

        assertFalse(firstPromise.isDone());
        assertFalse(secondPromise.isDone());
        channel.runPendingTasks();

        assertTrue(firstPromise.isSuccess());
        assertTrue(secondPromise.isSuccess());
        assertEquals(1, flushCounter.flushes);
        assertFrame(channel, 1);
        assertFrame(channel, 2);
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void overflowFailsAndReleasesRejectedAndQueuedFrames() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final DependencyDomainFrameScheduler scheduler =
                new DependencyDomainFrameScheduler(2, 1024);
        final FrameData first = frame(channel, 1);
        final FrameData second = frame(channel, 2);
        final FrameData overflow = frame(channel, 3);
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        final ChannelPromise overflowPromise = channel.newPromise();

        scheduler.schedule(
                capture.context,
                DependencyDomain.STRICT_WORLD,
                first,
                firstPromise
        );
        scheduler.schedule(
                capture.context,
                DependencyDomain.INDEPENDENT_CONTROL,
                second,
                secondPromise
        );
        scheduler.schedule(
                capture.context,
                DependencyDomain.EPHEMERAL_EFFECT,
                overflow,
                overflowPromise
        );

        assertThrows(CorruptedFrameException.class, channel::checkException);
        assertFailedAndReleased(firstPromise, first);
        assertFailedAndReleased(secondPromise, second);
        assertFailedAndReleased(overflowPromise, overflow);
        assertEquals(0, scheduler.size());
        assertEquals(0, scheduler.bytes());
        channel.runPendingTasks();
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void removalFailureReleasesEveryOwnedFrameWithoutWriting() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final DependencyDomainFrameScheduler scheduler =
                new DependencyDomainFrameScheduler(16, 1024);
        final FrameData first = frame(channel, 1);
        final FrameData second = frame(channel, 2);
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        final IllegalStateException cause = new IllegalStateException("removed");

        scheduler.schedule(
                capture.context,
                DependencyDomain.STRICT_WORLD,
                first,
                firstPromise
        );
        scheduler.schedule(
                capture.context,
                DependencyDomain.EPHEMERAL_EFFECT,
                second,
                secondPromise
        );
        scheduler.fail(capture.context, cause);

        assertSame(cause, firstPromise.cause());
        assertSame(cause, secondPromise.cause());
        assertEquals(0, first.refCnt());
        assertEquals(0, second.refCnt());
        assertEquals(0, scheduler.size());
        channel.runPendingTasks();
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void rejectedDrainTaskFailsAndReleasesTransferredFrameOwnership() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final RejectedExecutionException failure =
                new RejectedExecutionException("event loop stopped");
        final DependencyDomainFrameScheduler scheduler =
                new DependencyDomainFrameScheduler(
                        16,
                        1024,
                        (ctx, task) -> {
                            throw failure;
                        }
                );
        final FrameData frame = frame(channel, 1);
        final ChannelPromise promise = channel.newPromise();

        scheduler.schedule(
                capture.context,
                DependencyDomain.STRICT_WORLD,
                frame,
                promise
        );

        assertSame(failure, promise.cause());
        assertEquals(0, frame.refCnt());
        assertEquals(0, scheduler.size());
        assertEquals(0, scheduler.bytes());
        assertFalse(channel.isOpen());
        assertThrows(RejectedExecutionException.class, channel::checkException);
        assertFalse(channel.finishAndReleaseAll());
    }

    private static FrameData frame(EmbeddedChannel channel, int packetId) {
        final ByteBuf payload = channel.alloc().buffer(1).writeByte(packetId);
        try {
            return FrameData.create(channel.alloc(), packetId, payload);
        } finally {
            payload.release();
        }
    }

    private static void assertFrame(EmbeddedChannel channel, int packetId) {
        final FrameData frame = channel.readOutbound();
        try {
            assertEquals(packetId, frame.getPacketId());
        } finally {
            frame.release();
        }
    }

    private static void assertFailedAndReleased(
            ChannelPromise promise,
            FrameData frame
    ) {
        assertTrue(promise.isDone());
        assertFalse(promise.isSuccess());
        assertEquals(0, frame.refCnt());
    }

    private static final class ContextCapture extends ChannelOutboundHandlerAdapter {

        private ChannelHandlerContext context;

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            this.context = ctx;
        }
    }

    private static final class FlushCounter extends ChannelOutboundHandlerAdapter {

        private int flushes;

        @Override
        public void flush(ChannelHandlerContext ctx) {
            flushes++;
            ctx.flush();
        }
    }
}
