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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboundGameplayEpochGateTest {

    @Test
    void replayStopsWhenAQueuedBarrierClosesTheNextGeneration() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundGameplayEpochGate gate =
                new OutboundGameplayEpochGate(16, 1024);
        final OutboundGameplayEpochGate.Hold firstHold = gate.beginHold();
        final Object nextBarrier = new Object();
        final ChannelPromise barrierPromise = channel.newPromise();
        final ByteBuf laterWrite = Unpooled.buffer(1).writeByte(7);
        final ChannelPromise laterPromise = channel.newPromise();
        assertNull(gate.hold(
                capture.context,
                nextBarrier,
                barrierPromise
        ));
        assertNull(gate.hold(
                capture.context,
                laterWrite,
                laterPromise
        ));

        assertTrue(gate.open(firstHold));
        final AtomicReference<OutboundGameplayEpochGate.Hold> secondHold =
                new AtomicReference<>();
        assertNull(gate.replay(
                capture.context,
                (message, promise) -> {
                    if (message == nextBarrier) {
                        promise.trySuccess();
                        secondHold.set(gate.beginHold());
                    } else {
                        capture.context.write(message, promise);
                    }
                }
        ));

        assertNotNull(secondHold.get());
        assertTrue(gate.isHolding());
        assertTrue(barrierPromise.isSuccess());
        assertFalse(laterPromise.isDone());
        assertEquals(1, gate.pendingWriteCount());

        // A late failure from the already opened generation cannot clear the
        // newer start barrier.
        gate.fail(
                capture.context,
                firstHold,
                new IOException("late first-generation failure")
        );
        assertTrue(gate.isHolding());
        assertFalse(laterPromise.isDone());

        assertTrue(gate.open(secondHold.get()));
        assertNull(gate.replay(
                capture.context,
                capture.context::write
        ));
        assertTrue(laterPromise.isSuccess());
        assertEquals(0, gate.pendingWriteCount());
        final ByteBuf outbound = channel.readOutbound();
        assertSame(laterWrite, outbound);
        outbound.release();
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void epochAdvanceIsSequentialAndOpensTheCurrentGate() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundGameplayEpochGate gate =
                new OutboundGameplayEpochGate(4, 1024);
        final OutboundGameplayEpochGate.Hold hold = gate.beginHold();

        gate.advance(1);

        assertEquals(1, gate.currentEpoch());
        assertFalse(gate.isHolding());
        gate.fail(capture.context, hold, new IOException("stale"));
        assertFalse(gate.isHolding());
        assertThrows(CorruptedFrameException.class, () -> gate.advance(3));
        gate.advance(2);
        assertEquals(2, gate.currentEpoch());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void overflowFailsAndReleasesRejectedAndRetainedWrites() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundGameplayEpochGate gate =
                new OutboundGameplayEpochGate(2, 4);
        gate.beginHold();
        final ByteBuf first = Unpooled.buffer(2).writeZero(2);
        final ByteBuf second = Unpooled.buffer(2).writeZero(2);
        final ByteBuf overflow = Unpooled.buffer(1).writeZero(1);
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        final ChannelPromise overflowPromise = channel.newPromise();

        assertNull(gate.hold(capture.context, first, firstPromise));
        assertNull(gate.hold(capture.context, second, secondPromise));
        final CorruptedFrameException exception =
                gate.hold(capture.context, overflow, overflowPromise);

        assertNotNull(exception);
        assertFailedAndReleased(firstPromise, first);
        assertFailedAndReleased(secondPromise, second);
        assertFailedAndReleased(overflowPromise, overflow);
        assertFalse(gate.isHolding());
        assertEquals(0, gate.pendingWriteCount());
        assertEquals(0, gate.pendingBytes());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void replayFailureFailsAndReleasesEveryRemainingWrite() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundGameplayEpochGate gate =
                new OutboundGameplayEpochGate(4, 1024);
        final OutboundGameplayEpochGate.Hold hold = gate.beginHold();
        final ByteBuf first = Unpooled.buffer(1).writeByte(1);
        final ByteBuf second = Unpooled.buffer(1).writeByte(2);
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        gate.hold(capture.context, first, firstPromise);
        gate.hold(capture.context, second, secondPromise);
        assertTrue(gate.open(hold));
        final IOException failure = new IOException("replay failed");

        assertSame(failure, gate.replay(
                capture.context,
                (message, promise) -> {
                    throw failure;
                }
        ));

        assertFailedAndReleased(firstPromise, first);
        assertFailedAndReleased(secondPromise, second);
        assertFalse(gate.isHolding());
        assertEquals(0, gate.pendingWriteCount());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void removalFailsEveryOwnedPromiseAndReference() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundGameplayEpochGate gate =
                new OutboundGameplayEpochGate(4, 1024);
        gate.beginHold();
        final ByteBuf write = Unpooled.buffer(1).writeByte(1);
        final ChannelPromise promise = channel.newPromise();
        gate.hold(capture.context, write, promise);
        final IllegalStateException cause = new IllegalStateException("removed");

        gate.close(capture.context, cause);

        assertSame(cause, promise.cause());
        assertEquals(0, write.refCnt());
        assertFalse(gate.isHolding());
        assertEquals(0, gate.pendingWriteCount());
        assertFalse(channel.finishAndReleaseAll());
    }

    private static void assertFailedAndReleased(
            ChannelPromise promise,
            ByteBuf buffer
    ) {
        assertTrue(promise.isDone());
        assertFalse(promise.isSuccess());
        assertEquals(0, buffer.refCnt());
    }

    private static final class ContextCapture extends ChannelOutboundHandlerAdapter {

        private ChannelHandlerContext context;

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            this.context = ctx;
        }
    }
}
