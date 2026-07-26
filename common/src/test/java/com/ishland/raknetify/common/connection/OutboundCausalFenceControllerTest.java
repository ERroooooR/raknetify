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
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboundCausalFenceControllerTest {

    @Test
    void replayCanBeginTheNextFenceWithoutCompletingItsPromise() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundCausalFenceController controller =
                new OutboundCausalFenceController(16, 1024);
        final ChannelPromise firstFencePromise = channel.newPromise();
        final OutboundCausalFenceController.Fence firstFence =
                controller.begin(capture.context, firstFencePromise);
        assertFence(firstFence, 1, 1);

        final ChannelPromise secondFencePromise = channel.newPromise();
        final ByteBuf laterWrite = Unpooled.buffer(1).writeByte(7);
        final ChannelPromise laterWritePromise = channel.newPromise();
        controller.hold(
                capture.context,
                SynchronizationLayer.SYNC_REQUEST_OBJECT,
                secondFencePromise
        );
        controller.hold(
                capture.context,
                laterWrite,
                laterWritePromise
        );

        final AtomicReference<OutboundCausalFenceController.Fence> secondFence =
                new AtomicReference<>();
        final OutboundCausalFenceController.Completion firstCompletion =
                controller.acknowledge(
                        capture.context,
                        new CausalFenceProtocol.Ack(1, 1),
                        (message, promise) -> {
                            if (message == SynchronizationLayer.SYNC_REQUEST_OBJECT) {
                                secondFence.set(
                                        controller.begin(capture.context, promise)
                                );
                            } else {
                                capture.context.write(message, promise);
                            }
                        }
                );

        assertNotNull(firstCompletion);
        assertFence(secondFence.get(), 2, 2);
        assertFalse(firstFencePromise.isDone());
        assertFalse(secondFencePromise.isDone());
        assertFalse(laterWritePromise.isDone());
        assertEquals(1, controller.queuedWriteCount());
        firstCompletion.succeed();
        assertTrue(firstFencePromise.isSuccess());
        assertFalse(secondFencePromise.isDone());

        final OutboundCausalFenceController.Completion secondCompletion =
                controller.acknowledge(
                        capture.context,
                        new CausalFenceProtocol.Ack(2, 2),
                        capture.context::write
                );
        assertNotNull(secondCompletion);
        secondCompletion.succeed();

        assertTrue(secondFencePromise.isSuccess());
        assertTrue(laterWritePromise.isSuccess());
        assertEquals(0, controller.queuedWriteCount());
        final ByteBuf outbound = channel.readOutbound();
        assertSame(laterWrite, outbound);
        outbound.release();
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void staleAckIsIgnoredButFutureOrMismatchedAckFailsClosed() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundCausalFenceController controller =
                new OutboundCausalFenceController(16, 1024);

        final ChannelPromise firstPromise = channel.newPromise();
        controller.begin(capture.context, firstPromise);
        final OutboundCausalFenceController.Completion firstCompletion =
                controller.acknowledge(
                        capture.context,
                        new CausalFenceProtocol.Ack(1, 1),
                        (message, promise) -> {
                        }
                );
        assertNotNull(firstCompletion);
        firstCompletion.succeed();

        final ChannelPromise secondPromise = channel.newPromise();
        controller.begin(capture.context, secondPromise);
        assertNull(controller.acknowledge(
                capture.context,
                new CausalFenceProtocol.Ack(1, 1),
                (message, promise) -> {
                }
        ));
        assertThrows(
                CorruptedFrameException.class,
                () -> controller.acknowledge(
                        capture.context,
                        new CausalFenceProtocol.Ack(3, 2),
                        (message, promise) -> {
                        }
                )
        );
        assertThrows(
                CorruptedFrameException.class,
                () -> controller.acknowledge(
                        capture.context,
                        new CausalFenceProtocol.Ack(2, 3),
                        (message, promise) -> {
                        }
                )
        );

        final OutboundCausalFenceController.Completion secondCompletion =
                controller.acknowledge(
                        capture.context,
                        new CausalFenceProtocol.Ack(2, 2),
                        (message, promise) -> {
                        }
                );
        assertNotNull(secondCompletion);
        secondCompletion.succeed();
        assertTrue(secondPromise.isSuccess());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void lateFailureFromCompletedFenceCannotFailTheNextFence() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundCausalFenceController controller =
                new OutboundCausalFenceController(16, 1024);
        final ChannelPromise firstPromise = channel.newPromise();
        final OutboundCausalFenceController.Fence firstFence =
                controller.begin(capture.context, firstPromise);
        final OutboundCausalFenceController.Completion firstCompletion =
                controller.acknowledge(
                        capture.context,
                        new CausalFenceProtocol.Ack(1, 1),
                        (message, promise) -> {
                        }
                );
        assertNotNull(firstCompletion);
        firstCompletion.succeed();

        final ChannelPromise secondPromise = channel.newPromise();
        final OutboundCausalFenceController.Fence secondFence =
                controller.begin(capture.context, secondPromise);
        controller.fail(
                capture.context,
                firstFence,
                new IOException("late old-fence failure")
        );

        assertTrue(controller.waitingForAck());
        assertFalse(secondPromise.isDone());
        final IOException activeFailure =
                new IOException("active fence failed");
        controller.fail(capture.context, secondFence, activeFailure);
        assertThrows(IOException.class, channel::checkException);
        assertSame(activeFailure, secondPromise.cause());
        assertFalse(controller.waitingForAck());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void overflowFailsFenceAndReleasesRejectedAndRetainedWrites() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundCausalFenceController controller =
                new OutboundCausalFenceController(2, 4);
        final ChannelPromise fencePromise = channel.newPromise();
        controller.begin(capture.context, fencePromise);

        final ByteBuf first = Unpooled.buffer(2).writeZero(2);
        final ByteBuf second = Unpooled.buffer(2).writeZero(2);
        final ByteBuf overflow = Unpooled.buffer(1).writeZero(1);
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        final ChannelPromise overflowPromise = channel.newPromise();
        controller.hold(capture.context, first, firstPromise);
        controller.hold(capture.context, second, secondPromise);
        controller.hold(capture.context, overflow, overflowPromise);

        assertThrows(CorruptedFrameException.class, channel::checkException);
        assertFailedAndReleased(firstPromise, first);
        assertFailedAndReleased(secondPromise, second);
        assertFailedAndReleased(overflowPromise, overflow);
        assertTrue(fencePromise.isDone());
        assertFalse(fencePromise.isSuccess());
        assertFalse(controller.waitingForAck());
        assertEquals(0, controller.queuedWriteCount());
        assertEquals(0, controller.queuedBytes());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void replayFailureFailsDetachedFenceAndEveryRemainingWrite() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundCausalFenceController controller =
                new OutboundCausalFenceController(4, 1024);
        final ChannelPromise fencePromise = channel.newPromise();
        controller.begin(capture.context, fencePromise);

        final ByteBuf first = Unpooled.buffer(1).writeByte(1);
        final ByteBuf second = Unpooled.buffer(1).writeByte(2);
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        controller.hold(capture.context, first, firstPromise);
        controller.hold(capture.context, second, secondPromise);
        final IOException failure = new IOException("replay failed");

        assertNull(controller.acknowledge(
                capture.context,
                new CausalFenceProtocol.Ack(1, 1),
                (message, promise) -> {
                    throw failure;
                }
        ));

        assertThrows(IOException.class, channel::checkException);
        assertSame(failure, fencePromise.cause());
        assertFailedAndReleased(firstPromise, first);
        assertFailedAndReleased(secondPromise, second);
        assertEquals(0, controller.queuedWriteCount());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void cancelledMarkerUsesANonNullFailureAndReleasesOwnedWrites() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundCausalFenceController controller =
                new OutboundCausalFenceController(4, 1024);
        final ChannelPromise fencePromise = channel.newPromise();
        final OutboundCausalFenceController.Fence fence =
                controller.begin(capture.context, fencePromise);
        final ByteBuf write = Unpooled.buffer(1).writeByte(1);
        final ChannelPromise writePromise = channel.newPromise();
        controller.hold(capture.context, write, writePromise);

        controller.fail(capture.context, fence, null);

        assertTrue(fencePromise.cause() instanceof CancellationException);
        assertTrue(writePromise.cause() instanceof CancellationException);
        assertEquals(0, write.refCnt());
        assertFalse(controller.waitingForAck());
        assertEquals(0, controller.queuedWriteCount());
        assertFalse(channel.isOpen());
        assertThrows(CancellationException.class, channel::checkException);
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void removalFailsEveryOwnedPromiseAndReference() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundCausalFenceController controller =
                new OutboundCausalFenceController(4, 1024);
        final ChannelPromise fencePromise = channel.newPromise();
        controller.begin(capture.context, fencePromise);
        final ByteBuf write = Unpooled.buffer(1).writeByte(1);
        final ChannelPromise writePromise = channel.newPromise();
        controller.hold(capture.context, write, writePromise);
        final IllegalStateException cause = new IllegalStateException("removed");

        controller.close(capture.context, cause);

        assertSame(cause, fencePromise.cause());
        assertSame(cause, writePromise.cause());
        assertEquals(0, write.refCnt());
        assertFalse(controller.waitingForAck());
        assertEquals(0, controller.queuedWriteCount());
        assertFalse(channel.finishAndReleaseAll());
    }

    private static void assertFence(
            OutboundCausalFenceController.Fence fence,
            long expectedId,
            int expectedEpoch
    ) {
        assertNotNull(fence);
        assertEquals(expectedId, fence.id());
        assertEquals(expectedEpoch, fence.epoch());
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
