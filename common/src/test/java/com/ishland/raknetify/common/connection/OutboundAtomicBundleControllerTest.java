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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboundAtomicBundleControllerTest {

    @Test
    void completedBundleReplaysHeldControlsInOriginalOrder() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundAtomicBundleController controller =
                new OutboundAtomicBundleController(8, 1024);
        final ChannelPromise openingPromise = channel.newPromise();
        final ChannelPromise contentPromise = channel.newPromise();
        final ChannelPromise closingPromise = channel.newPromise();

        assertNull(accept(
                controller,
                capture.context,
                openingPromise,
                true,
                0
        ));
        final ByteBuf firstControl = Unpooled.buffer(1).writeByte(10);
        final ByteBuf secondControl = Unpooled.buffer(1).writeByte(11);
        final ChannelPromise firstControlPromise = channel.newPromise();
        final ChannelPromise secondControlPromise = channel.newPromise();
        assertNull(controller.holdControl(
                capture.context,
                firstControl,
                firstControlPromise
        ));
        assertNull(controller.holdControl(
                capture.context,
                secondControl,
                secondControlPromise
        ));
        assertNull(accept(
                controller,
                capture.context,
                contentPromise,
                false,
                1
        ));
        final OutboundAtomicBundleController.CompletedBundle completed =
                accept(
                        controller,
                        capture.context,
                        closingPromise,
                        true,
                        0
                );

        assertNotNull(completed);
        assertFalse(controller.isOpen());
        assertEquals(3, completed.promises().size());
        assertSame(openingPromise, completed.promises().get(0));
        assertSame(contentPromise, completed.promises().get(1));
        assertSame(closingPromise, completed.promises().get(2));
        completed.payload().release();
        completed.promises().forEach(ChannelPromise::trySuccess);

        assertNull(controller.replayControls(
                capture.context,
                capture.context::write
        ));
        channel.flushOutbound();
        assertTrue(firstControlPromise.isSuccess());
        assertTrue(secondControlPromise.isSuccess());
        assertSame(firstControl, channel.readOutbound());
        assertSame(secondControl, channel.readOutbound());
        firstControl.release();
        secondControl.release();
        assertEquals(0, controller.pendingControlCount());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void controlOverflowAbortsOpenBundleAndEveryOwnedControl() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundAtomicBundleController controller =
                new OutboundAtomicBundleController(2, 1024);
        final ChannelPromise openingPromise = channel.newPromise();
        accept(controller, capture.context, openingPromise, true, 0);
        final ByteBuf first = Unpooled.buffer(1).writeByte(1);
        final ByteBuf second = Unpooled.buffer(1).writeByte(2);
        final ByteBuf overflow = Unpooled.buffer(1).writeByte(3);
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        final ChannelPromise overflowPromise = channel.newPromise();
        controller.holdControl(capture.context, first, firstPromise);
        controller.holdControl(capture.context, second, secondPromise);

        final CorruptedFrameException exception =
                controller.holdControl(
                        capture.context,
                        overflow,
                        overflowPromise
                );

        assertNotNull(exception);
        assertFailedAndReleased(firstPromise, first);
        assertFailedAndReleased(secondPromise, second);
        assertFailedAndReleased(overflowPromise, overflow);
        assertTrue(openingPromise.isDone());
        assertFalse(openingPromise.isSuccess());
        assertFalse(controller.isOpen());
        assertEquals(0, controller.pendingControlCount());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void assemblyFailureAbortsBundleAndControlsAlreadyBehindIt() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundAtomicBundleController controller =
                new OutboundAtomicBundleController(4, 1024);
        final ChannelPromise openingPromise = channel.newPromise();
        accept(controller, capture.context, openingPromise, true, 0);
        final ByteBuf control = Unpooled.buffer(1).writeByte(1);
        final ChannelPromise controlPromise = channel.newPromise();
        controller.holdControl(
                capture.context,
                control,
                controlPromise
        );
        final ByteBuf releasedPacket = Unpooled.buffer(1).writeByte(2);
        releasedPacket.release();
        final ChannelPromise rejectedPromise = channel.newPromise();

        assertThrows(
                RuntimeException.class,
                () -> controller.accept(
                        capture.context,
                        releasedPacket,
                        rejectedPromise,
                        false,
                        0,
                        false
                )
        );

        assertTrue(openingPromise.isDone());
        assertFalse(openingPromise.isSuccess());
        assertFailedAndReleased(controlPromise, control);
        assertFalse(controller.isOpen());
        assertEquals(0, controller.pendingControlCount());
        assertFalse(rejectedPromise.isDone());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void controlReplayFailureReleasesEveryRemainingControl() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundAtomicBundleController controller =
                new OutboundAtomicBundleController(4, 1024);
        final ChannelPromise openingPromise = channel.newPromise();
        final ChannelPromise closingPromise = channel.newPromise();
        accept(controller, capture.context, openingPromise, true, 0);
        final ByteBuf first = Unpooled.buffer(1).writeByte(1);
        final ByteBuf second = Unpooled.buffer(1).writeByte(2);
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        controller.holdControl(capture.context, first, firstPromise);
        controller.holdControl(capture.context, second, secondPromise);
        final OutboundAtomicBundleController.CompletedBundle completed =
                accept(
                        controller,
                        capture.context,
                        closingPromise,
                        true,
                        0
                );
        assertNotNull(completed);
        completed.payload().release();
        completed.promises().forEach(ChannelPromise::trySuccess);
        final IOException failure = new IOException("control replay failed");

        assertSame(failure, controller.replayControls(
                capture.context,
                (message, promise) -> {
                    throw failure;
                }
        ));

        assertFailedAndReleased(firstPromise, first);
        assertFailedAndReleased(secondPromise, second);
        assertEquals(0, controller.pendingControlCount());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void removalAbortsBundleAndHeldControl() {
        final ContextCapture capture = new ContextCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);
        final OutboundAtomicBundleController controller =
                new OutboundAtomicBundleController(4, 1024);
        final ChannelPromise openingPromise = channel.newPromise();
        accept(controller, capture.context, openingPromise, true, 0);
        final ByteBuf control = Unpooled.buffer(1).writeByte(1);
        final ChannelPromise controlPromise = channel.newPromise();
        controller.holdControl(capture.context, control, controlPromise);
        final IllegalStateException cause = new IllegalStateException("removed");

        controller.abort(capture.context, cause);

        assertSame(cause, openingPromise.cause());
        assertSame(cause, controlPromise.cause());
        assertEquals(0, control.refCnt());
        assertFalse(controller.isOpen());
        assertEquals(0, controller.pendingControlCount());
        assertFalse(channel.finishAndReleaseAll());
    }

    private static OutboundAtomicBundleController.CompletedBundle accept(
            OutboundAtomicBundleController controller,
            ChannelHandlerContext ctx,
            ChannelPromise promise,
            boolean delimiter,
            int value
    ) {
        final ByteBuf packet = Unpooled.buffer(1).writeByte(value);
        try {
            return controller.accept(
                    ctx,
                    packet,
                    promise,
                    delimiter,
                    0,
                    false
            );
        } finally {
            packet.release();
        }
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
