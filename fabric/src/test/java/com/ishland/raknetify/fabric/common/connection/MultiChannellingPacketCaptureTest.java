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

package com.ishland.raknetify.fabric.common.connection;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiChannellingPacketCaptureTest {

    @Test
    void nestedWriteRestoresOuterPacketClassificationScope() {
        final MultiChannellingPacketCapture capture =
                new MultiChannellingPacketCapture();
        final NestedWriteHandler nested = new NestedWriteHandler(capture);
        final EmbeddedChannel channel = new EmbeddedChannel(nested, capture);

        channel.writeOutbound(new OuterPacket());

        assertEquals(OuterPacket.class, nested.classAfterNestedWrite);
        assertNull(capture.getPacketClass());
        assertEquals(InnerPacket.class, channel.readOutbound().getClass());
        assertEquals(OuterPacket.class, channel.readOutbound().getClass());
        channel.finishAndReleaseAll();
    }

    @Test
    void captureCanBeReattachedDuringPipelineReorder() {
        final MultiChannellingPacketCapture capture =
                new MultiChannellingPacketCapture();
        final EmbeddedChannel channel = new EmbeddedChannel(capture);

        channel.pipeline().remove(capture);
        channel.pipeline().addLast(capture);

        assertTrue(channel.writeOutbound(new OuterPacket()));
        assertEquals(OuterPacket.class, channel.readOutbound().getClass());
        channel.finishAndReleaseAll();
    }

    @Test
    void sharedCaptureDoesNotLeakClassificationAcrossThreads()
            throws InterruptedException {
        final MultiChannellingPacketCapture capture =
                new MultiChannellingPacketCapture();
        final AtomicReference<Class<?>> otherThreadInitial =
                new AtomicReference<>();
        final AtomicReference<Class<?>> otherThreadScoped =
                new AtomicReference<>();
        capture.setPacketClass(OuterPacket.class);

        final Thread thread = new Thread(() -> {
            otherThreadInitial.set(capture.getPacketClass());
            capture.setPacketClass(InnerPacket.class);
            otherThreadScoped.set(capture.getPacketClass());
            capture.setPacketClass(null);
        });
        thread.start();
        thread.join();

        assertNull(otherThreadInitial.get());
        assertEquals(InnerPacket.class, otherThreadScoped.get());
        assertEquals(OuterPacket.class, capture.getPacketClass());
        capture.setPacketClass(null);
    }

    private static final class NestedWriteHandler
            extends ChannelOutboundHandlerAdapter {

        private final MultiChannellingPacketCapture capture;
        private Class<?> classAfterNestedWrite;

        private NestedWriteHandler(MultiChannellingPacketCapture capture) {
            this.capture = capture;
        }

        @Override
        public void write(
                ChannelHandlerContext ctx,
                Object msg,
                ChannelPromise promise
        ) {
            if (msg instanceof OuterPacket) {
                ctx.channel().pipeline().write(new InnerPacket());
                classAfterNestedWrite = capture.getPacketClass();
            }
            ctx.write(msg, promise);
        }
    }

    private static final class OuterPacket {
    }

    private static final class InnerPacket {
    }
}
