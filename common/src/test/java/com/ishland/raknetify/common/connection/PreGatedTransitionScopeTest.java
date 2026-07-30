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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreGatedTransitionScopeTest {

    @Test
    void nestedScopesStayActiveUntilEveryCommandBoundaryCompletes() {
        final EmbeddedChannel channel = new EmbeddedChannel();

        PreGatedTransitionScope.requestFence(channel);
        PreGatedTransitionScope.requestFence(channel);
        channel.runPendingTasks();

        assertSame(SynchronizationLayer.SYNC_REQUEST_OBJECT, channel.readOutbound());
        assertSame(SynchronizationLayer.SYNC_REQUEST_OBJECT, channel.readOutbound());
        assertEquals(2, PreGatedTransitionScope.depth(channel));
        assertTrue(PreGatedTransitionScope.isActive(channel));

        assertEquals(1, PreGatedTransitionScope.complete(channel));
        assertTrue(PreGatedTransitionScope.isActive(channel));
        assertEquals(0, PreGatedTransitionScope.complete(channel));
        assertFalse(PreGatedTransitionScope.isActive(channel));
        assertEquals(0, PreGatedTransitionScope.complete(channel));
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void failedFenceWriteClearsScopesAndClosesFailClosed() {
        final IllegalStateException failure =
                new IllegalStateException("injected fence failure");
        final AtomicReference<Throwable> captured = new AtomicReference<>();
        final EmbeddedChannel channel = new EmbeddedChannel(
                new ChannelOutboundHandlerAdapter() {
                    @Override
                    public void write(
                            ChannelHandlerContext ctx,
                            Object msg,
                            ChannelPromise promise
                    ) {
                        promise.tryFailure(failure);
                    }
                },
                new ChannelInboundHandlerAdapter() {
                    @Override
                    public void exceptionCaught(
                            ChannelHandlerContext ctx,
                            Throwable cause
                    ) {
                        captured.set(cause);
                    }
                }
        );

        PreGatedTransitionScope.requestFence(channel);
        channel.runPendingTasks();

        assertEquals(0, PreGatedTransitionScope.depth(channel));
        assertFalse(channel.isOpen());
        assertSame(failure, captured.get());
        assertFalse(channel.finishAndReleaseAll());
    }

}
