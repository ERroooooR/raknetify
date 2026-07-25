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

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;

import java.util.Objects;

/**
 * Tracks platform transition sequences whose fence is inserted before the
 * proxy emits its synthetic Join/Respawn/configuration packets.
 *
 * <p>The depth is deliberately counted instead of represented by a boolean:
 * redirects can begin another backend transition before the preceding command
 * tree closes its scope. Multichannel delivery may restart only after the last
 * outstanding scope completes.</p>
 */
public final class PreGatedTransitionScope {

    private static final int MAX_DEPTH = 1024;
    private static final AttributeKey<Integer> DEPTH =
            AttributeKey.valueOf("raknetify:pre-gated-transition-depth");

    private PreGatedTransitionScope() {
    }

    /**
     * Opens one scope and writes its fence from the channel event loop.
     *
     * <p>Scheduling both operations as one task preserves their order relative
     * to the proxy's synthetic packets even when its connection event runs on
     * a backend event loop.</p>
     */
    public static void requestFence(Channel channel) {
        Objects.requireNonNull(channel, "channel");
        final Runnable request = () -> {
            try {
                increment(channel);
                channel.writeAndFlush(SynchronizationLayer.SYNC_REQUEST_OBJECT)
                        .addListener(future -> handleFenceWrite(channel, future));
            } catch (Throwable throwable) {
                clear(channel);
                channel.pipeline().fireExceptionCaught(throwable);
                channel.close();
            }
        };
        if (channel.eventLoop().inEventLoop()) {
            request.run();
        } else {
            channel.eventLoop().execute(request);
        }
    }

    public static boolean isActive(Channel channel) {
        return depth(channel) != 0;
    }

    public static int depth(Channel channel) {
        final Integer value = Objects.requireNonNull(channel, "channel")
                .attr(DEPTH)
                .get();
        return value != null ? value : 0;
    }

    /**
     * Completes one scope and returns the remaining depth.
     *
     * <p>An unmatched completion is idempotent. Command-tree refreshes outside
     * a server switch therefore retain their existing start signal behavior.</p>
     */
    public static int complete(Channel channel) {
        final Attribute<Integer> attribute =
                Objects.requireNonNull(channel, "channel").attr(DEPTH);
        while (true) {
            final Integer currentValue = attribute.get();
            final int current = currentValue != null ? currentValue : 0;
            if (current == 0) {
                return 0;
            }
            final int updated = current - 1;
            if (attribute.compareAndSet(currentValue, updated)) {
                return updated;
            }
        }
    }

    static int clear(Channel channel) {
        final Integer previous = Objects.requireNonNull(channel, "channel")
                .attr(DEPTH)
                .getAndSet(0);
        return previous != null ? previous : 0;
    }

    private static void increment(Channel channel) {
        final Attribute<Integer> attribute = channel.attr(DEPTH);
        while (true) {
            final Integer currentValue = attribute.get();
            final int current = currentValue != null ? currentValue : 0;
            if (current >= MAX_DEPTH) {
                throw new IllegalStateException(
                        "Too many nested pre-gated transition scopes: " + current
                );
            }
            if (attribute.compareAndSet(currentValue, current + 1)) {
                return;
            }
        }
    }

    private static void handleFenceWrite(Channel channel, Future<?> future) {
        if (future.isSuccess()) {
            return;
        }
        clear(channel);
        final Throwable cause = future.cause() != null
                ? future.cause()
                : new IllegalStateException("Pre-gated transition fence was cancelled");
        channel.pipeline().fireExceptionCaught(cause);
        channel.close();
    }

}
