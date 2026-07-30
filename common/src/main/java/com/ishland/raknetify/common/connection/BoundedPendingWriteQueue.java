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
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import network.ycc.raknet.frame.FrameData;

import java.util.ArrayDeque;

/**
 * Owns retained outbound writes while a causal barrier is unresolved.
 *
 * <p>The caller retains ownership when {@link #tryAdd(Object, ChannelPromise)}
 * returns {@code false}. A successful add transfers ownership to this queue
 * until {@link #poll()} returns the write or {@link #failAll(Throwable)}
 * releases it.</p>
 */
final class BoundedPendingWriteQueue {

    private final int maxWrites;
    private final long maxBytes;
    private final ArrayDeque<PendingWrite> writes = new ArrayDeque<>();
    private long bytes;

    BoundedPendingWriteQueue(int maxWrites, long maxBytes) {
        if (maxWrites <= 0 || maxBytes <= 0L) {
            throw new IllegalArgumentException("Pending write queue limits must be positive");
        }
        this.maxWrites = maxWrites;
        this.maxBytes = maxBytes;
    }

    boolean tryAdd(Object message, ChannelPromise promise) {
        final int messageBytes = messageBytes(message);
        if (writes.size() >= maxWrites || messageBytes > maxBytes - bytes) {
            return false;
        }
        writes.addLast(new PendingWrite(message, promise, messageBytes));
        bytes += messageBytes;
        return true;
    }

    PendingWrite poll() {
        final PendingWrite pendingWrite = writes.pollFirst();
        if (pendingWrite != null) {
            bytes -= pendingWrite.bytes;
        }
        return pendingWrite;
    }

    void failAll(Throwable cause) {
        PendingWrite pendingWrite;
        while ((pendingWrite = poll()) != null) {
            pendingWrite.promise.tryFailure(cause);
            ReferenceCountUtil.safeRelease(pendingWrite.message);
        }
    }

    int size() {
        return writes.size();
    }

    long bytes() {
        return bytes;
    }

    private static int messageBytes(Object message) {
        if (message instanceof ByteBuf byteBuf) {
            return byteBuf.readableBytes();
        }
        if (message instanceof FrameData frameData) {
            return frameData.getDataSize();
        }
        return 0;
    }

    record PendingWrite(
            Object message,
            ChannelPromise promise,
            int bytes
    ) {
    }
}
