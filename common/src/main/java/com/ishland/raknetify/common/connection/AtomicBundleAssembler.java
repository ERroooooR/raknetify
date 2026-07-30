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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects the encoded packets between identical Minecraft bundle delimiters.
 * Packet promises remain pending until the resulting logical message has been
 * accepted or failed by the outbound pipeline.
 */
final class AtomicBundleAssembler {

    private final List<ByteBuf> packets = new ArrayList<>();
    private final List<ChannelPromise> promises = new ArrayList<>();
    private boolean open;
    private int payloadBytes;

    boolean isOpen() {
        return open;
    }

    CompletedBundle accept(
            ByteBufAllocator allocator,
            ByteBuf packet,
            ChannelPromise promise,
            boolean delimiter,
            int epoch,
            boolean epochFraming
    ) {
        return accept(
                allocator,
                packet,
                promise,
                delimiter,
                epoch,
                epochFraming,
                false,
                0
        );
    }

    CompletedBundle accept(
            ByteBufAllocator allocator,
            ByteBuf packet,
            ChannelPromise promise,
            boolean delimiter,
            int epoch,
            boolean epochFraming,
            boolean bulkDependencyFraming,
            int requiredBulkSequence
    ) {
        if (!open && !delimiter) {
            throw new IllegalStateException("Cannot start an atomic bundle without a delimiter");
        }
        if (packets.size() >= CausalTransportProtocol.MAX_ATOMIC_BUNDLE_PACKETS) {
            throw new IllegalArgumentException("Atomic bundle exceeds "
                    + CausalTransportProtocol.MAX_ATOMIC_BUNDLE_PACKETS + " packets");
        }
        if (packet.readableBytes() > CausalTransportProtocol.MAX_ATOMIC_BUNDLE_BYTES - payloadBytes) {
            throw new IllegalArgumentException("Atomic bundle exceeds "
                    + CausalTransportProtocol.MAX_ATOMIC_BUNDLE_BYTES + " payload bytes");
        }

        packets.add(packet.retainedDuplicate());
        promises.add(promise);
        payloadBytes += packet.readableBytes();

        if (!open) {
            open = true;
            return null;
        }
        if (!delimiter) {
            return null;
        }

        final List<ByteBuf> completedPackets = new ArrayList<>(packets);
        final List<ChannelPromise> completedPromises = new ArrayList<>(promises);
        reset();
        try {
            final ByteBuf payload = bulkDependencyFraming
                    ? CausalTransportProtocol.encodeDependencyAtomicBundle(
                            allocator,
                            epoch,
                            requiredBulkSequence,
                            completedPackets
                    )
                    : epochFraming
                    ? CausalTransportProtocol.encodeAtomicBundle(
                            allocator,
                            epoch,
                            completedPackets
                    )
                    : CausalTransportProtocol.encodeAtomicBundle(
                            allocator,
                            completedPackets
                    );
            return new CompletedBundle(payload, completedPromises);
        } catch (RuntimeException exception) {
            completedPromises.forEach(promise1 -> promise1.tryFailure(exception));
            throw exception;
        } finally {
            completedPackets.forEach(ReferenceCountUtil::safeRelease);
        }
    }

    void abort(Throwable cause) {
        packets.forEach(ReferenceCountUtil::safeRelease);
        promises.forEach(promise -> promise.tryFailure(cause));
        reset();
    }

    private void reset() {
        packets.clear();
        promises.clear();
        payloadBytes = 0;
        open = false;
    }

    record CompletedBundle(ByteBuf payload, List<ChannelPromise> promises) {
    }

}
