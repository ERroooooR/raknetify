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
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.CorruptedFrameException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalTransportProtocolTest {

    @Test
    void capabilitiesRoundTrip() {
        final ByteBuf encoded = CausalTransportProtocol.encodeCapabilities(
                UnpooledByteBufAllocator.DEFAULT,
                CausalTransportProtocol.CAPABILITY_ATOMIC_BUNDLE
        );
        try {
            final var decoded = CausalTransportProtocol.decodeCapabilities(encoded);
            assertEquals(CausalTransportProtocol.VERSION, decoded.version());
            assertEquals(CausalTransportProtocol.CAPABILITY_ATOMIC_BUNDLE, decoded.capabilities());
        } finally {
            encoded.release();
        }
    }

    @Test
    void gameplayEpochRequiresBothAtomicBundlesAndLosslessFences() {
        assertEquals(
                CausalTransportProtocol.CAPABILITY_ATOMIC_BUNDLE,
                CausalTransportProtocol.negotiateCapabilities(
                        CausalTransportProtocol.CAPABILITY_ATOMIC_BUNDLE
                                | CausalTransportProtocol.CAPABILITY_GAMEPLAY_EPOCH
                )
        );
        assertEquals(
                CausalTransportProtocol.LOCAL_CAPABILITIES,
                CausalTransportProtocol.negotiateCapabilities(
                        CausalTransportProtocol.LOCAL_CAPABILITIES
                )
        );
    }

    @Test
    void atomicBundleRoundTripPreservesEveryPacket() {
        final List<ByteBuf> source = List.of(
                Unpooled.wrappedBuffer(new byte[]{0}),
                Unpooled.wrappedBuffer(new byte[]{1, 2, 3}),
                Unpooled.wrappedBuffer(new byte[]{0})
        );
        final ByteBuf encoded = CausalTransportProtocol.encodeAtomicBundle(
                UnpooledByteBufAllocator.DEFAULT,
                source
        );
        try {
            assertTrue(CausalTransportProtocol.isAtomicBundle(encoded));
            final List<ByteBuf> decoded = CausalTransportProtocol.decodeAtomicBundle(encoded);
            try {
                assertEquals(3, decoded.size());
                assertArrayEquals(new byte[]{0}, bytes(decoded.get(0)));
                assertArrayEquals(new byte[]{1, 2, 3}, bytes(decoded.get(1)));
                assertArrayEquals(new byte[]{0}, bytes(decoded.get(2)));
            } finally {
                decoded.forEach(ByteBuf::release);
            }
        } finally {
            encoded.release();
            source.forEach(ByteBuf::release);
        }
    }

    @Test
    void decoderRejectsMismatchedDelimiters() {
        final List<ByteBuf> source = List.of(
                Unpooled.wrappedBuffer(new byte[]{0}),
                Unpooled.wrappedBuffer(new byte[]{1}),
                Unpooled.wrappedBuffer(new byte[]{2})
        );
        final ByteBuf encoded = CausalTransportProtocol.encodeAtomicBundle(
                UnpooledByteBufAllocator.DEFAULT,
                source
        );
        try {
            assertThrows(
                    CorruptedFrameException.class,
                    () -> CausalTransportProtocol.decodeAtomicBundle(encoded)
            );
        } finally {
            encoded.release();
            source.forEach(ByteBuf::release);
        }
    }

    @Test
    void epochGameplayFramesPreserveSinglePacketsAndBundles() {
        final ByteBuf singlePacket = Unpooled.wrappedBuffer(new byte[]{5, 6});
        final ByteBuf encodedSingle = CausalTransportProtocol.encodeGameplayFrame(
                UnpooledByteBufAllocator.DEFAULT,
                3,
                singlePacket
        );
        try {
            assertTrue(CausalTransportProtocol.isEpochGameplayFrame(encodedSingle));
            assertEquals(3, CausalTransportProtocol.peekGameplayEpoch(encodedSingle));
            final var decoded = CausalTransportProtocol.decodeGameplayFrame(encodedSingle);
            try {
                assertEquals(3, decoded.epoch());
                assertEquals(false, decoded.atomicBundle());
                assertArrayEquals(new byte[]{5, 6}, bytes(decoded.packets().get(0)));
            } finally {
                decoded.packets().forEach(ByteBuf::release);
            }
        } finally {
            encodedSingle.release();
            singlePacket.release();
        }

        final List<ByteBuf> bundlePackets = List.of(
                Unpooled.wrappedBuffer(new byte[]{0}),
                Unpooled.wrappedBuffer(new byte[]{9}),
                Unpooled.wrappedBuffer(new byte[]{0})
        );
        final ByteBuf encodedBundle = CausalTransportProtocol.encodeAtomicBundle(
                UnpooledByteBufAllocator.DEFAULT,
                4,
                bundlePackets
        );
        try {
            assertTrue(CausalTransportProtocol.isEpochAtomicBundle(encodedBundle));
            final var decoded = CausalTransportProtocol.decodeGameplayFrame(encodedBundle);
            try {
                assertEquals(4, decoded.epoch());
                assertEquals(true, decoded.atomicBundle());
                assertEquals(3, decoded.packets().size());
            } finally {
                decoded.packets().forEach(ByteBuf::release);
            }
        } finally {
            encodedBundle.release();
            bundlePackets.forEach(ByteBuf::release);
        }
    }

    @Test
    void epochHeaderRejectsUnknownTypesAndTruncationBeforeQueueing() {
        final ByteBuf packet = Unpooled.wrappedBuffer(new byte[]{5});
        final ByteBuf encoded = CausalTransportProtocol.encodeGameplayFrame(
                UnpooledByteBufAllocator.DEFAULT,
                1,
                packet
        );
        final ByteBuf truncated = encoded.retainedSlice(
                encoded.readerIndex(),
                7
        );
        try {
            assertThrows(
                    CorruptedFrameException.class,
                    () -> CausalTransportProtocol.peekGameplayEpoch(truncated)
            );

            encoded.setByte(encoded.readerIndex() + 6, 99);
            assertThrows(
                    CorruptedFrameException.class,
                    () -> CausalTransportProtocol.peekGameplayEpoch(encoded)
            );
            assertThrows(
                    CorruptedFrameException.class,
                    () -> CausalTransportProtocol.decodeGameplayFrame(encoded)
            );
        } finally {
            truncated.release();
            encoded.release();
            packet.release();
        }
    }

    private static byte[] bytes(ByteBuf buffer) {
        final byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), bytes);
        return bytes;
    }

}
