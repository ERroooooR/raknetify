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

import com.ishland.raknetify.common.util.MathUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Wire format shared by the causal transport stages. Control messages use a
 * dedicated RakNet packet id, while an atomic bundle remains a normal game
 * FrameData payload so existing compression, fragmentation and recovery still
 * apply to the complete logical message.
 */
public final class CausalTransportProtocol {

    public static final int VERSION = 1;
    public static final long CAPABILITY_ATOMIC_BUNDLE = 1L;
    public static final long LOCAL_CAPABILITIES = CAPABILITY_ATOMIC_BUNDLE;

    public static final int MAX_ATOMIC_BUNDLE_PACKETS = 4096;
    public static final int MAX_ATOMIC_BUNDLE_BYTES = 64 * 1024 * 1024;

    private static final int CONTROL_MAGIC = 0x524b4331; // "RKC1"
    private static final int CONTROL_CAPABILITIES = 1;
    // The canonical five-byte VarInt encoding of -1. Negative Minecraft packet
    // ids are invalid, making this marker unambiguous inside a game frame.
    private static final byte[] BUNDLE_MARKER = new byte[]{
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x0f
    };

    private CausalTransportProtocol() {
    }

    public static ByteBuf encodeCapabilities(ByteBufAllocator allocator, long capabilities) {
        return allocator.buffer(14, 14)
                .writeInt(CONTROL_MAGIC)
                .writeByte(VERSION)
                .writeByte(CONTROL_CAPABILITIES)
                .writeLong(capabilities);
    }

    public static Capabilities decodeCapabilities(ByteBuf payload) {
        if (payload.readableBytes() != 14) {
            throw new CorruptedFrameException("Invalid causal capability payload length: "
                    + payload.readableBytes());
        }
        final int magic = payload.readInt();
        if (magic != CONTROL_MAGIC) {
            throw new CorruptedFrameException("Invalid causal control magic");
        }
        final int version = payload.readUnsignedByte();
        final int type = payload.readUnsignedByte();
        if (type != CONTROL_CAPABILITIES) {
            throw new CorruptedFrameException("Unexpected causal control message type: " + type);
        }
        return new Capabilities(version, payload.readLong());
    }

    public static boolean isAtomicBundle(ByteBuf payload) {
        if (payload.readableBytes() < BUNDLE_MARKER.length + 2) {
            return false;
        }
        final int offset = payload.readerIndex();
        for (int i = 0; i < BUNDLE_MARKER.length; i++) {
            if (payload.getByte(offset + i) != BUNDLE_MARKER[i]) {
                return false;
            }
        }
        return true;
    }

    public static ByteBuf encodeAtomicBundle(ByteBufAllocator allocator, List<ByteBuf> packets) {
        if (packets.size() < 2 || packets.size() > MAX_ATOMIC_BUNDLE_PACKETS) {
            throw new IllegalArgumentException("Invalid atomic bundle packet count: " + packets.size());
        }

        long encodedBytes = BUNDLE_MARKER.length + 1L + MathUtil.varIntSize(packets.size());
        for (ByteBuf packet : packets) {
            final int length = packet.readableBytes();
            if (length <= 0) {
                throw new IllegalArgumentException("Atomic bundle contains an empty packet");
            }
            encodedBytes += MathUtil.varIntSize(length) + (long) length;
            if (encodedBytes > MAX_ATOMIC_BUNDLE_BYTES) {
                throw new IllegalArgumentException("Atomic bundle exceeds "
                        + MAX_ATOMIC_BUNDLE_BYTES + " encoded bytes");
            }
        }

        final ByteBuf out = allocator.buffer((int) encodedBytes, (int) encodedBytes);
        out.writeBytes(BUNDLE_MARKER);
        out.writeByte(VERSION);
        MathUtil.writeVarInt(out, packets.size());
        for (ByteBuf packet : packets) {
            MathUtil.writeVarInt(out, packet.readableBytes());
            out.writeBytes(packet, packet.readerIndex(), packet.readableBytes());
        }
        return out;
    }

    public static List<ByteBuf> decodeAtomicBundle(ByteBuf payload) {
        if (!isAtomicBundle(payload)) {
            throw new CorruptedFrameException("Missing atomic bundle marker");
        }
        payload.skipBytes(BUNDLE_MARKER.length);
        final int version = payload.readUnsignedByte();
        if (version != VERSION) {
            throw new CorruptedFrameException("Unsupported atomic bundle version: " + version);
        }

        final int packetCount;
        try {
            packetCount = MathUtil.readVarInt(payload);
        } catch (RuntimeException exception) {
            throw new CorruptedFrameException("Invalid atomic bundle packet count", exception);
        }
        if (packetCount < 2 || packetCount > MAX_ATOMIC_BUNDLE_PACKETS) {
            throw new CorruptedFrameException("Invalid atomic bundle packet count: " + packetCount);
        }

        final List<ByteBuf> packets = new ArrayList<>(packetCount);
        int decodedBytes = 0;
        try {
            for (int i = 0; i < packetCount; i++) {
                final int packetLength;
                try {
                    packetLength = MathUtil.readVarInt(payload);
                } catch (RuntimeException exception) {
                    throw new CorruptedFrameException("Invalid atomic bundle packet length", exception);
                }
                if (packetLength <= 0 || packetLength > payload.readableBytes()) {
                    throw new CorruptedFrameException("Invalid atomic bundle packet length: "
                            + packetLength);
                }
                decodedBytes += packetLength;
                if (decodedBytes > MAX_ATOMIC_BUNDLE_BYTES) {
                    throw new CorruptedFrameException("Atomic bundle decoded payload exceeds "
                            + MAX_ATOMIC_BUNDLE_BYTES + " bytes");
                }
                packets.add(payload.readRetainedSlice(packetLength));
            }
            if (payload.isReadable()) {
                throw new CorruptedFrameException("Trailing bytes after atomic bundle");
            }
            if (!ByteBufUtil.equals(packets.get(0), packets.get(packets.size() - 1))) {
                throw new CorruptedFrameException("Atomic bundle delimiters do not match");
            }
            return packets;
        } catch (RuntimeException throwable) {
            packets.forEach(ReferenceCountUtil::safeRelease);
            throw throwable;
        }
    }

    public record Capabilities(int version, long capabilities) {
    }

}
