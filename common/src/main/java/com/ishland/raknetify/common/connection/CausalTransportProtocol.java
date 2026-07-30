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
import io.netty.channel.Channel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.AttributeKey;

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
    public static final long CAPABILITY_LOSSLESS_FENCE = 1L << 1;
    public static final long CAPABILITY_GAMEPLAY_EPOCH = 1L << 2;
    /**
     * The peer confirms receipt of our advertisement before we emit any
     * capability-dependent frame. Without this handshake, gameplay on another
     * order channel can overtake the channel-7 advertisement.
     */
    public static final long CAPABILITY_CONFIRMATION = 1L << 3;
    /**
     * Guarded bulk may use channel 6 when strict channel-7 frames carry the
     * highest bulk sequence that must be delivered before them.
     */
    public static final long CAPABILITY_GUARDED_BULK_WATERMARK = 1L << 4;
    public static final long LOCAL_CAPABILITIES = CAPABILITY_ATOMIC_BUNDLE
            | CAPABILITY_LOSSLESS_FENCE
            | CAPABILITY_GAMEPLAY_EPOCH
            | CAPABILITY_CONFIRMATION
            | CAPABILITY_GUARDED_BULK_WATERMARK;
    public static final AttributeKey<Long> NEGOTIATED_CAPABILITIES =
            AttributeKey.valueOf("raknetify:causal-capabilities");
    public static final AttributeKey<Long> INBOUND_CAPABILITIES =
            AttributeKey.valueOf("raknetify:causal-inbound-capabilities");
    public static final AttributeKey<Long> OUTBOUND_CAPABILITIES =
            AttributeKey.valueOf("raknetify:causal-outbound-capabilities");

    /**
     * Vanilla permits 4096 packets inside a bundle. PacketUnbundler adds the
     * opening and closing delimiter on the wire, so a maximum-size legal
     * bundle contains 4098 encoded packets.
     */
    public static final int MAX_ATOMIC_BUNDLE_CONTENT_PACKETS = 4096;
    public static final int MAX_ATOMIC_BUNDLE_PACKETS =
            MAX_ATOMIC_BUNDLE_CONTENT_PACKETS + 2;
    public static final int MAX_ATOMIC_BUNDLE_BYTES = 64 * 1024 * 1024;
    public static final int MAX_PENDING_CAUSAL_WRITES = 16 * 1024;

    private static final int CONTROL_MAGIC = 0x524b4331; // "RKC1"
    private static final int CONTROL_CAPABILITIES = 1;
    private static final int CONTROL_CAPABILITIES_ACK = 2;
    private static final int EPOCH_FRAME_VERSION = 2;
    private static final int DEPENDENCY_FRAME_VERSION = 3;
    private static final int EPOCH_FRAME_SINGLE = 1;
    private static final int EPOCH_FRAME_BUNDLE = 2;
    private static final int DEPENDENCY_STRICT = 1;
    private static final int DEPENDENCY_GUARDED_BULK = 2;
    // The canonical five-byte VarInt encoding of -1. Negative Minecraft packet
    // ids are invalid, making this marker unambiguous inside a game frame.
    private static final byte[] BUNDLE_MARKER = new byte[]{
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x0f
    };

    private CausalTransportProtocol() {
    }

    public static ByteBuf encodeCapabilities(ByteBufAllocator allocator, long capabilities) {
        return encodeControl(allocator, CONTROL_CAPABILITIES, capabilities);
    }

    public static ByteBuf encodeCapabilitiesAck(
            ByteBufAllocator allocator,
            long acknowledgedCapabilities
    ) {
        return encodeControl(
                allocator,
                CONTROL_CAPABILITIES_ACK,
                acknowledgedCapabilities
        );
    }

    private static ByteBuf encodeControl(
            ByteBufAllocator allocator,
            int type,
            long capabilities
    ) {
        return allocator.buffer(14, 14)
                .writeInt(CONTROL_MAGIC)
                .writeByte(VERSION)
                .writeByte(type)
                .writeLong(capabilities);
    }

    public static Capabilities decodeCapabilities(ByteBuf payload) {
        final ControlMessage message = decodeControl(payload);
        if (message instanceof Capabilities capabilities) {
            return capabilities;
        }
        throw new CorruptedFrameException("Expected causal capabilities advertisement");
    }

    public static CapabilitiesAck decodeCapabilitiesAck(ByteBuf payload) {
        final ControlMessage message = decodeControl(payload);
        if (message instanceof CapabilitiesAck ack) {
            return ack;
        }
        throw new CorruptedFrameException("Expected causal capabilities acknowledgement");
    }

    public static ControlMessage decodeControl(ByteBuf payload) {
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
        final long capabilities = payload.readLong();
        return switch (type) {
            case CONTROL_CAPABILITIES ->
                    new Capabilities(version, capabilities);
            case CONTROL_CAPABILITIES_ACK ->
                    new CapabilitiesAck(version, capabilities);
            default -> throw new CorruptedFrameException(
                    "Unexpected causal control message type: " + type
            );
        };
    }

    public static long negotiateCapabilities(long remoteCapabilities) {
        long negotiated = remoteCapabilities & LOCAL_CAPABILITIES;
        if ((negotiated & CAPABILITY_ATOMIC_BUNDLE) == 0
                || (negotiated & CAPABILITY_LOSSLESS_FENCE) == 0) {
            negotiated &= ~CAPABILITY_GAMEPLAY_EPOCH;
        }
        if ((negotiated & CAPABILITY_GAMEPLAY_EPOCH) == 0) {
            negotiated &= ~CAPABILITY_GUARDED_BULK_WATERMARK;
        }
        return negotiated;
    }

    public static void setNegotiatedCapabilities(Channel channel, long capabilities) {
        setCapabilities(channel, INBOUND_CAPABILITIES, capabilities);
        setCapabilities(channel, OUTBOUND_CAPABILITIES, capabilities);
        setCapabilities(channel, NEGOTIATED_CAPABILITIES, capabilities);
    }

    public static void setInboundCapabilities(Channel channel, long capabilities) {
        setCapabilities(channel, INBOUND_CAPABILITIES, capabilities);
        updateCombinedCapabilities(channel);
    }

    public static void setOutboundCapabilities(Channel channel, long capabilities) {
        setCapabilities(channel, OUTBOUND_CAPABILITIES, capabilities);
        updateCombinedCapabilities(channel);
    }

    public static long getNegotiatedCapabilities(Channel channel) {
        return getCapabilities(channel, NEGOTIATED_CAPABILITIES);
    }

    public static long getInboundCapabilities(Channel channel) {
        return getCapabilities(channel, INBOUND_CAPABILITIES);
    }

    public static long getOutboundCapabilities(Channel channel) {
        return getCapabilities(channel, OUTBOUND_CAPABILITIES);
    }

    public static boolean hasCapability(Channel channel, long capability) {
        return (getNegotiatedCapabilities(channel) & capability) != 0;
    }

    public static boolean hasInboundCapability(Channel channel, long capability) {
        return (getInboundCapabilities(channel) & capability) != 0;
    }

    public static boolean hasOutboundCapability(Channel channel, long capability) {
        return (getOutboundCapabilities(channel) & capability) != 0;
    }

    private static void updateCombinedCapabilities(Channel channel) {
        setCapabilities(
                channel,
                NEGOTIATED_CAPABILITIES,
                getInboundCapabilities(channel) & getOutboundCapabilities(channel)
        );
    }

    private static void setCapabilities(
            Channel channel,
            AttributeKey<Long> key,
            long capabilities
    ) {
        channel.attr(key).set(capabilities);
        if (channel.parent() != null) {
            channel.parent().attr(key).set(capabilities);
        }
    }

    private static long getCapabilities(
            Channel channel,
            AttributeKey<Long> key
    ) {
        final Long capabilities = channel.attr(key).get();
        return capabilities == null ? 0L : capabilities;
    }

    public static boolean isAtomicBundle(ByteBuf payload) {
        return hasCausalMarker(payload)
                && payload.getUnsignedByte(payload.readerIndex() + BUNDLE_MARKER.length) == VERSION;
    }

    public static boolean isEpochGameplayFrame(ByteBuf payload) {
        if (!hasCausalMarker(payload)) {
            return false;
        }
        final int version =
                payload.getUnsignedByte(payload.readerIndex() + BUNDLE_MARKER.length);
        return version == EPOCH_FRAME_VERSION
                || version == DEPENDENCY_FRAME_VERSION;
    }

    public static boolean isBulkDependencyFrame(ByteBuf payload) {
        if (!isDependencyGameplayFrame(payload)) {
            return false;
        }
        final ByteBuf duplicate = payload.duplicate();
        return readEpochHeader(duplicate).dependencyKind()
                == DependencyKind.GUARDED_BULK;
    }

    public static boolean isDependencyGameplayFrame(ByteBuf payload) {
        return hasCausalMarker(payload)
                && payload.getUnsignedByte(
                        payload.readerIndex() + BUNDLE_MARKER.length
                ) == DEPENDENCY_FRAME_VERSION;
    }

    public static boolean isEpochAtomicBundle(ByteBuf payload) {
        return isEpochGameplayFrame(payload)
                && payload.readableBytes() >= BUNDLE_MARKER.length + 2
                && payload.getUnsignedByte(payload.readerIndex() + BUNDLE_MARKER.length + 1)
                == EPOCH_FRAME_BUNDLE;
    }

    private static boolean hasCausalMarker(ByteBuf payload) {
        if (payload.readableBytes() < BUNDLE_MARKER.length + 1) {
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

    public static ByteBuf encodeGameplayFrame(
            ByteBufAllocator allocator,
            int epoch,
            ByteBuf packet
    ) {
        requireValidEpoch(epoch);
        if (!packet.isReadable()) {
            throw new IllegalArgumentException("Cannot encode an empty gameplay frame");
        }
        final int encodedBytes = BUNDLE_MARKER.length + 2
                + MathUtil.varIntSize(epoch)
                + packet.readableBytes();
        final ByteBuf out = allocator.buffer(encodedBytes, encodedBytes);
        out.writeBytes(BUNDLE_MARKER);
        out.writeByte(EPOCH_FRAME_VERSION);
        out.writeByte(EPOCH_FRAME_SINGLE);
        MathUtil.writeVarInt(out, epoch);
        out.writeBytes(packet, packet.readerIndex(), packet.readableBytes());
        return out;
    }

    public static ByteBuf encodeDependencyGameplayFrame(
            ByteBufAllocator allocator,
            int epoch,
            DependencyKind dependencyKind,
            int sequence,
            ByteBuf packet
    ) {
        requireValidEpoch(epoch);
        requireDependency(dependencyKind, sequence);
        if (!packet.isReadable()) {
            throw new IllegalArgumentException("Cannot encode an empty gameplay frame");
        }
        final int encodedBytes = BUNDLE_MARKER.length + 3
                + MathUtil.varIntSize(epoch)
                + MathUtil.varIntSize(sequence)
                + packet.readableBytes();
        final ByteBuf out = allocator.buffer(encodedBytes, encodedBytes);
        out.writeBytes(BUNDLE_MARKER);
        out.writeByte(DEPENDENCY_FRAME_VERSION);
        out.writeByte(EPOCH_FRAME_SINGLE);
        MathUtil.writeVarInt(out, epoch);
        out.writeByte(dependencyKind.wireId);
        MathUtil.writeVarInt(out, sequence);
        out.writeBytes(packet, packet.readerIndex(), packet.readableBytes());
        return out;
    }

    public static ByteBuf encodeAtomicBundle(ByteBufAllocator allocator, List<ByteBuf> packets) {
        return encodeAtomicBundle0(
                allocator,
                -1,
                DependencyKind.NONE,
                0,
                packets
        );
    }

    public static ByteBuf encodeAtomicBundle(
            ByteBufAllocator allocator,
            int epoch,
            List<ByteBuf> packets
    ) {
        requireValidEpoch(epoch);
        return encodeAtomicBundle0(
                allocator,
                epoch,
                DependencyKind.NONE,
                0,
                packets
        );
    }

    public static ByteBuf encodeDependencyAtomicBundle(
            ByteBufAllocator allocator,
            int epoch,
            int requiredBulkSequence,
            List<ByteBuf> packets
    ) {
        requireValidEpoch(epoch);
        requireDependency(DependencyKind.STRICT, requiredBulkSequence);
        return encodeAtomicBundle0(
                allocator,
                epoch,
                DependencyKind.STRICT,
                requiredBulkSequence,
                packets
        );
    }

    private static ByteBuf encodeAtomicBundle0(
            ByteBufAllocator allocator,
            int epoch,
            DependencyKind dependencyKind,
            int dependencySequence,
            List<ByteBuf> packets
    ) {
        if (packets.size() < 2 || packets.size() > MAX_ATOMIC_BUNDLE_PACKETS) {
            throw new IllegalArgumentException("Invalid atomic bundle packet count: " + packets.size());
        }

        long encodedBytes = BUNDLE_MARKER.length + 1L + MathUtil.varIntSize(packets.size());
        if (epoch >= 0) {
            encodedBytes += 1L + MathUtil.varIntSize(epoch);
        }
        if (dependencyKind != DependencyKind.NONE) {
            encodedBytes += 1L + MathUtil.varIntSize(dependencySequence);
        }
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
        if (epoch >= 0) {
            out.writeByte(dependencyKind == DependencyKind.NONE
                    ? EPOCH_FRAME_VERSION
                    : DEPENDENCY_FRAME_VERSION);
            out.writeByte(EPOCH_FRAME_BUNDLE);
            MathUtil.writeVarInt(out, epoch);
            if (dependencyKind != DependencyKind.NONE) {
                out.writeByte(dependencyKind.wireId);
                MathUtil.writeVarInt(out, dependencySequence);
            }
        } else {
            out.writeByte(VERSION);
        }
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

        return decodeBundlePackets(payload);
    }

    public static int peekGameplayEpoch(ByteBuf payload) {
        final ByteBuf duplicate = payload.duplicate();
        return readEpochHeader(duplicate).epoch();
    }

    public static GameplayFrame decodeGameplayFrame(ByteBuf payload) {
        final EpochHeader header = readEpochHeader(payload);
        final int type = header.type();
        final int epoch = header.epoch();
        if (type == EPOCH_FRAME_SINGLE) {
            if (!payload.isReadable()) {
                throw new CorruptedFrameException("Empty causal gameplay frame");
            }
            final List<ByteBuf> packets = new ArrayList<>(1);
            packets.add(payload.readRetainedSlice(payload.readableBytes()));
            return new GameplayFrame(
                    epoch,
                    false,
                    header.dependencyKind(),
                    header.dependencySequence(),
                    packets
            );
        }
        if (type == EPOCH_FRAME_BUNDLE) {
            return new GameplayFrame(
                    epoch,
                    true,
                    header.dependencyKind(),
                    header.dependencySequence(),
                    decodeBundlePackets(payload)
            );
        }
        throw new CorruptedFrameException("Unknown causal gameplay frame type: " + type);
    }

    private static List<ByteBuf> decodeBundlePackets(ByteBuf payload) {
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

    private static void requireEpochGameplayFrame(ByteBuf payload) {
        if (!isEpochGameplayFrame(payload)) {
            throw new CorruptedFrameException("Missing causal gameplay frame marker");
        }
    }

    private static EpochHeader readEpochHeader(ByteBuf payload) {
        requireEpochGameplayFrame(payload);
        if (payload.readableBytes() < BUNDLE_MARKER.length + 3) {
            throw new CorruptedFrameException("Truncated causal gameplay frame header");
        }
        payload.skipBytes(BUNDLE_MARKER.length);
        final int version = payload.readUnsignedByte();
        final int type = payload.readUnsignedByte();
        if (type != EPOCH_FRAME_SINGLE && type != EPOCH_FRAME_BUNDLE) {
            throw new CorruptedFrameException(
                    "Unknown causal gameplay frame type: " + type
            );
        }
        final int epoch = readNonNegativeVarInt(payload, "gameplay epoch");
        if (version == EPOCH_FRAME_VERSION) {
            return new EpochHeader(type, epoch, DependencyKind.NONE, 0);
        }
        if (!payload.isReadable()) {
            throw new CorruptedFrameException(
                    "Truncated guarded-bulk dependency header"
            );
        }
        final DependencyKind dependencyKind =
                DependencyKind.fromWireId(payload.readUnsignedByte());
        final int dependencySequence =
                readNonNegativeVarInt(payload, "guarded-bulk dependency sequence");
        if (type == EPOCH_FRAME_BUNDLE
                && dependencyKind != DependencyKind.STRICT) {
            throw new CorruptedFrameException(
                    "Atomic bundle must use the strict dependency kind"
            );
        }
        return new EpochHeader(
                type,
                epoch,
                dependencyKind,
                dependencySequence
        );
    }

    private static int readNonNegativeVarInt(ByteBuf payload, String description) {
        final int value;
        try {
            value = MathUtil.readVarInt(payload);
        } catch (RuntimeException exception) {
            throw new CorruptedFrameException("Invalid " + description, exception);
        }
        if (value < 0) {
            throw new CorruptedFrameException("Negative " + description + ": " + value);
        }
        return value;
    }

    private static void requireValidEpoch(int epoch) {
        if (epoch < 0) {
            throw new IllegalArgumentException("Negative gameplay epoch: " + epoch);
        }
    }

    private static void requireDependency(
            DependencyKind dependencyKind,
            int sequence
    ) {
        if (dependencyKind == null || dependencyKind == DependencyKind.NONE) {
            throw new IllegalArgumentException(
                    "A guarded-bulk dependency kind is required"
            );
        }
        if (sequence < 0) {
            throw new IllegalArgumentException(
                    "Negative guarded-bulk dependency sequence: " + sequence
            );
        }
    }

    public sealed interface ControlMessage
            permits Capabilities, CapabilitiesAck {
        int version();
    }

    public record Capabilities(int version, long capabilities)
            implements ControlMessage {
    }

    public record CapabilitiesAck(int version, long acknowledgedCapabilities)
            implements ControlMessage {
    }

    public record GameplayFrame(
            int epoch,
            boolean atomicBundle,
            DependencyKind dependencyKind,
            int dependencySequence,
            List<ByteBuf> packets
    ) {
    }

    public enum DependencyKind {
        NONE(0),
        STRICT(DEPENDENCY_STRICT),
        GUARDED_BULK(DEPENDENCY_GUARDED_BULK);

        private final int wireId;

        DependencyKind(int wireId) {
            this.wireId = wireId;
        }

        private static DependencyKind fromWireId(int wireId) {
            return switch (wireId) {
                case DEPENDENCY_STRICT -> STRICT;
                case DEPENDENCY_GUARDED_BULK -> GUARDED_BULK;
                default -> throw new CorruptedFrameException(
                        "Unknown guarded-bulk dependency kind: " + wireId
                );
            };
        }
    }

    private record EpochHeader(
            int type,
            int epoch,
            DependencyKind dependencyKind,
            int dependencySequence
    ) {
    }

}
