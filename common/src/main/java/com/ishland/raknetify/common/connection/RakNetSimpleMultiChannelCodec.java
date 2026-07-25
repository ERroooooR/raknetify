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

import com.ishland.raknetify.common.Constants;
import com.ishland.raknetify.common.util.MathUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.FramedPacket;
import network.ycc.raknet.RakNet;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class RakNetSimpleMultiChannelCodec extends ChannelDuplexHandler {

    public static final String NAME = "raknetify-simple-multi-channel-data-codec";

    public static final Object SIGNAL_START_MULTICHANNEL = new Object();

    private final int packetId;

    public RakNetSimpleMultiChannelCodec(int packetId) {
        this.packetId = packetId;
    }

    private final ObjectArrayList<OverrideHandler> handlers = new ObjectArrayList<>();

    public RakNetSimpleMultiChannelCodec addHandler(OverrideHandler handler) {
        synchronized (handlers) {
            handlers.add(handler);
        }
        return this;
    }

    public void removeHandler(OverrideHandler handler) {
        synchronized (handlers) {
            handlers.remove(handler);
        }
    }

    public <T> T getHandler(Class<T> clazz) {
        synchronized (handlers) {
            for (OverrideHandler handler : handlers) {
                if (clazz.isInstance(handler)) return clazz.cast(handler);
            }
        }
        return null;
    }

    private boolean isMultichannelEnabled;
    private boolean localCapabilitiesSent;
    private long remoteCapabilities;

    private boolean queuePendingWrites = false;
    private final Queue<PendingWrite> pendingWrites = new LinkedList<>();
    private final AtomicBundleAssembler atomicBundleAssembler = new AtomicBundleAssembler();
    private final Queue<PendingControlWrite> pendingControlWrites = new LinkedList<>();

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        final IllegalStateException cause = new IllegalStateException("Channel closed");
        failPendingWrites(cause);
        atomicBundleAssembler.abort(cause);
        PendingControlWrite pendingControlWrite;
        while ((pendingControlWrite = pendingControlWrites.poll()) != null) {
            pendingControlWrite.promise.tryFailure(cause);
            ReferenceCountUtil.safeRelease(pendingControlWrite.message);
        }
        super.handlerRemoved(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (this.queuePendingWrites && msg instanceof ByteBuf buf) {
            pendingWrites.add(new PendingWrite(buf, promise));
            return;
        }

        if (atomicBundleAssembler.isOpen()
                && (msg == SIGNAL_START_MULTICHANNEL || msg == SynchronizationLayer.SYNC_REQUEST_OBJECT)) {
            pendingControlWrites.add(new PendingControlWrite(msg, promise));
            return;
        }

        if (msg == SIGNAL_START_MULTICHANNEL) {
            promise.trySuccess();
            if (this.isMultichannelEnabled) return;
            if (!this.isMultichannelAvailable()) {
                System.out.println("Raknetify: [MultiChannellingDataCodec] Failed to start multichannel: not available");
                return;
            }
            sendCapabilities(ctx, false);
            final ByteBuf buf = ctx.alloc().buffer(1).writeByte(0);
            try {
                final FrameData frameData = FrameData.create(ctx.alloc(), Constants.RAKNET_PING_PACKET_ID, buf);
                frameData.setOrderChannel(7);
                this.queuePendingWrites = true;
                ctx.write(frameData).addListener(future -> {
                    if (future.isSuccess()) {
                        isMultichannelEnabled = true;
                        if (Constants.DEBUG)
                            System.out.println("Raknetify: [MultiChannellingDataCodec] Started multichannel");
                        flushPendingWrites(ctx);
                    } else {
                        queuePendingWrites = false;
                        failPendingWrites(future.cause());
                    }
                });
            } finally {
                buf.release();
            }
            return;
        }
        if (msg == SynchronizationLayer.SYNC_REQUEST_OBJECT) {
            if (this.isMultichannelEnabled) {
                if (Constants.DEBUG) System.out.println("Raknetify: [MultiChannellingDataCodec] Stopped multichannel");
                this.isMultichannelEnabled = false;
                super.write(ctx, msg, promise);
            }
            promise.setSuccess();
            return; // discard sync request when multichannel is not active
        }

        if (msg instanceof ByteBuf buf && buf.isReadable()) {
            writeGamePacket(ctx, buf, promise);
            return;
        }

        super.write(ctx, msg, promise);
    }

    private void writeGamePacket(ChannelHandlerContext ctx, ByteBuf buf, ChannelPromise promise) {
        try {
            final OverrideResult decision = getOverrideResult(buf, !isMultichannelEnabled);
            if (isAtomicBundleEnabled()
                    && (atomicBundleAssembler.isOpen() || decision.isBundleDelimiter())) {
                final AtomicBundleAssembler.CompletedBundle completedBundle =
                        atomicBundleAssembler.accept(
                                ctx.alloc(),
                                buf,
                                promise,
                                decision.isBundleDelimiter()
                        );
                if (completedBundle != null) {
                    writeAtomicBundle(ctx, completedBundle);
                }
                return;
            }

            final FrameData frameData = encode0(ctx, buf, decision);
            if (frameData != null) {
                ctx.write(frameData, promise);
            } else {
                promise.trySuccess();
            }
        } catch (Throwable throwable) {
            if (atomicBundleAssembler.isOpen()) {
                atomicBundleAssembler.abort(throwable);
            }
            promise.tryFailure(throwable);
            ctx.fireExceptionCaught(throwable);
        } finally {
            buf.release();
        }
    }

    private FrameData encode0(ChannelHandlerContext ctx, ByteBuf buf, OverrideResult decision) {
        if (buf.isReadable()) {
            if (ctx.pipeline().get("zstd_encoder") != null
                    && ctx.channel().config() instanceof RakNet.Config config) {
                config.getMetrics().applicationBatch(buf.readableBytes());
            }
            final int packetChannelOverride = decision.isBundleDelimiter() ? 7 : decision.channel();
            final FrameData frameData = FrameData.create(ctx.alloc(), packetId, buf);
            if (isMultichannelEnabled) {
                if (packetChannelOverride >= 0)
                    frameData.setOrderChannel(packetChannelOverride);
                else if (packetChannelOverride == -1)
                    frameData.setReliability(FramedPacket.Reliability.RELIABLE);
                else if (packetChannelOverride == -2)
                    frameData.setReliability(FramedPacket.Reliability.UNRELIABLE);
            }
            return frameData;
        }
        return null;
    }

    private void writeAtomicBundle(
            ChannelHandlerContext ctx,
            AtomicBundleAssembler.CompletedBundle completedBundle
    ) {
        final ByteBuf envelope = completedBundle.payload();
        final List<ChannelPromise> promises = completedBundle.promises();
        FrameData frameData = null;
        try {
            frameData = FrameData.create(ctx.alloc(), packetId, envelope);
            frameData.setOrderChannel(7);

            final ChannelPromise envelopePromise = ctx.newPromise();
            envelopePromise.addListener(future -> {
                for (ChannelPromise promise : promises) {
                    if (future.isSuccess()) {
                        promise.trySuccess();
                    } else if (future.isCancelled()) {
                        promise.cancel(false);
                    } else {
                        promise.tryFailure(future.cause());
                    }
                }
            });
            ctx.write(frameData, envelopePromise);
            frameData = null;
        } catch (Throwable throwable) {
            for (ChannelPromise promise : promises) {
                promise.tryFailure(throwable);
            }
            ctx.fireExceptionCaught(throwable);
        } finally {
            ReferenceCountUtil.safeRelease(envelope);
            ReferenceCountUtil.safeRelease(frameData);
        }

        flushPendingControlWrites(ctx);
    }

    private void flushPendingControlWrites(ChannelHandlerContext ctx) {
        PendingControlWrite pendingControlWrite;
        while ((pendingControlWrite = pendingControlWrites.poll()) != null) {
            try {
                write(ctx, pendingControlWrite.message, pendingControlWrite.promise);
            } catch (Throwable throwable) {
                pendingControlWrite.promise.tryFailure(throwable);
                ctx.fireExceptionCaught(throwable);
            }
        }
    }

    private void flushPendingWrites(ChannelHandlerContext ctx) {
        this.queuePendingWrites = false;
        PendingWrite pendingWrite;
        while ((pendingWrite = this.pendingWrites.poll()) != null) {
            try {
                writeGamePacket(ctx, pendingWrite.packet, pendingWrite.promise);
            } catch (Throwable t) {
                pendingWrite.promise.tryFailure(t);
                ReferenceCountUtil.safeRelease(pendingWrite.packet);
                ctx.fireExceptionCaught(t);
            }
        }
    }

    private void failPendingWrites(Throwable cause) {
        PendingWrite pendingWrite;
        while ((pendingWrite = pendingWrites.poll()) != null) {
            pendingWrite.promise.tryFailure(cause);
            ReferenceCountUtil.safeRelease(pendingWrite.packet);
        }
    }

    private void sendCapabilities(ChannelHandlerContext ctx, boolean flush) {
        if (localCapabilitiesSent) {
            return;
        }
        final ByteBuf payload = CausalTransportProtocol.encodeCapabilities(
                ctx.alloc(),
                CausalTransportProtocol.LOCAL_CAPABILITIES
        );
        FrameData frameData = null;
        try {
            frameData = FrameData.create(
                    ctx.alloc(),
                    Constants.RAKNET_CAUSAL_CONTROL_PACKET_ID,
                    payload
            );
            frameData.setOrderChannel(7);
            localCapabilitiesSent = true;
            final ChannelFuture writeFuture = flush
                    ? ctx.writeAndFlush(frameData)
                    : ctx.write(frameData);
            frameData = null;
            writeFuture.addListener(future -> {
                if (!future.isSuccess()) {
                    localCapabilitiesSent = false;
                }
            });
        } finally {
            payload.release();
            ReferenceCountUtil.safeRelease(frameData);
        }
    }

    public boolean isAtomicBundleEnabled() {
        return isMultichannelEnabled && isAtomicBundleNegotiated();
    }

    private boolean isAtomicBundleNegotiated() {
        return localCapabilitiesSent
                && (remoteCapabilities & CausalTransportProtocol.CAPABILITY_ATOMIC_BUNDLE) != 0;
    }

    protected boolean isMultichannelAvailable() {
        synchronized (handlers) {
            return !handlers.isEmpty();
        }
    }

    protected int getChannelOverride(ByteBuf buf, boolean suppressWarning) {
        return getOverrideResult(buf, suppressWarning).channel();
    }

    protected OverrideResult getOverrideResult(ByteBuf buf, boolean suppressWarning) {
        OverrideResult firstMatch = OverrideResult.pass();
        synchronized (handlers) {
            for (OverrideHandler handler : handlers) {
                final OverrideResult override = handler.getChannelOverride(buf, suppressWarning);
                if (override.isBundleDelimiter()) {
                    return override;
                }
                if (!firstMatch.matched() && override.matched()) {
                    firstMatch = override;
                }
            }
        }
        return firstMatch.matched() ? firstMatch : OverrideResult.route(0);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FrameData packet && !packet.isFragment() && packet.getDataSize() > 0) {
            try {
                if (packetId == packet.getPacketId()) {
                    final ByteBuf payload = packet.createData().skipBytes(1);
                    if (isAtomicBundleNegotiated() && CausalTransportProtocol.isAtomicBundle(payload)) {
                        if (!packet.getReliability().isReliable
                                || !packet.getReliability().isOrdered
                                || packet.getOrderChannel() != 7) {
                            payload.release();
                            throw new CorruptedFrameException(
                                    "Atomic bundle was not delivered on reliable ordered channel 7"
                            );
                        }
                        fireAtomicBundle(ctx, payload);
                    } else {
                        ctx.fireChannelRead(payload);
                    }
                } else if (packet.getPacketId() == Constants.RAKNET_CAUSAL_CONTROL_PACKET_ID) {
                    handleCapabilities(ctx, packet);
                } else if (packet.getPacketId() == Constants.RAKNET_PING_PACKET_ID) {
                    return;
                } else {
                    ctx.fireChannelRead(packet.retain());
                }
            } finally {
                packet.release();
            }
            return;
        }
        super.channelRead(ctx, msg);
    }

    private void handleCapabilities(ChannelHandlerContext ctx, FrameData packet) {
        if (!packet.getReliability().isReliable
                || !packet.getReliability().isOrdered
                || packet.getOrderChannel() != 7) {
            throw new CorruptedFrameException(
                    "Causal capabilities were not delivered on reliable ordered channel 7"
            );
        }
        final ByteBuf payload = packet.createData().skipBytes(1);
        try {
            final CausalTransportProtocol.Capabilities capabilities =
                    CausalTransportProtocol.decodeCapabilities(payload);
            remoteCapabilities = capabilities.version() == CausalTransportProtocol.VERSION
                    ? capabilities.capabilities() & CausalTransportProtocol.LOCAL_CAPABILITIES
                    : 0L;
            sendCapabilities(ctx, true);
        } finally {
            payload.release();
        }
    }

    private void fireAtomicBundle(ChannelHandlerContext ctx, ByteBuf payload) {
        final List<ByteBuf> packets;
        try {
            packets = CausalTransportProtocol.decodeAtomicBundle(payload);
        } finally {
            payload.release();
        }
        try {
            for (int i = 0; i < packets.size(); i++) {
                final ByteBuf packet = packets.set(i, null);
                ctx.fireChannelRead(packet);
            }
        } finally {
            packets.forEach(ReferenceCountUtil::safeRelease);
        }
    }

    public interface OverrideHandler {
        OverrideResult getChannelOverride(ByteBuf buf, boolean suppressWarning);
    }

    /**
     * Separates an explicit channel-zero decision from a handler that did not
     * recognize the packet, and identifies bundle delimiters as protocol
     * boundaries rather than disposable packets.
     */
    public record OverrideResult(boolean matched, int channel) {

        private static final OverrideResult PASS = new OverrideResult(false, 0);
        private static final OverrideResult BUNDLE_DELIMITER =
                new OverrideResult(true, Integer.MIN_VALUE);
        private static final OverrideResult RELIABLE_UNORDERED = new OverrideResult(true, -1);
        private static final OverrideResult UNRELIABLE = new OverrideResult(true, -2);
        private static final OverrideResult[] ORDERED = new OverrideResult[]{
                new OverrideResult(true, 0),
                new OverrideResult(true, 1),
                new OverrideResult(true, 2),
                new OverrideResult(true, 3),
                new OverrideResult(true, 4),
                new OverrideResult(true, 5),
                new OverrideResult(true, 6),
                new OverrideResult(true, 7)
        };

        public static OverrideResult pass() {
            return PASS;
        }

        public static OverrideResult bundleDelimiter() {
            return BUNDLE_DELIMITER;
        }

        public boolean isBundleDelimiter() {
            return matched && channel == Integer.MIN_VALUE;
        }

        public static OverrideResult route(int channel) {
            if (channel >= 0 && channel < ORDERED.length) return ORDERED[channel];
            if (channel == Integer.MIN_VALUE) return BUNDLE_DELIMITER;
            if (channel == -1) return RELIABLE_UNORDERED;
            if (channel == -2) return UNRELIABLE;
            return new OverrideResult(true, channel);
        }
    }

    public static class PacketIdBasedOverrideHandler implements OverrideHandler {

        private final IntOpenHashSet unknownPacketIds = new IntOpenHashSet();
        private final Int2IntOpenHashMap channelMapping;
        private final String descriptiveProtocolStatus;

        public PacketIdBasedOverrideHandler(Int2IntMap channelMapping, String descriptiveProtocolStatus) {
            this.channelMapping = new Int2IntOpenHashMap(channelMapping);
            this.descriptiveProtocolStatus = descriptiveProtocolStatus;
        }

        @Override
        public OverrideResult getChannelOverride(ByteBuf buf, boolean suppressWarning) {
            final ByteBuf slice = buf.slice();
            final int packetId = MathUtil.readVarInt(slice);
            final int override = this.channelMapping.get(packetId);
            if (override == Integer.MAX_VALUE) {
                if (!suppressWarning) {
                    if (this.unknownPacketIds.add(packetId)) {
                        System.err.println("Raknetify: Unknown packet id %d for %s".formatted(packetId, descriptiveProtocolStatus));
                    }
                }
                return OverrideResult.route(7);
            }
            return OverrideResult.route(override);
        }
    }

    private record PendingWrite(ByteBuf packet, ChannelPromise promise) {
    }

    private record PendingControlWrite(Object message, ChannelPromise promise) {
    }

}
