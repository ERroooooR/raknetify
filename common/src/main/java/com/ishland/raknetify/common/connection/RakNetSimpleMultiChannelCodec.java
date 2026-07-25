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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
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

    private boolean queuePendingWrites = false;
    private final Queue<PendingWrite> pendingWrites = new LinkedList<>();

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        for (PendingWrite pendingWrite : pendingWrites) {
            pendingWrite.promise.setFailure(new IllegalStateException("Channel closed"));
            pendingWrite.frameData.release();
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (this.queuePendingWrites && msg instanceof ByteBuf buf) {
            final FrameData data = encode0(ctx, buf);
            if (data != null) {
                pendingWrites.add(new PendingWrite(data, promise));
            } else {
                promise.setSuccess();
            }
            buf.release();
            return;
        }

        if (msg == SIGNAL_START_MULTICHANNEL) {
            promise.trySuccess();
            if (this.isMultichannelEnabled) return;
            if (!this.isMultichannelAvailable()) {
                System.out.println("Raknetify: [MultiChannellingDataCodec] Failed to start multichannel: not available");
                return;
            }
            final ByteBuf buf = ctx.alloc().buffer(1).writeByte(0);
            try {
                final FrameData frameData = FrameData.create(ctx.alloc(), Constants.RAKNET_PING_PACKET_ID, buf);
                frameData.setOrderChannel(7);
                this.queuePendingWrites = true;
                ctx.write(frameData).addListener(future -> {
                    isMultichannelEnabled = true;
                    if (Constants.DEBUG) System.out.println("Raknetify: [MultiChannellingDataCodec] Started multichannel");
                    flushPendingWrites(ctx);
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
            try {
                final FrameData frameData = encode0(ctx, buf);
                if (frameData != null) {
                    ctx.write(frameData, promise);
                } else {
                    promise.setSuccess();
                }
            } finally {
                buf.release();
            }
            return;
        }

        super.write(ctx, msg, promise);
    }

    private FrameData encode0(ChannelHandlerContext ctx, ByteBuf buf) {
        if (buf.isReadable()) {
            if (ctx.pipeline().get("zstd_encoder") != null
                    && ctx.channel().config() instanceof RakNet.Config config) {
                config.getMetrics().applicationBatch(buf.readableBytes());
            }
            final int packetChannelOverride = getChannelOverride(buf, !isMultichannelEnabled);
            if (packetChannelOverride == Integer.MIN_VALUE) {
                return null; // the void
            }
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

    private void flushPendingWrites(ChannelHandlerContext ctx) {
        this.queuePendingWrites = false;
        PendingWrite pendingWrite;
        while ((pendingWrite = this.pendingWrites.poll()) != null) {
            try {
                super.write(ctx, pendingWrite.frameData, pendingWrite.promise);
            } catch (Throwable t) {
                ctx.fireExceptionCaught(t);
            }
        }
    }

    protected boolean isMultichannelAvailable() {
        synchronized (handlers) {
            return !handlers.isEmpty();
        }
    }

    protected int getChannelOverride(ByteBuf buf, boolean suppressWarning) {
        synchronized (handlers) {
            for (OverrideHandler handler : handlers) {
                final OverrideResult override = handler.getChannelOverride(buf, suppressWarning);
                if (override.matched()) return override.channel();
            }
        }
        return 0;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FrameData packet && !packet.isFragment() && packet.getDataSize() > 0) {
            try {
                if (packetId == packet.getPacketId()) {
                    ctx.fireChannelRead(packet.createData().skipBytes(1));
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

    protected void decode(ChannelHandlerContext ctx, FrameData packet, List<Object> out) {
        assert !packet.isFragment();
        if (packet.getDataSize() > 0) {
            if (packetId == packet.getPacketId()) {
                out.add(packet.createData().skipBytes(1));
            } else if (packet.getPacketId() == Constants.RAKNET_PING_PACKET_ID) {
                return;
            } else {
                out.add(packet.retain());
            }
        }
    }

    public interface OverrideHandler {
        OverrideResult getChannelOverride(ByteBuf buf, boolean suppressWarning);
    }

    /**
     * Separates an explicit channel-zero decision from a handler that did not
     * recognize the packet. The old integer API used zero for both meanings,
     * which made conservative fallback policies dependent on handler order.
     */
    public record OverrideResult(boolean matched, int channel) {

        private static final OverrideResult PASS = new OverrideResult(false, 0);
        private static final OverrideResult DISCARD = new OverrideResult(true, Integer.MIN_VALUE);
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

        public static OverrideResult route(int channel) {
            if (channel >= 0 && channel < ORDERED.length) return ORDERED[channel];
            if (channel == Integer.MIN_VALUE) return DISCARD;
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

    private record PendingWrite(FrameData frameData, ChannelPromise promise) {
    }

}
