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
import com.ishland.raknetify.common.connection.multichannel.DependencyDomain;
import com.ishland.raknetify.common.connection.multichannel.MultichannelPolicy;
import com.ishland.raknetify.common.util.MathUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
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

import java.util.List;

public class RakNetSimpleMultiChannelCodec extends ChannelDuplexHandler {

    public static final String NAME = "raknetify-simple-multi-channel-data-codec";

    public static final Object SIGNAL_START_MULTICHANNEL = new Object();

    private final int packetId;
    private final BoundedPendingWriteQueue pendingWrites;
    private final BoundedPendingWriteQueue pendingControlWrites;
    private final DependencyDomainFrameScheduler domainFrameScheduler;
    private final CausalCapabilityNegotiator capabilityNegotiator =
            new CausalCapabilityNegotiator();
    private final InboundGameplayEpochGate inboundEpochGate =
            new InboundGameplayEpochGate();

    public RakNetSimpleMultiChannelCodec(int packetId) {
        this(
                packetId,
                CausalTransportProtocol.MAX_PENDING_CAUSAL_WRITES,
                Constants.MAX_QUEUED_SIZE
        );
    }

    RakNetSimpleMultiChannelCodec(
            int packetId,
            int maxPendingWrites,
            int maxPendingWriteBytes
    ) {
        if (maxPendingWrites <= 0 || maxPendingWriteBytes <= 0) {
            throw new IllegalArgumentException("Pending causal queue limits must be positive");
        }
        this.packetId = packetId;
        this.pendingWrites = new BoundedPendingWriteQueue(
                maxPendingWrites,
                maxPendingWriteBytes
        );
        this.pendingControlWrites = new BoundedPendingWriteQueue(
                maxPendingWrites,
                maxPendingWriteBytes
        );
        this.domainFrameScheduler = new DependencyDomainFrameScheduler(
                maxPendingWrites,
                maxPendingWriteBytes
        );
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
    private boolean waitingForEpochFence;
    private int outboundEpoch;

    private boolean queuePendingWrites = false;
    private final AtomicBundleAssembler atomicBundleAssembler = new AtomicBundleAssembler();
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        final IllegalStateException cause = new IllegalStateException("Channel closed");
        failPendingWrites(ctx, cause);
        atomicBundleAssembler.abort(cause);
        failPendingControlWrites(ctx, cause);
        inboundEpochGate.close(ctx);
        domainFrameScheduler.fail(ctx, cause);
        capabilityNegotiator.clear(ctx.channel());
        super.handlerRemoved(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (this.queuePendingWrites) {
            queuePendingWrite(ctx, msg, promise);
            return;
        }

        if (atomicBundleAssembler.isOpen()
                && (msg == SIGNAL_START_MULTICHANNEL || msg == SynchronizationLayer.SYNC_REQUEST_OBJECT)) {
            queuePendingControlWrite(ctx, msg, promise);
            return;
        }

        if (msg == SIGNAL_START_MULTICHANNEL) {
            promise.trySuccess();
            if (this.isMultichannelEnabled) return;
            if (!this.isMultichannelAvailable()) {
                System.out.println("Raknetify: [MultiChannellingDataCodec] Failed to start multichannel: not available");
                return;
            }
            capabilityNegotiator.sendAdvertisement(ctx, false);
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
                        failPendingWrites(ctx, future.cause());
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
                // The dependency-domain scheduler may still contain writes
                // from this event-loop turn. They causally precede the fence
                // request and must enter FrameOrderOut before its markers.
                domainFrameScheduler.drain(ctx);
                this.isMultichannelEnabled = false;
                if (isOutboundGameplayEpochNegotiated()) {
                    waitingForEpochFence = true;
                    queuePendingWrites = true;
                    promise.addListener(future -> {
                        if (!future.isSuccess()) {
                            waitingForEpochFence = false;
                            queuePendingWrites = false;
                            failPendingWrites(ctx, future.cause());
                        }
                    });
                }
                super.write(ctx, msg, promise);
                return;
            }
            promise.trySuccess();
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
                                decision.isBundleDelimiter(),
                                outboundEpoch,
                                isGameplayEpochFramingEnabled()
                        );
                if (completedBundle != null) {
                    writeAtomicBundle(ctx, completedBundle);
                }
                return;
            }

            final FrameData frameData = encode0(ctx, buf, decision);
            if (frameData != null) {
                writeFrame(ctx, frameData, promise, decision.domain());
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
            final int packetChannelOverride = decision.isBundleDelimiter()
                    ? MultichannelPolicy.STRICT_GAME_CHANNEL
                    : MultichannelPolicy.selectChannel(
                            MultichannelPolicy.configuredProfile(),
                            decision.domain(),
                            decision.channel(),
                            isDependencyDomainsEnabled()
                    );
            ByteBuf gameplayPayload = null;
            final FrameData frameData;
            try {
                gameplayPayload = isGameplayEpochFramingEnabled()
                        ? CausalTransportProtocol.encodeGameplayFrame(ctx.alloc(), outboundEpoch, buf)
                        : buf.retain();
                frameData = FrameData.create(ctx.alloc(), packetId, gameplayPayload);
            } finally {
                ReferenceCountUtil.safeRelease(gameplayPayload);
            }
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
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalAtomicBundleOutbound(promises.size());
            }
            writeFrame(
                    ctx,
                    frameData,
                    envelopePromise,
                    DependencyDomain.STRICT_WORLD
            );
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
        BoundedPendingWriteQueue.PendingWrite pendingControlWrite;
        while ((pendingControlWrite = pendingControlWrites.poll()) != null) {
            recordPendingWriteQueueState(
                    ctx,
                    pendingControlWrites,
                    SimpleMetricsLogger.CausalOutboundQueue.BUNDLE_CONTROL
            );
            try {
                write(
                        ctx,
                        pendingControlWrite.message(),
                        pendingControlWrite.promise()
                );
            } catch (Throwable throwable) {
                pendingControlWrite.promise().tryFailure(throwable);
                ReferenceCountUtil.safeRelease(pendingControlWrite.message());
                failAllCausalWrites(ctx, throwable);
                ctx.fireExceptionCaught(throwable);
                ctx.close();
                return;
            }
        }
    }

    private void flushPendingWrites(ChannelHandlerContext ctx) {
        this.queuePendingWrites = false;
        BoundedPendingWriteQueue.PendingWrite pendingWrite;
        boolean replayedWrite = false;
        while (!this.queuePendingWrites
                && (pendingWrite = this.pendingWrites.poll()) != null) {
            recordPendingWriteQueueState(
                    ctx,
                    pendingWrites,
                    SimpleMetricsLogger.CausalOutboundQueue.APPLICATION
            );
            try {
                write(ctx, pendingWrite.message(), pendingWrite.promise());
                replayedWrite = true;
            } catch (Throwable t) {
                pendingWrite.promise().tryFailure(t);
                ReferenceCountUtil.safeRelease(pendingWrite.message());
                failAllCausalWrites(ctx, t);
                ctx.fireExceptionCaught(t);
                ctx.close();
                return;
            }
        }
        if (replayedWrite) {
            ctx.flush();
        }
    }

    private void failPendingWrites(ChannelHandlerContext ctx, Throwable cause) {
        pendingWrites.failAll(cause);
        recordPendingWriteQueueState(
                ctx,
                pendingWrites,
                SimpleMetricsLogger.CausalOutboundQueue.APPLICATION
        );
    }

    private void failPendingControlWrites(
            ChannelHandlerContext ctx,
            Throwable cause
    ) {
        pendingControlWrites.failAll(cause);
        recordPendingWriteQueueState(
                ctx,
                pendingControlWrites,
                SimpleMetricsLogger.CausalOutboundQueue.BUNDLE_CONTROL
        );
    }

    private void queuePendingWrite(
            ChannelHandlerContext ctx,
            Object message,
            ChannelPromise promise
    ) {
        if (!pendingWrites.tryAdd(message, promise)) {
            failPendingQueueOverflow(
                    ctx,
                    pendingWrites,
                    SimpleMetricsLogger.CausalOutboundQueue.APPLICATION,
                    "application",
                    message,
                    promise
            );
            return;
        }
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalOutboundFrameQueued(
                    SimpleMetricsLogger.CausalOutboundQueue.APPLICATION,
                    pendingWrites.size(),
                    pendingWrites.bytes()
            );
        }
    }

    private void queuePendingControlWrite(
            ChannelHandlerContext ctx,
            Object message,
            ChannelPromise promise
    ) {
        if (!pendingControlWrites.tryAdd(message, promise)) {
            failPendingQueueOverflow(
                    ctx,
                    pendingControlWrites,
                    SimpleMetricsLogger.CausalOutboundQueue.BUNDLE_CONTROL,
                    "bundle control",
                    message,
                    promise
            );
            return;
        }
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalOutboundFrameQueued(
                    SimpleMetricsLogger.CausalOutboundQueue.BUNDLE_CONTROL,
                    pendingControlWrites.size(),
                    pendingControlWrites.bytes()
            );
        }
    }

    private void failPendingQueueOverflow(
            ChannelHandlerContext ctx,
            BoundedPendingWriteQueue queue,
            SimpleMetricsLogger.CausalOutboundQueue queueType,
            String description,
            Object message,
            ChannelPromise promise
    ) {
        final CorruptedFrameException exception = new CorruptedFrameException(
                "Pending causal " + description + " queue exceeded its bound "
                        + "(frames=" + queue.size()
                        + ", bytes=" + queue.bytes() + ")"
        );
        promise.tryFailure(exception);
        ReferenceCountUtil.safeRelease(message);
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalOutboundQueueOverflow(queueType);
        }
        failAllCausalWrites(ctx, exception);
        ctx.fireExceptionCaught(exception);
        ctx.close();
    }

    private void failAllCausalWrites(
            ChannelHandlerContext ctx,
            Throwable cause
    ) {
        queuePendingWrites = false;
        waitingForEpochFence = false;
        atomicBundleAssembler.abort(cause);
        failPendingControlWrites(ctx, cause);
        failPendingWrites(ctx, cause);
    }

    private void recordPendingWriteQueueState(
            ChannelHandlerContext ctx,
            BoundedPendingWriteQueue queue,
            SimpleMetricsLogger.CausalOutboundQueue queueType
    ) {
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalOutboundQueueState(
                    queueType,
                    queue.size(),
                    queue.bytes()
            );
        }
    }

    public boolean isAtomicBundleEnabled() {
        return isMultichannelEnabled && isOutboundAtomicBundleNegotiated();
    }

    public boolean isDependencyDomainsEnabled() {
        final long required = CausalTransportProtocol.CAPABILITY_ATOMIC_BUNDLE
                | CausalTransportProtocol.CAPABILITY_LOSSLESS_FENCE
                | CausalTransportProtocol.CAPABILITY_GAMEPLAY_EPOCH;
        return isMultichannelEnabled
                && capabilityNegotiator.hasAllOutbound(required);
    }

    private boolean isOutboundAtomicBundleNegotiated() {
        return capabilityNegotiator.hasOutbound(
                CausalTransportProtocol.CAPABILITY_ATOMIC_BUNDLE
        );
    }

    private boolean isInboundAtomicBundleNegotiated() {
        return capabilityNegotiator.hasInbound(
                CausalTransportProtocol.CAPABILITY_ATOMIC_BUNDLE
        );
    }

    private boolean isOutboundGameplayEpochNegotiated() {
        return capabilityNegotiator.hasOutbound(
                CausalTransportProtocol.CAPABILITY_GAMEPLAY_EPOCH
        );
    }

    private boolean isInboundGameplayEpochNegotiated() {
        return capabilityNegotiator.hasInbound(
                CausalTransportProtocol.CAPABILITY_GAMEPLAY_EPOCH
        );
    }

    private boolean isGameplayEpochFramingEnabled() {
        return isOutboundGameplayEpochNegotiated()
                && (isMultichannelEnabled || outboundEpoch > 0);
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
                // Strict is a safety veto, not an ordinary route preference.
                // This makes custom payload and unknown-packet handling robust
                // against platform-specific handler registration order.
                if (override.isStrict()) {
                    return override;
                }
                if (!firstMatch.matched() && override.matched()) {
                    firstMatch = override;
                }
            }
        }
        return firstMatch.matched() ? firstMatch : OverrideResult.strict();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        domainFrameScheduler.drain(ctx);
        super.flush(ctx);
    }

    private void writeFrame(
            ChannelHandlerContext ctx,
            FrameData frameData,
            ChannelPromise promise,
            DependencyDomain domain
    ) {
        if (!isDependencyDomainsEnabled()) {
            ctx.write(frameData, promise);
            return;
        }

        domainFrameScheduler.schedule(ctx, domain, frameData, promise);
    }

    private static SimpleMetricsLogger metrics(ChannelHandlerContext ctx) {
        if (ctx.channel().config() instanceof RakNet.Config config
                && config.getMetrics() instanceof SimpleMetricsLogger logger) {
            return logger;
        }
        return null;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FrameData packet && !packet.isFragment() && packet.getDataSize() > 0) {
            try {
                if (packetId == packet.getPacketId()) {
                    final ByteBuf payload = packet.createData().skipBytes(1);
                    if (isInboundGameplayEpochNegotiated()
                            && CausalTransportProtocol.isEpochGameplayFrame(payload)) {
                        if (CausalTransportProtocol.isEpochAtomicBundle(payload)
                                && (!packet.getReliability().isReliable
                                || !packet.getReliability().isOrdered
                                || packet.getOrderChannel() != 7)) {
                            payload.release();
                            throw new CorruptedFrameException(
                                    "Atomic bundle was not delivered on reliable ordered channel 7"
                            );
                        }
                        inboundEpochGate.handle(ctx, payload);
                    } else if (isInboundGameplayEpochNegotiated()
                            && inboundEpochGate.currentEpoch() > 0) {
                        // A completed drain fence proves that no unframed packet
                        // from the previous epoch may still be valid.
                        final SimpleMetricsLogger metrics = metrics(ctx);
                        if (metrics != null) {
                            metrics.causalStaleFrameDropped();
                        }
                        payload.release();
                    } else if (isInboundAtomicBundleNegotiated()
                            && CausalTransportProtocol.isAtomicBundle(payload)) {
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
                    capabilityNegotiator.handleControl(ctx, packet);
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

    private void fireAtomicBundle(ChannelHandlerContext ctx, ByteBuf payload) {
        final List<ByteBuf> packets;
        try {
            packets = CausalTransportProtocol.decodeAtomicBundle(payload);
        } finally {
            payload.release();
        }
        final SimpleMetricsLogger metrics = metrics(ctx);
        if (metrics != null) {
            metrics.causalAtomicBundleInbound(packets.size());
        }
        InboundGameplayEpochGate.firePackets(ctx, packets);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SynchronizationLayer.InboundEpochAdvanced advanced) {
            inboundEpochGate.advance(ctx, advanced.epoch());
        } else if (evt instanceof SynchronizationLayer.OutboundEpochAdvanced advanced) {
            if (advanced.epoch() != outboundEpoch + 1) {
                throw new CorruptedFrameException("Outbound gameplay epoch skipped from "
                        + outboundEpoch + " to " + advanced.epoch());
            }
            outboundEpoch = advanced.epoch();
            waitingForEpochFence = false;
            queuePendingWrites = false;
            // A restart signal is a barrier for the new epoch. Process it
            // before releasing application packets so those packets cannot be
            // encoded on the pre-start/default order channel.
            flushPendingControlWrites(ctx);
            if (!queuePendingWrites) {
                flushPendingWrites(ctx);
            }
        }
        super.userEventTriggered(ctx, evt);
    }

    public interface OverrideHandler {
        OverrideResult getChannelOverride(ByteBuf buf, boolean suppressWarning);
    }

    /**
     * Separates an explicit channel-zero decision from a handler that did not
     * recognize the packet, and identifies bundle delimiters as protocol
     * boundaries rather than disposable packets.
     */
    public record OverrideResult(
            boolean matched,
            int channel,
            DependencyDomain domain
    ) {

        private static final OverrideResult PASS =
                new OverrideResult(false, 0, DependencyDomain.STRICT_WORLD);
        private static final OverrideResult STRICT =
                new OverrideResult(
                        true,
                        MultichannelPolicy.STRICT_GAME_CHANNEL,
                        DependencyDomain.STRICT_WORLD
                );
        private static final OverrideResult BUNDLE_DELIMITER =
                new OverrideResult(
                        true,
                        Integer.MIN_VALUE,
                        DependencyDomain.STRICT_WORLD
                );
        private static final OverrideResult[] ORDERED = new OverrideResult[]{
                legacy(0),
                legacy(1),
                legacy(2),
                legacy(3),
                legacy(4),
                legacy(5),
                legacy(6),
                legacy(7)
        };

        public static OverrideResult pass() {
            return PASS;
        }

        public static OverrideResult bundleDelimiter() {
            return BUNDLE_DELIMITER;
        }

        public static OverrideResult strict() {
            return STRICT;
        }

        public static OverrideResult classify(
                DependencyDomain domain,
                int aggressiveChannel
        ) {
            return new OverrideResult(true, aggressiveChannel, domain);
        }

        public boolean isBundleDelimiter() {
            return this == BUNDLE_DELIMITER;
        }

        public boolean isStrict() {
            return this == STRICT;
        }

        public static OverrideResult route(int channel) {
            if (channel >= 0 && channel < ORDERED.length) return ORDERED[channel];
            if (channel == Integer.MIN_VALUE) return BUNDLE_DELIMITER;
            return legacy(channel);
        }

        private static OverrideResult legacy(int channel) {
            return new OverrideResult(
                    true,
                    channel,
                    DependencyDomain.fromLegacyChannel(channel)
            );
        }
    }

    public static class PacketIdBasedOverrideHandler implements OverrideHandler {

        private final IntOpenHashSet unknownPacketIds = new IntOpenHashSet();
        private final Int2IntOpenHashMap channelMapping;
        private final String descriptiveProtocolStatus;

        public PacketIdBasedOverrideHandler(Int2IntMap channelMapping, String descriptiveProtocolStatus) {
            this.channelMapping = new Int2IntOpenHashMap(channelMapping);
            // Fastutil copy constructors do not preserve the source map's
            // default return value. Zero is a valid order channel, so leaving
            // the copied map at its implicit zero default silently classifies
            // every absent packet ID as channel 0.
            this.channelMapping.defaultReturnValue(Integer.MAX_VALUE);
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
                return OverrideResult.strict();
            }
            return OverrideResult.route(override);
        }
    }

}
