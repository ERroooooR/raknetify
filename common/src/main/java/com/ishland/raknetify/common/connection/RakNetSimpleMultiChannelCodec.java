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

import java.util.List;

public class RakNetSimpleMultiChannelCodec extends ChannelDuplexHandler {

    public static final String NAME = "raknetify-simple-multi-channel-data-codec";

    public static final Object SIGNAL_START_MULTICHANNEL = new Object();

    private final int packetId;
    private final OutboundGameplayEpochGate outboundGameplayGate;
    private final OutboundAtomicBundleController outboundBundleController;
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
        this.outboundGameplayGate = new OutboundGameplayEpochGate(
                maxPendingWrites,
                maxPendingWriteBytes
        );
        this.outboundBundleController = new OutboundAtomicBundleController(
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
    private int outboundBulkSequence;
    private Object replayingPendingBundleMessage;
    private ChannelPromise replayingPendingBundlePromise;

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        final IllegalStateException cause = new IllegalStateException("Channel closed");
        outboundGameplayGate.close(ctx, cause);
        outboundBundleController.abort(ctx, cause);
        inboundEpochGate.close(ctx);
        domainFrameScheduler.fail(ctx, cause);
        capabilityNegotiator.clear(ctx.channel());
        super.handlerRemoved(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (outboundGameplayGate.isHolding()) {
            final CorruptedFrameException overflow =
                    outboundGameplayGate.hold(ctx, msg, promise);
            if (overflow != null) {
                failAllCausalWrites(ctx, overflow);
                ctx.fireExceptionCaught(overflow);
                ctx.close();
            }
            return;
        }

        if (!(msg == replayingPendingBundleMessage
                && promise == replayingPendingBundlePromise)
                && !outboundBundleController.isOpen()
                && outboundBundleController.hasPendingWrites()) {
            final CorruptedFrameException overflow =
                    outboundBundleController.holdBehindBundleBarrier(
                            ctx,
                            msg,
                            promise
                    );
            if (overflow != null) {
                failAllCausalWrites(ctx, overflow);
                ctx.fireExceptionCaught(overflow);
                ctx.close();
            }
            return;
        }

        if (outboundBundleController.isOpen()
                && (msg == SIGNAL_START_MULTICHANNEL || msg == SynchronizationLayer.SYNC_REQUEST_OBJECT)) {
            final CorruptedFrameException overflow =
                    outboundBundleController.holdBehindBundleBarrier(
                            ctx,
                            msg,
                            promise
                    );
            if (overflow != null) {
                failAllCausalWrites(ctx, overflow);
                ctx.fireExceptionCaught(overflow);
                ctx.close();
            }
            return;
        }

        if (msg == SIGNAL_START_MULTICHANNEL) {
            if (this.isMultichannelEnabled) {
                promise.trySuccess();
                return;
            }
            if (!this.isMultichannelAvailable()) {
                System.out.println("Raknetify: [MultiChannellingDataCodec] Failed to start multichannel: not available");
                promise.trySuccess();
                return;
            }
            ByteBuf buf = null;
            OutboundGameplayEpochGate.Hold hold = null;
            FrameData frameData = null;
            try {
                capabilityNegotiator.sendAdvertisement(ctx, false);
                buf = ctx.alloc().buffer(1).writeByte(0);
                hold = outboundGameplayGate.beginHold();
                final OutboundGameplayEpochGate.Hold startHold = hold;
                frameData = FrameData.create(
                        ctx.alloc(),
                        Constants.RAKNET_PING_PACKET_ID,
                        buf
                );
                frameData.setOrderChannel(7);
                final ChannelFuture startFuture = ctx.write(frameData);
                frameData = null;
                startFuture.addListener(future -> {
                    if (future.isSuccess()) {
                        if (!outboundGameplayGate.open(startHold)) {
                            promise.tryFailure(new IllegalStateException(
                                    "Multichannel start barrier completed after its gate was closed"
                            ));
                            return;
                        }
                        isMultichannelEnabled = true;
                        promise.trySuccess();
                        if (Constants.DEBUG)
                            System.out.println("Raknetify: [MultiChannellingDataCodec] Started multichannel");
                        replayPendingGameplayWrites(ctx);
                    } else {
                        final Throwable cause = CausalFutureUtil.failureCause(
                                future,
                                "Multichannel start barrier write"
                        );
                        promise.tryFailure(cause);
                        outboundGameplayGate.fail(ctx, startHold, cause);
                        failAllCausalWrites(ctx, cause);
                        ctx.fireExceptionCaught(cause);
                        ctx.close();
                    }
                });
            } catch (Throwable throwable) {
                if (hold != null) {
                    outboundGameplayGate.fail(ctx, hold, throwable);
                }
                promise.tryFailure(throwable);
                failAllCausalWrites(ctx, throwable);
                ctx.fireExceptionCaught(throwable);
                ctx.close();
            } finally {
                ReferenceCountUtil.safeRelease(buf);
                ReferenceCountUtil.safeRelease(frameData);
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
                    final OutboundGameplayEpochGate.Hold hold =
                            outboundGameplayGate.beginHold();
                    promise.addListener(future -> {
                        if (!future.isSuccess()) {
                            final Throwable cause = CausalFutureUtil.failureCause(
                                    future,
                                    "Causal fence write"
                            );
                            outboundGameplayGate.fail(ctx, hold, cause);
                            failAllCausalWrites(ctx, cause);
                            ctx.fireExceptionCaught(cause);
                            ctx.close();
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
        boolean bundlePath = false;
        FrameData frameData = null;
        try {
            final OverrideResult decision = getOverrideResult(buf, !isMultichannelEnabled);
            if (isAtomicBundleEnabled()
                    && (outboundBundleController.isOpen()
                    || decision.isBundleDelimiter())) {
                bundlePath = true;
                final OutboundAtomicBundleController.CompletedBundle completedBundle =
                        outboundBundleController.accept(
                                ctx,
                                buf,
                                promise,
                                decision.isBundleDelimiter(),
                                outboundGameplayGate.currentEpoch(),
                                isGameplayEpochFramingEnabled(),
                                isGuardedBulkWatermarkEnabled(),
                                outboundBulkSequence
                        );
                if (completedBundle != null) {
                    writeAtomicBundle(ctx, completedBundle);
                }
                return;
            }

            frameData = encode0(ctx, buf, decision);
            if (frameData != null) {
                if (isGuardedBulkWatermarkEnabled()
                        && decision.domain()
                        == DependencyDomain.GUARDED_BULK) {
                    promise.addListener(future -> {
                        if (!future.isSuccess() && ctx.channel().isOpen()) {
                            final Throwable cause =
                                    CausalFutureUtil.failureCause(
                                            future,
                                            "Guarded-bulk dependency write"
                                    );
                            failAllCausalWrites(ctx, cause);
                            ctx.fireExceptionCaught(cause);
                            ctx.close();
                        }
                    });
                }
                writeFrame(ctx, frameData, promise, decision.domain());
                frameData = null;
            } else {
                promise.trySuccess();
            }
        } catch (Throwable throwable) {
            final boolean abortConnection =
                    bundlePath || outboundBundleController.isOpen();
            outboundBundleController.abort(ctx, throwable);
            promise.tryFailure(throwable);
            ctx.fireExceptionCaught(throwable);
            if (abortConnection) {
                outboundGameplayGate.close(ctx, throwable);
                ctx.close();
            }
        } finally {
            ReferenceCountUtil.safeRelease(frameData);
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
                            isDependencyDomainsEnabled(),
                            isGuardedBulkWatermarkEnabled()
                    );
            final boolean guardedBulkFrame =
                    isGuardedBulkWatermarkEnabled()
                            && decision.domain()
                            == DependencyDomain.GUARDED_BULK;
            final int dependencySequence;
            if (guardedBulkFrame) {
                if (outboundBulkSequence == Integer.MAX_VALUE) {
                    throw new CorruptedFrameException(
                            "Guarded-bulk sequence exhausted"
                    );
                }
                dependencySequence = outboundBulkSequence + 1;
            } else {
                dependencySequence = outboundBulkSequence;
            }
            ByteBuf gameplayPayload = null;
            final FrameData frameData;
            try {
                gameplayPayload = isGuardedBulkWatermarkEnabled()
                        && (decision.domain() == DependencyDomain.STRICT_WORLD
                        || guardedBulkFrame)
                        ? CausalTransportProtocol.encodeDependencyGameplayFrame(
                                ctx.alloc(),
                                outboundGameplayGate.currentEpoch(),
                                guardedBulkFrame
                                        ? CausalTransportProtocol.DependencyKind.GUARDED_BULK
                                        : CausalTransportProtocol.DependencyKind.STRICT,
                                dependencySequence,
                                buf
                        )
                        : isGameplayEpochFramingEnabled()
                        ? CausalTransportProtocol.encodeGameplayFrame(
                                ctx.alloc(),
                                outboundGameplayGate.currentEpoch(),
                                buf
                        )
                        : buf.retain();
                frameData = FrameData.create(ctx.alloc(), packetId, gameplayPayload);
            } finally {
                ReferenceCountUtil.safeRelease(gameplayPayload);
            }
            if (guardedBulkFrame) {
                outboundBulkSequence = dependencySequence;
                final SimpleMetricsLogger metrics = metrics(ctx);
                if (metrics != null) {
                    metrics.causalBulkFrameOutbound(buf.readableBytes());
                }
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
            OutboundAtomicBundleController.CompletedBundle completedBundle
    ) {
        final ByteBuf envelope = completedBundle.payload();
        final List<ChannelPromise> promises = completedBundle.promises();
        FrameData frameData = null;
        try {
            frameData = FrameData.create(ctx.alloc(), packetId, envelope);
            frameData.setOrderChannel(7);

            final ChannelPromise envelopePromise = ctx.newPromise();
            envelopePromise.addListener(future -> {
                if (future.isSuccess()) {
                    for (ChannelPromise promise : promises) {
                        promise.trySuccess();
                    }
                    replayPendingBundleControls(ctx);
                    return;
                }

                final Throwable failure = CausalFutureUtil.failureCause(
                        future,
                        "Atomic bundle envelope write"
                );
                for (ChannelPromise promise : promises) {
                    promise.tryFailure(failure);
                }
                outboundBundleController.abort(ctx, failure);
                outboundGameplayGate.close(ctx, failure);
                ctx.fireExceptionCaught(failure);
                ctx.close();
            });
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalAtomicBundleOutbound(
                        promises.size(),
                        envelope.readableBytes()
                );
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
            outboundBundleController.abort(ctx, throwable);
            outboundGameplayGate.close(ctx, throwable);
            ctx.fireExceptionCaught(throwable);
            ctx.close();
        } finally {
            ReferenceCountUtil.safeRelease(envelope);
            ReferenceCountUtil.safeRelease(frameData);
        }
    }

    private void failAllCausalWrites(
            ChannelHandlerContext ctx,
            Throwable cause
    ) {
        outboundBundleController.abort(ctx, cause);
        outboundGameplayGate.close(ctx, cause);
    }

    private void replayPendingBundleControls(ChannelHandlerContext ctx) {
        final Throwable replayFailure =
                outboundBundleController.replayBarrierWrites(
                        ctx,
                        (message, promise) -> {
                            replayingPendingBundleMessage = message;
                            replayingPendingBundlePromise = promise;
                            try {
                                write(ctx, message, promise);
                            } finally {
                                replayingPendingBundleMessage = null;
                                replayingPendingBundlePromise = null;
                            }
                        }
                );
        if (replayFailure != null) {
            failAllCausalWrites(ctx, replayFailure);
            ctx.fireExceptionCaught(replayFailure);
            ctx.close();
        }
    }

    private void replayPendingGameplayWrites(ChannelHandlerContext ctx) {
        final Throwable replayFailure =
                outboundGameplayGate.replay(
                        ctx,
                        (message, promise) -> write(ctx, message, promise)
                );
        if (replayFailure != null) {
            failAllCausalWrites(ctx, replayFailure);
            ctx.fireExceptionCaught(replayFailure);
            ctx.close();
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

    public boolean isGuardedBulkWatermarkEnabled() {
        return isDependencyDomainsEnabled()
                && capabilityNegotiator.hasOutbound(
                        CausalTransportProtocol
                                .CAPABILITY_GUARDED_BULK_WATERMARK
                );
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
                && (isMultichannelEnabled
                || outboundGameplayGate.currentEpoch() > 0);
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
        domainFrameScheduler.drainAvailable(ctx);
        super.flush(ctx);
        domainFrameScheduler.resume(ctx);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
            throws Exception {
        domainFrameScheduler.resume(ctx);
        super.channelWritabilityChanged(ctx);
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

        final DependencyDomain schedulingDomain =
                domain == DependencyDomain.GUARDED_BULK
                        && !isGuardedBulkWatermarkEnabled()
                        ? DependencyDomain.STRICT_WORLD
                        : domain;
        frameData.setPriority(schedulingDomain.transportPriority());
        domainFrameScheduler.schedule(
                ctx,
                domain,
                schedulingDomain,
                frameData,
                promise
        );
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
                        if (CausalTransportProtocol.isDependencyGameplayFrame(
                                payload
                        ) && !capabilityNegotiator.hasInbound(
                                CausalTransportProtocol
                                        .CAPABILITY_GUARDED_BULK_WATERMARK
                        )) {
                            payload.release();
                            throw new CorruptedFrameException(
                                    "Guarded-bulk dependency frame arrived without negotiation"
                            );
                        }
                        if (CausalTransportProtocol.isEpochAtomicBundle(payload)
                                && (!packet.getReliability().isReliable
                                || !packet.getReliability().isOrdered
                                || packet.getOrderChannel() != 7)) {
                            payload.release();
                            throw new CorruptedFrameException(
                                    "Atomic bundle was not delivered on reliable ordered channel 7"
                            );
                        }
                        inboundEpochGate.handle(
                                ctx,
                                payload,
                                packet.getReliability().isReliable
                                        && packet.getReliability().isOrdered
                                        && !packet.getReliability().isSequenced,
                                packet.getOrderChannel()
                        );
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
            outboundGameplayGate.advance(advanced.epoch());
            // A restart signal is a barrier for the new epoch. Process it
            // before releasing application packets so those packets cannot be
            // encoded on the pre-start/default order channel.
            replayPendingBundleControls(ctx);
            if (!outboundGameplayGate.isHolding()) {
                replayPendingGameplayWrites(ctx);
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
