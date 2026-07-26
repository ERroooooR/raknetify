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
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.RakNet;

/**
 * Lossless cross-channel drain fence.
 *
 * <p>A request marker is appended to every active ordered channel. The peer
 * acknowledges only after every marker reaches this handler, proving that all
 * earlier ordered frames and their fragments were delivered. Later writes stay
 * queued until that acknowledgement. No reliability queue, order index or
 * packet promise is deleted or rewritten.</p>
 */
public class SynchronizationLayer extends ChannelDuplexHandler {

    public static final Object SYNC_REQUEST_OBJECT = new Object();

    private final int activeChannelMask;
    private final OutboundCausalFenceController outboundFenceController;
    private final InboundCausalFenceTracker inboundFenceTracker;

    public SynchronizationLayer(int... channelsToIgnore) {
        this(
                channelsToIgnore,
                CausalTransportProtocol.MAX_PENDING_CAUSAL_WRITES,
                Constants.MAX_QUEUED_SIZE
        );
    }

    SynchronizationLayer(
            int[] channelsToIgnore,
            int maxPendingWrites,
            int maxPendingWriteBytes
    ) {
        this.outboundFenceController = new OutboundCausalFenceController(
                maxPendingWrites,
                maxPendingWriteBytes
        );
        int ignoredChannelMask = 0;
        for (int channel : channelsToIgnore) {
            if (channel < 0 || channel >= CausalFenceProtocol.ORDER_CHANNEL_COUNT) {
                throw new IllegalArgumentException("Invalid ignored order channel: " + channel);
            }
            ignoredChannelMask |= 1 << channel;
        }
        this.activeChannelMask =
                ((1 << CausalFenceProtocol.ORDER_CHANNEL_COUNT) - 1)
                        & ~ignoredChannelMask;
        if (activeChannelMask == 0) {
            throw new IllegalArgumentException(
                    "Causal fence has no active channels"
            );
        }
        this.inboundFenceTracker =
                new InboundCausalFenceTracker(activeChannelMask);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg == SYNC_REQUEST_OBJECT) {
            if (!CausalTransportProtocol.hasOutboundCapability(
                    ctx.channel(),
                    CausalTransportProtocol.CAPABILITY_LOSSLESS_FENCE
            )) {
                // A pre-fence peer cannot acknowledge the new protocol. Preserve
                // every queued frame and let per-channel ordering continue.
                promise.trySuccess();
                return;
            }
            if (outboundFenceController.waitingForAck()) {
                outboundFenceController.hold(ctx, msg, promise);
                return;
            }
            beginFence(ctx, promise);
            return;
        }
        if (outboundFenceController.waitingForAck()) {
            outboundFenceController.hold(ctx, msg, promise);
            return;
        }
        super.write(ctx, msg, promise);
    }

    private void beginFence(ChannelHandlerContext ctx, ChannelPromise promise) {
        final OutboundCausalFenceController.Fence fence =
                outboundFenceController.begin(ctx, promise);
        if (fence == null) {
            return;
        }

        try {
            for (int channel = 0;
                 channel < CausalFenceProtocol.ORDER_CHANNEL_COUNT;
                 channel++) {
                if ((activeChannelMask & 1 << channel) == 0) {
                    continue;
                }
                writeFenceRequest(
                        ctx,
                        fence,
                        activeChannelMask,
                        channel
                );
            }
            ctx.flush();
        } catch (Throwable throwable) {
            outboundFenceController.fail(ctx, fence, throwable);
        }
    }

    private void writeFenceRequest(
            ChannelHandlerContext ctx,
            OutboundCausalFenceController.Fence fence,
            int channelMask,
            int channel
    ) {
        final ByteBuf payload = CausalFenceProtocol.encodeRequest(
                ctx.alloc(),
                fence.id(),
                fence.epoch(),
                channelMask
        );
        FrameData frameData = null;
        try {
            frameData = FrameData.create(ctx.alloc(), Constants.RAKNET_SYNC_PACKET_ID, payload);
            frameData.setOrderChannel(channel);
            final ChannelFuture writeFuture = ctx.write(frameData);
            frameData = null;
            writeFuture.addListener(future -> {
                if (!future.isSuccess()) {
                    outboundFenceController.fail(
                            ctx,
                            fence,
                            CausalFutureUtil.failureCause(
                                    future,
                                    "Causal fence marker write"
                            )
                    );
                }
            });
        } finally {
            payload.release();
            ReferenceCountUtil.safeRelease(frameData);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof FrameData packet)
                || packet.isFragment()
                || packet.getDataSize() <= 0
                || packet.getPacketId() != Constants.RAKNET_SYNC_PACKET_ID) {
            super.channelRead(ctx, msg);
            return;
        }

        try {
            if (!CausalTransportProtocol.hasInboundCapability(
                    ctx.channel(),
                    CausalTransportProtocol.CAPABILITY_LOSSLESS_FENCE
            )) {
                return;
            }
            final ByteBuf payload = packet.createData().skipBytes(1);
            try {
                final CausalFenceProtocol.Message message = CausalFenceProtocol.decode(payload);
                if (message instanceof CausalFenceProtocol.Request request) {
                    handleFenceRequest(ctx, packet, request);
                } else if (message instanceof CausalFenceProtocol.Ack ack) {
                    handleFenceAck(ctx, packet, ack);
                }
            } finally {
                payload.release();
            }
        } finally {
            packet.release();
        }
    }

    private void handleFenceRequest(
            ChannelHandlerContext ctx,
            FrameData packet,
            CausalFenceProtocol.Request request
    ) {
        requireReliableOrdered(packet, "request");
        final int channel = packet.getOrderChannel();
        final InboundCausalFenceTracker.Result result =
                inboundFenceTracker.accept(request, channel);
        if (result.action()
                == InboundCausalFenceTracker.Action.COMMIT_AND_ACKNOWLEDGE) {
            final SimpleMetricsLogger metrics = metrics(ctx);
            if (metrics != null) {
                metrics.causalInboundFenceCompleted(result.epoch());
            }
            ctx.fireUserEventTriggered(new InboundEpochAdvanced(result.epoch()));
            writeFenceAck(ctx, request.fenceId(), result.epoch());
        } else if (result.action()
                == InboundCausalFenceTracker.Action.ACKNOWLEDGE) {
            writeFenceAck(ctx, request.fenceId(), result.epoch());
        }
    }

    private void writeFenceAck(ChannelHandlerContext ctx, long fenceId, int epoch) {
        final ByteBuf payload = CausalFenceProtocol.encodeAck(ctx.alloc(), fenceId, epoch);
        FrameData frameData = null;
        try {
            frameData = FrameData.create(ctx.alloc(), Constants.RAKNET_SYNC_PACKET_ID, payload);
            frameData.setOrderChannel(7);
            final ChannelFuture writeFuture = ctx.writeAndFlush(frameData);
            frameData = null;
            writeFuture.addListener(future -> {
                if (!future.isSuccess()) {
                    final Throwable cause = CausalFutureUtil.failureCause(
                            future,
                            "Causal fence acknowledgement write"
                    );
                    ctx.fireExceptionCaught(cause);
                    ctx.close();
                }
            });
        } finally {
            payload.release();
            ReferenceCountUtil.safeRelease(frameData);
        }
    }

    private void handleFenceAck(
            ChannelHandlerContext ctx,
            FrameData packet,
            CausalFenceProtocol.Ack ack
    ) {
        requireReliableOrdered(packet, "ack");
        if (packet.getOrderChannel() != 7) {
            throw new CorruptedFrameException("Causal fence ACK must use channel 7");
        }
        final OutboundCausalFenceController.Completion completion =
                outboundFenceController.acknowledge(
                        ctx,
                        ack,
                        (message, promise) -> write(ctx, message, promise)
                );
        if (completion == null) {
            return;
        }
        ctx.fireUserEventTriggered(
                new OutboundEpochAdvanced(completion.epoch())
        );
        completion.succeed();
    }

    private static void requireReliableOrdered(FrameData packet, String description) {
        if (!packet.getReliability().isReliable
                || !packet.getReliability().isOrdered
                || packet.getReliability().isSequenced) {
            throw new CorruptedFrameException("Causal fence " + description
                    + " is not reliable ordered");
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        final IllegalStateException cause = new IllegalStateException("Channel closed");
        outboundFenceController.close(ctx, cause);
        inboundFenceTracker.clearActive();
        super.handlerRemoved(ctx);
    }

    private static SimpleMetricsLogger metrics(ChannelHandlerContext ctx) {
        if (ctx.channel().config() instanceof RakNet.Config config
                && config.getMetrics() instanceof SimpleMetricsLogger logger) {
            return logger;
        }
        return null;
    }

    public record InboundEpochAdvanced(int epoch) {
    }

    public record OutboundEpochAdvanced(int epoch) {
    }

}
