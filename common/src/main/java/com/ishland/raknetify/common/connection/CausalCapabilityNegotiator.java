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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import network.ycc.raknet.frame.FrameData;

/**
 * Owns the application-level capability handshake for one RakNet connection.
 *
 * <p>Inbound and outbound readiness are deliberately separate. Receiving an
 * advertisement makes this endpoint ready to decode that peer's causal frames,
 * but this endpoint may not emit such frames until the peer acknowledges our
 * advertisement. That confirmation closes the cross-order-channel race between
 * the channel-7 control frame and gameplay sent on channels 1 or 4.</p>
 */
final class CausalCapabilityNegotiator {

    private boolean localAdvertisementSent;
    private boolean localAdvertisementConfirmed;
    private boolean remoteAdvertisementReceived;
    private int remoteVersion;
    private long remoteAdvertisedCapabilities;
    private long inboundCapabilities;
    private long outboundCapabilities;

    void sendAdvertisement(ChannelHandlerContext ctx, boolean flush) {
        if (localAdvertisementSent) {
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
            localAdvertisementSent = true;
            final ChannelFuture writeFuture = flush
                    ? ctx.writeAndFlush(frameData)
                    : ctx.write(frameData);
            frameData = null;
            writeFuture.addListener(future -> {
                if (!future.isSuccess()) {
                    localAdvertisementSent = false;
                    localAdvertisementConfirmed = false;
                    outboundCapabilities = 0L;
                    CausalTransportProtocol.setOutboundCapabilities(
                            ctx.channel(),
                            0L
                    );
                }
            });
        } finally {
            payload.release();
            ReferenceCountUtil.safeRelease(frameData);
        }
    }

    void handleControl(ChannelHandlerContext ctx, FrameData packet) {
        requireControlChannel(packet);
        final ByteBuf payload = packet.createData().skipBytes(1);
        try {
            final CausalTransportProtocol.ControlMessage message =
                    CausalTransportProtocol.decodeControl(payload);
            if (message instanceof CausalTransportProtocol.Capabilities capabilities) {
                handleAdvertisement(ctx, capabilities);
            } else if (message instanceof CausalTransportProtocol.CapabilitiesAck ack) {
                handleAcknowledgement(ctx, ack);
            }
        } finally {
            payload.release();
        }
    }

    boolean hasInbound(long capability) {
        return (inboundCapabilities & capability) != 0L;
    }

    boolean hasOutbound(long capability) {
        return (outboundCapabilities & capability) != 0L;
    }

    boolean hasAllOutbound(long requiredCapabilities) {
        return (outboundCapabilities & requiredCapabilities)
                == requiredCapabilities;
    }

    void clear(Channel channel) {
        localAdvertisementConfirmed = false;
        inboundCapabilities = 0L;
        outboundCapabilities = 0L;
        CausalTransportProtocol.setInboundCapabilities(channel, 0L);
        CausalTransportProtocol.setOutboundCapabilities(channel, 0L);
    }

    private void handleAdvertisement(
            ChannelHandlerContext ctx,
            CausalTransportProtocol.Capabilities capabilities
    ) {
        if (remoteAdvertisementReceived) {
            if (capabilities.version() != remoteVersion
                    || capabilities.capabilities()
                    != remoteAdvertisedCapabilities) {
                throw new CorruptedFrameException(
                        "Causal capabilities changed after negotiation"
                );
            }
            return;
        }
        remoteAdvertisementReceived = true;
        remoteVersion = capabilities.version();
        remoteAdvertisedCapabilities = capabilities.capabilities();
        inboundCapabilities =
                capabilities.version() == CausalTransportProtocol.VERSION
                        ? CausalTransportProtocol.negotiateCapabilities(
                                capabilities.capabilities()
                        )
                        : 0L;
        CausalTransportProtocol.setInboundCapabilities(
                ctx.channel(),
                inboundCapabilities
        );

        final boolean confirmationSupported =
                supportsConfirmation(capabilities);
        // Our advertisement must precede its acknowledgement on channel 7.
        // Without the extension there is no following ACK write to flush it.
        sendAdvertisement(ctx, !confirmationSupported);
        if (confirmationSupported) {
            sendAcknowledgement(ctx, capabilities.capabilities());
        }
    }

    private void handleAcknowledgement(
            ChannelHandlerContext ctx,
            CausalTransportProtocol.CapabilitiesAck acknowledgement
    ) {
        if (!localAdvertisementSent) {
            throw new CorruptedFrameException(
                    "Causal capability acknowledgement arrived before advertisement"
            );
        }
        if (!remoteAdvertisementReceived
                || remoteVersion != CausalTransportProtocol.VERSION
                || (remoteAdvertisedCapabilities
                & CausalTransportProtocol.CAPABILITY_CONFIRMATION) == 0L) {
            throw new CorruptedFrameException(
                    "Peer acknowledged capabilities without negotiating confirmation"
            );
        }
        if (acknowledgement.version() != CausalTransportProtocol.VERSION
                || acknowledgement.acknowledgedCapabilities()
                != CausalTransportProtocol.LOCAL_CAPABILITIES) {
            throw new CorruptedFrameException(
                    "Causal capability acknowledgement does not match advertisement"
            );
        }
        if (localAdvertisementConfirmed) {
            return;
        }

        localAdvertisementConfirmed = true;
        outboundCapabilities = CausalTransportProtocol.negotiateCapabilities(
                remoteAdvertisedCapabilities
        );
        CausalTransportProtocol.setOutboundCapabilities(
                ctx.channel(),
                outboundCapabilities
        );
    }

    private void sendAcknowledgement(
            ChannelHandlerContext ctx,
            long acknowledgedCapabilities
    ) {
        final ByteBuf payload = CausalTransportProtocol.encodeCapabilitiesAck(
                ctx.alloc(),
                acknowledgedCapabilities
        );
        FrameData frameData = null;
        try {
            frameData = FrameData.create(
                    ctx.alloc(),
                    Constants.RAKNET_CAUSAL_CONTROL_PACKET_ID,
                    payload
            );
            frameData.setOrderChannel(7);
            ctx.writeAndFlush(frameData).addListener(future -> {
                if (!future.isSuccess()) {
                    final Throwable cause = CausalFutureUtil.failureCause(
                            future,
                            "Causal capability acknowledgement write"
                    );
                    ctx.fireExceptionCaught(cause);
                    ctx.close();
                }
            });
            frameData = null;
        } finally {
            payload.release();
            ReferenceCountUtil.safeRelease(frameData);
        }
    }

    private static boolean supportsConfirmation(
            CausalTransportProtocol.Capabilities capabilities
    ) {
        return capabilities.version() == CausalTransportProtocol.VERSION
                && (capabilities.capabilities()
                & CausalTransportProtocol.CAPABILITY_CONFIRMATION) != 0L;
    }

    private static void requireControlChannel(FrameData packet) {
        if (!packet.getReliability().isReliable
                || !packet.getReliability().isOrdered
                || packet.getReliability().isSequenced
                || packet.getOrderChannel() != 7) {
            throw new CorruptedFrameException(
                    "Causal control frame was not delivered on reliable ordered channel 7"
            );
        }
    }

}
