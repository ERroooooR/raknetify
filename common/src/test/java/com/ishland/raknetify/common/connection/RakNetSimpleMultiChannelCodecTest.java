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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import network.ycc.raknet.frame.FrameData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RakNetSimpleMultiChannelCodecTest {

    @Test
    void explicitChannelZeroDoesNotFallThroughToLaterHandlers() {
        final TestCodec codec = new TestCodec();
        codec.addHandler((buf, suppressWarning) -> RakNetSimpleMultiChannelCodec.OverrideResult.route(0));
        codec.addHandler((buf, suppressWarning) -> RakNetSimpleMultiChannelCodec.OverrideResult.route(7));

        assertEquals(0, codec.classify());
    }

    @Test
    void firstMatchedHandlerPreventsLaterOrdinaryOverride() {
        final TestCodec codec = new TestCodec();
        codec.addHandler((buf, suppressWarning) -> RakNetSimpleMultiChannelCodec.OverrideResult.route(7));
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.route(2));

        assertEquals(7, codec.classify());
    }

    @Test
    void bundleBoundaryOutranksAnEarlierRoute() {
        final TestCodec codec = new TestCodec();
        codec.addHandler((buf, suppressWarning) -> RakNetSimpleMultiChannelCodec.OverrideResult.route(7));
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.bundleDelimiter());

        assertEquals(Integer.MIN_VALUE, codec.classify());
    }

    @Test
    void strictDecisionOutranksAnEarlierOptimisticRoute() {
        final TestCodec codec = new TestCodec();
        codec.addHandler((buf, suppressWarning) -> RakNetSimpleMultiChannelCodec.OverrideResult.route(4));
        codec.addHandler((buf, suppressWarning) -> RakNetSimpleMultiChannelCodec.OverrideResult.strict());

        assertEquals(7, codec.classify());
    }

    @Test
    void absentPacketIdIsStrictInsteadOfImplicitChannelZero() {
        final Int2IntOpenHashMap mapping = new Int2IntOpenHashMap();
        mapping.put(1, 4);

        final TestCodec codec = new TestCodec();
        codec.addHandler(new RakNetSimpleMultiChannelCodec.PacketIdBasedOverrideHandler(
                mapping,
                "test"
        ));

        assertEquals(7, codec.classify(2));
        assertEquals(4, codec.classify(1));
    }

    @Test
    void compatibilityProfileOpensOnlyExplicitSafeLegacyDomains() {
        final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(0xfd);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.route(-1));
        final EmbeddedChannel channel = new EmbeddedChannel(codec);
        negotiateCausalCapabilities(channel);

        assertTrue(channel.writeOutbound(Unpooled.wrappedBuffer(new byte[]{1})));
        final FrameData frame = channel.readOutbound();
        assertEquals(1, frame.getOrderChannel());
        assertTrue(frame.getReliability().isOrdered);
        frame.release();
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void unknownPacketsRemainOnStrictChannelAfterNegotiation() {
        final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(0xfd);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.pass());
        final EmbeddedChannel channel = new EmbeddedChannel(codec);
        negotiateCausalCapabilities(channel);

        assertTrue(channel.writeOutbound(Unpooled.wrappedBuffer(new byte[]{1})));
        final FrameData frame = channel.readOutbound();
        assertEquals(7, frame.getOrderChannel());
        frame.release();
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void removingCodecFailsAnUndrainedScheduledWrite() {
        final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(0xfd);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.route(-1));
        final EmbeddedChannel channel = new EmbeddedChannel(codec);
        negotiateCausalCapabilities(channel);

        final ChannelPromise promise = channel.newPromise();
        channel.pipeline().write(Unpooled.wrappedBuffer(new byte[]{1}), promise);
        assertFalse(promise.isDone());
        channel.pipeline().remove(codec);

        assertTrue(promise.isDone());
        assertFalse(promise.isSuccess());
        channel.runPendingTasks();
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void negotiatedBundleIsOneFrameAndIsReconstructedSynchronously() {
        final RakNetSimpleMultiChannelCodec codec = new RakNetSimpleMultiChannelCodec(0xfd);
        codec.addHandler((buf, suppressWarning) -> buf.getUnsignedByte(buf.readerIndex()) == 0
                ? RakNetSimpleMultiChannelCodec.OverrideResult.bundleDelimiter()
                : RakNetSimpleMultiChannelCodec.OverrideResult.route(2));
        final EmbeddedChannel channel = new EmbeddedChannel(codec);

        assertTrue(channel.writeOutbound(RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL));
        final FrameData capabilities = channel.readOutbound();
        final FrameData startBarrier = channel.readOutbound();
        assertEquals(Constants.RAKNET_CAUSAL_CONTROL_PACKET_ID, capabilities.getPacketId());
        assertEquals(Constants.RAKNET_PING_PACKET_ID, startBarrier.getPacketId());
        capabilities.release();
        startBarrier.release();

        final ByteBuf capabilityPayload = CausalTransportProtocol.encodeCapabilities(
                channel.alloc(),
                CausalTransportProtocol.LOCAL_CAPABILITIES
        );
        final FrameData remoteCapabilities;
        try {
            remoteCapabilities = FrameData.create(
                    channel.alloc(),
                    Constants.RAKNET_CAUSAL_CONTROL_PACKET_ID,
                    capabilityPayload
            );
            remoteCapabilities.setOrderChannel(7);
        } finally {
            capabilityPayload.release();
        }
        assertFalse(channel.writeInbound(remoteCapabilities));
        assertCapabilityAcknowledgement(
                channel.readOutbound(),
                CausalTransportProtocol.LOCAL_CAPABILITIES
        );
        assertFalse(channel.writeInbound(capabilityAckFrame(
                channel,
                CausalTransportProtocol.LOCAL_CAPABILITIES
        )));
        assertTrue(codec.isAtomicBundleEnabled());

        final ChannelPromise openingPromise = channel.newPromise();
        final ChannelPromise contentPromise = channel.newPromise();
        final ChannelPromise closingPromise = channel.newPromise();
        channel.pipeline().write(Unpooled.wrappedBuffer(new byte[]{0}), openingPromise);
        channel.pipeline().write(Unpooled.wrappedBuffer(new byte[]{1, 42}), contentPromise);
        assertFalse(openingPromise.isDone());
        assertFalse(contentPromise.isDone());
        channel.pipeline().write(Unpooled.wrappedBuffer(new byte[]{0}), closingPromise);
        channel.pipeline().flush();
        assertTrue(openingPromise.isSuccess());
        assertTrue(contentPromise.isSuccess());
        assertTrue(closingPromise.isSuccess());

        final FrameData envelope = channel.readOutbound();
        assertEquals(0xfd, envelope.getPacketId());
        assertEquals(7, envelope.getOrderChannel());
        final ByteBuf encodedEnvelope = envelope.createData().skipBytes(1);
        try {
            assertTrue(CausalTransportProtocol.isEpochAtomicBundle(encodedEnvelope));
        } finally {
            encodedEnvelope.release();
        }

        assertTrue(channel.writeInbound(envelope));
        assertPacket(channel.readInbound(), 0);
        assertPacket(channel.readInbound(), 1, 42);
        assertPacket(channel.readInbound(), 0);
        assertNull(channel.readInbound());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void bundleControlWaitsForEnvelopeWriteAcceptance() {
        final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(0xfd);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.bundleDelimiter()
        );
        final EmbeddedChannel channel = new EmbeddedChannel(codec);
        negotiateCausalCapabilities(channel);

        final ChannelPromise openingPromise = channel.newPromise();
        final ChannelPromise controlPromise = channel.newPromise();
        final ChannelPromise closingPromise = channel.newPromise();
        channel.pipeline().write(
                Unpooled.wrappedBuffer(new byte[]{0}),
                openingPromise
        );
        channel.pipeline().write(
                RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL,
                controlPromise
        );
        channel.pipeline().write(
                Unpooled.wrappedBuffer(new byte[]{0}),
                closingPromise
        );

        assertFalse(openingPromise.isDone());
        assertFalse(controlPromise.isDone());
        assertFalse(closingPromise.isDone());
        channel.runPendingTasks();

        assertTrue(openingPromise.isSuccess());
        assertTrue(controlPromise.isSuccess());
        assertTrue(closingPromise.isSuccess());
        final FrameData envelope = channel.readOutbound();
        assertEquals(0xfd, envelope.getPacketId());
        envelope.release();
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void futureEpochWaitsForItsFenceEvent() {
        final RakNetSimpleMultiChannelCodec codec = new RakNetSimpleMultiChannelCodec(0xfd);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.route(7));
        final EmbeddedChannel channel = new EmbeddedChannel(codec);
        negotiateCausalCapabilities(channel);

        final ByteBuf original = Unpooled.wrappedBuffer(new byte[]{3, 7});
        final ByteBuf encoded = CausalTransportProtocol.encodeGameplayFrame(
                channel.alloc(),
                1,
                original
        );
        original.release();
        final FrameData futureFrame;
        try {
            futureFrame = FrameData.create(channel.alloc(), 0xfd, encoded);
            futureFrame.setOrderChannel(7);
        } finally {
            encoded.release();
        }

        assertFalse(channel.writeInbound(futureFrame));
        assertNull(channel.readInbound());
        channel.pipeline().fireUserEventTriggered(
                new SynchronizationLayer.InboundEpochAdvanced(1)
        );
        channel.runPendingTasks();
        assertPacket(channel.readInbound(), 3, 7);
        assertNull(channel.readInbound());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void outboundFramesAdvanceEpochAndStaleInboundFramesAreDropped() {
        final RakNetSimpleMultiChannelCodec codec = new RakNetSimpleMultiChannelCodec(0xfd);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.route(7));
        final EmbeddedChannel channel = new EmbeddedChannel(codec);
        negotiateCausalCapabilities(channel);

        channel.pipeline().fireUserEventTriggered(
                new SynchronizationLayer.OutboundEpochAdvanced(1)
        );
        assertTrue(channel.writeOutbound(Unpooled.wrappedBuffer(new byte[]{4, 8})));
        final FrameData outbound = channel.readOutbound();
        final ByteBuf outboundPayload = outbound.createData().skipBytes(1);
        try {
            assertTrue(CausalTransportProtocol.isEpochGameplayFrame(outboundPayload));
            assertEquals(1, CausalTransportProtocol.peekGameplayEpoch(outboundPayload));
        } finally {
            outboundPayload.release();
            outbound.release();
        }

        channel.pipeline().fireUserEventTriggered(
                new SynchronizationLayer.InboundEpochAdvanced(1)
        );
        final ByteBuf stalePacket = Unpooled.wrappedBuffer(new byte[]{6});
        final ByteBuf stalePayload = CausalTransportProtocol.encodeGameplayFrame(
                channel.alloc(),
                0,
                stalePacket
        );
        stalePacket.release();
        final FrameData staleFrame;
        try {
            staleFrame = FrameData.create(channel.alloc(), 0xfd, stalePayload);
            staleFrame.setOrderChannel(7);
        } finally {
            stalePayload.release();
        }
        assertFalse(channel.writeInbound(staleFrame));
        assertNull(channel.readInbound());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void overflowingCausalWaitQueueFailsAndReleasesEveryWrite() {
        final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(0xfd, 4, 4);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.route(7));
        final EmbeddedChannel channel = new EmbeddedChannel(codec);
        negotiateCausalCapabilities(channel);

        final ChannelPromise fencePromise = channel.newPromise();
        channel.pipeline().write(
                SynchronizationLayer.SYNC_REQUEST_OBJECT,
                fencePromise
        );
        final ByteBuf first = Unpooled.buffer(2).writeZero(2);
        final ByteBuf second = Unpooled.buffer(2).writeZero(2);
        final ByteBuf overflow = Unpooled.buffer(1).writeZero(1);
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        final ChannelPromise overflowPromise = channel.newPromise();
        channel.pipeline().write(first, firstPromise);
        channel.pipeline().write(second, secondPromise);
        channel.pipeline().write(overflow, overflowPromise);

        assertThrows(CorruptedFrameException.class, channel::checkException);
        assertFalse(firstPromise.isSuccess());
        assertFalse(secondPromise.isSuccess());
        assertFalse(overflowPromise.isSuccess());
        assertEquals(0, first.refCnt());
        assertEquals(0, second.refCnt());
        assertEquals(0, overflow.refCnt());
        ReferenceCountUtil.safeRelease(channel.readOutbound());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void overflowingBundleControlQueueAbortsOpenBundleAndEveryControl() {
        final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(0xfd, 2, 64);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.bundleDelimiter());
        final EmbeddedChannel channel = new EmbeddedChannel(codec);
        negotiateCausalCapabilities(channel);

        final ChannelPromise openingPromise = channel.newPromise();
        channel.pipeline().write(
                Unpooled.wrappedBuffer(new byte[]{0}),
                openingPromise
        );
        assertFalse(openingPromise.isDone());

        final ChannelPromise firstControl = channel.newPromise();
        final ChannelPromise secondControl = channel.newPromise();
        final ChannelPromise overflowControl = channel.newPromise();
        channel.pipeline().write(
                SynchronizationLayer.SYNC_REQUEST_OBJECT,
                firstControl
        );
        channel.pipeline().write(
                SynchronizationLayer.SYNC_REQUEST_OBJECT,
                secondControl
        );
        channel.pipeline().write(
                SynchronizationLayer.SYNC_REQUEST_OBJECT,
                overflowControl
        );

        assertThrows(CorruptedFrameException.class, channel::checkException);
        assertTrue(openingPromise.isDone());
        assertFalse(openingPromise.isSuccess());
        assertTrue(firstControl.isDone());
        assertFalse(firstControl.isSuccess());
        assertTrue(secondControl.isDone());
        assertFalse(secondControl.isSuccess());
        assertTrue(overflowControl.isDone());
        assertFalse(overflowControl.isSuccess());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void overflowingDomainSchedulerFailsAndReleasesEveryScheduledWrite() {
        final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(0xfd, 2, 1024);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.route(-1));
        final EmbeddedChannel channel = new EmbeddedChannel(codec);
        negotiateCausalCapabilities(channel);

        final ByteBuf first = Unpooled.buffer(2).writeZero(2);
        final ByteBuf second = Unpooled.buffer(2).writeZero(2);
        final ByteBuf overflow = Unpooled.buffer(2).writeZero(2);
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        final ChannelPromise overflowPromise = channel.newPromise();
        channel.pipeline().write(first, firstPromise);
        channel.pipeline().write(second, secondPromise);
        assertFalse(firstPromise.isDone());
        assertFalse(secondPromise.isDone());
        channel.pipeline().write(overflow, overflowPromise);

        assertThrows(CorruptedFrameException.class, channel::checkException);
        assertTrue(firstPromise.isDone());
        assertFalse(firstPromise.isSuccess());
        assertTrue(secondPromise.isDone());
        assertFalse(secondPromise.isSuccess());
        assertTrue(overflowPromise.isDone());
        assertFalse(overflowPromise.isSuccess());
        assertEquals(0, first.refCnt());
        assertEquals(0, second.refCnt());
        assertEquals(0, overflow.refCnt());
        channel.runPendingTasks();
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void capabilitiesCannotChangeAfterFirstAdvertisement() {
        final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(0xfd);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.route(7));
        final EmbeddedChannel channel = new EmbeddedChannel(codec);
        negotiateCausalCapabilities(channel);

        assertFalse(channel.writeInbound(capabilityFrame(
                channel,
                CausalTransportProtocol.LOCAL_CAPABILITIES
        )));
        assertThrows(
                CorruptedFrameException.class,
                () -> channel.writeInbound(capabilityFrame(
                        channel,
                        CausalTransportProtocol.CAPABILITY_ATOMIC_BUNDLE
                ))
        );
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void peerAdvertisementAloneCannotEnableCrossChannelCausalFrames() {
        final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(0xfd);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.route(-1));
        final EmbeddedChannel channel = new EmbeddedChannel(codec);

        assertTrue(channel.writeOutbound(
                RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL
        ));
        ReferenceCountUtil.safeRelease(channel.readOutbound());
        ReferenceCountUtil.safeRelease(channel.readOutbound());

        assertFalse(channel.writeInbound(capabilityFrame(
                channel,
                CausalTransportProtocol.LOCAL_CAPABILITIES
        )));
        assertCapabilityAcknowledgement(
                channel.readOutbound(),
                CausalTransportProtocol.LOCAL_CAPABILITIES
        );
        assertFalse(codec.isAtomicBundleEnabled());
        assertFalse(codec.isDependencyDomainsEnabled());

        assertTrue(channel.writeOutbound(
                Unpooled.wrappedBuffer(new byte[]{1})
        ));
        final FrameData preConfirmation = channel.readOutbound();
        assertEquals(7, preConfirmation.getOrderChannel());
        final ByteBuf preConfirmationPayload =
                preConfirmation.createData().skipBytes(1);
        try {
            assertFalse(CausalTransportProtocol.isEpochGameplayFrame(
                    preConfirmationPayload
            ));
        } finally {
            preConfirmationPayload.release();
            preConfirmation.release();
        }

        assertFalse(channel.writeInbound(capabilityAckFrame(
                channel,
                CausalTransportProtocol.LOCAL_CAPABILITIES
        )));
        assertTrue(codec.isAtomicBundleEnabled());
        assertTrue(codec.isDependencyDomainsEnabled());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void peerWithoutConfirmationKeepsOutboundStrictButInboundCompatible() {
        final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(0xfd);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.route(-1));
        final EmbeddedChannel channel = new EmbeddedChannel(codec);
        final long legacyCapabilities =
                CausalTransportProtocol.LOCAL_CAPABILITIES
                        & ~CausalTransportProtocol.CAPABILITY_CONFIRMATION;

        assertTrue(channel.writeOutbound(
                RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL
        ));
        ReferenceCountUtil.safeRelease(channel.readOutbound());
        ReferenceCountUtil.safeRelease(channel.readOutbound());
        assertFalse(channel.writeInbound(capabilityFrame(
                channel,
                legacyCapabilities
        )));
        assertNull(channel.readOutbound());
        assertFalse(codec.isAtomicBundleEnabled());
        assertFalse(codec.isDependencyDomainsEnabled());
        assertEquals(
                legacyCapabilities,
                CausalTransportProtocol.getInboundCapabilities(channel)
        );
        assertEquals(
                0L,
                CausalTransportProtocol.getOutboundCapabilities(channel)
        );

        final ByteBuf original = Unpooled.wrappedBuffer(new byte[]{9, 4});
        final ByteBuf encoded = CausalTransportProtocol.encodeGameplayFrame(
                channel.alloc(),
                0,
                original
        );
        original.release();
        final FrameData legacyPeerFrame;
        try {
            legacyPeerFrame = FrameData.create(
                    channel.alloc(),
                    0xfd,
                    encoded
            );
            legacyPeerFrame.setOrderChannel(7);
        } finally {
            encoded.release();
        }
        assertTrue(channel.writeInbound(legacyPeerFrame));
        assertPacket(channel.readInbound(), 9, 4);
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void mismatchedCapabilityAcknowledgementFailsClosed() {
        final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(0xfd);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.route(7));
        final EmbeddedChannel channel = new EmbeddedChannel(codec);

        assertTrue(channel.writeOutbound(
                RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL
        ));
        ReferenceCountUtil.safeRelease(channel.readOutbound());
        ReferenceCountUtil.safeRelease(channel.readOutbound());
        assertFalse(channel.writeInbound(capabilityFrame(
                channel,
                CausalTransportProtocol.LOCAL_CAPABILITIES
        )));
        ReferenceCountUtil.safeRelease(channel.readOutbound());

        assertThrows(
                CorruptedFrameException.class,
                () -> channel.writeInbound(capabilityAckFrame(
                        channel,
                        CausalTransportProtocol.CAPABILITY_ATOMIC_BUNDLE
                ))
        );
        assertFalse(codec.isDependencyDomainsEnabled());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void gameplayFrameCannotSkipBeyondTheNextInboundEpoch() {
        final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(0xfd);
        codec.addHandler((buf, suppressWarning) ->
                RakNetSimpleMultiChannelCodec.OverrideResult.route(7));
        final EmbeddedChannel channel = new EmbeddedChannel(codec);
        negotiateCausalCapabilities(channel);

        final ByteBuf packet = Unpooled.wrappedBuffer(new byte[]{5});
        final ByteBuf encoded = CausalTransportProtocol.encodeGameplayFrame(
                channel.alloc(),
                2,
                packet
        );
        packet.release();
        final FrameData futureFrame;
        try {
            futureFrame = FrameData.create(channel.alloc(), 0xfd, encoded);
            futureFrame.setOrderChannel(7);
        } finally {
            encoded.release();
        }

        assertThrows(
                CorruptedFrameException.class,
                () -> channel.writeInbound(futureFrame)
        );
        assertFalse(channel.finishAndReleaseAll());
    }

    private static void negotiateCausalCapabilities(EmbeddedChannel channel) {
        assertTrue(channel.writeOutbound(RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL));
        ReferenceCountUtil.safeRelease(channel.readOutbound());
        ReferenceCountUtil.safeRelease(channel.readOutbound());

        assertFalse(channel.writeInbound(capabilityFrame(
                channel,
                CausalTransportProtocol.LOCAL_CAPABILITIES
        )));
        assertCapabilityAcknowledgement(
                channel.readOutbound(),
                CausalTransportProtocol.LOCAL_CAPABILITIES
        );
        assertFalse(channel.writeInbound(capabilityAckFrame(
                channel,
                CausalTransportProtocol.LOCAL_CAPABILITIES
        )));
    }

    private static FrameData capabilityFrame(
            EmbeddedChannel channel,
            long capabilities
    ) {
        final ByteBuf capabilityPayload = CausalTransportProtocol.encodeCapabilities(
                channel.alloc(),
                capabilities
        );
        final FrameData remoteCapabilities;
        try {
            remoteCapabilities = FrameData.create(
                    channel.alloc(),
                    Constants.RAKNET_CAUSAL_CONTROL_PACKET_ID,
                    capabilityPayload
            );
            remoteCapabilities.setOrderChannel(7);
        } finally {
            capabilityPayload.release();
        }
        return remoteCapabilities;
    }

    private static FrameData capabilityAckFrame(
            EmbeddedChannel channel,
            long acknowledgedCapabilities
    ) {
        final ByteBuf payload =
                CausalTransportProtocol.encodeCapabilitiesAck(
                        channel.alloc(),
                        acknowledgedCapabilities
                );
        final FrameData acknowledgement;
        try {
            acknowledgement = FrameData.create(
                    channel.alloc(),
                    Constants.RAKNET_CAUSAL_CONTROL_PACKET_ID,
                    payload
            );
            acknowledgement.setOrderChannel(7);
        } finally {
            payload.release();
        }
        return acknowledgement;
    }

    private static void assertCapabilityAcknowledgement(
            FrameData frame,
            long expectedCapabilities
    ) {
        assertEquals(Constants.RAKNET_CAUSAL_CONTROL_PACKET_ID, frame.getPacketId());
        assertEquals(7, frame.getOrderChannel());
        final ByteBuf payload = frame.createData().skipBytes(1);
        try {
            final CausalTransportProtocol.CapabilitiesAck acknowledgement =
                    CausalTransportProtocol.decodeCapabilitiesAck(payload);
            assertEquals(
                    expectedCapabilities,
                    acknowledgement.acknowledgedCapabilities()
            );
        } finally {
            payload.release();
            frame.release();
        }
    }

    private static void assertPacket(ByteBuf packet, int... expected) {
        try {
            assertEquals(expected.length, packet.readableBytes());
            for (int value : expected) {
                assertEquals(value, packet.readUnsignedByte());
            }
        } finally {
            packet.release();
        }
    }

    private static final class TestCodec extends RakNetSimpleMultiChannelCodec {

        private TestCodec() {
            super(0xFD);
        }

        private int classify() {
            return classify(0);
        }

        private int classify(int packetId) {
            final ByteBuf packet = Unpooled.buffer(5);
            try {
                do {
                    int part = packetId & 0x7F;
                    packetId >>>= 7;
                    if (packetId != 0) {
                        part |= 0x80;
                    }
                    packet.writeByte(part);
                } while (packetId != 0);
                return getChannelOverride(packet, true);
            } finally {
                packet.release();
            }
        }

    }

}
