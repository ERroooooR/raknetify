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
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import network.ycc.raknet.frame.FrameData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalMultichannelFaultInjectionTest {

    private static final int ENTITY_SPAWN = 1;
    private static final int ENTITY_PAIRING = 2;
    private static final int ENTITY_METADATA = 3;
    private static final int ENTITY_PASSENGERS = 4;
    private static final int CUSTOM_ENTITY_PAYLOAD = 5;
    private static final int INDEPENDENT_CONTROL = 6;
    private static final int EPHEMERAL_EFFECT = 7;
    private static final int GUARDED_BULK = 8;
    private static final int BUNDLE_DELIMITER = 9;
    private static final int TRANSITIONS = 100;

    @Test
    void entityCausalitySurvivesOneHundredFaultInjectedTransitions() {
        final Endpoint sender = new Endpoint();
        final Endpoint receiver = new Endpoint();
        final FaultyOrderedLink link = new FaultyOrderedLink(0x5eed_cafeL);
        negotiate(sender, receiver);

        final List<ChannelPromise> initialPromises =
                writeEntityChain(sender.channel, 0, true);
        sender.channel.pipeline().flush();
        link.deliver(drainOutbound(sender.channel), receiver.channel, 1);
        assertPromisesSucceeded(initialPromises);

        for (int nextEpoch = 1; nextEpoch <= TRANSITIONS; nextEpoch++) {
            final int previousEpoch = nextEpoch - 1;
            writeLargeBulk(sender.channel, previousEpoch);
            writePacket(sender.channel, INDEPENDENT_CONTROL, previousEpoch);
            writePacket(sender.channel, EPHEMERAL_EFFECT, previousEpoch);

            final ChannelPromise fencePromise = sender.channel.newPromise();
            sender.channel.pipeline().write(
                    SynchronizationLayer.SYNC_REQUEST_OBJECT,
                    fencePromise
            );
            final ChannelPromise restartPromise = sender.channel.newPromise();
            sender.channel.pipeline().write(
                    RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL,
                    restartPromise
            );
            final List<ChannelPromise> nextEpochPromises = writeEntityChain(
                    sender.channel,
                    nextEpoch,
                    (nextEpoch & 1) == 0
            );
            sender.channel.pipeline().flush();

            final List<FrameData> beforeAck = drainOutbound(sender.channel);
            assertApplicationFramesPrecedeFenceMarkers(beforeAck);
            link.deliver(
                    beforeAck,
                    receiver.channel,
                    1 + nextEpoch % 5
            );

            final List<FrameData> acknowledgements =
                    drainOutbound(receiver.channel);
            assertEquals(1, acknowledgements.size());
            assertEquals(
                    Constants.RAKNET_SYNC_PACKET_ID,
                    acknowledgements.get(0).getPacketId()
            );
            link.deliver(
                    acknowledgements,
                    sender.channel,
                    1 + nextEpoch % 5
            );

            final List<FrameData> afterAck = drainOutbound(sender.channel);
            assertRestartBarrierPrecedesNewEpoch(afterAck);
            link.deliver(
                    afterAck,
                    receiver.channel,
                    1 + nextEpoch % 5
            );

            assertTrue(fencePromise.isSuccess());
            assertTrue(restartPromise.isSuccess());
            assertPromisesSucceeded(nextEpochPromises);
            assertEquals(nextEpoch, receiver.capture.currentEpoch);
        }

        assertTrue(link.simulatedLosses > 0);
        for (int epoch = 0; epoch <= TRANSITIONS; epoch++) {
            assertEquals(
                    Arrays.asList(
                            ENTITY_SPAWN,
                            ENTITY_PAIRING,
                            ENTITY_METADATA,
                            ENTITY_PASSENGERS,
                            CUSTOM_ENTITY_PAYLOAD
                    ),
                    receiver.capture.entityPackets(epoch),
                    "entity causal chain changed in epoch " + epoch
            );
        }
        assertEquals(TRANSITIONS + 1, receiver.capture.entityEpochs.size());
        assertFalse(sender.channel.finishAndReleaseAll());
        assertFalse(receiver.channel.finishAndReleaseAll());
    }

    @Test
    void oneHundredBackToBackTransitionsRetainEveryEpochBoundary() {
        final Endpoint sender = new Endpoint();
        final Endpoint receiver = new Endpoint();
        final FaultyOrderedLink link = new FaultyOrderedLink(0x51a1_1eedL);
        negotiate(sender, receiver);

        final List<ChannelPromise> fencePromises = new ArrayList<>();
        final List<ChannelPromise> restartPromises = new ArrayList<>();
        final List<ChannelPromise> gamePromises = new ArrayList<>();
        for (int nextEpoch = 1; nextEpoch <= TRANSITIONS; nextEpoch++) {
            final ChannelPromise fencePromise = sender.channel.newPromise();
            sender.channel.pipeline().write(
                    SynchronizationLayer.SYNC_REQUEST_OBJECT,
                    fencePromise
            );
            fencePromises.add(fencePromise);

            final ChannelPromise restartPromise = sender.channel.newPromise();
            sender.channel.pipeline().write(
                    RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL,
                    restartPromise
            );
            restartPromises.add(restartPromise);
            gamePromises.addAll(writeEntityChain(
                    sender.channel,
                    nextEpoch,
                    (nextEpoch & 1) == 0
            ));
            writePacket(sender.channel, INDEPENDENT_CONTROL, nextEpoch);
            writePacket(sender.channel, EPHEMERAL_EFFECT, nextEpoch);
        }
        sender.channel.pipeline().flush();

        List<FrameData> outbound = drainOutbound(sender.channel);
        assertEquals(CausalFenceProtocol.ORDER_CHANNEL_COUNT, outbound.size());
        for (int completedEpoch = 1;
             completedEpoch <= TRANSITIONS;
             completedEpoch++) {
            link.deliver(outbound, receiver.channel, 1 + completedEpoch % 5);
            final List<FrameData> acknowledgements =
                    drainOutbound(receiver.channel);
            assertEquals(1, acknowledgements.size());
            link.deliver(
                    acknowledgements,
                    sender.channel,
                    1 + completedEpoch % 5
            );

            outbound = drainOutbound(sender.channel);
            assertRestartBarrierPrecedesAnyNewEpoch(outbound);
            if (completedEpoch < TRANSITIONS) {
                assertApplicationFramesPrecedeFenceMarkers(
                        outbound,
                        "after completed epoch " + completedEpoch
                );
            }
        }

        link.deliver(outbound, receiver.channel, 5);
        assertTrue(drainOutbound(receiver.channel).isEmpty());
        assertPromisesSucceeded(fencePromises);
        assertPromisesSucceeded(restartPromises);
        assertPromisesSucceeded(gamePromises);
        assertEquals(TRANSITIONS, receiver.capture.currentEpoch);
        for (int epoch = 1; epoch <= TRANSITIONS; epoch++) {
            assertEquals(
                    Arrays.asList(
                            ENTITY_SPAWN,
                            ENTITY_PAIRING,
                            ENTITY_METADATA,
                            ENTITY_PASSENGERS,
                            CUSTOM_ENTITY_PAYLOAD
                    ),
                    receiver.capture.entityPackets(epoch),
                    "entity causal chain changed in queued epoch " + epoch
            );
        }
        assertEquals(TRANSITIONS, receiver.capture.entityEpochs.size());
        assertTrue(link.simulatedLosses > 0);
        assertFalse(sender.channel.finishAndReleaseAll());
        assertFalse(receiver.channel.finishAndReleaseAll());
    }

    private static void negotiate(Endpoint first, Endpoint second) {
        assertTrue(first.channel.writeOutbound(
                RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL
        ));
        assertTrue(second.channel.writeOutbound(
                RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL
        ));

        deliverInOrder(drainOutbound(first.channel), second.channel);
        deliverInOrder(drainOutbound(second.channel), first.channel);
        assertTrue(first.codec.isDependencyDomainsEnabled());
        assertTrue(second.codec.isDependencyDomainsEnabled());
    }

    private static void deliverInOrder(
            List<FrameData> frames,
            EmbeddedChannel receiver
    ) {
        for (FrameData frame : frames) {
            receiver.writeInbound(frame);
        }
        receiver.runPendingTasks();
    }

    private static List<ChannelPromise> writeEntityChain(
            EmbeddedChannel channel,
            int epoch,
            boolean bundled
    ) {
        final List<ChannelPromise> promises = new ArrayList<>();
        if (bundled) {
            promises.add(writeDelimiter(channel));
        }
        promises.add(writePacket(channel, ENTITY_SPAWN, epoch));
        promises.add(writePacket(channel, ENTITY_PAIRING, epoch));
        promises.add(writePacket(channel, ENTITY_METADATA, epoch));
        promises.add(writePacket(channel, ENTITY_PASSENGERS, epoch));
        promises.add(writePacket(channel, CUSTOM_ENTITY_PAYLOAD, epoch));
        if (bundled) {
            promises.add(writeDelimiter(channel));
        }
        return promises;
    }

    private static ChannelPromise writePacket(
            EmbeddedChannel channel,
            int packetId,
            int epoch
    ) {
        final ByteBuf packet = channel.alloc().buffer(5);
        packet.writeByte(packetId);
        packet.writeInt(epoch);
        final ChannelPromise promise = channel.newPromise();
        channel.pipeline().write(packet, promise);
        return promise;
    }

    private static ChannelPromise writeDelimiter(EmbeddedChannel channel) {
        final ByteBuf packet = channel.alloc().buffer(1).writeByte(BUNDLE_DELIMITER);
        final ChannelPromise promise = channel.newPromise();
        channel.pipeline().write(packet, promise);
        return promise;
    }

    private static void writeLargeBulk(EmbeddedChannel channel, int epoch) {
        final ByteBuf packet = channel.alloc().buffer(256 * 1024);
        packet.writeByte(GUARDED_BULK);
        packet.writeInt(epoch);
        packet.writeZero(256 * 1024 - packet.writerIndex());
        channel.pipeline().write(packet);
    }

    private static void assertPromisesSucceeded(List<ChannelPromise> promises) {
        for (ChannelPromise promise : promises) {
            assertTrue(promise.isSuccess());
        }
    }

    private static void assertApplicationFramesPrecedeFenceMarkers(
            List<FrameData> frames
    ) {
        assertApplicationFramesPrecedeFenceMarkers(frames, "");
    }

    private static void assertApplicationFramesPrecedeFenceMarkers(
            List<FrameData> frames,
            String context
    ) {
        boolean sawFence = false;
        int fenceMarkers = 0;
        final List<Integer> applicationChannels = new ArrayList<>();
        for (FrameData frame : frames) {
            if (frame.getPacketId() == Constants.RAKNET_SYNC_PACKET_ID) {
                sawFence = true;
                fenceMarkers++;
            } else {
                assertFalse(
                        sawFence,
                        "an application frame was emitted after a fence marker"
                );
                applicationChannels.add(frame.getOrderChannel());
            }
        }
        assertEquals(
                CausalFenceProtocol.ORDER_CHANNEL_COUNT,
                fenceMarkers,
                context + " packetIds=" + frames.stream()
                        .map(FrameData::getPacketId)
                        .toList()
        );
        assertTrue(applicationChannels.contains(7));
        assertTrue(applicationChannels.contains(1));
        assertTrue(applicationChannels.contains(4));
    }

    private static void assertRestartBarrierPrecedesNewEpoch(
            List<FrameData> frames
    ) {
        assertRestartBarrierPrecedesAnyNewEpoch(frames);
        for (int i = 1; i < frames.size(); i++) {
            final FrameData frame = frames.get(i);
            if (frame.getPacketId() == Constants.RAKNET_GAME_PACKET_ID) {
                assertEquals(7, frame.getOrderChannel());
            }
        }
    }

    private static void assertRestartBarrierPrecedesAnyNewEpoch(
            List<FrameData> frames
    ) {
        assertFalse(frames.isEmpty());
        assertEquals(Constants.RAKNET_PING_PACKET_ID, frames.get(0).getPacketId());
        boolean sawGameFrame = false;
        for (int i = 1; i < frames.size(); i++) {
            final FrameData frame = frames.get(i);
            if (frame.getPacketId() == Constants.RAKNET_GAME_PACKET_ID) {
                sawGameFrame = true;
            }
        }
        assertTrue(sawGameFrame);
    }

    private static List<FrameData> drainOutbound(EmbeddedChannel channel) {
        channel.flushOutbound();
        channel.runPendingTasks();
        final List<FrameData> frames = new ArrayList<>();
        Object outbound;
        while ((outbound = channel.readOutbound()) != null) {
            if (outbound instanceof FrameData frameData) {
                frames.add(frameData);
            } else {
                ReferenceCountUtil.safeRelease(outbound);
                throw new AssertionError(
                        "unexpected outbound message " + outbound.getClass()
                );
            }
        }
        return frames;
    }

    private static final class Endpoint {
        private final ApplicationCapture capture = new ApplicationCapture();
        private final RakNetSimpleMultiChannelCodec codec =
                new RakNetSimpleMultiChannelCodec(Constants.RAKNET_GAME_PACKET_ID);
        private final EmbeddedChannel channel;

        private Endpoint() {
            codec.addHandler((buf, suppressWarning) -> {
                final int packetId = buf.getUnsignedByte(buf.readerIndex());
                return switch (packetId) {
                    case BUNDLE_DELIMITER ->
                            RakNetSimpleMultiChannelCodec.OverrideResult
                                    .bundleDelimiter();
                    case INDEPENDENT_CONTROL ->
                            RakNetSimpleMultiChannelCodec.OverrideResult.classify(
                                    DependencyDomain.INDEPENDENT_CONTROL,
                                    -1
                            );
                    case EPHEMERAL_EFFECT ->
                            RakNetSimpleMultiChannelCodec.OverrideResult.classify(
                                    DependencyDomain.EPHEMERAL_EFFECT,
                                    4
                            );
                    case GUARDED_BULK ->
                            RakNetSimpleMultiChannelCodec.OverrideResult.classify(
                                    DependencyDomain.GUARDED_BULK,
                                    7
                            );
                    default -> RakNetSimpleMultiChannelCodec.OverrideResult.strict();
                };
            });
            channel = new EmbeddedChannel(
                    new SynchronizationLayer(),
                    codec,
                    capture
            );
        }
    }

    private static final class ApplicationCapture
            extends ChannelInboundHandlerAdapter {
        private final List<CapturedPacket> packets = new ArrayList<>();
        private final List<Integer> entityEpochs = new ArrayList<>();
        private int currentEpoch;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (!(msg instanceof ByteBuf packet)) {
                ReferenceCountUtil.safeRelease(msg);
                throw new AssertionError(
                        "unexpected inbound message " + msg.getClass()
                );
            }
            try {
                final int packetId = packet.readUnsignedByte();
                if (packetId >= ENTITY_SPAWN
                        && packetId <= CUSTOM_ENTITY_PAYLOAD) {
                    final int encodedEpoch = packet.readInt();
                    assertEquals(currentEpoch, encodedEpoch);
                    packets.add(new CapturedPacket(encodedEpoch, packetId));
                    if (packetId == ENTITY_SPAWN) {
                        entityEpochs.add(encodedEpoch);
                    }
                }
            } finally {
                packet.release();
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof SynchronizationLayer.InboundEpochAdvanced advanced) {
                currentEpoch = advanced.epoch();
            }
            ctx.fireUserEventTriggered(evt);
        }

        private List<Integer> entityPackets(int epoch) {
            return packets.stream()
                    .filter(packet -> packet.epoch == epoch)
                    .map(CapturedPacket::packetId)
                    .toList();
        }
    }

    private static final class FaultyOrderedLink {
        private final Random random;
        private long serial;
        private int simulatedLosses;

        private FaultyOrderedLink(long seed) {
            random = new Random(seed);
        }

        private void deliver(
                List<FrameData> frames,
                EmbeddedChannel receiver,
                int lossPercent
        ) {
            final long[] lastDeliveryByChannel = new long[8];
            Arrays.fill(lastDeliveryByChannel, -1L);
            final List<InFlight> inFlight = new ArrayList<>(frames.size());
            for (FrameData frame : frames) {
                final int channel = frame.getReliability().isOrdered
                        ? frame.getOrderChannel()
                        : 0;
                long deliveryTime = random.nextInt(50);
                if (random.nextInt(100) < lossPercent) {
                    // Reliability retransmits the frame later. The model does
                    // not duplicate delivery, matching ordered de-duplication.
                    deliveryTime += 100 + random.nextInt(200);
                    simulatedLosses++;
                }
                deliveryTime = Math.max(
                        deliveryTime,
                        lastDeliveryByChannel[channel] + 1
                );
                lastDeliveryByChannel[channel] = deliveryTime;
                inFlight.add(new InFlight(frame, deliveryTime, serial++));
            }
            inFlight.sort(
                    Comparator.comparingLong(InFlight::deliveryTime)
                            .thenComparingLong(InFlight::serial)
            );
            for (InFlight delivery : inFlight) {
                receiver.writeInbound(delivery.frame);
            }
            receiver.runPendingTasks();
        }
    }

    private record CapturedPacket(int epoch, int packetId) {
    }

    private record InFlight(
            FrameData frame,
            long deliveryTime,
            long serial
    ) {
    }
}
