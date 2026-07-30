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

import com.ishland.raknetify.common.connection.multichannel.DependencyDomain;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsSynchronizationHandlerTest {

    @Test
    void extendedPayloadRoundTripsWithoutMetricsJsonl() {
        final SimpleMetricsLogger sender = new SimpleMetricsLogger();
        sender.currentQueuedBytes(1234);
        sender.adaptivePacingRate(37.5D);
        sender.adaptiveBytePacingRate(524288L);
        sender.adaptiveAdmissionDiagnostics(786432L, 250D, true);
        sender.adaptivePathModel(
                65536L,
                false,
                "CALIBRATING",
                1
        );
        sender.adaptiveDeliveryRate(45678L);
        sender.adaptiveLoss(0.125D, 70L, 10L);
        sender.adaptiveLossType("RATE_LIMIT");
        sender.congestionControl("DRAIN", 8192L, 2048L, 65536L, 0L, 0D);
        sender.reliableFrameDuplicate(7);
        sender.nackDeferred(11);
        sender.reorderedPacket(5);
        sender.nackDeferredExpired(4);
        sender.nackDeferredConfirmed(2);
        sender.nackGraceBypassed(13);
        sender.adaptiveNackGrace(true);
        sender.nackRepeated(5);
        sender.nackRetransmit(4096);
        sender.timeoutRetransmit(1024);
        sender.fragmentReassemblyPending(2, 65536L, 20_000_000L);
        sender.fragmentReassemblyComplete(32768, 30_000_000L);
        sender.orderedQueuePending(3, 40_000_000L);
        sender.orderedQueueRelease(4, 50_000_000L);
        sender.applicationBatch(16384);
        sender.applicationBatch(32768);
        sender.ackRepeated(3);
        sender.adaptiveAckPolicy(true, 8_000_000L, 10_000_000L);
        sender.adaptiveDemand(false, "BULK", 150_000_000L, 4L);
        sender.fecRecovered(3);
        sender.fecParity(7, 8192);
        sender.fecExpired(2);
        sender.fecBudget(10, 1, 0.25D);
        sender.congestionDiagnostics("RTT_INFLATION", 1.2D, true, true);
        sender.rackRetransmit(2048);
        sender.rackSpuriousAck(2);
        sender.ptoProbe(512);
        sender.ptoProbeAcked(512);
        sender.orderedHolProbe(7, 600);
        sender.orderedHolProbeAcked(600);
        sender.ptoState(1, 90_000_000L);
        sender.applicationLimitedRecovery(384);
        sender.recoveryQueueState(2, 100_000_000L);
        sender.recoveryDebt(2.5D, 3);
        sender.targetedFecRepair(3, 400);
        sender.targetedFecRecovered(1);
        sender.orderedChannelPending(3, 4, 110_000_000L, 27);
        sender.orderedChannelRelease(3, 5, 120_000_000L);
        sender.dependencyDomainQueued(
                DependencyDomain.GUARDED_BULK,
                12345
        );
        sender.causalBulkFrameOutbound(8192);
        sender.causalAtomicBundleOutbound(5, 65536);

        final ByteBuf payload = Unpooled.buffer();
        try {
            MetricsSynchronizationHandler.writePayload(payload, sender, 8, 12345L);
            assertEquals(895, payload.readableBytes());

            final MetricsSynchronizationHandler receiver = new MetricsSynchronizationHandler();
            assertTrue(receiver.readPayload(payload));
            assertTrue(receiver.isRemoteSupported());
            assertTrue(receiver.isRemoteAdaptiveSupported());
            assertEquals(1234, receiver.getQueuedBytes());
            assertEquals("RATE_LIMIT", receiver.getLossType());
            assertEquals("DRAIN", receiver.getCongestionMode());
            assertEquals("RTT_INFLATION", receiver.getCongestionReason());
            assertEquals(37.5D, receiver.getPacingRate());
            assertEquals(7L, receiver.getReliableFrameDuplicates());
            assertTrue(receiver.isRemoteRecoverySupported());
            assertEquals(11L, receiver.getNacksDeferred());
            assertEquals(5L, receiver.getReorderedPackets());
            assertTrue(receiver.isRemoteNackOutcomeSupported());
            assertEquals(4L, receiver.getNacksDeferredExpired());
            assertEquals(2L, receiver.getNacksDeferredConfirmed());
            assertTrue(receiver.isRemoteNackPolicySupported());
            assertEquals(13L, receiver.getNackGraceBypassed());
            assertTrue(receiver.isNackGraceBypass());
            assertTrue(receiver.isRemoteNackRepeatSupported());
            assertEquals(1L, receiver.getNackRepeatedPackets());
            assertEquals(5L, receiver.getNackRepeatedFrameSets());
            assertEquals(4096L, receiver.getNackRetransmitBytes());
            assertEquals(1024L, receiver.getTimeoutRetransmitBytes());
            assertTrue(receiver.isRemoteBytePacingSupported());
            assertEquals(524288L, receiver.getBytePacingRate());
            assertTrue(receiver.isRemoteHolSupported());
            assertEquals(2, receiver.getFragmentPendingBuilders());
            assertEquals(65536L, receiver.getFragmentPendingBytes());
            assertEquals(20_000_000L, receiver.getFragmentOldestAgeNanos());
            assertEquals(1L, receiver.getFragmentCompleted());
            assertEquals(30_000_000L, receiver.getFragmentMaxAgeNanos());
            assertEquals(3, receiver.getOrderedPendingFrames());
            assertEquals(40_000_000L, receiver.getOrderedOldestAgeNanos());
            assertEquals(4L, receiver.getOrderedReleasedFrames());
            assertEquals(50_000_000L, receiver.getOrderedMaxWaitNanos());
            assertTrue(receiver.isRemoteApplicationBatchSupported());
            assertEquals(2L, receiver.getApplicationBatches());
            assertEquals(49152L, receiver.getApplicationBatchBytes());
            assertEquals(32768L, receiver.getApplicationBatchMaxBytes());
            assertTrue(receiver.isRemoteAckPolicySupported());
            assertEquals(1L, receiver.getAckRepeatedPackets());
            assertEquals(3L, receiver.getAckRepeatedFrameSets());
            assertTrue(receiver.isAckProtection());
            assertEquals(8_000_000L, receiver.getAckFlushDelayNanos());
            assertEquals(10_000_000L, receiver.getAckRepeatDelayNanos());
            assertTrue(receiver.isRemoteDemandSupported());
            assertFalse(receiver.isApplicationLimited());
            assertEquals("BULK", receiver.getBacklogState());
            assertEquals(150_000_000L, receiver.getBacklogAgeNanos());
            assertEquals(4L, receiver.getBacklogProbes());
            assertTrue(receiver.isRemoteFecSupported());
            assertEquals(3L, receiver.getFecRecovered());
            assertEquals(7L, receiver.getFecParityPackets());
            assertEquals(8192L, receiver.getFecParityBytes());
            assertEquals(2L, receiver.getFecExpired());
            assertEquals(10, receiver.getFecDataShards());
            assertEquals(1, receiver.getFecParityShards());
            assertEquals(0.25D, receiver.getFecRecoveryRatio());
            assertTrue(receiver.isPacingCapped());
            assertTrue(receiver.isBandwidthProbeSuppressed());
            assertTrue(receiver.isRemoteAdvancedRecoverySupported());
            assertEquals(2048L, receiver.getRackRetransmitBytes());
            assertEquals(1L, receiver.getRackRetransmitFrameSets());
            assertEquals(2L, receiver.getRackSpuriousAcks());
            assertEquals(1L, receiver.getPtoProbes());
            assertEquals(512L, receiver.getPtoProbeBytes());
            assertEquals(512L, receiver.getPtoProbeAckedBytes());
            assertEquals(1, receiver.getPtoCount());
            assertEquals(1L, receiver.getApplicationLimitedRecoveryPackets());
            assertEquals(384L, receiver.getApplicationLimitedRecoveryBytes());
            assertEquals(2, receiver.getRecoveryQueueDepth());
            assertEquals(2.5D, receiver.getRecoveryDebt());
            assertEquals(3, receiver.getRecoveryDebtChannel());
            assertEquals(1L, receiver.getTargetedFecPackets());
            assertEquals(400L, receiver.getTargetedFecBytes());
            assertEquals(1L, receiver.getTargetedFecRecovered());
            assertEquals(3, receiver.getOrderedWorstChannel());
            assertEquals(4, receiver.getOrderedChannelPending()[3]);
            assertEquals(27, receiver.getOrderedChannelBlockedOrderIndex()[3]);
            assertEquals(5L, receiver.getOrderedChannelReleasedFrames()[3]);
            assertTrue(receiver.isRemoteOrderedHolProbeSupported());
            assertEquals(1L, receiver.getOrderedHolProbes());
            assertEquals(600L, receiver.getOrderedHolProbeBytes());
            assertEquals(600L, receiver.getOrderedHolProbeAckedBytes());
            assertEquals(7, receiver.getOrderedHolProbeChannel());
            assertTrue(receiver.isRemoteCausalSchedulerSupported());
            assertEquals(
                    1,
                    receiver.getDependencyDomainPendingFrames()[
                            DependencyDomain.GUARDED_BULK.ordinal()
                    ]
            );
            assertEquals(
                    12345L,
                    receiver.getDependencyDomainPendingBytes()[
                            DependencyDomain.GUARDED_BULK.ordinal()
                    ]
            );
            assertEquals(1L, receiver.getCausalBulkFramesOutbound());
            assertEquals(8192L, receiver.getCausalBulkBytesOutbound());
            assertEquals(
                    65536L,
                    receiver.getCausalAtomicBundleMaxBytesOutbound()
            );
            assertTrue(receiver.isRemoteAdmissionDiagnosticsSupported());
            assertEquals(786432L, receiver.getBytePacingTarget());
            assertEquals(250D, receiver.getBurstFloorPps());
            assertTrue(receiver.isRttPressureActive());
            assertEquals("CALIBRATING", receiver.getResumeState());
            assertEquals(1, receiver.getResumeValidatedRounds());
        } finally {
            payload.release();
        }
    }

    @Test
    void previousCausalTailRemainsReadableWithoutAdmissionDiagnostics() {
        final SimpleMetricsLogger sender = new SimpleMetricsLogger();
        sender.causalBulkFrameOutbound(512);
        final ByteBuf payload = Unpooled.buffer();
        try {
            MetricsSynchronizationHandler.writePayload(
                    payload,
                    sender,
                    8,
                    12345L
            );
            payload.writerIndex(873);

            final MetricsSynchronizationHandler receiver =
                    new MetricsSynchronizationHandler();
            assertTrue(receiver.readPayload(payload));
            assertTrue(receiver.isRemoteCausalSchedulerSupported());
            assertFalse(
                    receiver.isRemoteAdmissionDiagnosticsSupported()
            );
        } finally {
            payload.release();
        }
    }

    @Test
    void previousOrderedHolExtensionRemainsReadableWithoutCausalTail() {
        final SimpleMetricsLogger sender = new SimpleMetricsLogger();
        sender.orderedHolProbe(7, 600);
        final ByteBuf payload = Unpooled.buffer();
        try {
            MetricsSynchronizationHandler.writePayload(
                    payload,
                    sender,
                    8,
                    12345L
            );
            payload.writerIndex(801);

            final MetricsSynchronizationHandler receiver =
                    new MetricsSynchronizationHandler();
            assertTrue(receiver.readPayload(payload));
            assertTrue(receiver.isRemoteOrderedHolProbeSupported());
            assertFalse(receiver.isRemoteCausalSchedulerSupported());
        } finally {
            payload.release();
        }
    }

    @Test
    void previousAdvancedRecoveryExtensionRemainsReadableWithoutOrderedHolProbeTail() {
        final SimpleMetricsLogger sender = new SimpleMetricsLogger();
        sender.rackRetransmit(512);
        final ByteBuf payload = Unpooled.buffer();
        try {
            MetricsSynchronizationHandler.writePayload(payload, sender, 8, 12345L);
            payload.writerIndex(773);

            final MetricsSynchronizationHandler receiver = new MetricsSynchronizationHandler();
            assertTrue(receiver.readPayload(payload));
            assertTrue(receiver.isRemoteAdvancedRecoverySupported());
            assertEquals(512L, receiver.getRackRetransmitBytes());
            assertFalse(receiver.isRemoteOrderedHolProbeSupported());
        } finally {
            payload.release();
        }
    }

    @Test
    void previousFecExtensionRemainsReadableWithoutAdvancedRecoveryTail() {
        final SimpleMetricsLogger sender = new SimpleMetricsLogger();
        sender.fecRecovered(3);
        final ByteBuf payload = Unpooled.buffer();
        try {
            MetricsSynchronizationHandler.writePayload(payload, sender, 8, 12345L);
            payload.writerIndex(385);

            final MetricsSynchronizationHandler receiver = new MetricsSynchronizationHandler();
            assertTrue(receiver.readPayload(payload));
            assertTrue(receiver.isRemoteFecSupported());
            assertEquals(3L, receiver.getFecRecovered());
            assertFalse(receiver.isRemoteAdvancedRecoverySupported());
        } finally {
            payload.release();
        }
    }

    @Test
    void previousAckPolicyExtensionRemainsReadableWithoutDemandTail() {
        final SimpleMetricsLogger sender = new SimpleMetricsLogger();
        sender.adaptiveAckPolicy(true, 8_000_000L, 10_000_000L);
        final ByteBuf payload = Unpooled.buffer();
        try {
            MetricsSynchronizationHandler.writePayload(payload, sender, 8, 12345L);
            payload.writerIndex(278);

            final MetricsSynchronizationHandler receiver = new MetricsSynchronizationHandler();
            assertTrue(receiver.readPayload(payload));
            assertTrue(receiver.isRemoteAckPolicySupported());
            assertFalse(receiver.isRemoteDemandSupported());
        } finally {
            payload.release();
        }
    }

    @Test
    void previousNackPolicyExtensionRemainsReadableWithoutNackRepeatAndFecTail() {
        final SimpleMetricsLogger sender = new SimpleMetricsLogger();
        sender.nackGraceBypassed(2);
        final ByteBuf payload = Unpooled.buffer();
        try {
            MetricsSynchronizationHandler.writePayload(payload, sender, 8, 12345L);
            payload.writerIndex(321);

            final MetricsSynchronizationHandler receiver = new MetricsSynchronizationHandler();
            assertTrue(receiver.readPayload(payload));
            assertTrue(receiver.isRemoteNackPolicySupported());
            assertFalse(receiver.isRemoteNackRepeatSupported());
            assertFalse(receiver.isRemoteFecSupported());
        } finally {
            payload.release();
        }
    }

    @Test
    void previousRecoveryExtensionRemainsReadableWithoutBytePacingTail() {
        final SimpleMetricsLogger sender = new SimpleMetricsLogger();
        sender.nackDeferred(9);
        final ByteBuf payload = Unpooled.buffer();
        try {
            MetricsSynchronizationHandler.writePayload(payload, sender, 8, 12345L);
            payload.writerIndex(149);

            final MetricsSynchronizationHandler receiver = new MetricsSynchronizationHandler();
            assertTrue(receiver.readPayload(payload));
            assertTrue(receiver.isRemoteRecoverySupported());
            assertEquals(9L, receiver.getNacksDeferred());
            assertFalse(receiver.isRemoteBytePacingSupported());
            assertFalse(receiver.isRemoteHolSupported());
            assertFalse(receiver.isRemoteApplicationBatchSupported());
            assertFalse(receiver.isRemoteAckPolicySupported());
        } finally {
            payload.release();
        }
    }

    @Test
    void previousApplicationBatchExtensionRemainsReadableWithoutAckPolicyTail() {
        final SimpleMetricsLogger sender = new SimpleMetricsLogger();
        sender.applicationBatch(4096);
        final ByteBuf payload = Unpooled.buffer();
        try {
            MetricsSynchronizationHandler.writePayload(payload, sender, 8, 12345L);
            payload.writerIndex(245);

            final MetricsSynchronizationHandler receiver = new MetricsSynchronizationHandler();
            assertTrue(receiver.readPayload(payload));
            assertTrue(receiver.isRemoteApplicationBatchSupported());
            assertEquals(1L, receiver.getApplicationBatches());
            assertFalse(receiver.isRemoteAckPolicySupported());
        } finally {
            payload.release();
        }
    }

    @Test
    void previousAdaptiveExtensionRemainsReadableWithoutRecoveryTail() {
        final SimpleMetricsLogger sender = new SimpleMetricsLogger();
        sender.adaptivePacingRate(123D);
        final ByteBuf payload = Unpooled.buffer();
        try {
            MetricsSynchronizationHandler.writePayload(payload, sender, 8, 12345L);
            payload.writerIndex(117); // base 33 bytes + the previous 84-byte adaptive extension

            final MetricsSynchronizationHandler receiver = new MetricsSynchronizationHandler();
            assertTrue(receiver.readPayload(payload));
            assertTrue(receiver.isRemoteAdaptiveSupported());
            assertEquals(123D, receiver.getPacingRate());
            assertFalse(receiver.isRemoteRecoverySupported());
        } finally {
            payload.release();
        }
    }
}
