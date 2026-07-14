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
        sender.adaptiveDeliveryRate(45678L);
        sender.adaptiveLoss(0.125D, 70L, 10L);
        sender.adaptiveLossType("RATE_LIMIT");
        sender.congestionControl("DRAIN", 8192L, 2048L, 65536L, 0L, 0D);
        sender.reliableFrameDuplicate(7);
        sender.nackDeferred(11);
        sender.reorderedPacket(5);
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
        sender.congestionDiagnostics("NON_CONGESTIVE_HIGH_LOSS", 1.2D, true, true);

        final ByteBuf payload = Unpooled.buffer();
        try {
            MetricsSynchronizationHandler.writePayload(payload, sender, 8, 12345L);
            assertEquals(278, payload.readableBytes());

            final MetricsSynchronizationHandler receiver = new MetricsSynchronizationHandler();
            assertTrue(receiver.readPayload(payload));
            assertTrue(receiver.isRemoteSupported());
            assertTrue(receiver.isRemoteAdaptiveSupported());
            assertEquals(1234, receiver.getQueuedBytes());
            assertEquals("RATE_LIMIT", receiver.getLossType());
            assertEquals("DRAIN", receiver.getCongestionMode());
            assertEquals("NON_CONGESTIVE_HIGH_LOSS", receiver.getCongestionReason());
            assertEquals(37.5D, receiver.getPacingRate());
            assertEquals(7L, receiver.getReliableFrameDuplicates());
            assertTrue(receiver.isRemoteRecoverySupported());
            assertEquals(11L, receiver.getNacksDeferred());
            assertEquals(5L, receiver.getReorderedPackets());
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
            assertTrue(receiver.isPacingCapped());
            assertTrue(receiver.isBandwidthProbeSuppressed());
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
