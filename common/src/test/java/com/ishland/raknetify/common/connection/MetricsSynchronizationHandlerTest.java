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
        sender.adaptiveDeliveryRate(45678L);
        sender.adaptiveLoss(0.125D, 70L, 10L);
        sender.adaptiveLossType("RATE_LIMIT");
        sender.congestionControl("DRAIN", 8192L, 2048L, 65536L, 0L, 0D);
        sender.reliableFrameDuplicate(7);
        sender.nackDeferred(11);
        sender.reorderedPacket(5);
        sender.nackRetransmit(4096);
        sender.timeoutRetransmit(1024);
        sender.congestionDiagnostics("NON_CONGESTIVE_HIGH_LOSS", 1.2D, true, true);

        final ByteBuf payload = Unpooled.buffer();
        try {
            MetricsSynchronizationHandler.writePayload(payload, sender, 8, 12345L);
            assertEquals(149, payload.readableBytes());

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
            assertTrue(receiver.isPacingCapped());
            assertTrue(receiver.isBandwidthProbeSuppressed());
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
