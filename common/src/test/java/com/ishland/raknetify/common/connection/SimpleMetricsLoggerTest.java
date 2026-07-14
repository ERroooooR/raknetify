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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleMetricsLoggerTest {

    @Test
    void extendedMetricsJsonlFormatsWithoutMissingArguments() {
        final SimpleMetricsLogger logger = new SimpleMetricsLogger();
        logger.congestionDiagnostics("RTT_INFLATION_LOSS", 4.5D, true, true);
        logger.nackDeferred(3);
        logger.reorderedPacket(2);
        logger.nackRetransmit(1400);
        logger.timeoutRetransmit(700);

        final String line = assertDoesNotThrow(() -> logger.formatMetricsJsonl(123L));

        assertTrue(line.contains("\"congestion_reason\":\"RTT_INFLATION_LOSS\""));
        assertTrue(line.contains("\"rtt_inflation\":4.500000"));
        assertTrue(line.contains("\"remote_adaptive_supported\":false"));
        assertTrue(line.contains("\"nack_deferred\":3"));
        assertTrue(line.contains("\"reordered_packets\":2"));
        assertTrue(line.contains("\"nack_retransmit_bytes\":1400"));
        assertTrue(line.contains("\"timeout_retransmit_bytes\":700"));
        assertTrue(line.contains("\"remote_supported\":false"));
        assertTrue(line.contains("\"remote_recovery_supported\":false"));
        assertTrue(line.contains("\"remote_loss_type\":\"UNAVAILABLE\""));
    }
}
