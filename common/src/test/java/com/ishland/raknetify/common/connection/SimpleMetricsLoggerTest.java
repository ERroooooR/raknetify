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
        logger.nackDeferredExpired(1);
        logger.nackDeferredConfirmed(2);
        logger.nackGraceBypassed(7);
        logger.adaptiveNackGrace(true);
        logger.nackRepeated(2);
        logger.nackRetransmit(1400);
        logger.timeoutRetransmit(700);
        logger.rackRetransmit(512);
        logger.rackSpuriousAck(2);
        logger.ptoProbe(256);
        logger.ptoProbeAcked(256);
        logger.orderedHolProbe(7, 320);
        logger.orderedHolProbeAcked(320);
        logger.ptoState(1, 42_000_000L);
        logger.orderedChannelPending(2, 3, 90_000_000L, 17);
        logger.orderedChannelRelease(2, 4, 100_000_000L);
        logger.applicationLimitedRecovery(300);
        logger.recoveryQueueState(2, 75_000_000L);
        logger.recoveryDebt(2.5D, 2);
        logger.targetedFecRepair(2, 384);
        logger.targetedFecRecovered(1);
        logger.adaptiveBytePacingRate(512000L);
        logger.fragmentReassemblyPending(2, 8192L, 5_000_000L);
        logger.fragmentReassemblyComplete(4096, 8_000_000L);
        logger.orderedQueuePending(3, 6_000_000L);
        logger.orderedQueueRelease(4, 9_000_000L);
        logger.applicationBatch(32768);
        logger.dependencyDomainQueued(DependencyDomain.GUARDED_BULK, 8192);
        logger.dependencyDomainSent(DependencyDomain.GUARDED_BULK, 8192);
        logger.ackRepeated(3);
        logger.adaptiveAckPolicy(true, 8_000_000L, 10_000_000L);
        logger.adaptiveDemand(false, "BULK", 150_000_000L, 4L);
        logger.adaptivePathModel(1048576L, true, "UNVALIDATED", 1);
        logger.pacingScheduler(5_000_000L, 3);
        logger.fecRecovered(2);
        logger.fecParity(4, 4096);
        logger.fecExpired(1);
        logger.fecBudget(10, 1, 0.25D);

        final String line = assertDoesNotThrow(() -> logger.formatMetricsJsonl(123L));

        assertTrue(line.contains("\"congestion_reason\":\"RTT_INFLATION_LOSS\""));
        assertTrue(line.contains("\"rtt_inflation\":4.500000"));
        assertTrue(line.contains("\"remote_adaptive_supported\":false"));
        assertTrue(line.contains("\"nack_deferred\":3"));
        assertTrue(line.contains("\"reordered_packets\":2"));
        assertTrue(line.contains("\"nack_deferred_expired\":1"));
        assertTrue(line.contains("\"nack_deferred_confirmed\":2"));
        assertTrue(line.contains("\"nack_grace_bypassed\":7"));
        assertTrue(line.contains("\"nack_grace_bypass\":true"));
        assertTrue(line.contains("\"nack_repeated_packets\":1"));
        assertTrue(line.contains("\"nack_repeated_framesets\":2"));
        assertTrue(line.contains("\"nack_retransmit_bytes\":1400"));
        assertTrue(line.contains("\"timeout_retransmit_bytes\":700"));
        assertTrue(line.contains("\"rack_retransmit_bytes\":512"));
        assertTrue(line.contains("\"rack_retransmit_framesets\":1"));
        assertTrue(line.contains("\"rack_spurious_acks\":2"));
        assertTrue(line.contains("\"pto_probes\":1"));
        assertTrue(line.contains("\"pto_probe_bytes\":256"));
        assertTrue(line.contains("\"pto_probe_acked_bytes\":256"));
        assertTrue(line.contains("\"ordered_hol_probes\":1"));
        assertTrue(line.contains("\"ordered_hol_probe_bytes\":320"));
        assertTrue(line.contains("\"ordered_hol_probe_acked_bytes\":320"));
        assertTrue(line.contains("\"ordered_hol_probe_channel\":7"));
        assertTrue(line.contains("\"pto_count\":1"));
        assertTrue(line.contains("\"last_ack_progress_age_ns\":42000000"));
        assertTrue(line.contains("\"ordered_worst_channel\":2"));
        assertTrue(line.contains("\"ordered_channel_pending\":[0, 0, 3, 0, 0, 0, 0, 0]"));
        assertTrue(line.contains("\"ordered_channel_blocked_order_index\":[-1, -1, 17, -1, -1, -1, -1, -1]"));
        assertTrue(line.contains("\"application_limited_recovery_packets\":1"));
        assertTrue(line.contains("\"application_limited_recovery_bytes\":300"));
        assertTrue(line.contains("\"recovery_queue_depth\":2"));
        assertTrue(line.contains("\"recovery_debt\":2.500000"));
        assertTrue(line.contains("\"recovery_debt_channel\":2"));
        assertTrue(line.contains("\"targeted_fec_packets\":1"));
        assertTrue(line.contains("\"targeted_fec_bytes\":384"));
        assertTrue(line.contains("\"targeted_fec_recovered\":1"));
        assertTrue(line.contains("\"byte_pacing_bps\":512000"));
        assertTrue(line.contains("\"validated_path_bps\":1048576"));
        assertTrue(line.contains("\"delivery_sample_application_limited\":true"));
        assertTrue(line.contains("\"resume_state\":\"UNVALIDATED\""));
        assertTrue(line.contains("\"resume_validated_rounds\":1"));
        assertTrue(line.contains("\"pacer_wakeup_lateness_ns\":5000000"));
        assertTrue(line.contains("\"pacer_batch_datagrams\":3"));
        assertTrue(line.contains("\"fragment_pending_builders\":2"));
        assertTrue(line.contains("\"ordered_pending_frames\":3"));
        assertTrue(line.contains("\"application_batches\":1"));
        assertTrue(line.contains("\"application_batch_max_bytes\":32768"));
        assertTrue(line.contains("\"dependency_domain_queued_frames\":[0, 0, 0, 1]"));
        assertTrue(line.contains("\"dependency_domain_sent_bytes\":[0, 0, 0, 8192]"));
        assertTrue(line.contains("\"dependency_domain_pending_frames\":[0, 0, 0, 0]"));
        assertTrue(line.contains("\"ack_repeated_packets\":1"));
        assertTrue(line.contains("\"ack_repeated_framesets\":3"));
        assertTrue(line.contains("\"ack_protection\":true"));
        assertTrue(line.contains("\"ack_flush_delay_ns\":8000000"));
        assertTrue(line.contains("\"ack_repeat_delay_ns\":10000000"));
        assertTrue(line.contains("\"remote_ack_policy_supported\":false"));
        assertTrue(line.contains("\"remote_ordered_hol_probe_supported\":false"));
        assertTrue(line.contains("\"application_limited\":false"));
        assertTrue(line.contains("\"backlog_state\":\"BULK\""));
        assertTrue(line.contains("\"backlog_age_ns\":150000000"));
        assertTrue(line.contains("\"backlog_probes\":4"));
        assertTrue(line.contains("\"remote_demand_supported\":false"));
        assertTrue(line.contains("\"remote_supported\":false"));
        assertTrue(line.contains("\"remote_recovery_supported\":false"));
        assertTrue(line.contains("\"remote_nack_outcome_supported\":false"));
        assertTrue(line.contains("\"remote_nack_policy_supported\":false"));
        assertTrue(line.contains("\"remote_nack_repeat_supported\":false"));
        assertTrue(line.contains("\"remote_fec_supported\":false"));
        assertTrue(line.contains("\"remote_loss_type\":\"UNAVAILABLE\""));
    }
}
