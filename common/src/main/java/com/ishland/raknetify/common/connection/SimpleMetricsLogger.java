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
import com.ishland.raknetify.common.util.MathUtil;
import network.ycc.raknet.RakNet;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This implementation is only designed to be modified single-threaded
 */
@SuppressWarnings("NonAtomicOperationOnVolatileField")
public class SimpleMetricsLogger implements RakNet.MetricsLogger {

    // ========== Loggers ==========

    private volatile long packetsIn = 0L;
    private volatile long framesIn = 0L;
    private volatile long reliableFrameDuplicates = 0L;
    private volatile long nacksDeferred = 0L;
    private volatile long reorderedPackets = 0L;
    private volatile long nacksDeferredExpired = 0L;
    private volatile long nacksDeferredConfirmed = 0L;
    private volatile long nackGraceBypassed = 0L;
    private volatile boolean adaptiveNackGraceBypass;
    private volatile long nackRepeatedPackets = 0L;
    private volatile long nackRepeatedFrameSets = 0L;
    private volatile long nackRetransmitBytes = 0L;
    private volatile long timeoutRetransmitBytes = 0L;
    private volatile long rackRetransmitBytes = 0L;
    private volatile long rackRetransmitFrameSets = 0L;
    private volatile long rackSpuriousAcks = 0L;
    private volatile long ptoProbes = 0L;
    private volatile long ptoProbeBytes = 0L;
    private volatile long ptoProbeAckedBytes = 0L;
    private volatile long orderedHolProbes = 0L;
    private volatile long orderedHolProbeBytes = 0L;
    private volatile long orderedHolProbeAckedBytes = 0L;
    private volatile int orderedHolProbeChannel = -1;
    private volatile int ptoCount = 0;
    private volatile long lastAckProgressAgeNanos = 0L;
    private volatile long applicationLimitedRecoveryPackets = 0L;
    private volatile long applicationLimitedRecoveryBytes = 0L;
    private volatile int recoveryQueueDepth = 0;
    private volatile long recoveryQueueOldestAgeNanos = 0L;
    private volatile double recoveryDebt = 0D;
    private volatile int recoveryDebtChannel = -1;
    private volatile long targetedFecPackets = 0L;
    private volatile long targetedFecBytes = 0L;
    private volatile long targetedFecRecovered = 0L;
    private volatile int targetedFecChannel = -1;
    private volatile int fragmentPendingBuilders = 0;
    private volatile long fragmentPendingBytes = 0L;
    private volatile long fragmentOldestAgeNanos = 0L;
    private volatile long fragmentCompleted = 0L;
    private volatile long fragmentCompletedBytes = 0L;
    private volatile long fragmentMaxAgeNanos = 0L;
    private volatile int orderedPendingFrames = 0;
    private volatile long orderedOldestAgeNanos = 0L;
    private volatile long orderedReleasedFrames = 0L;
    private volatile long orderedMaxWaitNanos = 0L;
    private final int[] orderedChannelPending = new int[8];
    private final long[] orderedChannelOldestAgeNanos = new long[8];
    private final int[] orderedChannelBlockedOrderIndex = new int[8];
    private final long[] orderedChannelReleasedFrames = new long[8];
    private final long[] orderedChannelMaxWaitNanos = new long[8];
    private volatile int orderedWorstChannel = -1;
    private volatile long applicationBatches = 0L;
    private volatile long applicationBatchBytes = 0L;
    private volatile long applicationBatchMaxBytes = 0L;
    private final long[] dependencyDomainQueuedFrames =
            new long[DependencyDomain.values().length];
    private final long[] dependencyDomainQueuedBytes =
            new long[DependencyDomain.values().length];
    private final long[] dependencyDomainSentFrames =
            new long[DependencyDomain.values().length];
    private final long[] dependencyDomainSentBytes =
            new long[DependencyDomain.values().length];
    private final long[] dependencyDomainPendingFrames =
            new long[DependencyDomain.values().length];
    private final long[] dependencyDomainPendingBytes =
            new long[DependencyDomain.values().length];
    private volatile long ackRepeatedPackets = 0L;
    private volatile long ackRepeatedFrameSets = 0L;
    private volatile long framesError = 0L;
    private volatile long bytesIn = 0L;
    private volatile long packetsOut = 0L;
    private volatile long framesOut = 0L;
    private volatile long bytesOut = 0L;
    private volatile long bytesRecalled = 0L;
    private volatile long bytesACKd = 0L;
    private volatile long bytesNACKd = 0L;
    private volatile long acksSent = 0L;
    private volatile long nacksSent = 0L;

    private volatile long measureRTTns = 0L;
    private volatile long measureRTTnsStdDev = 0L;
    private volatile long measureBurstTokens = 0L;
    private volatile int currentQueuedBytes = 0;
    private volatile double adaptivePacingRate;
    private volatile long adaptiveBytePacingRate;
    private volatile boolean adaptiveAckProtection;
    private volatile long adaptiveAckFlushDelayNanos;
    private volatile long adaptiveAckRepeatDelayNanos;
    private volatile boolean applicationLimited = true;
    private volatile String backlogState = "IDLE";
    private volatile long backlogAgeNanos;
    private volatile long backlogProbes;
    private volatile long validatedPathRate;
    private volatile boolean deliverySampleApplicationLimited;
    private volatile String resumeState = "IDLE";
    private volatile int resumeValidatedRounds;
    private volatile long adaptiveDeliveryRate;
    private volatile double adaptiveLossRatio;
    private volatile long adaptiveAcknowledged;
    private volatile long adaptiveLost;
    private volatile String adaptiveLossType = "NONE";
    private volatile int adaptiveMTU;
    private volatile long fecRecovered;
    private volatile long fecParityPackets;
    private volatile long fecParityBytes;
    private volatile long fecExpired;
    private volatile long mtuProbesSent;
    private volatile long mtuProbesAcknowledged;
    private volatile long mtuProbesTimedOut;
    private volatile int adaptiveDscp = -1;
    private volatile long smallWriteBatches;
    private volatile long smallWriteFrames;
    private volatile long smallWriteDelayNanos;
    private volatile long pacingDelayNanos;
    private volatile long pacerWakeupLatenessNanos;
    private volatile int pacerBatchDatagrams;
    private volatile int pacerMaxBatchDatagrams;
    private volatile String congestionMode = "STARTUP";
    private volatile long congestionWindowBytes;
    private volatile long inFlightBytes;
    private volatile long bandwidthBytesPerSecond;
    private volatile long ackAggregationBytes;
    private volatile double ecnCeRatio;
    private volatile String congestionReason = "NONE";
    private volatile double rttInflation = 1D;
    private volatile boolean pacingCapped;
    private volatile boolean bandwidthProbeSuppressed;
    private volatile String pathMtuState = "SEARCHING";
    private volatile int pathMtuProbe;
    private volatile int pathMtuMaximum;
    private volatile int fecDataShards;
    private volatile int fecParityShards;
    private volatile double fecRecoveryRatio;

    private static final AtomicBoolean METRICS_FILE_DISABLED = new AtomicBoolean();
    private static final Path METRICS_FILE = metricsFile();
    private static final BlockingQueue<String> METRICS_LINES = new ArrayBlockingQueue<>(8192);
    private static final AtomicLong METRICS_LINES_DROPPED = new AtomicLong();
    private static final AtomicBoolean METRICS_WRITER_STARTED = new AtomicBoolean();

    public SimpleMetricsLogger() {
        Arrays.fill(orderedChannelBlockedOrderIndex, -1);
        initializeMetricsExport();
    }

    /**
     * Initializes the optional JSONL exporter without waiting for the first
     * RakNet connection. Platform entry points call this method during startup.
     */
    public static void initializeMetricsExport() {
        if (METRICS_FILE == null || METRICS_FILE_DISABLED.get()
                || !METRICS_WRITER_STARTED.compareAndSet(false, true)) {
            return;
        }

        try {
            final Path parent = METRICS_FILE.getParent();
            if (parent != null) Files.createDirectories(parent);
            // Validate and create the configured file synchronously. This makes
            // startup failures visible even before a connection produces data.
            try (BufferedWriter ignored = Files.newBufferedWriter(METRICS_FILE, StandardCharsets.UTF_8,
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND)) {
                // Intentionally empty.
            }
        } catch (IOException | RuntimeException e) {
            disableMetricsExport("failed to initialize", e);
            return;
        }

        System.out.println("Raknetify: metrics JSONL export enabled: " + METRICS_FILE);
        startMetricsWriter();
    }

    @Override
    public void packetsIn(int delta) {
        packetsIn += delta;
    }

    @Override
    public void framesIn(int delta) {
        framesIn += delta;
    }

    @Override
    public void reliableFrameDuplicate(int delta) {
        reliableFrameDuplicates += delta;
    }

    @Override
    public void nackDeferred(int delta) { nacksDeferred += delta; }

    @Override
    public void reorderedPacket(int delta) { reorderedPackets += delta; }

    @Override
    public void nackDeferredExpired(int delta) { nacksDeferredExpired += delta; }

    @Override
    public void nackDeferredConfirmed(int delta) { nacksDeferredConfirmed += delta; }

    @Override
    public void nackGraceBypassed(int delta) { nackGraceBypassed += delta; }

    @Override
    public void adaptiveNackGrace(boolean bypassed) { adaptiveNackGraceBypass = bypassed; }

    @Override
    public void nackRepeated(int requestedFrameSets) {
        nackRepeatedPackets++;
        nackRepeatedFrameSets += requestedFrameSets;
    }

    @Override
    public void nackRetransmit(int bytes) { nackRetransmitBytes += bytes; }

    @Override
    public void timeoutRetransmit(int bytes) { timeoutRetransmitBytes += bytes; }

    @Override
    public void rackRetransmit(int bytes) {
        rackRetransmitBytes += bytes;
        rackRetransmitFrameSets++;
    }

    @Override
    public void rackSpuriousAck(int delta) { rackSpuriousAcks += delta; }

    @Override
    public void ptoProbe(int bytes) {
        ptoProbes++;
        ptoProbeBytes += bytes;
    }

    @Override
    public void ptoProbeAcked(int bytes) { ptoProbeAckedBytes += bytes; }

    @Override
    public void orderedHolProbe(int channel, int bytes) {
        orderedHolProbes++;
        orderedHolProbeBytes += bytes;
        orderedHolProbeChannel = channel;
    }

    @Override
    public void orderedHolProbeAcked(int bytes) { orderedHolProbeAckedBytes += bytes; }

    @Override
    public void ptoState(int count, long lastAckProgressAgeNanos) {
        ptoCount = count;
        this.lastAckProgressAgeNanos = lastAckProgressAgeNanos;
    }

    @Override
    public void applicationLimitedRecovery(int bytes) {
        applicationLimitedRecoveryPackets++;
        applicationLimitedRecoveryBytes += bytes;
    }

    @Override
    public void recoveryQueueState(int depth, long oldestAgeNanos) {
        recoveryQueueDepth = depth;
        recoveryQueueOldestAgeNanos = oldestAgeNanos;
    }

    @Override
    public void recoveryDebt(double debt, int channel) {
        recoveryDebt = debt;
        recoveryDebtChannel = channel;
    }

    @Override
    public void targetedFecRepair(int channel, int bytes) {
        targetedFecPackets++;
        targetedFecBytes += bytes;
        targetedFecChannel = channel;
    }

    @Override
    public void targetedFecRecovered(int packets) { targetedFecRecovered += packets; }

    @Override
    public void fragmentReassemblyPending(int builders, long bytes, long oldestAgeNanos) {
        fragmentPendingBuilders = builders;
        fragmentPendingBytes = bytes;
        fragmentOldestAgeNanos = oldestAgeNanos;
    }

    @Override
    public void fragmentReassemblyComplete(int bytes, long ageNanos) {
        fragmentCompleted++;
        fragmentCompletedBytes += bytes;
        fragmentMaxAgeNanos = Math.max(fragmentMaxAgeNanos, ageNanos);
    }

    @Override
    public void orderedQueuePending(int frames, long oldestAgeNanos) {
        orderedPendingFrames = frames;
        orderedOldestAgeNanos = oldestAgeNanos;
    }

    @Override
    public void orderedQueueRelease(int frames, long oldestWaitNanos) {
        orderedReleasedFrames += frames;
        orderedMaxWaitNanos = Math.max(orderedMaxWaitNanos, oldestWaitNanos);
    }

    @Override
    public void orderedChannelPending(int channel, int frames, long oldestAgeNanos,
                                      int blockedOrderIndex) {
        if (channel < 0 || channel >= orderedChannelPending.length) return;
        orderedChannelPending[channel] = frames;
        orderedChannelOldestAgeNanos[channel] = oldestAgeNanos;
        orderedChannelBlockedOrderIndex[channel] = blockedOrderIndex;
        int worst = -1;
        long worstAge = -1L;
        for (int i = 0; i < orderedChannelPending.length; i++) {
            if (orderedChannelPending[i] > 0 && orderedChannelOldestAgeNanos[i] > worstAge) {
                worst = i;
                worstAge = orderedChannelOldestAgeNanos[i];
            }
        }
        orderedWorstChannel = worst;
    }

    @Override
    public void orderedChannelRelease(int channel, int frames, long oldestWaitNanos) {
        if (channel < 0 || channel >= orderedChannelPending.length) return;
        orderedChannelReleasedFrames[channel] += frames;
        orderedChannelMaxWaitNanos[channel] = Math.max(
                orderedChannelMaxWaitNanos[channel], oldestWaitNanos);
    }

    @Override
    public void applicationBatch(int bytes) {
        applicationBatches++;
        applicationBatchBytes += bytes;
        applicationBatchMaxBytes = Math.max(applicationBatchMaxBytes, bytes);
    }

    public synchronized void dependencyDomainQueued(
            DependencyDomain domain,
            int bytes
    ) {
        final int index = domain.ordinal();
        dependencyDomainQueuedFrames[index]++;
        dependencyDomainQueuedBytes[index] += bytes;
        dependencyDomainPendingFrames[index]++;
        dependencyDomainPendingBytes[index] += bytes;
    }

    public synchronized void dependencyDomainSent(
            DependencyDomain domain,
            int bytes
    ) {
        final int index = domain.ordinal();
        dependencyDomainSentFrames[index]++;
        dependencyDomainSentBytes[index] += bytes;
        dependencyDomainPendingFrames[index] = Math.max(
                0,
                dependencyDomainPendingFrames[index] - 1
        );
        dependencyDomainPendingBytes[index] = Math.max(
                0,
                dependencyDomainPendingBytes[index] - bytes
        );
    }

    public synchronized void dependencyDomainDiscarded(
            DependencyDomain domain,
            int bytes
    ) {
        final int index = domain.ordinal();
        dependencyDomainPendingFrames[index] = Math.max(
                0,
                dependencyDomainPendingFrames[index] - 1
        );
        dependencyDomainPendingBytes[index] = Math.max(
                0,
                dependencyDomainPendingBytes[index] - bytes
        );
    }

    @Override
    public void ackRepeated(int acknowledgedFrameSets) {
        ackRepeatedPackets++;
        ackRepeatedFrameSets += acknowledgedFrameSets;
    }

    @Override
    public void frameError(int delta) {
        framesError += delta;
    }

    @Override
    public void bytesIn(int delta) {
        bytesIn += delta;
    }

    @Override
    public void packetsOut(int delta) {
        packetsOut += delta;
    }

    @Override
    public void framesOut(int delta) {
        framesOut += delta;
    }

    @Override
    public void bytesOut(int delta) {
        bytesOut += delta;
        tick();
    }

    @Override
    public void bytesRecalled(int delta) {
        bytesRecalled += delta;
    }

    @Override
    public void bytesACKd(int delta) {
        bytesACKd += delta;
    }

    @Override
    public void bytesNACKd(int delta) {
        bytesNACKd += delta;
    }

    @Override
    public void acksSent(int delta) {
        acksSent += delta;
    }

    @Override
    public void nacksSent(int delta) {
        nacksSent += delta;
    }

    @Override
    public void measureRTTns(long n) {
        measureRTTns = n;
    }

    @Override
    public void measureRTTnsStdDev(long n) {
        measureRTTnsStdDev = n;
        tick();
    }

    @Override
    public void measureBurstTokens(int n) {
        measureBurstTokens = n;
    }

    @Override
    public void currentQueuedBytes(int bytes) {
        currentQueuedBytes = bytes;
    }

    @Override
    public void adaptivePacingRate(double packetsPerSecond) { adaptivePacingRate = packetsPerSecond; }

    @Override
    public void adaptiveBytePacingRate(long bytesPerSecond) { adaptiveBytePacingRate = bytesPerSecond; }

    @Override
    public void adaptiveAckPolicy(boolean protectedMode, long flushDelayNanos, long repeatDelayNanos) {
        adaptiveAckProtection = protectedMode;
        adaptiveAckFlushDelayNanos = flushDelayNanos;
        adaptiveAckRepeatDelayNanos = repeatDelayNanos;
    }

    @Override
    public void adaptiveDemand(boolean applicationLimited, String backlogState,
                               long backlogAgeNanos, long backlogProbes) {
        this.applicationLimited = applicationLimited;
        this.backlogState = backlogState;
        this.backlogAgeNanos = backlogAgeNanos;
        this.backlogProbes = backlogProbes;
    }

    @Override
    public void adaptivePathModel(long validatedRateBytesPerSecond, boolean sampleApplicationLimited,
                                  String resumeState, int validatedRounds) {
        this.validatedPathRate = validatedRateBytesPerSecond;
        this.deliverySampleApplicationLimited = sampleApplicationLimited;
        this.resumeState = resumeState;
        this.resumeValidatedRounds = validatedRounds;
    }

    @Override
    public void adaptiveDeliveryRate(long bytesPerSecond) { adaptiveDeliveryRate = bytesPerSecond; }

    @Override
    public void adaptiveLoss(double ratio, long acknowledged, long lost) {
        adaptiveLossRatio = ratio;
        adaptiveAcknowledged = acknowledged;
        adaptiveLost = lost;
    }

    @Override
    public void adaptiveLossType(String type) { adaptiveLossType = type; }

    @Override
    public void adaptiveMTU(int mtu) { adaptiveMTU = mtu; }

    @Override
    public void fecRecovered(int delta) { fecRecovered += delta; }

    @Override
    public void fecParity(int packets, int bytes) {
        fecParityPackets += packets;
        fecParityBytes += bytes;
    }

    @Override
    public void fecExpired(int delta) { fecExpired += delta; }

    @Override
    public void pathMtuProbeResult(String result, int mtu) {
        if ("sent".equals(result)) mtuProbesSent++;
        else if ("acknowledged".equals(result)) mtuProbesAcknowledged++;
        else if ("timeout".equals(result)) mtuProbesTimedOut++;
    }

    @Override
    public void adaptiveDscp(int ipTos) { adaptiveDscp = ipTos; }

    @Override
    public void smallWriteBatch(int frames, long delayNanos) {
        smallWriteBatches++;
        smallWriteFrames += frames;
        smallWriteDelayNanos = delayNanos;
    }

    @Override
    public void pacingDelay(long delayNanos) { pacingDelayNanos = delayNanos; }

    @Override
    public void pacingScheduler(long wakeupLatenessNanos, int datagrams) {
        pacerWakeupLatenessNanos = wakeupLatenessNanos;
        pacerBatchDatagrams = datagrams;
        pacerMaxBatchDatagrams = Math.max(pacerMaxBatchDatagrams, datagrams);
    }

    @Override
    public void congestionControl(String mode, long congestionWindowBytes, long inFlightBytes,
                                  long bandwidthBytesPerSecond, long ackAggregationBytes,
                                  double ecnCeRatio) {
        this.congestionMode = mode;
        this.congestionWindowBytes = congestionWindowBytes;
        this.inFlightBytes = inFlightBytes;
        this.bandwidthBytesPerSecond = bandwidthBytesPerSecond;
        this.ackAggregationBytes = ackAggregationBytes;
        this.ecnCeRatio = ecnCeRatio;
    }

    @Override
    public void congestionDiagnostics(String reason, double rttInflation, boolean pacingCapped,
                                      boolean bandwidthProbeSuppressed) {
        this.congestionReason = reason;
        this.rttInflation = rttInflation;
        this.pacingCapped = pacingCapped;
        this.bandwidthProbeSuppressed = bandwidthProbeSuppressed;
    }

    @Override
    public void pathMtuState(String state, int confirmedMtu, int probeMtu, int maximumMtu) {
        this.pathMtuState = state;
        this.adaptiveMTU = confirmedMtu;
        this.pathMtuProbe = probeMtu;
        this.pathMtuMaximum = maximumMtu;
    }

    @Override
    public void fecBudget(int dataShards, int parityShards, double recoveryRatio) {
        this.fecDataShards = dataShards;
        this.fecParityShards = parityShards;
        this.fecRecoveryRatio = recoveryRatio;
    }

    // ========== Calculations ==========

    private long lastMeasureMillis = System.currentTimeMillis();

    private synchronized void tick() {
        final long measureMillis = System.currentTimeMillis();
        final long deltaTime = measureMillis - lastMeasureMillis;
        if (deltaTime < 990) return; // throttle
        this.lastMeasureMillis = measureMillis;

        tickErrorRate();
        tickRXTX(deltaTime);
        appendMetricsJsonl(measureMillis);
    }

    private final DescriptiveStatistics errorStats = new DescriptiveStatistics(16);
    private long lastBytesTotal = 0L;
    private long lastBytesRecalled = 0L;
    private volatile double measureErrorRate = 0.0D;

    private void tickErrorRate() {
        final long bytesTotal = this.bytesIn + this.bytesOut;
        final long bytesRecalled = this.bytesRecalled;

        final long bytesTotalDelta = bytesTotal - lastBytesTotal;
        final long bytesRecalledDelta = bytesRecalled - this.lastBytesRecalled;

        if (bytesTotalDelta != 0) {
            this.errorStats.addValue(bytesRecalledDelta / (double) bytesTotalDelta);
            this.measureErrorRate = this.errorStats.getMean();
        }

        this.lastBytesTotal = bytesTotal;
        this.lastBytesRecalled = bytesRecalled;
    }

    private final DescriptiveStatistics rxStats = new DescriptiveStatistics(8);
    private final DescriptiveStatistics txStats = new DescriptiveStatistics(8);
    private long lastPacketsIn = 0L;
    private long lastPacketsOut = 0L;
    private long lastBytesIn = 0L;
    private long lastBytesOut = 0L;
    private volatile int measureRX = 0;
    private volatile int measureTX = 0;
    private volatile long measureBytesInRate = 0;
    private volatile long measureBytesOutRate = 0;
    private volatile String measureTrafficInFormatted = "...";
    private volatile String measureTrafficOutFormatted = "...";

    private void tickRXTX(long deltaTime) {

        final long packetsIn = this.packetsIn;
        final long packetsOut = this.packetsOut;
        final long bytesIn = this.bytesIn;
        final long bytesOut = this.bytesOut;

        final double timeDeltaS = deltaTime / 1000.0;

        this.rxStats.addValue((packetsIn - this.lastPacketsIn) / timeDeltaS);
        this.txStats.addValue((packetsOut - this.lastPacketsOut) / timeDeltaS);

        this.measureRX = (int) this.rxStats.getMean();
        this.measureTX = (int) this.txStats.getMean();

        this.measureBytesInRate = (long) ((bytesIn - this.lastBytesIn) / timeDeltaS);
        this.measureBytesOutRate = (long) ((bytesOut - this.lastBytesOut) / timeDeltaS);

        this.measureTrafficInFormatted = MathUtil.humanReadableByteCountBin(this.measureBytesInRate) + "/s";
        this.measureTrafficOutFormatted = MathUtil.humanReadableByteCountBin(this.measureBytesOutRate) + "/s";

        this.lastPacketsIn = packetsIn;
        this.lastPacketsOut = packetsOut;
        this.lastBytesIn = bytesIn;
        this.lastBytesOut = bytesOut;
    }

    // ========== Getters ==========

    public long getMeasureRTTns() {
        return measureRTTns;
    }

    public long getMeasureRTTnsStdDev() {
        return measureRTTnsStdDev;
    }

    public double getMeasureErrorRate() {
        return measureErrorRate;
    }

    public int getMeasureRX() {
        return measureRX;
    }

    public int getMeasureTX() {
        return measureTX;
    }

    public int getCurrentQueuedBytes() {
        return currentQueuedBytes;
    }

    public long getMeasureBurstTokens() {
        return measureBurstTokens;
    }

    public long getMeasureBytesInRate() {
        return measureBytesInRate;
    }

    public long getMeasureBytesOutRate() {
        return measureBytesOutRate;
    }

    public String getMeasureTrafficInFormatted() {
        return measureTrafficInFormatted;
    }

    public String getMeasureTrafficOutFormatted() {
        return measureTrafficOutFormatted;
    }

    public long getBytesIn() {
        return bytesIn;
    }

    public double getAdaptivePacingRate() { return adaptivePacingRate; }
    public long getAdaptiveBytePacingRate() { return adaptiveBytePacingRate; }
    public long getAdaptiveDeliveryRate() { return adaptiveDeliveryRate; }
    public double getAdaptiveLossRatio() { return adaptiveLossRatio; }
    public long getAdaptiveAcknowledged() { return adaptiveAcknowledged; }
    public long getAdaptiveLost() { return adaptiveLost; }
    public String getAdaptiveLossType() { return adaptiveLossType; }
    public int getAdaptiveMTU() { return adaptiveMTU; }
    public long getFecRecovered() { return fecRecovered; }
    public long getFecParityPackets() { return fecParityPackets; }
    public long getFecParityBytes() { return fecParityBytes; }
    public long getFecExpired() { return fecExpired; }
    public long getMtuProbesSent() { return mtuProbesSent; }
    public long getMtuProbesAcknowledged() { return mtuProbesAcknowledged; }
    public long getMtuProbesTimedOut() { return mtuProbesTimedOut; }
    public int getAdaptiveDscp() { return adaptiveDscp; }
    public long getSmallWriteBatches() { return smallWriteBatches; }
    public long getSmallWriteFrames() { return smallWriteFrames; }
    public long getSmallWriteDelayNanos() { return smallWriteDelayNanos; }
    public long getPacingDelayNanos() { return pacingDelayNanos; }
    public long getValidatedPathRate() { return validatedPathRate; }
    public boolean isDeliverySampleApplicationLimited() { return deliverySampleApplicationLimited; }
    public String getResumeState() { return resumeState; }
    public int getResumeValidatedRounds() { return resumeValidatedRounds; }
    public long getPacerWakeupLatenessNanos() { return pacerWakeupLatenessNanos; }
    public int getPacerBatchDatagrams() { return pacerBatchDatagrams; }
    public int getPacerMaxBatchDatagrams() { return pacerMaxBatchDatagrams; }
    public String getCongestionMode() { return congestionMode; }
    public long getCongestionWindowBytes() { return congestionWindowBytes; }
    public long getInFlightBytes() { return inFlightBytes; }
    public long getBandwidthBytesPerSecond() { return bandwidthBytesPerSecond; }
    public long getAckAggregationBytes() { return ackAggregationBytes; }
    public double getEcnCeRatio() { return ecnCeRatio; }
    public String getCongestionReason() { return congestionReason; }
    public double getRttInflation() { return rttInflation; }
    public boolean isPacingCapped() { return pacingCapped; }
    public boolean isBandwidthProbeSuppressed() { return bandwidthProbeSuppressed; }
    public String getPathMtuState() { return pathMtuState; }
    public int getPathMtuProbe() { return pathMtuProbe; }
    public int getPathMtuMaximum() { return pathMtuMaximum; }
    public long getReliableFrameDuplicates() { return reliableFrameDuplicates; }
    public long getNacksDeferred() { return nacksDeferred; }
    public long getReorderedPackets() { return reorderedPackets; }
    public long getNacksDeferredExpired() { return nacksDeferredExpired; }
    public long getNacksDeferredConfirmed() { return nacksDeferredConfirmed; }
    public long getNackGraceBypassed() { return nackGraceBypassed; }
    public boolean isAdaptiveNackGraceBypass() { return adaptiveNackGraceBypass; }
    public long getNackRepeatedPackets() { return nackRepeatedPackets; }
    public long getNackRepeatedFrameSets() { return nackRepeatedFrameSets; }
    public long getNackRetransmitBytes() { return nackRetransmitBytes; }
    public long getTimeoutRetransmitBytes() { return timeoutRetransmitBytes; }
    public long getRackRetransmitBytes() { return rackRetransmitBytes; }
    public long getRackRetransmitFrameSets() { return rackRetransmitFrameSets; }
    public long getRackSpuriousAcks() { return rackSpuriousAcks; }
    public long getPtoProbes() { return ptoProbes; }
    public long getPtoProbeBytes() { return ptoProbeBytes; }
    public long getPtoProbeAckedBytes() { return ptoProbeAckedBytes; }
    public long getOrderedHolProbes() { return orderedHolProbes; }
    public long getOrderedHolProbeBytes() { return orderedHolProbeBytes; }
    public long getOrderedHolProbeAckedBytes() { return orderedHolProbeAckedBytes; }
    public int getOrderedHolProbeChannel() { return orderedHolProbeChannel; }
    public int getPtoCount() { return ptoCount; }
    public long getLastAckProgressAgeNanos() { return lastAckProgressAgeNanos; }
    public long getApplicationLimitedRecoveryPackets() { return applicationLimitedRecoveryPackets; }
    public long getApplicationLimitedRecoveryBytes() { return applicationLimitedRecoveryBytes; }
    public int getRecoveryQueueDepth() { return recoveryQueueDepth; }
    public long getRecoveryQueueOldestAgeNanos() { return recoveryQueueOldestAgeNanos; }
    public double getRecoveryDebt() { return recoveryDebt; }
    public int getRecoveryDebtChannel() { return recoveryDebtChannel; }
    public long getTargetedFecPackets() { return targetedFecPackets; }
    public long getTargetedFecBytes() { return targetedFecBytes; }
    public long getTargetedFecRecovered() { return targetedFecRecovered; }
    public int getTargetedFecChannel() { return targetedFecChannel; }
    public int getFragmentPendingBuilders() { return fragmentPendingBuilders; }
    public long getFragmentPendingBytes() { return fragmentPendingBytes; }
    public long getFragmentOldestAgeNanos() { return fragmentOldestAgeNanos; }
    public long getFragmentCompleted() { return fragmentCompleted; }
    public long getFragmentCompletedBytes() { return fragmentCompletedBytes; }
    public long getFragmentMaxAgeNanos() { return fragmentMaxAgeNanos; }
    public int getOrderedPendingFrames() { return orderedPendingFrames; }
    public long getOrderedOldestAgeNanos() { return orderedOldestAgeNanos; }
    public long getOrderedReleasedFrames() { return orderedReleasedFrames; }
    public long getOrderedMaxWaitNanos() { return orderedMaxWaitNanos; }
    public int getOrderedWorstChannel() { return orderedWorstChannel; }
    public int[] getOrderedChannelPending() { return Arrays.copyOf(orderedChannelPending, 8); }
    public long[] getOrderedChannelOldestAgeNanos() { return Arrays.copyOf(orderedChannelOldestAgeNanos, 8); }
    public int[] getOrderedChannelBlockedOrderIndex() { return Arrays.copyOf(orderedChannelBlockedOrderIndex, 8); }
    public long[] getOrderedChannelReleasedFrames() { return Arrays.copyOf(orderedChannelReleasedFrames, 8); }
    public long[] getOrderedChannelMaxWaitNanos() { return Arrays.copyOf(orderedChannelMaxWaitNanos, 8); }
    public long getApplicationBatches() { return applicationBatches; }
    public long getApplicationBatchBytes() { return applicationBatchBytes; }
    public long getApplicationBatchMaxBytes() { return applicationBatchMaxBytes; }
    public synchronized long[] getDependencyDomainQueuedFrames() {
        return Arrays.copyOf(dependencyDomainQueuedFrames, dependencyDomainQueuedFrames.length);
    }
    public synchronized long[] getDependencyDomainQueuedBytes() {
        return Arrays.copyOf(dependencyDomainQueuedBytes, dependencyDomainQueuedBytes.length);
    }
    public synchronized long[] getDependencyDomainSentFrames() {
        return Arrays.copyOf(dependencyDomainSentFrames, dependencyDomainSentFrames.length);
    }
    public synchronized long[] getDependencyDomainSentBytes() {
        return Arrays.copyOf(dependencyDomainSentBytes, dependencyDomainSentBytes.length);
    }
    public synchronized long[] getDependencyDomainPendingFrames() {
        return Arrays.copyOf(dependencyDomainPendingFrames, dependencyDomainPendingFrames.length);
    }
    public synchronized long[] getDependencyDomainPendingBytes() {
        return Arrays.copyOf(dependencyDomainPendingBytes, dependencyDomainPendingBytes.length);
    }
    public long getAckRepeatedPackets() { return ackRepeatedPackets; }
    public long getAckRepeatedFrameSets() { return ackRepeatedFrameSets; }
    public boolean isAdaptiveAckProtection() { return adaptiveAckProtection; }
    public long getAdaptiveAckFlushDelayNanos() { return adaptiveAckFlushDelayNanos; }
    public long getAdaptiveAckRepeatDelayNanos() { return adaptiveAckRepeatDelayNanos; }
    public boolean isApplicationLimited() { return applicationLimited; }
    public String getBacklogState() { return backlogState; }
    public long getBacklogAgeNanos() { return backlogAgeNanos; }
    public long getBacklogProbes() { return backlogProbes; }
    public int getFecDataShards() { return fecDataShards; }
    public int getFecParityShards() { return fecParityShards; }
    public double getFecRecoveryRatio() { return fecRecoveryRatio; }

    private static Path metricsFile() {
        if (!Boolean.getBoolean("raknetify.metricsJsonl")) return null;
        try {
            return Paths.get("logs", "raknetify-metrics.jsonl").toAbsolutePath().normalize();
        } catch (RuntimeException e) {
            METRICS_FILE_DISABLED.set(true);
            System.err.println("Raknetify: failed to resolve the default metrics JSONL path: " + e);
            return null;
        }
    }

    private void appendMetricsJsonl(long timestamp) {
        if (METRICS_FILE == null || METRICS_FILE_DISABLED.get()) return;
        final String line = formatMetricsJsonl(timestamp);
        if (!METRICS_LINES.offer(line)) {
            METRICS_LINES_DROPPED.incrementAndGet();
        }
    }

    String formatMetricsJsonl(long timestamp) {
        return String.format(Locale.ROOT,
                "{\"timestamp\":%d,\"connection\":\"%08x\",\"rtt_ns\":%d,\"rtt_stddev_ns\":%d," +
                        "\"rx_pps\":%d,\"tx_pps\":%d,\"rx_bps\":%d,\"tx_bps\":%d,\"queued_bytes\":%d," +
                        "\"reliable_frame_duplicates\":%d,\"nack_deferred\":%d," +
                        "\"reordered_packets\":%d,\"nack_deferred_expired\":%d," +
                        "\"nack_deferred_confirmed\":%d,\"nack_grace_bypassed\":%d," +
                        "\"nack_grace_bypass\":%s,\"nack_repeated_packets\":%d," +
                        "\"nack_repeated_framesets\":%d,\"nack_retransmit_bytes\":%d," +
                        "\"timeout_retransmit_bytes\":%d,\"rack_retransmit_bytes\":%d," +
                        "\"rack_retransmit_framesets\":%d,\"rack_spurious_acks\":%d," +
                        "\"pto_probes\":%d,\"pto_probe_bytes\":%d,\"pto_probe_acked_bytes\":%d," +
                        "\"ordered_hol_probes\":%d,\"ordered_hol_probe_bytes\":%d," +
                        "\"ordered_hol_probe_acked_bytes\":%d,\"ordered_hol_probe_channel\":%d," +
                        "\"pto_count\":%d,\"last_ack_progress_age_ns\":%d," +
                        "\"application_limited_recovery_packets\":%d," +
                        "\"application_limited_recovery_bytes\":%d," +
                        "\"recovery_queue_depth\":%d,\"recovery_queue_oldest_age_ns\":%d," +
                        "\"recovery_debt\":%.6f,\"recovery_debt_channel\":%d," +
                        "\"targeted_fec_packets\":%d,\"targeted_fec_bytes\":%d," +
                        "\"targeted_fec_recovered\":%d,\"targeted_fec_channel\":%d," +
                        "\"fragment_pending_builders\":%d,\"fragment_pending_bytes\":%d," +
                        "\"fragment_oldest_age_ns\":%d,\"fragment_completed\":%d," +
                        "\"fragment_completed_bytes\":%d,\"fragment_max_age_ns\":%d," +
                        "\"ordered_pending_frames\":%d,\"ordered_oldest_age_ns\":%d," +
                        "\"ordered_released_frames\":%d,\"ordered_max_wait_ns\":%d," +
                        "\"ordered_worst_channel\":%d,\"ordered_channel_pending\":%s," +
                        "\"ordered_channel_oldest_age_ns\":%s," +
                        "\"ordered_channel_blocked_order_index\":%s," +
                        "\"ordered_channel_released_frames\":%s," +
                        "\"ordered_channel_max_wait_ns\":%s," +
                        "\"application_batches\":%d,\"application_batch_bytes\":%d," +
                        "\"application_batch_max_bytes\":%d," +
                        "\"dependency_domain_names\":[\"STRICT_WORLD\",\"INDEPENDENT_CONTROL\",\"EPHEMERAL_EFFECT\",\"GUARDED_BULK\"]," +
                        "\"dependency_domain_queued_frames\":%s,\"dependency_domain_queued_bytes\":%s," +
                        "\"dependency_domain_sent_frames\":%s,\"dependency_domain_sent_bytes\":%s," +
                        "\"dependency_domain_pending_frames\":%s,\"dependency_domain_pending_bytes\":%s," +
                        "\"ack_repeated_packets\":%d,\"ack_repeated_framesets\":%d," +
                        "\"ack_protection\":%s,\"ack_flush_delay_ns\":%d,\"ack_repeat_delay_ns\":%d," +
                        "\"application_limited\":%s,\"backlog_state\":\"%s\"," +
                        "\"backlog_age_ns\":%d,\"backlog_probes\":%d," +
                        "\"validated_path_bps\":%d,\"delivery_sample_application_limited\":%s," +
                        "\"resume_state\":\"%s\",\"resume_validated_rounds\":%d," +
                        "\"pacing_pps\":%.3f,\"byte_pacing_bps\":%d,\"delivery_bps\":%d,\"loss_ratio\":%.6f," +
                        "\"acked\":%d,\"lost\":%d,\"loss_type\":\"%s\",\"mtu\":%d," +
                        "\"fec_recovered\":%d,\"fec_parity_packets\":%d,\"fec_parity_bytes\":%d,\"fec_expired\":%d," +
                        "\"mtu_probe_sent\":%d,\"mtu_probe_acked\":%d,\"mtu_probe_timeout\":%d," +
                        "\"dscp\":%d,\"small_write_batches\":%d,\"small_write_frames\":%d," +
                        "\"small_write_delay_ns\":%d,\"pacing_delay_ns\":%d," +
                        "\"pacer_wakeup_lateness_ns\":%d,\"pacer_batch_datagrams\":%d," +
                        "\"pacer_max_batch_datagrams\":%d," +
                        "\"cc_mode\":\"%s\",\"cwnd_bytes\":%d,\"inflight_bytes\":%d," +
                        "\"bandwidth_bps\":%d,\"ack_aggregation_bytes\":%d,\"ecn_ce_ratio\":%.6f," +
                        "\"congestion_reason\":\"%s\",\"rtt_inflation\":%.6f," +
                        "\"pacing_capped\":%s,\"bandwidth_probe_suppressed\":%s," +
                        "\"plpmtud_state\":\"%s\",\"plpmtud_probe\":%d,\"plpmtud_max\":%d," +
                        "\"fec_data_shards\":%d,\"fec_parity_shards\":%d,\"fec_recovery_ratio\":%.6f," +
                        "\"remote_supported\":%s,\"remote_queued_bytes\":%d,\"remote_burst\":%d," +
                        "\"remote_error_rate\":%.6f,\"remote_tx_pps\":%d,\"remote_rx_pps\":%d," +
                        "\"remote_adaptive_supported\":%s,\"remote_rtt_ns\":%d,\"remote_rtt_stddev_ns\":%d," +
                        "\"remote_pacing_pps\":%.3f,\"remote_byte_pacing_bps\":%d," +
                        "\"remote_delivery_bps\":%d,\"remote_loss_ratio\":%.6f," +
                        "\"remote_loss_type\":\"%s\",\"remote_cc_mode\":\"%s\"," +
                        "\"remote_cwnd_bytes\":%d,\"remote_inflight_bytes\":%d,\"remote_bandwidth_bps\":%d," +
                        "\"remote_reliable_frame_duplicates\":%d,\"remote_recovery_supported\":%s," +
                        "\"remote_nack_deferred\":%d,\"remote_reordered_packets\":%d," +
                        "\"remote_nack_outcome_supported\":%s," +
                        "\"remote_nack_deferred_expired\":%d,\"remote_nack_deferred_confirmed\":%d," +
                        "\"remote_nack_policy_supported\":%s,\"remote_nack_grace_bypassed\":%d," +
                        "\"remote_nack_grace_bypass\":%s,\"remote_nack_repeat_supported\":%s," +
                        "\"remote_nack_repeated_packets\":%d,\"remote_nack_repeated_framesets\":%d," +
                        "\"remote_nack_retransmit_bytes\":%d,\"remote_timeout_retransmit_bytes\":%d," +
                        "\"remote_fragment_pending_builders\":%d,\"remote_fragment_pending_bytes\":%d," +
                        "\"remote_fragment_oldest_age_ns\":%d,\"remote_fragment_completed\":%d," +
                        "\"remote_fragment_max_age_ns\":%d,\"remote_ordered_pending_frames\":%d," +
                        "\"remote_ordered_oldest_age_ns\":%d,\"remote_ordered_released_frames\":%d," +
                        "\"remote_ordered_max_wait_ns\":%d," +
                        "\"remote_application_batch_supported\":%s," +
                        "\"remote_application_batches\":%d,\"remote_application_batch_bytes\":%d," +
                        "\"remote_application_batch_max_bytes\":%d," +
                        "\"remote_ack_policy_supported\":%s," +
                        "\"remote_ack_repeated_packets\":%d,\"remote_ack_repeated_framesets\":%d," +
                        "\"remote_ack_protection\":%s,\"remote_ack_flush_delay_ns\":%d," +
                        "\"remote_ack_repeat_delay_ns\":%d," +
                        "\"remote_demand_supported\":%s,\"remote_application_limited\":%s," +
                        "\"remote_backlog_state\":\"%s\",\"remote_backlog_age_ns\":%d," +
                        "\"remote_backlog_probes\":%d," +
                        "\"remote_fec_supported\":%s,\"remote_fec_recovered\":%d," +
                        "\"remote_fec_parity_packets\":%d,\"remote_fec_parity_bytes\":%d," +
                        "\"remote_fec_expired\":%d,\"remote_fec_data_shards\":%d," +
                        "\"remote_fec_parity_shards\":%d,\"remote_fec_recovery_ratio\":%.6f," +
                        "\"remote_advanced_recovery_supported\":%s," +
                        "\"remote_rack_retransmit_bytes\":%d," +
                        "\"remote_rack_retransmit_framesets\":%d,\"remote_rack_spurious_acks\":%d," +
                        "\"remote_pto_probes\":%d,\"remote_pto_probe_bytes\":%d," +
                        "\"remote_pto_probe_acked_bytes\":%d,\"remote_pto_count\":%d," +
                        "\"remote_last_ack_progress_age_ns\":%d," +
                        "\"remote_application_limited_recovery_packets\":%d," +
                        "\"remote_application_limited_recovery_bytes\":%d," +
                        "\"remote_recovery_queue_depth\":%d," +
                        "\"remote_recovery_queue_oldest_age_ns\":%d," +
                        "\"remote_recovery_debt\":%.6f,\"remote_recovery_debt_channel\":%d," +
                        "\"remote_targeted_fec_packets\":%d,\"remote_targeted_fec_bytes\":%d," +
                        "\"remote_targeted_fec_recovered\":%d," +
                        "\"remote_targeted_fec_channel\":%d,\"remote_ordered_worst_channel\":%d," +
                        "\"remote_ordered_channel_pending\":%s," +
                        "\"remote_ordered_channel_oldest_age_ns\":%s," +
                        "\"remote_ordered_channel_blocked_order_index\":%s," +
                        "\"remote_ordered_channel_released_frames\":%s," +
                        "\"remote_ordered_channel_max_wait_ns\":%s," +
                        "\"remote_ordered_hol_probe_supported\":%s," +
                        "\"remote_ordered_hol_probes\":%d," +
                        "\"remote_ordered_hol_probe_bytes\":%d," +
                        "\"remote_ordered_hol_probe_acked_bytes\":%d," +
                        "\"remote_ordered_hol_probe_channel\":%d," +
                        "\"remote_congestion_reason\":\"%s\"," +
                        "\"remote_rtt_inflation\":%.6f,\"remote_pacing_capped\":%s," +
                        "\"remote_bandwidth_probe_suppressed\":%s," +
                        "\"export_dropped\":%d}%n",
                timestamp, System.identityHashCode(this), measureRTTns, measureRTTnsStdDev,
                measureRX, measureTX, measureBytesInRate, measureBytesOutRate, currentQueuedBytes,
                reliableFrameDuplicates, nacksDeferred, reorderedPackets,
                nacksDeferredExpired, nacksDeferredConfirmed, nackGraceBypassed,
                adaptiveNackGraceBypass, nackRepeatedPackets, nackRepeatedFrameSets, nackRetransmitBytes,
                timeoutRetransmitBytes, rackRetransmitBytes, rackRetransmitFrameSets, rackSpuriousAcks,
                ptoProbes, ptoProbeBytes, ptoProbeAckedBytes,
                orderedHolProbes, orderedHolProbeBytes, orderedHolProbeAckedBytes, orderedHolProbeChannel,
                ptoCount, lastAckProgressAgeNanos,
                applicationLimitedRecoveryPackets, applicationLimitedRecoveryBytes,
                recoveryQueueDepth, recoveryQueueOldestAgeNanos,
                recoveryDebt, recoveryDebtChannel, targetedFecPackets, targetedFecBytes,
                targetedFecRecovered, targetedFecChannel,
                fragmentPendingBuilders, fragmentPendingBytes, fragmentOldestAgeNanos, fragmentCompleted,
                fragmentCompletedBytes, fragmentMaxAgeNanos, orderedPendingFrames, orderedOldestAgeNanos,
                orderedReleasedFrames, orderedMaxWaitNanos,
                orderedWorstChannel, Arrays.toString(orderedChannelPending),
                Arrays.toString(orderedChannelOldestAgeNanos),
                Arrays.toString(orderedChannelBlockedOrderIndex),
                Arrays.toString(orderedChannelReleasedFrames), Arrays.toString(orderedChannelMaxWaitNanos),
                applicationBatches, applicationBatchBytes, applicationBatchMaxBytes,
                Arrays.toString(getDependencyDomainQueuedFrames()),
                Arrays.toString(getDependencyDomainQueuedBytes()),
                Arrays.toString(getDependencyDomainSentFrames()),
                Arrays.toString(getDependencyDomainSentBytes()),
                Arrays.toString(getDependencyDomainPendingFrames()),
                Arrays.toString(getDependencyDomainPendingBytes()),
                ackRepeatedPackets, ackRepeatedFrameSets, adaptiveAckProtection,
                adaptiveAckFlushDelayNanos, adaptiveAckRepeatDelayNanos,
                applicationLimited, backlogState, backlogAgeNanos, backlogProbes,
                validatedPathRate, deliverySampleApplicationLimited, resumeState, resumeValidatedRounds,
                adaptivePacingRate, adaptiveBytePacingRate, adaptiveDeliveryRate, adaptiveLossRatio, adaptiveAcknowledged,
                adaptiveLost, adaptiveLossType, adaptiveMTU, fecRecovered, fecParityPackets,
                fecParityBytes, fecExpired, mtuProbesSent, mtuProbesAcknowledged, mtuProbesTimedOut,
                adaptiveDscp, smallWriteBatches, smallWriteFrames, smallWriteDelayNanos, pacingDelayNanos,
                pacerWakeupLatenessNanos, pacerBatchDatagrams, pacerMaxBatchDatagrams,
                congestionMode, congestionWindowBytes, inFlightBytes, bandwidthBytesPerSecond,
                ackAggregationBytes, ecnCeRatio, congestionReason, rttInflation,
                pacingCapped, bandwidthProbeSuppressed,
                pathMtuState, pathMtuProbe, pathMtuMaximum,
                fecDataShards, fecParityShards, fecRecoveryRatio,
                isRemoteSupported(), remoteQueuedBytes(), remoteBurst(), remoteErrorRate(), remoteTX(), remoteRX(),
                isRemoteAdaptiveSupported(), remoteRttNanos(), remoteRttStdDevNanos(),
                remotePacingRate(), remoteBytePacingRate(), remoteDeliveryRate(), remoteLossRatio(), remoteLossType(), remoteCongestionMode(),
                remoteCongestionWindow(), remoteInFlight(), remoteBandwidth(), remoteReliableFrameDuplicates(),
                isRemoteRecoverySupported(), remoteNacksDeferred(), remoteReorderedPackets(),
                isRemoteNackOutcomeSupported(), remoteNacksDeferredExpired(), remoteNacksDeferredConfirmed(),
                isRemoteNackPolicySupported(), remoteNackGraceBypassed(), remoteNackGraceBypass(),
                isRemoteNackRepeatSupported(), remoteNackRepeatedPackets(), remoteNackRepeatedFrameSets(),
                remoteNackRetransmitBytes(), remoteTimeoutRetransmitBytes(),
                remoteFragmentPendingBuilders(), remoteFragmentPendingBytes(), remoteFragmentOldestAgeNanos(),
                remoteFragmentCompleted(), remoteFragmentMaxAgeNanos(), remoteOrderedPendingFrames(),
                remoteOrderedOldestAgeNanos(), remoteOrderedReleasedFrames(), remoteOrderedMaxWaitNanos(),
                isRemoteApplicationBatchSupported(), remoteApplicationBatches(),
                remoteApplicationBatchBytes(), remoteApplicationBatchMaxBytes(),
                isRemoteAckPolicySupported(), remoteAckRepeatedPackets(), remoteAckRepeatedFrameSets(),
                remoteAckProtection(), remoteAckFlushDelayNanos(), remoteAckRepeatDelayNanos(),
                isRemoteDemandSupported(), remoteApplicationLimited(), remoteBacklogState(),
                remoteBacklogAgeNanos(), remoteBacklogProbes(),
                isRemoteFecSupported(), remoteFecRecovered(), remoteFecParityPackets(),
                remoteFecParityBytes(), remoteFecExpired(), remoteFecDataShards(),
                remoteFecParityShards(), remoteFecRecoveryRatio(),
                isRemoteAdvancedRecoverySupported(), remoteRackRetransmitBytes(),
                remoteRackRetransmitFrameSets(), remoteRackSpuriousAcks(),
                remotePtoProbes(), remotePtoProbeBytes(), remotePtoProbeAckedBytes(), remotePtoCount(),
                remoteLastAckProgressAgeNanos(), remoteApplicationLimitedRecoveryPackets(),
                remoteApplicationLimitedRecoveryBytes(), remoteRecoveryQueueDepth(),
                remoteRecoveryQueueOldestAgeNanos(), remoteRecoveryDebt(), remoteRecoveryDebtChannel(),
                remoteTargetedFecPackets(), remoteTargetedFecBytes(), remoteTargetedFecRecovered(),
                remoteTargetedFecChannel(),
                remoteOrderedWorstChannel(), Arrays.toString(remoteOrderedChannelPending()),
                Arrays.toString(remoteOrderedChannelOldestAgeNanos()),
                Arrays.toString(remoteOrderedChannelBlockedOrderIndex()),
                Arrays.toString(remoteOrderedChannelReleasedFrames()),
                Arrays.toString(remoteOrderedChannelMaxWaitNanos()),
                isRemoteOrderedHolProbeSupported(), remoteOrderedHolProbes(),
                remoteOrderedHolProbeBytes(), remoteOrderedHolProbeAckedBytes(), remoteOrderedHolProbeChannel(),
                remoteCongestionReason(), remoteRttInflation(), remotePacingCapped(), remoteBandwidthProbeSuppressed(),
                METRICS_LINES_DROPPED.get());
    }

    private boolean isRemoteAdaptiveSupported() {
        return metricsSynchronizationHandler != null
                && metricsSynchronizationHandler.isRemoteAdaptiveSupported();
    }

    private boolean isRemoteSupported() {
        return metricsSynchronizationHandler != null && metricsSynchronizationHandler.isRemoteSupported();
    }

    private boolean isRemoteRecoverySupported() {
        return metricsSynchronizationHandler != null && metricsSynchronizationHandler.isRemoteRecoverySupported();
    }

    private int remoteQueuedBytes() { return isRemoteSupported() ? metricsSynchronizationHandler.getQueuedBytes() : 0; }
    private int remoteBurst() { return isRemoteSupported() ? metricsSynchronizationHandler.getBurst() : 0; }
    private double remoteErrorRate() { return isRemoteSupported() ? metricsSynchronizationHandler.getErrorRate() : 0D; }
    private int remoteTX() { return isRemoteSupported() ? metricsSynchronizationHandler.getTX() : 0; }
    private int remoteRX() { return isRemoteSupported() ? metricsSynchronizationHandler.getRX() : 0; }

    private long remoteRttNanos() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getRttNanos() : 0L; }
    private long remoteRttStdDevNanos() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getRttStdDevNanos() : 0L; }
    private double remotePacingRate() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getPacingRate() : 0D; }
    private long remoteBytePacingRate() { return metricsSynchronizationHandler != null
            && metricsSynchronizationHandler.isRemoteBytePacingSupported()
            ? metricsSynchronizationHandler.getBytePacingRate() : 0L; }
    private long remoteDeliveryRate() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getDeliveryRate() : 0L; }
    private double remoteLossRatio() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getLossRatio() : 0D; }
    private String remoteLossType() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getLossType() : "UNAVAILABLE"; }
    private String remoteCongestionMode() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getCongestionMode() : "UNAVAILABLE"; }
    private long remoteCongestionWindow() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getCongestionWindow() : 0L; }
    private long remoteInFlight() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getInFlight() : 0L; }
    private long remoteBandwidth() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getBandwidth() : 0L; }
    private long remoteReliableFrameDuplicates() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getReliableFrameDuplicates() : 0L; }
    private long remoteNacksDeferred() { return isRemoteRecoverySupported() ? metricsSynchronizationHandler.getNacksDeferred() : 0L; }
    private long remoteReorderedPackets() { return isRemoteRecoverySupported() ? metricsSynchronizationHandler.getReorderedPackets() : 0L; }
    private boolean isRemoteNackOutcomeSupported() { return metricsSynchronizationHandler != null
            && metricsSynchronizationHandler.isRemoteNackOutcomeSupported(); }
    private long remoteNacksDeferredExpired() { return isRemoteNackOutcomeSupported()
            ? metricsSynchronizationHandler.getNacksDeferredExpired() : 0L; }
    private long remoteNacksDeferredConfirmed() { return isRemoteNackOutcomeSupported()
            ? metricsSynchronizationHandler.getNacksDeferredConfirmed() : 0L; }
    private boolean isRemoteNackPolicySupported() { return metricsSynchronizationHandler != null
            && metricsSynchronizationHandler.isRemoteNackPolicySupported(); }
    private long remoteNackGraceBypassed() { return isRemoteNackPolicySupported()
            ? metricsSynchronizationHandler.getNackGraceBypassed() : 0L; }
    private boolean remoteNackGraceBypass() { return isRemoteNackPolicySupported()
            && metricsSynchronizationHandler.isNackGraceBypass(); }
    private boolean isRemoteNackRepeatSupported() { return metricsSynchronizationHandler != null
            && metricsSynchronizationHandler.isRemoteNackRepeatSupported(); }
    private long remoteNackRepeatedPackets() { return isRemoteNackRepeatSupported()
            ? metricsSynchronizationHandler.getNackRepeatedPackets() : 0L; }
    private long remoteNackRepeatedFrameSets() { return isRemoteNackRepeatSupported()
            ? metricsSynchronizationHandler.getNackRepeatedFrameSets() : 0L; }
    private long remoteNackRetransmitBytes() { return isRemoteRecoverySupported() ? metricsSynchronizationHandler.getNackRetransmitBytes() : 0L; }
    private long remoteTimeoutRetransmitBytes() { return isRemoteRecoverySupported() ? metricsSynchronizationHandler.getTimeoutRetransmitBytes() : 0L; }
    private int remoteFragmentPendingBuilders() { return isRemoteHolSupported() ? metricsSynchronizationHandler.getFragmentPendingBuilders() : 0; }
    private long remoteFragmentPendingBytes() { return isRemoteHolSupported() ? metricsSynchronizationHandler.getFragmentPendingBytes() : 0L; }
    private long remoteFragmentOldestAgeNanos() { return isRemoteHolSupported() ? metricsSynchronizationHandler.getFragmentOldestAgeNanos() : 0L; }
    private long remoteFragmentCompleted() { return isRemoteHolSupported() ? metricsSynchronizationHandler.getFragmentCompleted() : 0L; }
    private long remoteFragmentMaxAgeNanos() { return isRemoteHolSupported() ? metricsSynchronizationHandler.getFragmentMaxAgeNanos() : 0L; }
    private int remoteOrderedPendingFrames() { return isRemoteHolSupported() ? metricsSynchronizationHandler.getOrderedPendingFrames() : 0; }
    private long remoteOrderedOldestAgeNanos() { return isRemoteHolSupported() ? metricsSynchronizationHandler.getOrderedOldestAgeNanos() : 0L; }
    private long remoteOrderedReleasedFrames() { return isRemoteHolSupported() ? metricsSynchronizationHandler.getOrderedReleasedFrames() : 0L; }
    private long remoteOrderedMaxWaitNanos() { return isRemoteHolSupported() ? metricsSynchronizationHandler.getOrderedMaxWaitNanos() : 0L; }
    private boolean isRemoteHolSupported() { return metricsSynchronizationHandler != null
            && metricsSynchronizationHandler.isRemoteHolSupported(); }
    private boolean isRemoteApplicationBatchSupported() { return metricsSynchronizationHandler != null
            && metricsSynchronizationHandler.isRemoteApplicationBatchSupported(); }
    private long remoteApplicationBatches() { return isRemoteApplicationBatchSupported()
            ? metricsSynchronizationHandler.getApplicationBatches() : 0L; }
    private long remoteApplicationBatchBytes() { return isRemoteApplicationBatchSupported()
            ? metricsSynchronizationHandler.getApplicationBatchBytes() : 0L; }
    private long remoteApplicationBatchMaxBytes() { return isRemoteApplicationBatchSupported()
            ? metricsSynchronizationHandler.getApplicationBatchMaxBytes() : 0L; }
    private boolean isRemoteAckPolicySupported() { return metricsSynchronizationHandler != null
            && metricsSynchronizationHandler.isRemoteAckPolicySupported(); }
    private long remoteAckRepeatedPackets() { return isRemoteAckPolicySupported()
            ? metricsSynchronizationHandler.getAckRepeatedPackets() : 0L; }
    private long remoteAckRepeatedFrameSets() { return isRemoteAckPolicySupported()
            ? metricsSynchronizationHandler.getAckRepeatedFrameSets() : 0L; }
    private boolean remoteAckProtection() { return isRemoteAckPolicySupported()
            && metricsSynchronizationHandler.isAckProtection(); }
    private long remoteAckFlushDelayNanos() { return isRemoteAckPolicySupported()
            ? metricsSynchronizationHandler.getAckFlushDelayNanos() : 0L; }
    private long remoteAckRepeatDelayNanos() { return isRemoteAckPolicySupported()
            ? metricsSynchronizationHandler.getAckRepeatDelayNanos() : 0L; }
    private boolean isRemoteDemandSupported() { return metricsSynchronizationHandler != null
            && metricsSynchronizationHandler.isRemoteDemandSupported(); }
    private boolean remoteApplicationLimited() { return !isRemoteDemandSupported()
            || metricsSynchronizationHandler.isApplicationLimited(); }
    private String remoteBacklogState() { return isRemoteDemandSupported()
            ? metricsSynchronizationHandler.getBacklogState() : "UNAVAILABLE"; }
    private long remoteBacklogAgeNanos() { return isRemoteDemandSupported()
            ? metricsSynchronizationHandler.getBacklogAgeNanos() : 0L; }
    private long remoteBacklogProbes() { return isRemoteDemandSupported()
            ? metricsSynchronizationHandler.getBacklogProbes() : 0L; }
    private boolean isRemoteFecSupported() { return metricsSynchronizationHandler != null
            && metricsSynchronizationHandler.isRemoteFecSupported(); }
    private long remoteFecRecovered() { return isRemoteFecSupported()
            ? metricsSynchronizationHandler.getFecRecovered() : 0L; }
    private long remoteFecParityPackets() { return isRemoteFecSupported()
            ? metricsSynchronizationHandler.getFecParityPackets() : 0L; }
    private long remoteFecParityBytes() { return isRemoteFecSupported()
            ? metricsSynchronizationHandler.getFecParityBytes() : 0L; }
    private long remoteFecExpired() { return isRemoteFecSupported()
            ? metricsSynchronizationHandler.getFecExpired() : 0L; }
    private int remoteFecDataShards() { return isRemoteFecSupported()
            ? metricsSynchronizationHandler.getFecDataShards() : 0; }
    private int remoteFecParityShards() { return isRemoteFecSupported()
            ? metricsSynchronizationHandler.getFecParityShards() : 0; }
    private double remoteFecRecoveryRatio() { return isRemoteFecSupported()
            ? metricsSynchronizationHandler.getFecRecoveryRatio() : 0D; }
    private boolean isRemoteAdvancedRecoverySupported() { return metricsSynchronizationHandler != null
            && metricsSynchronizationHandler.isRemoteAdvancedRecoverySupported(); }
    private boolean isRemoteOrderedHolProbeSupported() { return metricsSynchronizationHandler != null
            && metricsSynchronizationHandler.isRemoteOrderedHolProbeSupported(); }
    private long remoteOrderedHolProbes() { return isRemoteOrderedHolProbeSupported()
            ? metricsSynchronizationHandler.getOrderedHolProbes() : 0L; }
    private long remoteOrderedHolProbeBytes() { return isRemoteOrderedHolProbeSupported()
            ? metricsSynchronizationHandler.getOrderedHolProbeBytes() : 0L; }
    private long remoteOrderedHolProbeAckedBytes() { return isRemoteOrderedHolProbeSupported()
            ? metricsSynchronizationHandler.getOrderedHolProbeAckedBytes() : 0L; }
    private int remoteOrderedHolProbeChannel() { return isRemoteOrderedHolProbeSupported()
            ? metricsSynchronizationHandler.getOrderedHolProbeChannel() : -1; }
    private long remoteRackRetransmitBytes() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getRackRetransmitBytes() : 0L; }
    private long remoteRackRetransmitFrameSets() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getRackRetransmitFrameSets() : 0L; }
    private long remoteRackSpuriousAcks() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getRackSpuriousAcks() : 0L; }
    private long remotePtoProbes() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getPtoProbes() : 0L; }
    private long remotePtoProbeBytes() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getPtoProbeBytes() : 0L; }
    private long remotePtoProbeAckedBytes() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getPtoProbeAckedBytes() : 0L; }
    private int remotePtoCount() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getPtoCount() : 0; }
    private long remoteLastAckProgressAgeNanos() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getLastAckProgressAgeNanos() : 0L; }
    private long remoteApplicationLimitedRecoveryPackets() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getApplicationLimitedRecoveryPackets() : 0L; }
    private long remoteApplicationLimitedRecoveryBytes() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getApplicationLimitedRecoveryBytes() : 0L; }
    private int remoteRecoveryQueueDepth() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getRecoveryQueueDepth() : 0; }
    private long remoteRecoveryQueueOldestAgeNanos() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getRecoveryQueueOldestAgeNanos() : 0L; }
    private double remoteRecoveryDebt() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getRecoveryDebt() : 0D; }
    private int remoteRecoveryDebtChannel() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getRecoveryDebtChannel() : -1; }
    private long remoteTargetedFecPackets() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getTargetedFecPackets() : 0L; }
    private long remoteTargetedFecBytes() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getTargetedFecBytes() : 0L; }
    private long remoteTargetedFecRecovered() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getTargetedFecRecovered() : 0L; }
    private int remoteTargetedFecChannel() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getTargetedFecChannel() : -1; }
    private int remoteOrderedWorstChannel() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getOrderedWorstChannel() : -1; }
    private int[] remoteOrderedChannelPending() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getOrderedChannelPending() : new int[8]; }
    private long[] remoteOrderedChannelOldestAgeNanos() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getOrderedChannelOldestAgeNanos() : new long[8]; }
    private int[] remoteOrderedChannelBlockedOrderIndex() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getOrderedChannelBlockedOrderIndex() : new int[8]; }
    private long[] remoteOrderedChannelReleasedFrames() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getOrderedChannelReleasedFrames() : new long[8]; }
    private long[] remoteOrderedChannelMaxWaitNanos() { return isRemoteAdvancedRecoverySupported()
            ? metricsSynchronizationHandler.getOrderedChannelMaxWaitNanos() : new long[8]; }
    private String remoteCongestionReason() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getCongestionReason() : "UNAVAILABLE"; }
    private double remoteRttInflation() { return isRemoteAdaptiveSupported() ? metricsSynchronizationHandler.getRttInflation() : 1D; }
    private boolean remotePacingCapped() { return isRemoteAdaptiveSupported() && metricsSynchronizationHandler.isPacingCapped(); }
    private boolean remoteBandwidthProbeSuppressed() { return isRemoteAdaptiveSupported() && metricsSynchronizationHandler.isBandwidthProbeSuppressed(); }

    private static void startMetricsWriter() {
        final Thread writerThread = new Thread(() -> {
            try {
                try (BufferedWriter writer = Files.newBufferedWriter(METRICS_FILE, StandardCharsets.UTF_8,
                        java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND)) {
                    while (!Thread.currentThread().isInterrupted()) {
                        writer.write(METRICS_LINES.take());
                        String next;
                        while ((next = METRICS_LINES.poll()) != null) writer.write(next);
                        writer.flush();
                    }
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } catch (IOException | RuntimeException e) {
                disableMetricsExport("writer stopped", e);
            }
        }, "raknetify-metrics-writer");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    private static void disableMetricsExport(String reason, Exception e) {
        if (METRICS_FILE_DISABLED.compareAndSet(false, true)) {
            System.err.println("Raknetify: metrics JSONL export " + reason + " for " + METRICS_FILE + ": " + e);
        }
        METRICS_LINES.clear();
    }

    // ========== Misc ==========

    private MetricsSynchronizationHandler metricsSynchronizationHandler;

    public MetricsSynchronizationHandler getMetricsSynchronizationHandler() {
        return this.metricsSynchronizationHandler;
    }

    public void setMetricsSynchronizationHandler(MetricsSynchronizationHandler metricsSynchronizationHandler) {
        this.metricsSynchronizationHandler = metricsSynchronizationHandler;
    }
}
