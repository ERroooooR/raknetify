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

import com.ishland.raknetify.common.util.MathUtil;
import network.ycc.raknet.RakNet;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private volatile String congestionMode = "STARTUP";
    private volatile long congestionWindowBytes;
    private volatile long inFlightBytes;
    private volatile long bandwidthBytesPerSecond;
    private volatile long ackAggregationBytes;
    private volatile double ecnCeRatio;
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
    public String getCongestionMode() { return congestionMode; }
    public long getCongestionWindowBytes() { return congestionWindowBytes; }
    public long getInFlightBytes() { return inFlightBytes; }
    public long getBandwidthBytesPerSecond() { return bandwidthBytesPerSecond; }
    public long getAckAggregationBytes() { return ackAggregationBytes; }
    public double getEcnCeRatio() { return ecnCeRatio; }
    public String getPathMtuState() { return pathMtuState; }
    public int getPathMtuProbe() { return pathMtuProbe; }
    public int getPathMtuMaximum() { return pathMtuMaximum; }
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
        final String line = String.format(Locale.ROOT,
                "{\"timestamp\":%d,\"connection\":\"%08x\",\"rtt_ns\":%d,\"rtt_stddev_ns\":%d," +
                        "\"rx_pps\":%d,\"tx_pps\":%d,\"rx_bps\":%d,\"tx_bps\":%d,\"queued_bytes\":%d," +
                        "\"pacing_pps\":%.3f,\"delivery_bps\":%d,\"loss_ratio\":%.6f," +
                        "\"acked\":%d,\"lost\":%d,\"loss_type\":\"%s\",\"mtu\":%d," +
                        "\"fec_recovered\":%d,\"fec_parity_packets\":%d,\"fec_parity_bytes\":%d,\"fec_expired\":%d," +
                        "\"mtu_probe_sent\":%d,\"mtu_probe_acked\":%d,\"mtu_probe_timeout\":%d," +
                        "\"dscp\":%d,\"small_write_batches\":%d,\"small_write_frames\":%d," +
                        "\"small_write_delay_ns\":%d,\"pacing_delay_ns\":%d," +
                        "\"cc_mode\":\"%s\",\"cwnd_bytes\":%d,\"inflight_bytes\":%d," +
                        "\"bandwidth_bps\":%d,\"ack_aggregation_bytes\":%d,\"ecn_ce_ratio\":%.6f," +
                        "\"plpmtud_state\":\"%s\",\"plpmtud_probe\":%d,\"plpmtud_max\":%d," +
                        "\"fec_data_shards\":%d,\"fec_parity_shards\":%d,\"fec_recovery_ratio\":%.6f," +
                        "\"export_dropped\":%d}%n",
                timestamp, System.identityHashCode(this), measureRTTns, measureRTTnsStdDev,
                measureRX, measureTX, measureBytesInRate, measureBytesOutRate, currentQueuedBytes,
                adaptivePacingRate, adaptiveDeliveryRate, adaptiveLossRatio, adaptiveAcknowledged,
                adaptiveLost, adaptiveLossType, adaptiveMTU, fecRecovered, fecParityPackets,
                fecParityBytes, fecExpired, mtuProbesSent, mtuProbesAcknowledged, mtuProbesTimedOut,
                adaptiveDscp, smallWriteBatches, smallWriteFrames, smallWriteDelayNanos, pacingDelayNanos,
                congestionMode, congestionWindowBytes, inFlightBytes, bandwidthBytesPerSecond,
                ackAggregationBytes, ecnCeRatio, pathMtuState, pathMtuProbe, pathMtuMaximum,
                fecDataShards, fecParityShards, fecRecoveryRatio,
                METRICS_LINES_DROPPED.get());
        if (!METRICS_LINES.offer(line)) {
            METRICS_LINES_DROPPED.incrementAndGet();
        }
    }

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
