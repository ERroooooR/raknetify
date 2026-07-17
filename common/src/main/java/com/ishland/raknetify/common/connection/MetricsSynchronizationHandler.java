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
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.frame.FrameData;
import network.ycc.raknet.packet.FramedPacket;

import java.util.concurrent.TimeUnit;

public class MetricsSynchronizationHandler extends ChannelDuplexHandler {

    // Packet format:
    // byte: version (currently 0x00)
    // long: packet sent time (used to filter out old packets)
    // int: buffer size in bytes
    // int: burst size
    // double: error rate
    // int: tx
    // int: rx
    // Optional extension (new peers; old peers safely ignore trailing bytes):
    // RTT, pacing, loss classification, congestion state and diagnostics.

    private static final byte VERSION = 0x00;
    private static final int ADAPTIVE_EXTENDED_SIZE = 84;
    private static final int RECOVERY_EXTENDED_SIZE = 32;
    private static final int BYTE_PACING_EXTENDED_SIZE = 8;
    private static final int HOL_EXTENDED_SIZE = 64;
    private static final int APPLICATION_BATCH_EXTENDED_SIZE = 24;
    private static final int ACK_POLICY_EXTENDED_SIZE = 33;
    private static final int DEMAND_EXTENDED_SIZE = 18;
    private static final int NACK_OUTCOME_EXTENDED_SIZE = 16;
    private static final int NACK_POLICY_EXTENDED_SIZE = 9;
    private static final int NACK_REPEAT_EXTENDED_SIZE = 16;
    private static final int FEC_EXTENDED_SIZE = 48;
    private static final int ADVANCED_RECOVERY_EXTENDED_SIZE = 388;

    private ScheduledFuture<?> future;
    private ChannelHandlerContext ctx;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        this.future = ctx.channel().eventLoop().scheduleAtFixedRate(this::sendSyncPacket, 200, 200, TimeUnit.MILLISECONDS);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (future != null) future.cancel(false);
    }

    private void sendSyncPacket() {
        if (this.ctx.channel().config() instanceof RakNet.Config config && config.getMetrics() instanceof SimpleMetricsLogger logger){
            ByteBuf buffer = null;
           try {
               buffer = this.ctx.alloc().buffer(1 + 8 + 4 + 4 + 8 + 4 + 4
                       + ADAPTIVE_EXTENDED_SIZE + RECOVERY_EXTENDED_SIZE
                       + BYTE_PACING_EXTENDED_SIZE + HOL_EXTENDED_SIZE
                       + APPLICATION_BATCH_EXTENDED_SIZE + ACK_POLICY_EXTENDED_SIZE
                       + DEMAND_EXTENDED_SIZE + NACK_OUTCOME_EXTENDED_SIZE
                       + NACK_POLICY_EXTENDED_SIZE + NACK_REPEAT_EXTENDED_SIZE
                       + FEC_EXTENDED_SIZE + ADVANCED_RECOVERY_EXTENDED_SIZE);
               writePayload(buffer, logger, config.getDefaultPendingFrameSets(), System.currentTimeMillis());
               final FrameData frameData = FrameData.create(this.ctx.alloc(), Constants.RAKNET_METRICS_SYNC_PACKET_ID, buffer);
               frameData.setReliability(FramedPacket.Reliability.UNRELIABLE);
               this.ctx.write(frameData);
           } finally {
               if (buffer != null) buffer.release();
           }
        }
    }

    private boolean isRemoteSupported = false;
    private long lastRecv = 0L;
    private int queuedBytes;
    private int burst;
    private double errorRate;
    private int tx;
    private int rx;
    private boolean isRemoteAdaptiveSupported;
    private long rttNanos;
    private long rttStdDevNanos;
    private double pacingRate;
    private long deliveryRate;
    private double lossRatio;
    private String lossType = "NONE";
    private String congestionMode = "STARTUP";
    private long congestionWindow;
    private long inFlight;
    private long bandwidth;
    private long reliableFrameDuplicates;
    private String congestionReason = "NONE";
    private double rttInflation = 1D;
    private boolean pacingCapped;
    private boolean bandwidthProbeSuppressed;
    private boolean isRemoteRecoverySupported;
    private long nacksDeferred;
    private long reorderedPackets;
    private long nacksDeferredExpired;
    private long nacksDeferredConfirmed;
    private boolean isRemoteNackOutcomeSupported;
    private boolean isRemoteNackPolicySupported;
    private long nackGraceBypassed;
    private boolean nackGraceBypass;
    private boolean isRemoteNackRepeatSupported;
    private long nackRepeatedPackets;
    private long nackRepeatedFrameSets;
    private long nackRetransmitBytes;
    private long timeoutRetransmitBytes;
    private boolean isRemoteBytePacingSupported;
    private long bytePacingRate;
    private boolean isRemoteHolSupported;
    private int fragmentPendingBuilders;
    private long fragmentPendingBytes;
    private long fragmentOldestAgeNanos;
    private long fragmentCompleted;
    private long fragmentMaxAgeNanos;
    private int orderedPendingFrames;
    private long orderedOldestAgeNanos;
    private long orderedReleasedFrames;
    private long orderedMaxWaitNanos;
    private boolean isRemoteApplicationBatchSupported;
    private long applicationBatches;
    private long applicationBatchBytes;
    private long applicationBatchMaxBytes;
    private boolean isRemoteAckPolicySupported;
    private long ackRepeatedPackets;
    private long ackRepeatedFrameSets;
    private boolean ackProtection;
    private long ackFlushDelayNanos;
    private long ackRepeatDelayNanos;
    private boolean isRemoteDemandSupported;
    private boolean applicationLimited = true;
    private String backlogState = "IDLE";
    private long backlogAgeNanos;
    private long backlogProbes;
    private boolean isRemoteFecSupported;
    private long fecRecovered;
    private long fecParityPackets;
    private long fecParityBytes;
    private long fecExpired;
    private int fecDataShards;
    private int fecParityShards;
    private double fecRecoveryRatio;
    private boolean isRemoteAdvancedRecoverySupported;
    private long rackRetransmitBytes;
    private long rackRetransmitFrameSets;
    private long rackSpuriousAcks;
    private long ptoProbes;
    private long ptoProbeBytes;
    private long ptoProbeAckedBytes;
    private int ptoCount;
    private long lastAckProgressAgeNanos;
    private long applicationLimitedRecoveryPackets;
    private long applicationLimitedRecoveryBytes;
    private int recoveryQueueDepth;
    private long recoveryQueueOldestAgeNanos;
    private double recoveryDebt;
    private int recoveryDebtChannel = -1;
    private long targetedFecPackets;
    private long targetedFecBytes;
    private long targetedFecRecovered;
    private int targetedFecChannel = -1;
    private int orderedWorstChannel = -1;
    private final int[] orderedChannelPending = new int[8];
    private final long[] orderedChannelOldestAgeNanos = new long[8];
    private final int[] orderedChannelBlockedOrderIndex = new int[8];
    private final long[] orderedChannelReleasedFrames = new long[8];
    private final long[] orderedChannelMaxWaitNanos = new long[8];

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FrameData frameData && frameData.getDataSize() > 0 && frameData.getPacketId() == Constants.RAKNET_METRICS_SYNC_PACKET_ID) {
            ByteBuf byteBuf = null;
            try {
                byteBuf = frameData.createData().skipBytes(1);
                readPayload(byteBuf);
            } finally {
                if (byteBuf != null) byteBuf.release();
                frameData.release();
            }
            return;
        }
        super.channelRead(ctx, msg);
    }

    public boolean isRemoteSupported() {
        return this.isRemoteSupported;
    }

    public int getQueuedBytes() {
        return queuedBytes;
    }

    public int getBurst() {
        return burst;
    }

    public double getErrorRate() {
        return errorRate;
    }

    public int getTX() {
        return tx;
    }

    public int getRX() {
        return rx;
    }

    public boolean isRemoteAdaptiveSupported() { return isRemoteAdaptiveSupported; }
    public long getRttNanos() { return rttNanos; }
    public long getRttStdDevNanos() { return rttStdDevNanos; }
    public double getPacingRate() { return pacingRate; }
    public long getDeliveryRate() { return deliveryRate; }
    public double getLossRatio() { return lossRatio; }
    public String getLossType() { return lossType; }
    public String getCongestionMode() { return congestionMode; }
    public long getCongestionWindow() { return congestionWindow; }
    public long getInFlight() { return inFlight; }
    public long getBandwidth() { return bandwidth; }
    public long getReliableFrameDuplicates() { return reliableFrameDuplicates; }
    public String getCongestionReason() { return congestionReason; }
    public double getRttInflation() { return rttInflation; }
    public boolean isPacingCapped() { return pacingCapped; }
    public boolean isBandwidthProbeSuppressed() { return bandwidthProbeSuppressed; }
    public boolean isRemoteRecoverySupported() { return isRemoteRecoverySupported; }
    public long getNacksDeferred() { return nacksDeferred; }
    public long getReorderedPackets() { return reorderedPackets; }
    public boolean isRemoteNackOutcomeSupported() { return isRemoteNackOutcomeSupported; }
    public long getNacksDeferredExpired() { return nacksDeferredExpired; }
    public long getNacksDeferredConfirmed() { return nacksDeferredConfirmed; }
    public boolean isRemoteNackPolicySupported() { return isRemoteNackPolicySupported; }
    public long getNackGraceBypassed() { return nackGraceBypassed; }
    public boolean isNackGraceBypass() { return nackGraceBypass; }
    public boolean isRemoteNackRepeatSupported() { return isRemoteNackRepeatSupported; }
    public long getNackRepeatedPackets() { return nackRepeatedPackets; }
    public long getNackRepeatedFrameSets() { return nackRepeatedFrameSets; }
    public long getNackRetransmitBytes() { return nackRetransmitBytes; }
    public long getTimeoutRetransmitBytes() { return timeoutRetransmitBytes; }
    public boolean isRemoteBytePacingSupported() { return isRemoteBytePacingSupported; }
    public long getBytePacingRate() { return bytePacingRate; }
    public boolean isRemoteHolSupported() { return isRemoteHolSupported; }
    public int getFragmentPendingBuilders() { return fragmentPendingBuilders; }
    public long getFragmentPendingBytes() { return fragmentPendingBytes; }
    public long getFragmentOldestAgeNanos() { return fragmentOldestAgeNanos; }
    public long getFragmentCompleted() { return fragmentCompleted; }
    public long getFragmentMaxAgeNanos() { return fragmentMaxAgeNanos; }
    public int getOrderedPendingFrames() { return orderedPendingFrames; }
    public long getOrderedOldestAgeNanos() { return orderedOldestAgeNanos; }
    public long getOrderedReleasedFrames() { return orderedReleasedFrames; }
    public long getOrderedMaxWaitNanos() { return orderedMaxWaitNanos; }
    public boolean isRemoteApplicationBatchSupported() { return isRemoteApplicationBatchSupported; }
    public long getApplicationBatches() { return applicationBatches; }
    public long getApplicationBatchBytes() { return applicationBatchBytes; }
    public long getApplicationBatchMaxBytes() { return applicationBatchMaxBytes; }
    public boolean isRemoteAckPolicySupported() { return isRemoteAckPolicySupported; }
    public long getAckRepeatedPackets() { return ackRepeatedPackets; }
    public long getAckRepeatedFrameSets() { return ackRepeatedFrameSets; }
    public boolean isAckProtection() { return ackProtection; }
    public long getAckFlushDelayNanos() { return ackFlushDelayNanos; }
    public long getAckRepeatDelayNanos() { return ackRepeatDelayNanos; }
    public boolean isRemoteDemandSupported() { return isRemoteDemandSupported; }
    public boolean isApplicationLimited() { return applicationLimited; }
    public String getBacklogState() { return backlogState; }
    public long getBacklogAgeNanos() { return backlogAgeNanos; }
    public long getBacklogProbes() { return backlogProbes; }
    public boolean isRemoteFecSupported() { return isRemoteFecSupported; }
    public long getFecRecovered() { return fecRecovered; }
    public long getFecParityPackets() { return fecParityPackets; }
    public long getFecParityBytes() { return fecParityBytes; }
    public long getFecExpired() { return fecExpired; }
    public int getFecDataShards() { return fecDataShards; }
    public int getFecParityShards() { return fecParityShards; }
    public double getFecRecoveryRatio() { return fecRecoveryRatio; }
    public boolean isRemoteAdvancedRecoverySupported() { return isRemoteAdvancedRecoverySupported; }
    public long getRackRetransmitBytes() { return rackRetransmitBytes; }
    public long getRackRetransmitFrameSets() { return rackRetransmitFrameSets; }
    public long getRackSpuriousAcks() { return rackSpuriousAcks; }
    public long getPtoProbes() { return ptoProbes; }
    public long getPtoProbeBytes() { return ptoProbeBytes; }
    public long getPtoProbeAckedBytes() { return ptoProbeAckedBytes; }
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
    public int getOrderedWorstChannel() { return orderedWorstChannel; }
    public int[] getOrderedChannelPending() { return java.util.Arrays.copyOf(orderedChannelPending, 8); }
    public long[] getOrderedChannelOldestAgeNanos() { return java.util.Arrays.copyOf(orderedChannelOldestAgeNanos, 8); }
    public int[] getOrderedChannelBlockedOrderIndex() { return java.util.Arrays.copyOf(orderedChannelBlockedOrderIndex, 8); }
    public long[] getOrderedChannelReleasedFrames() { return java.util.Arrays.copyOf(orderedChannelReleasedFrames, 8); }
    public long[] getOrderedChannelMaxWaitNanos() { return java.util.Arrays.copyOf(orderedChannelMaxWaitNanos, 8); }

    static void writePayload(ByteBuf buffer, SimpleMetricsLogger logger, int defaultPendingFrameSets, long nowMillis) {
        buffer.writeByte(VERSION);
        buffer.writeLong(nowMillis);
        buffer.writeInt(logger.getCurrentQueuedBytes());
        buffer.writeInt((int) (logger.getMeasureBurstTokens() + defaultPendingFrameSets));
        buffer.writeDouble(logger.getMeasureErrorRate());
        buffer.writeInt(logger.getMeasureTX());
        buffer.writeInt(logger.getMeasureRX());
        buffer.writeLong(logger.getMeasureRTTns());
        buffer.writeLong(logger.getMeasureRTTnsStdDev());
        buffer.writeDouble(logger.getAdaptivePacingRate());
        buffer.writeLong(logger.getAdaptiveDeliveryRate());
        buffer.writeDouble(logger.getAdaptiveLossRatio());
        buffer.writeByte(lossTypeCode(logger.getAdaptiveLossType()));
        buffer.writeByte(congestionModeCode(logger.getCongestionMode()));
        buffer.writeLong(logger.getCongestionWindowBytes());
        buffer.writeLong(logger.getInFlightBytes());
        buffer.writeLong(logger.getBandwidthBytesPerSecond());
        buffer.writeLong(logger.getReliableFrameDuplicates());
        buffer.writeByte(congestionReasonCode(logger.getCongestionReason()));
        buffer.writeDouble(logger.getRttInflation());
        int flags = logger.isPacingCapped() ? 1 : 0;
        if (logger.isBandwidthProbeSuppressed()) flags |= 2;
        buffer.writeByte(flags);
        buffer.writeLong(logger.getNacksDeferred());
        buffer.writeLong(logger.getReorderedPackets());
        buffer.writeLong(logger.getNackRetransmitBytes());
        buffer.writeLong(logger.getTimeoutRetransmitBytes());
        buffer.writeLong(logger.getAdaptiveBytePacingRate());
        buffer.writeInt(logger.getFragmentPendingBuilders());
        buffer.writeLong(logger.getFragmentPendingBytes());
        buffer.writeLong(logger.getFragmentOldestAgeNanos());
        buffer.writeLong(logger.getFragmentCompleted());
        buffer.writeLong(logger.getFragmentMaxAgeNanos());
        buffer.writeInt(logger.getOrderedPendingFrames());
        buffer.writeLong(logger.getOrderedOldestAgeNanos());
        buffer.writeLong(logger.getOrderedReleasedFrames());
        buffer.writeLong(logger.getOrderedMaxWaitNanos());
        buffer.writeLong(logger.getApplicationBatches());
        buffer.writeLong(logger.getApplicationBatchBytes());
        buffer.writeLong(logger.getApplicationBatchMaxBytes());
        buffer.writeLong(logger.getAckRepeatedPackets());
        buffer.writeLong(logger.getAckRepeatedFrameSets());
        buffer.writeByte(logger.isAdaptiveAckProtection() ? 1 : 0);
        buffer.writeLong(logger.getAdaptiveAckFlushDelayNanos());
        buffer.writeLong(logger.getAdaptiveAckRepeatDelayNanos());
        buffer.writeByte(logger.isApplicationLimited() ? 1 : 0);
        buffer.writeByte(backlogStateCode(logger.getBacklogState()));
        buffer.writeLong(logger.getBacklogAgeNanos());
        buffer.writeLong(logger.getBacklogProbes());
        buffer.writeLong(logger.getNacksDeferredExpired());
        buffer.writeLong(logger.getNacksDeferredConfirmed());
        buffer.writeLong(logger.getNackGraceBypassed());
        buffer.writeByte(logger.isAdaptiveNackGraceBypass() ? 1 : 0);
        buffer.writeLong(logger.getNackRepeatedPackets());
        buffer.writeLong(logger.getNackRepeatedFrameSets());
        buffer.writeLong(logger.getFecRecovered());
        buffer.writeLong(logger.getFecParityPackets());
        buffer.writeLong(logger.getFecParityBytes());
        buffer.writeLong(logger.getFecExpired());
        buffer.writeInt(logger.getFecDataShards());
        buffer.writeInt(logger.getFecParityShards());
        buffer.writeDouble(logger.getFecRecoveryRatio());
        buffer.writeLong(logger.getRackRetransmitBytes());
        buffer.writeLong(logger.getRackRetransmitFrameSets());
        buffer.writeLong(logger.getRackSpuriousAcks());
        buffer.writeLong(logger.getPtoProbes());
        buffer.writeLong(logger.getPtoProbeBytes());
        buffer.writeLong(logger.getPtoProbeAckedBytes());
        buffer.writeInt(logger.getPtoCount());
        buffer.writeLong(logger.getLastAckProgressAgeNanos());
        buffer.writeLong(logger.getApplicationLimitedRecoveryPackets());
        buffer.writeLong(logger.getApplicationLimitedRecoveryBytes());
        buffer.writeInt(logger.getRecoveryQueueDepth());
        buffer.writeLong(logger.getRecoveryQueueOldestAgeNanos());
        buffer.writeDouble(logger.getRecoveryDebt());
        buffer.writeInt(logger.getRecoveryDebtChannel());
        buffer.writeLong(logger.getTargetedFecPackets());
        buffer.writeLong(logger.getTargetedFecBytes());
        buffer.writeLong(logger.getTargetedFecRecovered());
        buffer.writeInt(logger.getTargetedFecChannel());
        buffer.writeInt(logger.getOrderedWorstChannel());
        final int[] channelPending = logger.getOrderedChannelPending();
        final long[] channelOldest = logger.getOrderedChannelOldestAgeNanos();
        final int[] channelBlocked = logger.getOrderedChannelBlockedOrderIndex();
        final long[] channelReleased = logger.getOrderedChannelReleasedFrames();
        final long[] channelMaxWait = logger.getOrderedChannelMaxWaitNanos();
        for (int value : channelPending) buffer.writeInt(value);
        for (long value : channelOldest) buffer.writeLong(value);
        for (int value : channelBlocked) buffer.writeInt(value);
        for (long value : channelReleased) buffer.writeLong(value);
        for (long value : channelMaxWait) buffer.writeLong(value);
    }

    boolean readPayload(ByteBuf byteBuf) {
        if (byteBuf.readableBytes() < 1 + 8 + 4 + 4 + 8 + 4 + 4) return false;
        final byte version = byteBuf.readByte();
        if (version != VERSION) return false;

        final long time = byteBuf.readLong();
        if (time < this.lastRecv) return false;
        this.lastRecv = time;
        this.isRemoteSupported = true;
        this.queuedBytes = byteBuf.readInt();
        this.burst = byteBuf.readInt();
        this.errorRate = byteBuf.readDouble();
        this.tx = byteBuf.readInt();
        this.rx = byteBuf.readInt();
        if (byteBuf.readableBytes() >= ADAPTIVE_EXTENDED_SIZE) {
            this.rttNanos = byteBuf.readLong();
            this.rttStdDevNanos = byteBuf.readLong();
            this.pacingRate = byteBuf.readDouble();
            this.deliveryRate = byteBuf.readLong();
            this.lossRatio = byteBuf.readDouble();
            this.lossType = lossType(byteBuf.readUnsignedByte());
            this.congestionMode = congestionMode(byteBuf.readUnsignedByte());
            this.congestionWindow = byteBuf.readLong();
            this.inFlight = byteBuf.readLong();
            this.bandwidth = byteBuf.readLong();
            this.reliableFrameDuplicates = byteBuf.readLong();
            this.congestionReason = congestionReason(byteBuf.readUnsignedByte());
            this.rttInflation = byteBuf.readDouble();
            final int flags = byteBuf.readUnsignedByte();
            this.pacingCapped = (flags & 1) != 0;
            this.bandwidthProbeSuppressed = (flags & 2) != 0;
            this.isRemoteAdaptiveSupported = true;
            if (byteBuf.readableBytes() >= RECOVERY_EXTENDED_SIZE) {
                this.nacksDeferred = byteBuf.readLong();
                this.reorderedPackets = byteBuf.readLong();
                this.nackRetransmitBytes = byteBuf.readLong();
                this.timeoutRetransmitBytes = byteBuf.readLong();
                this.isRemoteRecoverySupported = true;
                if (byteBuf.readableBytes() >= BYTE_PACING_EXTENDED_SIZE) {
                    this.bytePacingRate = byteBuf.readLong();
                    this.isRemoteBytePacingSupported = true;
                    if (byteBuf.readableBytes() >= HOL_EXTENDED_SIZE) {
                        this.fragmentPendingBuilders = byteBuf.readInt();
                        this.fragmentPendingBytes = byteBuf.readLong();
                        this.fragmentOldestAgeNanos = byteBuf.readLong();
                        this.fragmentCompleted = byteBuf.readLong();
                        this.fragmentMaxAgeNanos = byteBuf.readLong();
                        this.orderedPendingFrames = byteBuf.readInt();
                        this.orderedOldestAgeNanos = byteBuf.readLong();
                        this.orderedReleasedFrames = byteBuf.readLong();
                        this.orderedMaxWaitNanos = byteBuf.readLong();
                        this.isRemoteHolSupported = true;
                        if (byteBuf.readableBytes() >= APPLICATION_BATCH_EXTENDED_SIZE) {
                            this.applicationBatches = byteBuf.readLong();
                            this.applicationBatchBytes = byteBuf.readLong();
                            this.applicationBatchMaxBytes = byteBuf.readLong();
                            this.isRemoteApplicationBatchSupported = true;
                            if (byteBuf.readableBytes() >= ACK_POLICY_EXTENDED_SIZE) {
                                this.ackRepeatedPackets = byteBuf.readLong();
                                this.ackRepeatedFrameSets = byteBuf.readLong();
                                this.ackProtection = byteBuf.readUnsignedByte() != 0;
                                this.ackFlushDelayNanos = byteBuf.readLong();
                                this.ackRepeatDelayNanos = byteBuf.readLong();
                                this.isRemoteAckPolicySupported = true;
                                if (byteBuf.readableBytes() >= DEMAND_EXTENDED_SIZE) {
                                    this.applicationLimited = byteBuf.readUnsignedByte() != 0;
                                    this.backlogState = backlogState(byteBuf.readUnsignedByte());
                                    this.backlogAgeNanos = byteBuf.readLong();
                                    this.backlogProbes = byteBuf.readLong();
                                    this.isRemoteDemandSupported = true;
                                    if (byteBuf.readableBytes() >= NACK_OUTCOME_EXTENDED_SIZE) {
                                        this.nacksDeferredExpired = byteBuf.readLong();
                                        this.nacksDeferredConfirmed = byteBuf.readLong();
                                        this.isRemoteNackOutcomeSupported = true;
                                        if (byteBuf.readableBytes() >= NACK_POLICY_EXTENDED_SIZE) {
                                            this.nackGraceBypassed = byteBuf.readLong();
                                            this.nackGraceBypass = byteBuf.readUnsignedByte() != 0;
                                            this.isRemoteNackPolicySupported = true;
                                            if (byteBuf.readableBytes() >= NACK_REPEAT_EXTENDED_SIZE) {
                                                this.nackRepeatedPackets = byteBuf.readLong();
                                                this.nackRepeatedFrameSets = byteBuf.readLong();
                                                this.isRemoteNackRepeatSupported = true;
                                                if (byteBuf.readableBytes() >= FEC_EXTENDED_SIZE) {
                                                    this.fecRecovered = byteBuf.readLong();
                                                    this.fecParityPackets = byteBuf.readLong();
                                                    this.fecParityBytes = byteBuf.readLong();
                                                    this.fecExpired = byteBuf.readLong();
                                                    this.fecDataShards = byteBuf.readInt();
                                                    this.fecParityShards = byteBuf.readInt();
                                                    this.fecRecoveryRatio = byteBuf.readDouble();
                                                    this.isRemoteFecSupported = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        if (this.isRemoteFecSupported && byteBuf.readableBytes() >= ADVANCED_RECOVERY_EXTENDED_SIZE) {
            this.rackRetransmitBytes = byteBuf.readLong();
            this.rackRetransmitFrameSets = byteBuf.readLong();
            this.rackSpuriousAcks = byteBuf.readLong();
            this.ptoProbes = byteBuf.readLong();
            this.ptoProbeBytes = byteBuf.readLong();
            this.ptoProbeAckedBytes = byteBuf.readLong();
            this.ptoCount = byteBuf.readInt();
            this.lastAckProgressAgeNanos = byteBuf.readLong();
            this.applicationLimitedRecoveryPackets = byteBuf.readLong();
            this.applicationLimitedRecoveryBytes = byteBuf.readLong();
            this.recoveryQueueDepth = byteBuf.readInt();
            this.recoveryQueueOldestAgeNanos = byteBuf.readLong();
            this.recoveryDebt = byteBuf.readDouble();
            this.recoveryDebtChannel = byteBuf.readInt();
            this.targetedFecPackets = byteBuf.readLong();
            this.targetedFecBytes = byteBuf.readLong();
            this.targetedFecRecovered = byteBuf.readLong();
            this.targetedFecChannel = byteBuf.readInt();
            this.orderedWorstChannel = byteBuf.readInt();
            for (int i = 0; i < 8; i++) this.orderedChannelPending[i] = byteBuf.readInt();
            for (int i = 0; i < 8; i++) this.orderedChannelOldestAgeNanos[i] = byteBuf.readLong();
            for (int i = 0; i < 8; i++) this.orderedChannelBlockedOrderIndex[i] = byteBuf.readInt();
            for (int i = 0; i < 8; i++) this.orderedChannelReleasedFrames[i] = byteBuf.readLong();
            for (int i = 0; i < 8; i++) this.orderedChannelMaxWaitNanos[i] = byteBuf.readLong();
            this.isRemoteAdvancedRecoverySupported = true;
        }
        return true;
    }

    private static int lossTypeCode(String value) {
        return switch (value) {
            case "RANDOM" -> 1;
            case "BURST" -> 2;
            case "MTU_BLACK_HOLE" -> 3;
            case "QUEUE" -> 4;
            case "RATE_LIMIT" -> 5;
            default -> 0;
        };
    }

    private static int backlogStateCode(String value) {
        return switch (value) {
            case "WARMUP" -> 1;
            case "BULK" -> 2;
            default -> 0;
        };
    }

    private static String backlogState(int value) {
        return switch (value) {
            case 1 -> "WARMUP";
            case 2 -> "BULK";
            default -> "IDLE";
        };
    }

    private static String lossType(int code) {
        return switch (code) {
            case 1 -> "RANDOM";
            case 2 -> "BURST";
            case 3 -> "MTU_BLACK_HOLE";
            case 4 -> "QUEUE";
            case 5 -> "RATE_LIMIT";
            default -> "NONE";
        };
    }

    private static int congestionModeCode(String value) {
        return switch (value) {
            case "DRAIN" -> 1;
            case "PROBE_BW" -> 2;
            case "PROBE_RTT" -> 3;
            default -> 0;
        };
    }

    private static String congestionMode(int code) {
        return switch (code) {
            case 1 -> "DRAIN";
            case 2 -> "PROBE_BW";
            case 3 -> "PROBE_RTT";
            default -> "STARTUP";
        };
    }

    private static int congestionReasonCode(String value) {
        return switch (value) {
            case "RTT_INFLATION_LOSS" -> 1;
            case "TIMEOUT_LOSS" -> 2;
            case "CONSECUTIVE_LOSS" -> 3;
            case "ISOLATED_LOSS" -> 4;
            case "MTU_BLACK_HOLE" -> 5;
            case "ECN_CE" -> 6;
            case "NON_CONGESTIVE_HIGH_LOSS" -> 7;
            default -> 0;
        };
    }

    private static String congestionReason(int code) {
        return switch (code) {
            case 1 -> "RTT_INFLATION_LOSS";
            case 2 -> "TIMEOUT_LOSS";
            case 3 -> "CONSECUTIVE_LOSS";
            case 4 -> "ISOLATED_LOSS";
            case 5 -> "MTU_BLACK_HOLE";
            case 6 -> "ECN_CE";
            case 7 -> "NON_CONGESTIVE_HIGH_LOSS";
            default -> "NONE";
        };
    }
}
