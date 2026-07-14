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
    private static final int EXTENDED_SIZE = 84;

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
               buffer = this.ctx.alloc().buffer(1 + 8 + 4 + 4 + 8 + 4 + 4 + EXTENDED_SIZE);
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
        if (byteBuf.readableBytes() >= EXTENDED_SIZE) {
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
