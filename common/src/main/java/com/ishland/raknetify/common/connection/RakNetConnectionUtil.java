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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.AttributeKey;
import io.netty.handler.timeout.ReadTimeoutHandler;
import network.ycc.raknet.RakNet;
import network.ycc.raknet.client.channel.RakNetClientThreadedChannel;
import network.ycc.raknet.frame.Frame;
import network.ycc.raknet.pipeline.ReliabilityHandler;
import network.ycc.raknet.server.channel.RakNetApplicationChannel;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import static com.ishland.raknetify.common.util.ReflectionUtil.accessible;

public class RakNetConnectionUtil {

    private static final AttributeKey<Boolean> RAKNETIFY_INITIALIZED = AttributeKey.valueOf("raknetify:initialized");
    private static final AttributeKey<Boolean> RAKNETIFY_PARENT_INITIALIZED = AttributeKey.valueOf("raknetify:parent-initialized");
    private static final boolean BANDWIDTH_OPTIMIZER_COMPATIBILITY_ENABLED = Boolean.parseBoolean(
            System.getProperty("raknetify.bandwidthOptimizerCompatibility", "true")
    );
    private static final boolean ZSTD_COMPRESSER_COMPATIBILITY_ENABLED = Boolean.parseBoolean(
            System.getProperty("raknetify.zstdCompresserCompatibility", "true")
    );
    private static volatile boolean externalStreamingCompression = false;
    private static volatile boolean suppressVanillaCompression = false;

    public static final int IP_TOS_LOWDELAY = 0b00010000;
    public static final int IP_TOS_THROUGHPUT = 0b00001000;
    public static final int IP_TOS_RELIABILITY = 0b00000100;

    public static final int DEFAULT_IP_TOS = configuredInt("raknetify.ipTos", 0xA0, 0, 255); // DSCP CS5
    public static final int DEFAULT_RETRY_DELAY_MILLIS = configuredInt("raknetify.retryDelayMillis", 50, 1, 1000);
    public static final int DEFAULT_READ_TIMEOUT_SECONDS = configuredInt("raknetify.readTimeoutSeconds", 15, 1, 120);
    public static final boolean DEFAULT_NACK_ENABLED = Boolean.parseBoolean(System.getProperty("raknetify.nackEnabled", "true"));
    public static final boolean DEFAULT_ADAPTIVE_TRANSPORT = Boolean.parseBoolean(System.getProperty("raknetify.adaptiveTransport", "true"));
    public static final boolean DEFAULT_ADAPTIVE_DSCP = Boolean.parseBoolean(System.getProperty("raknetify.adaptiveDscp", "false"));
    public static final int DEFAULT_PROTOCOL_VERSION = configuredInt("raknetify.protocolVersion", 12, 9, 12);
    public static final int DEFAULT_ADAPTIVE_MIN_PPS = configuredInt("raknetify.adaptiveMinPps", 30, 1, 100_000);
    public static final int DEFAULT_ADAPTIVE_MAX_PPS = configuredInt("raknetify.adaptiveMaxPps", 600, 1, 100_000);
    public static final int DEFAULT_SMALL_WRITE_COALESCE_MICROS = configuredInt("raknetify.smallWriteCoalesceMicros", 500, 0, 100_000);
    public static final int DEFAULT_PLPMTUD_MAX_MTU = configuredInt("raknetify.plpmtudMaxMtu", 1452, 576, 65_507);

    private RakNetConnectionUtil() {
    }

    private static final Comparator<Frame> cmp =
            Comparator
                    .comparingInt((Frame frame) -> frame.getReliability().isReliable ? 1 : 0) // unreliable then reliable
                    .thenComparingInt(frame -> frame.getReliability().isOrdered ? 1 : 0) // unordered then ordered
                    .thenComparingInt(Frame::getOrderChannel) // lower channel first
                    .thenComparingInt(Frame::getOrderIndex); // lower index first

    public static void initChannel(Channel channel) {
        if (channel.config() instanceof RakNet.Config config) {
            initializeCompatibility();
            config.setMaxQueuedBytes(Constants.MAX_QUEUED_SIZE);
            config.setMaxPendingFrameSets(Constants.MAX_PENDING_FRAME_SETS);
            config.setRetryDelayNanos(TimeUnit.NANOSECONDS.convert(DEFAULT_RETRY_DELAY_MILLIS, TimeUnit.MILLISECONDS));
            config.setDefaultPendingFrameSets(Constants.DEFAULT_PENDING_FRAME_SETS);
            config.setNACKEnabled(DEFAULT_NACK_ENABLED);
            config.setNoDelayEnabled(false);
            config.setAdaptiveTransportEnabled(DEFAULT_ADAPTIVE_TRANSPORT);
            config.setAdaptiveDscpEnabled(DEFAULT_ADAPTIVE_DSCP);
            config.setAdaptiveMinPps(Math.min(DEFAULT_ADAPTIVE_MIN_PPS, DEFAULT_ADAPTIVE_MAX_PPS));
            config.setAdaptiveMaxPps(Math.max(DEFAULT_ADAPTIVE_MIN_PPS, DEFAULT_ADAPTIVE_MAX_PPS));
            config.setSmallWriteCoalesceMicros(DEFAULT_SMALL_WRITE_COALESCE_MICROS);
            config.setPlpmtudMaxMtu(Math.max(config.getMTU(), DEFAULT_PLPMTUD_MAX_MTU));
            // This is an explicit JVM override, not a minimum version. In particular,
            // test and compatibility runs must be able to force v11 when netty-raknet
            // itself defaults to v12.
            config.setProtocolVersion(DEFAULT_PROTOCOL_VERSION);
//            config.setIgnoreResendGauge(true);

            if (Boolean.TRUE.equals(channel.attr(RAKNETIFY_INITIALIZED).get())) {
                return;
            }
            channel.attr(RAKNETIFY_INITIALIZED).set(true);

            initRaknetChannel(channel);

//            channel.pipeline().addLast("raknetify-flush-enforcer", new FlushEnforcer());
//            channel.pipeline().addLast("raknetify-flush-consolidation", new FlushConsolidationHandler(Integer.MAX_VALUE, true));
            channel.pipeline().addLast("raknetify-no-flush", new NoFlush());
            channel.pipeline().addLast(MultiChannelingStreamingCompression.NAME, new MultiChannelingStreamingCompression(
                    Constants.RAKNET_GAME_PACKET_ID,
                    Constants.RAKNET_STREAMING_COMPRESSION_PACKET_ID,
                    !externalStreamingCompression
            ));
//            channel.pipeline().addLast(MultiChannellingDataCodec.NAME, new MultiChannellingDataCodec(Constants.RAKNET_GAME_PACKET_ID));
            channel.pipeline().addLast("raknetify-frame-data-blocker", new FrameDataBlocker());
        }
    }

    /**
     * Selects an encoded-packet transport compressor supplied by another mod.
     * Raknetify keeps its compression handler in the pipeline for stable handler
     * ordering, but does not negotiate or run Deflate on top of that transport.
     */
    public static void useExternalStreamingCompression(String source) {
        useExternalStreamingCompression(source, true);
    }

    public static synchronized void useExternalStreamingCompression(String source, boolean suppressVanillaCompression) {
        if (externalStreamingCompression) {
            RakNetConnectionUtil.suppressVanillaCompression |= suppressVanillaCompression;
            return;
        }
        externalStreamingCompression = true;
        RakNetConnectionUtil.suppressVanillaCompression = suppressVanillaCompression;
        System.out.println("Raknetify: Using " + source + " for packet compression; Raknetify streaming compression is disabled");
    }

    public static boolean isExternalStreamingCompressionEnabled() {
        return externalStreamingCompression;
    }

    public static boolean isBandwidthOptimizerCompatibilityEnabled() {
        return BANDWIDTH_OPTIMIZER_COMPATIBILITY_ENABLED;
    }

    public static boolean isZstdCompresserCompatibilityEnabled() {
        return ZSTD_COMPRESSER_COMPATIBILITY_ENABLED;
    }

    public static boolean shouldSuppressVanillaCompression() {
        return suppressVanillaCompression;
    }

    public static void initializeCompatibility() {
        if (!externalStreamingCompression && detectZstdCompresserClient()) {
            // ZSTD_Compresser uses the vanilla SetCompression packet as the signal
            // that both endpoints should replace their compression handlers. Keep
            // that packet, while preventing Raknetify from adding Deflate on top.
            // Probe it before BandwidthOptimizer so this required negotiation also
            // survives when both compatibility targets are installed.
            useExternalStreamingCompression("ZSTD_Compresser", false);
        } else if (!externalStreamingCompression && detectBandwidthOptimizer()) {
            useExternalStreamingCompression("BandwidthOptimizer", true);
        }
    }

    private static boolean detectBandwidthOptimizer() {
        if (!isBandwidthOptimizerCompatibilityEnabled()) {
            return false;
        }

        return detectClass("BandwidthOptimizer", "com.PinkCats.bandwidthoptimizer.Bandwidthoptimizer");
    }

    private static boolean detectZstdCompresserClient() {
        if (!isZstdCompresserCompatibilityEnabled()) {
            return false;
        }

        return detectClass("ZSTD_Compresser", "top.furryaxw.zstd_compresser.Zstd_compresser");
    }

    private static boolean detectClass(String source, String className) {
        // Do not rely solely on FabricLoader here. When Raknetify is translated by
        // Sinytra Connector, native NeoForge mods are not necessarily visible in
        // FabricLoader's mod list, even though they share the runtime class path.
        final ClassLoader classLoader = RakNetConnectionUtil.class.getClassLoader();
        final String classResource = className.replace('.', '/') + ".class";
        try {
            if (classLoader.getResource(classResource) != null) {
                return true;
            }
            Class.forName(
                    className,
                    false,
                    classLoader
            );
            return true;
        } catch (ClassNotFoundException ignored) {
            return false;
        } catch (LinkageError | SecurityException error) {
            System.err.println("Raknetify: " + source + " compatibility probe failed: " + error);
            return false;
        }
    }

    private static void initRaknetChannel(Channel appChannel) {
        final Channel channel;
        final String threadedReadHandlerName;
        if (appChannel instanceof RakNetApplicationChannel) {
            channel = appChannel.parent();
            threadedReadHandlerName = RakNetApplicationChannel.NAME_SERVER_PARENT_THREADED_READ_HANDLER;
        } else if (appChannel instanceof RakNetClientThreadedChannel) {
            channel = appChannel.parent();
            threadedReadHandlerName = RakNetClientThreadedChannel.NAME_CLIENT_PARENT_THREADED_READ_HANDLER;
        } else {
            channel = appChannel;
            threadedReadHandlerName = null;
        }
        if (Boolean.TRUE.equals(channel.attr(RAKNETIFY_PARENT_INITIALIZED).get())) {
            return;
        }
        channel.attr(RAKNETIFY_PARENT_INITIALIZED).set(true);
        channel.pipeline().addLast(new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                if (ch.pipeline().get("raknetify-metrics-sync") != null) {
                    return;
                }
                final RakNet.Config config = (RakNet.Config) ch.config();
                final SimpleMetricsLogger simpleMetricsLogger = new SimpleMetricsLogger();
                config.setMetrics(simpleMetricsLogger);
                final MetricsSynchronizationHandler metricsSynchronizationHandler = new MetricsSynchronizationHandler();
                simpleMetricsLogger.setMetricsSynchronizationHandler(metricsSynchronizationHandler);
                final SynchronizationLayer synchronizationLayer = new SynchronizationLayer(Constants.SYNC_IGNORE_CHANNELS);
                reInitChannelForOrdering(channel);
                if (threadedReadHandlerName != null) {
                    ch.pipeline().addBefore(threadedReadHandlerName, "raknetify-metrics-sync", metricsSynchronizationHandler);
                    ch.pipeline().addBefore(threadedReadHandlerName, "raknetify-synchronization-layer", synchronizationLayer);
                } else {
                    ch.pipeline().addLast("raknetify-metrics-sync", metricsSynchronizationHandler);
                    ch.pipeline().addLast("raknetify-synchronization-layer", synchronizationLayer);
                }
                ch.pipeline().addFirst("raknetify-timeout", new ReadTimeoutHandler(DEFAULT_READ_TIMEOUT_SECONDS));
                ch.pipeline().addAfter("raknetify-timeout", "raknetify-timeout-logger", new ChannelHandlerAdapter() {
                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                        if (cause instanceof ReadTimeoutException) {
                            System.err.println("Raknetify: read timeout after " + DEFAULT_READ_TIMEOUT_SECONDS + "s, closing connection " + ctx.channel().remoteAddress());
                        }
                        ctx.fireExceptionCaught(cause);
                    }
                });
            }
        });
    }

    private static int configuredInt(String name, int defaultValue, int min, int max) {
        final int value = Integer.getInteger(name, defaultValue);
        if (value < min || value > max) {
            System.err.println("Raknetify: ignoring " + name + "=" + value + " outside " + min + ".." + max);
            return defaultValue;
        }
        return value;
    }

//    public static void postInitChannel(Channel channel, boolean isClientSide) {
//        if (channel.config() instanceof RakNet.Config) {
//            ViaFabricCompatInjector.inject(channel, isClientSide);
//            channel.pipeline().replace("timeout", "timeout", new ChannelDuplexHandler()); // no-op
//            channel.pipeline().replace("splitter", "splitter", new ChannelDuplexHandler()); // no-op
//            channel.pipeline().replace("prepender", "prepender", new ChannelDuplexHandler()); // no-op
//            final MultiChannellingPacketCapture handler = new MultiChannellingPacketCapture();
//            channel.pipeline().addLast("raknetify-multi-channel-packet-cature", handler);
//            channel.pipeline().get(MultiChannellingDataCodec.class).setCapture(handler);
//        }
//    }

    @SuppressWarnings("unchecked")
    private static void reInitChannelForOrdering(Channel channel) {
        if (channel.config() instanceof RakNet.Config config) {
            try {
                final ReliabilityHandler reliabilityHandler = channel.pipeline().get(ReliabilityHandler.class);
                final Field frameQueueField = accessible(ReliabilityHandler.class.getDeclaredField("frameQueue"));
                PriorityQueue<Frame> reliabilityHandlerFrameQueue = (PriorityQueue<Frame>) frameQueueField.get(reliabilityHandler);

                final PriorityQueue<Frame> newSet = new PriorityQueue<>(cmp);
                newSet.addAll(reliabilityHandlerFrameQueue);

                frameQueueField.set(reliabilityHandler, newSet);

            } catch (Throwable t) {
                System.err.println("Raknetify: Error occurred while reinitializing channel ordering");
                t.printStackTrace();
            }
        }
    }

}
