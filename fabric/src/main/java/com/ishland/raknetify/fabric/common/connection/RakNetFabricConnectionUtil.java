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

package com.ishland.raknetify.fabric.common.connection;

import com.ishland.raknetify.common.Constants;
import com.ishland.raknetify.common.connection.MultiChannelingStreamingCompression;
import com.ishland.raknetify.common.connection.RakNetConnectionUtil;
import com.ishland.raknetify.common.connection.RakNetSimpleMultiChannelCodec;
import com.ishland.raknetify.fabric.common.compat.viafabric.ViaFabricCompatInjector;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.util.AttributeKey;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueDatagramChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import network.ycc.raknet.RakNet;

public class RakNetFabricConnectionUtil {

    public static final String NAME_RAKNETIFY_MULTI_CHANNEL_PACKET_CATURE = "raknetify-multi-channel-packet-cature";
    private static final AttributeKey<Boolean> RAKNETIFY_INITIALIZED = AttributeKey.valueOf("raknetify:fabric-initialized");
    private static final AttributeKey<Boolean> RAKNETIFY_POST_INITIALIZED = AttributeKey.valueOf("raknetify:fabric-post-initialized");

    private RakNetFabricConnectionUtil() {
    }

    public static void initChannel(Channel channel) {
        if (channel.config() instanceof RakNet.Config) {
            if (Boolean.TRUE.equals(channel.attr(RAKNETIFY_INITIALIZED).get())) {
                return;
            }
            channel.attr(RAKNETIFY_INITIALIZED).set(true);
            RakNetConnectionUtil.initChannel(channel);
            final RakNetSimpleMultiChannelCodec multiChannelCodec = new RakNetSimpleMultiChannelCodec(Constants.RAKNET_GAME_PACKET_ID);
            // ZSTD_Compresser can batch several Minecraft packets into one frame.
            // Keep those frames on one ordered RakNet channel because their
            // original packet boundaries are no longer available for prioritizing.
            multiChannelCodec.addHandler((buf, suppressWarning) ->
                    channel.pipeline().get("zstd_encoder") != null
                            ? RakNetSimpleMultiChannelCodec.OverrideResult.strict()
                            : RakNetSimpleMultiChannelCodec.OverrideResult.pass()
            );
            channel.pipeline().addAfter(MultiChannelingStreamingCompression.NAME, RakNetSimpleMultiChannelCodec.NAME, multiChannelCodec);
        }
    }

    public static void postInitChannel(Channel channel, boolean isClientSide) {
        if (channel.config() instanceof RakNet.Config) {
            if (Boolean.TRUE.equals(channel.attr(RAKNETIFY_POST_INITIALIZED).get())) {
                return;
            }
            channel.attr(RAKNETIFY_POST_INITIALIZED).set(true);
            ViaFabricCompatInjector.inject(channel, isClientSide);
            channel.pipeline().replace("timeout", "timeout", new ChannelDuplexHandler()); // no-op
            channel.pipeline().replace("splitter", "splitter", new ChannelDuplexHandler()); // no-op
            channel.pipeline().replace("prepender", "prepender", new ChannelDuplexHandler()); // no-op
            final MultiChannellingPacketCapture handler = new MultiChannellingPacketCapture();
            channel.pipeline().addLast(NAME_RAKNETIFY_MULTI_CHANNEL_PACKET_CATURE, handler);
            onPipelineReorder(channel.pipeline());
            channel.pipeline().get(RakNetSimpleMultiChannelCodec.class)
                    .addHandler(handler.getCustomPayloadHandler())
                    .addHandler(handler.getCaptureBasedHandler());
            channel.pipeline().addLast("raknetify-handle-compression-compatibility", new RakNetCompressionCompatibilityHandler());
            channel.pipeline().addBefore("packet_handler", RakNetFabricChannelEventListener.NAME, new RakNetFabricChannelEventListener());
        }
    }

    static void onPipelineReorder(ChannelPipeline pipeline) {
        if (pipeline.get("encoder") == null) {
//            System.out.println("Reordering failed: no encoder");
            return;
        }
//        System.out.println("Reordering");
        ChannelHandler handler = pipeline.remove(RakNetFabricConnectionUtil.NAME_RAKNETIFY_MULTI_CHANNEL_PACKET_CATURE);
        if (handler != null) {
            pipeline.addAfter("encoder", RakNetFabricConnectionUtil.NAME_RAKNETIFY_MULTI_CHANNEL_PACKET_CATURE, handler);
        }
    }

    public static DatagramChannel fromSocketChannel(Class<? extends SocketChannel> clazz) {
        if (clazz == NioSocketChannel.class) {
            return new NioDatagramChannel();
        } else if (clazz == EpollSocketChannel.class) {
            return new EpollDatagramChannel();
        } else if (clazz == KQueueSocketChannel.class) {
            return new KQueueDatagramChannel();
        } else {
            throw new UnsupportedOperationException(clazz.getName());
        }
    }

}
