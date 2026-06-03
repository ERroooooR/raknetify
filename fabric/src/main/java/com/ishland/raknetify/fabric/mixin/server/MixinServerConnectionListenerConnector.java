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

package com.ishland.raknetify.fabric.mixin.server;

import com.ishland.raknetify.common.Constants;
import com.ishland.raknetify.common.connection.RakNetConnectionUtil;
import com.ishland.raknetify.common.connection.RaknetifyEventLoops;
import com.ishland.raknetify.common.util.ThreadLocalUtil;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import network.ycc.raknet.server.channel.RakNetServerChannel;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Pseudo;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

import java.net.SocketAddress;

@Pseudo
@Mixin(targets = "net.minecraft.server.network.ServerConnectionListener")
public abstract class MixinServerConnectionListenerConnector {

    @Unique
    private void raknetify$logBootstrapStep(String step, Object detail) {
        if (ThreadLocalUtil.isInitializingRaknet()) {
            System.out.println("Raknetify: " + step + " -> " + detail);
        }
    }

    @Unique
    private static boolean raknetify$useEpoll(Object value) {
        if (value instanceof Class<?> clazz) {
            return clazz.getName().contains("epoll");
        }
        return value != null && value.getClass().getName().contains("epoll");
    }

    @Unique
    private ChannelFuture raknetify$forceAutoRead(ChannelFuture future, String source) {
        if (ThreadLocalUtil.isInitializingRaknet()) {
            future.addListener(listener -> {
                if (listener.isSuccess()) {
                    future.channel().config().setAutoRead(true);
                    System.out.println("Raknetify: " + source + " -> forced autoRead=true on " + future.channel().getClass().getName());
                }
            });
        }
        return future;
    }

    @Inject(method = "bind(Ljava/net/SocketAddress;)V", at = @At("HEAD"), require = 0, remap = false)
    private void raknetify$traceSocketBind(SocketAddress address, CallbackInfo ci) {
        if (ThreadLocalUtil.isInitializingRaknet()) {
            System.out.println("Raknetify: intercepted connector bind(SocketAddress) for " + address);
        }
    }

    @WrapOperation(method = "bind(Ljava/net/SocketAddress;)V", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/ServerBootstrap;group(Lio/netty/channel/EventLoopGroup;)Lio/netty/bootstrap/ServerBootstrap;", remap = false), require = 0, remap = false)
    private ServerBootstrap redirectGroup(ServerBootstrap instance, EventLoopGroup group, Operation<ServerBootstrap> original) {
        raknetify$logBootstrapStep("connector.server-bootstrap.group", group.getClass().getName());
        return ThreadLocalUtil.isInitializingRaknet()
                ? original.call(instance, raknetify$useEpoll(group) ? RaknetifyEventLoops.EPOLL_EVENT_LOOP_GROUP.get() : RaknetifyEventLoops.NIO_EVENT_LOOP_GROUP.get())
                : original.call(instance, group);
    }

    @WrapOperation(method = "bind(Ljava/net/SocketAddress;)V", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/AbstractBootstrap;group(Lio/netty/channel/EventLoopGroup;)Lio/netty/bootstrap/AbstractBootstrap;", remap = false), require = 0, remap = false)
    private AbstractBootstrap<ServerBootstrap, ServerChannel> redirectAbstractGroup(AbstractBootstrap<ServerBootstrap, ServerChannel> instance, EventLoopGroup group, Operation<AbstractBootstrap<ServerBootstrap, ServerChannel>> original) {
        raknetify$logBootstrapStep("connector.abstract-bootstrap.group", group.getClass().getName());
        return ThreadLocalUtil.isInitializingRaknet()
                ? original.call(instance, raknetify$useEpoll(group) ? RaknetifyEventLoops.EPOLL_EVENT_LOOP_GROUP.get() : RaknetifyEventLoops.NIO_EVENT_LOOP_GROUP.get())
                : original.call(instance, group);
    }

    @WrapOperation(method = "bind(Ljava/net/SocketAddress;)V", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/ServerBootstrap;channel(Ljava/lang/Class;)Lio/netty/bootstrap/AbstractBootstrap;", remap = false), require = 0, remap = false)
    private AbstractBootstrap<ServerBootstrap, ServerChannel> redirectChannel(ServerBootstrap instance, Class<? extends ServerSocketChannel> aClass, Operation<AbstractBootstrap<ServerBootstrap, ServerChannel>> original) {
        raknetify$logBootstrapStep("connector.server-bootstrap.channel", aClass.getName());
        return ThreadLocalUtil.isInitializingRaknet()
                ? instance.channelFactory(() -> {
                    final boolean useEpoll = raknetify$useEpoll(aClass);
                    raknetify$logBootstrapStep("connector.server-bootstrap.channelFactory", useEpoll ? "epoll-raknet" : "nio-raknet");
                    RakNetServerChannel channel = new RakNetServerChannel(() -> {
                        final DatagramChannel channel1 = useEpoll ? new EpollDatagramChannel() : new NioDatagramChannel();
                        channel1.config().setOption(ChannelOption.SO_REUSEADDR, true);
                        channel1.config().setOption(ChannelOption.IP_TOS, RakNetConnectionUtil.DEFAULT_IP_TOS);
                        channel1.config().setRecvByteBufAllocator(new FixedRecvByteBufAllocator(Constants.LARGE_MTU + 512).maxMessagesPerRead(128));
                        return channel1;
                    });
                    channel.setProvidedApplicationEventLoop(RaknetifyEventLoops.DEFAULT_EVENT_LOOP_GROUP.get().next());
                    return channel;
                })
                : original.call(instance, aClass);
    }

    @WrapOperation(method = "bind(Ljava/net/SocketAddress;)V", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/AbstractBootstrap;channel(Ljava/lang/Class;)Lio/netty/bootstrap/AbstractBootstrap;", remap = false), require = 0, remap = false)
    private AbstractBootstrap<ServerBootstrap, ServerChannel> redirectAbstractChannel(AbstractBootstrap<ServerBootstrap, ServerChannel> instance, Class<? extends ServerSocketChannel> aClass, Operation<AbstractBootstrap<ServerBootstrap, ServerChannel>> original) {
        raknetify$logBootstrapStep("connector.abstract-bootstrap.channel", aClass.getName());
        return ThreadLocalUtil.isInitializingRaknet()
                ? instance.channelFactory(() -> {
                    final boolean useEpoll = raknetify$useEpoll(aClass);
                    raknetify$logBootstrapStep("connector.abstract-bootstrap.channelFactory", useEpoll ? "epoll-raknet" : "nio-raknet");
                    RakNetServerChannel channel = new RakNetServerChannel(() -> {
                        final DatagramChannel channel1 = useEpoll ? new EpollDatagramChannel() : new NioDatagramChannel();
                        channel1.config().setOption(ChannelOption.SO_REUSEADDR, true);
                        channel1.config().setOption(ChannelOption.IP_TOS, RakNetConnectionUtil.DEFAULT_IP_TOS);
                        channel1.config().setRecvByteBufAllocator(new FixedRecvByteBufAllocator(Constants.LARGE_MTU + 512).maxMessagesPerRead(128));
                        return channel1;
                    });
                    channel.setProvidedApplicationEventLoop(RaknetifyEventLoops.DEFAULT_EVENT_LOOP_GROUP.get().next());
                    return channel;
                })
                : original.call(instance, aClass);
    }

    @WrapOperation(method = "bind(Ljava/net/SocketAddress;)V", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/AbstractBootstrap;option(Lio/netty/channel/ChannelOption;Ljava/lang/Object;)Lio/netty/bootstrap/AbstractBootstrap;", remap = false), require = 0, remap = false)
    private AbstractBootstrap<ServerBootstrap, ServerChannel> redirectAbstractOption(AbstractBootstrap<ServerBootstrap, ServerChannel> instance, ChannelOption<?> option, Object value, Operation<AbstractBootstrap<ServerBootstrap, ServerChannel>> original) {
        if (ThreadLocalUtil.isInitializingRaknet() && option == ChannelOption.AUTO_READ && Boolean.FALSE.equals(value)) {
            raknetify$logBootstrapStep("connector.abstract-bootstrap.option", "forcing AUTO_READ=true");
            return original.call(instance, option, Boolean.TRUE);
        }
        return original.call(instance, option, value);
    }

    @WrapOperation(method = "bind(Ljava/net/SocketAddress;)V", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/AbstractBootstrap;bind()Lio/netty/channel/ChannelFuture;", remap = false), require = 0, remap = false)
    private ChannelFuture redirectAbstractBind(AbstractBootstrap<ServerBootstrap, ServerChannel> instance, Operation<ChannelFuture> original) {
        return raknetify$forceAutoRead(original.call(instance), "connector.abstract-bootstrap.bind");
    }

    @WrapOperation(method = "bind(Ljava/net/SocketAddress;)V", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/ServerBootstrap;bind()Lio/netty/channel/ChannelFuture;", remap = false), require = 0, remap = false)
    private ChannelFuture redirectServerBootstrapBind(ServerBootstrap instance, Operation<ChannelFuture> original) {
        return raknetify$forceAutoRead(original.call(instance), "connector.server-bootstrap.bind");
    }
}
