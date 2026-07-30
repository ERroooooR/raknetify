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
import com.ishland.raknetify.common.util.NetworkInterfaceListener;
import com.llamalad7.mixinextras.injector.wrapoperation.Operation;
import com.llamalad7.mixinextras.injector.wrapoperation.WrapOperation;
import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.ServerNetworkIo;
import network.ycc.raknet.server.channel.RakNetServerChannel;
import org.jetbrains.annotations.Nullable;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

@Mixin(ServerNetworkIo.class)
public abstract class MixinServerNetworkIo {

    @Unique
    private static final int raknetify$portOverride = Integer.getInteger("raknetify.fabric.portOverride", -1);

    @Shadow
    @Final
    private MinecraftServer server;

    @Shadow
    public abstract void bind(@Nullable InetAddress address, int port) throws IOException;

    @Shadow
    public volatile boolean active;

    @Shadow
    @Final
    private List<ChannelFuture> channels;
    @Unique
    private Consumer<NetworkInterfaceListener.InterfaceAddressChangeEvent> raknetify$eventListener = null;

    @Inject(method = "bind", at = @At("HEAD"))
    private void bindUdp(InetAddress address, int port, CallbackInfo ci) throws IOException {
        if (!ThreadLocalUtil.isInitializingRaknet()) {
            try {
                ThreadLocalUtil.setInitializingRaknet(true);
                final boolean hasPortOverride = raknetify$portOverride > 0 && raknetify$portOverride < 65535;
                if (address == null) {
                    for (NetworkInterface networkInterface : NetworkInterface.networkInterfaces().toList()) {
                        final Iterator<InetAddress> iterator = networkInterface.getInetAddresses().asIterator();
                        while (iterator.hasNext()) {
                            final InetAddress inetAddress = iterator.next();
                            System.out.println("Starting raknetify server on %s".formatted(inetAddress));
                            try {
                                bind(inetAddress, hasPortOverride ? raknetify$portOverride : port);
                            } catch (IOException t) {
                                System.out.println("**** FAILED TO BIND TO PORT! %s".formatted(t.getMessage()));
                            }
                        }
                    }

                    if (this.raknetify$eventListener == null) {
                        this.raknetify$eventListener = event -> {
                            if (!this.active) {
                                NetworkInterfaceListener.removeListener(this.raknetify$eventListener);
                                return;
                            }
                            try {
                                ThreadLocalUtil.setInitializingRaknet(true);
                                final InetAddress inetAddress = event.address();
                                if (event.added()) {
                                    System.out.println("Starting raknetify server on %s".formatted(inetAddress));
                                    try {
                                        bind(inetAddress, hasPortOverride ? raknetify$portOverride : port);
                                    } catch (IOException t) {
                                        System.out.println("**** FAILED TO BIND TO PORT! %s".formatted(t.getMessage()));
                                    } catch (Throwable t) {
                                        t.printStackTrace();
                                    }
                                } else {
                                    synchronized (this.channels) {
                                        for (Iterator<ChannelFuture> iter = this.channels.iterator(); iter.hasNext(); ) {
                                            ChannelFuture channel = iter.next();
                                            final SocketAddress socketAddress = channel.channel().localAddress();
                                            if (socketAddress instanceof InetSocketAddress channelAddress) {
                                                if (inetAddress.equals(channelAddress.getAddress())) {
                                                    System.out.println("Stopping raknetify server on %s".formatted(inetAddress));
                                                    channel.channel().close();
                                                    iter.remove();
                                                }
                                            }
                                        }
                                    }
                                }
                            } catch (Throwable e) {
                                e.printStackTrace();
                            } finally {
                                ThreadLocalUtil.setInitializingRaknet(false);
                            }
                        };
                        NetworkInterfaceListener.addListener(event -> this.server.submit(() -> raknetify$eventListener.accept(event)));
                    }
                } else {
                    System.out.println("Starting raknetify server on %s".formatted(address));
                    bind(address, hasPortOverride ? raknetify$portOverride : port);
                }
            } finally {
                ThreadLocalUtil.setInitializingRaknet(false);
            }
        }
    }

    @Unique
    private void raknetify$logBootstrapStep(String step, Object detail) {
        if (ThreadLocalUtil.isInitializingRaknet()) {
            System.out.println("Raknetify: " + step + " -> " + detail);
        }
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

    @WrapOperation(method = "bind", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/ServerBootstrap;group(Lio/netty/channel/EventLoopGroup;)Lio/netty/bootstrap/ServerBootstrap;", remap = false))
    private ServerBootstrap redirectGroup(ServerBootstrap instance, EventLoopGroup group, Operation<ServerBootstrap> original) {
        final boolean useEpoll = Epoll.isAvailable() && this.server.isUsingNativeTransport();
        raknetify$logBootstrapStep("server-bootstrap.group", group.getClass().getName());
        return ThreadLocalUtil.isInitializingRaknet()
                ? original.call(instance, useEpoll ? RaknetifyEventLoops.EPOLL_EVENT_LOOP_GROUP.get() : RaknetifyEventLoops.NIO_EVENT_LOOP_GROUP.get())
                : original.call(instance, group);
    }

    @WrapOperation(method = "bind", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/AbstractBootstrap;group(Lio/netty/channel/EventLoopGroup;)Lio/netty/bootstrap/AbstractBootstrap;", remap = false), require = 0, remap = false)
    private AbstractBootstrap<ServerBootstrap, ServerChannel> redirectAbstractGroup(AbstractBootstrap<ServerBootstrap, ServerChannel> instance, EventLoopGroup group, Operation<AbstractBootstrap<ServerBootstrap, ServerChannel>> original) {
        final boolean useEpoll = Epoll.isAvailable() && this.server.isUsingNativeTransport();
        raknetify$logBootstrapStep("abstract-bootstrap.group", group.getClass().getName());
        return ThreadLocalUtil.isInitializingRaknet()
                ? original.call(instance, useEpoll ? RaknetifyEventLoops.EPOLL_EVENT_LOOP_GROUP.get() : RaknetifyEventLoops.NIO_EVENT_LOOP_GROUP.get())
                : original.call(instance, group);
    }

    @WrapOperation(method = "bind", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/ServerBootstrap;channel(Ljava/lang/Class;)Lio/netty/bootstrap/AbstractBootstrap;", remap = false))
    private AbstractBootstrap<ServerBootstrap, ServerChannel> redirectChannel(ServerBootstrap instance, Class<? extends ServerSocketChannel> aClass, Operation<AbstractBootstrap<ServerBootstrap, ServerChannel>> original) {
        final boolean useEpoll = Epoll.isAvailable() && this.server.isUsingNativeTransport();
        raknetify$logBootstrapStep("server-bootstrap.channel", aClass.getName());
        return ThreadLocalUtil.isInitializingRaknet()
                ? instance.channelFactory(() -> {
            raknetify$logBootstrapStep("server-bootstrap.channelFactory", useEpoll ? "epoll-raknet" : "nio-raknet");
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

    @WrapOperation(method = "bind", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/AbstractBootstrap;channel(Ljava/lang/Class;)Lio/netty/bootstrap/AbstractBootstrap;", remap = false), require = 0, remap = false)
    private AbstractBootstrap<ServerBootstrap, ServerChannel> redirectAbstractChannel(AbstractBootstrap<ServerBootstrap, ServerChannel> instance, Class<? extends ServerSocketChannel> aClass, Operation<AbstractBootstrap<ServerBootstrap, ServerChannel>> original) {
        final boolean useEpoll = Epoll.isAvailable() && this.server.isUsingNativeTransport();
        raknetify$logBootstrapStep("abstract-bootstrap.channel", aClass.getName());
        return ThreadLocalUtil.isInitializingRaknet()
                ? instance.channelFactory(() -> {
            raknetify$logBootstrapStep("abstract-bootstrap.channelFactory", useEpoll ? "epoll-raknet" : "nio-raknet");
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

    @WrapOperation(method = "bind", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/AbstractBootstrap;option(Lio/netty/channel/ChannelOption;Ljava/lang/Object;)Lio/netty/bootstrap/AbstractBootstrap;", remap = false), require = 0, remap = false)
    private AbstractBootstrap<ServerBootstrap, ServerChannel> redirectAbstractOption(AbstractBootstrap<ServerBootstrap, ServerChannel> instance, ChannelOption<?> option, Object value, Operation<AbstractBootstrap<ServerBootstrap, ServerChannel>> original) {
        if (ThreadLocalUtil.isInitializingRaknet() && option == ChannelOption.AUTO_READ && Boolean.FALSE.equals(value)) {
            raknetify$logBootstrapStep("abstract-bootstrap.option", "forcing AUTO_READ=true");
            return original.call(instance, option, Boolean.TRUE);
        }
        return original.call(instance, option, value);
    }

    @WrapOperation(method = "bind", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/AbstractBootstrap;bind()Lio/netty/channel/ChannelFuture;", remap = false), require = 0, remap = false)
    private ChannelFuture redirectAbstractBind(AbstractBootstrap<ServerBootstrap, ServerChannel> instance, Operation<ChannelFuture> original) {
        return raknetify$forceAutoRead(original.call(instance), "abstract-bootstrap.bind");
    }

    @WrapOperation(method = "bind", at = @At(value = "INVOKE", target = "Lio/netty/bootstrap/ServerBootstrap;bind()Lio/netty/channel/ChannelFuture;", remap = false), require = 0, remap = false)
    private ChannelFuture redirectServerBootstrapBind(ServerBootstrap instance, Operation<ChannelFuture> original) {
        return raknetify$forceAutoRead(original.call(instance), "server-bootstrap.bind");
    }

}
