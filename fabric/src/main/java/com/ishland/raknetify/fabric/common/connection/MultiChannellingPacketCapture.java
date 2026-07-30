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

import com.ishland.raknetify.common.connection.RakNetSimpleMultiChannelCodec;
import com.ishland.raknetify.common.connection.multichannel.CustomPayloadChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import net.minecraft.network.packet.c2s.common.CustomPayloadC2SPacket;
import net.minecraft.network.packet.s2c.common.CustomPayloadS2CPacket;

@ChannelHandler.Sharable
public class MultiChannellingPacketCapture extends ChannelOutboundHandlerAdapter {

    private final ThreadLocal<Class<?>> packetClass = new ThreadLocal<>();

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        final Class<?> previousPacketClass = this.packetClass.get();
        this.packetClass.set(msg.getClass());
        try {
            ctx.write(msg, promise);
        } finally {
            // Encoders and compatibility handlers may perform a nested write.
            // Restoring the outer scope prevents the remainder of that write
            // from being conservatively misclassified as an unknown packet.
            // Thread-local state also keeps this sharable handler safe when
            // several connection event loops encode at the same time.
            if (previousPacketClass == null) {
                this.packetClass.remove();
            } else {
                this.packetClass.set(previousPacketClass);
            }
        }
    }

    public Class<?> getPacketClass() {
        return this.packetClass.get();
    }

    public void setPacketClass(Class<?> packetClass) {
        if (packetClass == null) {
            this.packetClass.remove();
        } else {
            this.packetClass.set(packetClass);
        }
    }

    public RakNetSimpleMultiChannelCodec.OverrideHandler getCaptureBasedHandler() {
        return new CaptureBasedHandler();
    }

    public RakNetSimpleMultiChannelCodec.OverrideHandler getCustomPayloadHandler() {
        return new CustomPayloadChannel.OverrideHandler(value ->
                getPacketClass() == CustomPayloadS2CPacket.class
                        || getPacketClass() == CustomPayloadC2SPacket.class
        );
    }

    private class CaptureBasedHandler implements RakNetSimpleMultiChannelCodec.OverrideHandler {

        @Override
        public RakNetSimpleMultiChannelCodec.OverrideResult getChannelOverride(ByteBuf buf, boolean suppressWarning) {
            final Class<?> currentPacketClass = getPacketClass();
            final int legacyChannel = RakNetMultiChannel.getPacketChannelOverride(
                    currentPacketClass,
                    suppressWarning
            );
            return RakNetSimpleMultiChannelCodec.OverrideResult.classify(
                    RakNetMultiChannel.getPacketDependencyDomain(
                            currentPacketClass,
                            suppressWarning
                    ),
                    legacyChannel
            );
        }

    }

}
