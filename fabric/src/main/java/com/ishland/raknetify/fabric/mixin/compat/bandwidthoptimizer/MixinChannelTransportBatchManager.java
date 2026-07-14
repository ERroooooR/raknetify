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

package com.ishland.raknetify.fabric.mixin.compat.bandwidthoptimizer;

import com.ishland.raknetify.common.connection.RakNetConnectionUtil;
import io.netty.channel.ChannelHandlerContext;
import network.ycc.raknet.RakNet;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Pseudo;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/**
 * BandwidthOptimizer's delayed batches are emitted as a new custom-payload
 * packet. That loses the original packet class used by Raknetify to select a
 * RakNet order channel and collapses mixed traffic onto one ordered channel.
 * Keep BO's per-packet transport compression, but skip its batching only for
 * RakNet connections so Raknetify can retain packet QoS classification.
 */
@Pseudo
@Mixin(targets = "com.PinkCats.bandwidthoptimizer.channel.algorithm.batch.ChannelTransportBatchManager", remap = false)
public class MixinChannelTransportBatchManager {

    @Inject(
            method = "shouldBatchOutboundPacket(Lio/netty/channel/ChannelHandlerContext;)Z",
            at = @At("HEAD"),
            cancellable = true,
            require = 0,
            remap = false
    )
    private static void raknetify$preserveRakNetPacketChannels(
            ChannelHandlerContext context,
            CallbackInfoReturnable<Boolean> cir
    ) {
        if (RakNetConnectionUtil.isBandwidthOptimizerCompatibilityEnabled()
                && context != null
                && context.channel().config() instanceof RakNet.Config) {
            cir.setReturnValue(false);
        }
    }
}
