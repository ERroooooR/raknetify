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

package com.ishland.raknetify.common.connection.multichannel;

import com.ishland.raknetify.common.connection.RakNetSimpleMultiChannelCodec;
import com.ishland.raknetify.common.util.MathUtil;
import io.netty.buffer.ByteBuf;
import java.util.Objects;
import java.util.function.IntPredicate;

public class CustomPayloadChannel {

    public static class OverrideHandler implements RakNetSimpleMultiChannelCodec.OverrideHandler {

        private final IntPredicate isCustomPayload;

        public OverrideHandler(IntPredicate isCustomPayload) {
            this.isCustomPayload = Objects.requireNonNull(isCustomPayload);
        }

        @Override
        public RakNetSimpleMultiChannelCodec.OverrideResult getChannelOverride(ByteBuf origBuf, boolean suppressWarning) {
            ByteBuf buf = origBuf.slice();
            if (buf.readableBytes() < 1) return RakNetSimpleMultiChannelCodec.OverrideResult.pass();
            final int packetId;
            try {
                packetId = MathUtil.readVarInt(buf);
            } catch (RuntimeException e) {
                return RakNetSimpleMultiChannelCodec.OverrideResult.pass();
            }
            if (isCustomPayload.test(packetId)) {
                // A payload identifier is not a causal-independence proof.
                // Known entity-spawn payloads, unknown mod payloads and even
                // payload formats introduced by newer versions remain strict.
                return RakNetSimpleMultiChannelCodec.OverrideResult.strict();
            } else {
                return RakNetSimpleMultiChannelCodec.OverrideResult.pass();
            }
        }

    }

}
