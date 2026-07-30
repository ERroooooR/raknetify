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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CausalFenceProtocolTest {

    @Test
    void requestAndAckRoundTrip() {
        final ByteBuf request = CausalFenceProtocol.encodeRequest(
                UnpooledByteBufAllocator.DEFAULT,
                7,
                3,
                0xff
        );
        try {
            final var decoded = (CausalFenceProtocol.Request) CausalFenceProtocol.decode(request);
            assertEquals(7, decoded.fenceId());
            assertEquals(3, decoded.epoch());
            assertEquals(0xff, decoded.channelMask());
        } finally {
            request.release();
        }

        final ByteBuf ack = CausalFenceProtocol.encodeAck(
                UnpooledByteBufAllocator.DEFAULT,
                7,
                3
        );
        try {
            final var decoded = (CausalFenceProtocol.Ack) CausalFenceProtocol.decode(ack);
            assertEquals(7, decoded.fenceId());
            assertEquals(3, decoded.epoch());
        } finally {
            ack.release();
        }
    }

}
