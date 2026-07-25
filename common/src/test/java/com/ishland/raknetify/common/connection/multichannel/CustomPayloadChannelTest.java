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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CustomPayloadChannelTest {

    private static final int CUSTOM_PAYLOAD_PACKET_ID = 42;

    @Test
    void neoforgeComplexEntitySpawnDataUsesTheStrictDomain() {
        final CustomPayloadChannel.OverrideHandler handler =
                new CustomPayloadChannel.OverrideHandler(id -> id == CUSTOM_PAYLOAD_PACKET_ID);
        final ByteBuf packet = customPayloadPacket("neoforge:advanced_add_entity");

        try {
            final var result = handler.getChannelOverride(packet, true);
            assertEquals(true, result.matched());
            assertEquals(7, result.channel());
            assertEquals(DependencyDomain.STRICT_WORLD, result.domain());
        } finally {
            packet.release();
        }
    }

    @Test
    void unrelatedCustomPayloadUsesTheStrictDomain() {
        final CustomPayloadChannel.OverrideHandler handler =
                new CustomPayloadChannel.OverrideHandler(id -> id == CUSTOM_PAYLOAD_PACKET_ID);
        final ByteBuf packet = customPayloadPacket("example:unrelated");

        try {
            final var result = handler.getChannelOverride(packet, true);
            assertEquals(true, result.matched());
            assertEquals(7, result.channel());
            assertEquals(DependencyDomain.STRICT_WORLD, result.domain());
        } finally {
            packet.release();
        }
    }

    private static ByteBuf customPayloadPacket(String identifier) {
        final byte[] bytes = identifier.getBytes(StandardCharsets.UTF_8);
        final ByteBuf packet = Unpooled.buffer(2 + bytes.length);
        packet.writeByte(CUSTOM_PAYLOAD_PACKET_ID);
        packet.writeByte(bytes.length);
        packet.writeBytes(bytes);
        return packet;
    }

}
