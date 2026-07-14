/*
 * This file is a part of the Velocity implementation of the Raknetify
 * project, licensed under GPLv3.
 *
 * Copyright (c) 2022-2025 ishland
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.ishland.raknetify.velocity.connection;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

/**
 * Adapts ZSTD_Compresser's Velocity-side TCP framing to RakNet message
 * boundaries. Its encoder includes an outer packet-length VarInt because the
 * normal Velocity frame decoder removes it on the peer. Raknetify intentionally
 * no-ops that decoder, so the redundant prefix must be removed before creating
 * a RakNet frame.
 */
public class ZstdCompresserCompatibilityHandler extends ChannelOutboundHandlerAdapter {

    public static final String NAME = "raknetify-zstd-compresser-compatibility";
    public static final String ZSTD_ENCODER_NAME = "zstd_encoder";

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf buf && ctx.pipeline().get(ZSTD_ENCODER_NAME) != null) {
            final int savedReaderIndex = buf.readerIndex();
            final int frameLength = tryReadVarInt(buf);
            if (frameLength >= 0 && frameLength == buf.readableBytes()) {
                final ByteBuf payload = buf.readRetainedSlice(frameLength);
                ReferenceCountUtil.release(buf);
                ctx.write(payload, promise);
                return;
            }
            buf.readerIndex(savedReaderIndex);
        }
        super.write(ctx, msg, promise);
    }

    private static int tryReadVarInt(ByteBuf buf) {
        int value = 0;
        for (int position = 0; position < 5; position++) {
            if (!buf.isReadable()) {
                return -1;
            }
            final byte current = buf.readByte();
            value |= (current & 0x7F) << (position * 7);
            if ((current & 0x80) == 0) {
                return value >= 0 ? value : -1;
            }
        }
        return -1;
    }
}
