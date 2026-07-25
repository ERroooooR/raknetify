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

import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultichannelPolicyTest {

    @Test
    void compatibilityProfileForcesEveryGamePacketOntoTheCausalStream() {
        final var handler = MultichannelPolicy.profileHandler(MultichannelPolicy.Profile.COMPATIBILITY);
        final var result = handler.getChannelOverride(Unpooled.EMPTY_BUFFER, true);

        assertTrue(result.matched());
        assertEquals(MultichannelPolicy.STRICT_GAME_CHANNEL, result.channel());
    }

    @Test
    void aggressiveProfileFallsThroughToLegacyClassification() {
        final var handler = MultichannelPolicy.profileHandler(MultichannelPolicy.Profile.AGGRESSIVE);
        final var result = handler.getChannelOverride(Unpooled.EMPTY_BUFFER, true);

        assertFalse(result.matched());
    }

    @Test
    void profileAliasesAndInvalidValuesAreConservative() {
        assertEquals(MultichannelPolicy.Profile.AGGRESSIVE, MultichannelPolicy.parseProfile("AGGRESSIVE"));
        assertEquals(MultichannelPolicy.Profile.COMPATIBILITY, MultichannelPolicy.parseProfile("safe"));
        assertEquals(MultichannelPolicy.Profile.COMPATIBILITY, MultichannelPolicy.parseProfile("future-value"));
        assertEquals(MultichannelPolicy.Profile.COMPATIBILITY, MultichannelPolicy.parseProfile(null));
    }

}
