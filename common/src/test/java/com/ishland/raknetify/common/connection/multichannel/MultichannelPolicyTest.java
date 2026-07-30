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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MultichannelPolicyTest {

    @Test
    void compatibilityProfileOnlyOpensExplicitIndependentDomains() {
        assertEquals(7, MultichannelPolicy.selectChannel(
                MultichannelPolicy.Profile.COMPATIBILITY,
                DependencyDomain.STRICT_WORLD,
                2,
                true
        ));
        assertEquals(6, MultichannelPolicy.selectChannel(
                MultichannelPolicy.Profile.COMPATIBILITY,
                DependencyDomain.GUARDED_BULK,
                7,
                true,
                true
        ));
        assertEquals(7, MultichannelPolicy.selectChannel(
                MultichannelPolicy.Profile.COMPATIBILITY,
                DependencyDomain.GUARDED_BULK,
                7,
                true
        ));
        assertEquals(1, MultichannelPolicy.selectChannel(
                MultichannelPolicy.Profile.COMPATIBILITY,
                DependencyDomain.INDEPENDENT_CONTROL,
                -1,
                true
        ));
        assertEquals(4, MultichannelPolicy.selectChannel(
                MultichannelPolicy.Profile.COMPATIBILITY,
                DependencyDomain.EPHEMERAL_EFFECT,
                4,
                true
        ));
    }

    @Test
    void aggressiveProfileRetainsLegacyChannelForComparison() {
        assertEquals(-1, MultichannelPolicy.selectChannel(
                MultichannelPolicy.Profile.AGGRESSIVE,
                DependencyDomain.INDEPENDENT_CONTROL,
                -1,
                true
        ));
        assertEquals(2, MultichannelPolicy.selectChannel(
                MultichannelPolicy.Profile.AGGRESSIVE,
                DependencyDomain.STRICT_WORLD,
                2,
                true
        ));
        assertEquals(6, MultichannelPolicy.selectChannel(
                MultichannelPolicy.Profile.AGGRESSIVE,
                DependencyDomain.GUARDED_BULK,
                7,
                true,
                true
        ));
    }

    @Test
    void everyProfileStaysStrictUntilCausalCapabilitiesAreReady() {
        for (MultichannelPolicy.Profile profile : MultichannelPolicy.Profile.values()) {
            assertEquals(7, MultichannelPolicy.selectChannel(
                    profile,
                    DependencyDomain.EPHEMERAL_EFFECT,
                    4,
                    false
            ));
        }
    }

    @Test
    void profileAliasesAndInvalidValuesAreConservative() {
        assertEquals(MultichannelPolicy.Profile.AGGRESSIVE, MultichannelPolicy.parseProfile("AGGRESSIVE"));
        assertEquals(MultichannelPolicy.Profile.COMPATIBILITY, MultichannelPolicy.parseProfile("safe"));
        assertEquals(MultichannelPolicy.Profile.COMPATIBILITY, MultichannelPolicy.parseProfile("future-value"));
        assertEquals(MultichannelPolicy.Profile.COMPATIBILITY, MultichannelPolicy.parseProfile(null));
    }

}
