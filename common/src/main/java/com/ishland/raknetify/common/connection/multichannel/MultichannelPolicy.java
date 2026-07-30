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

import java.util.Locale;

/**
 * Connection-wide safety policy for Minecraft's originally single ordered
 * packet stream. Compatibility mode is deliberately the default: arbitrary
 * mod packets may have causal dependencies that cannot be inferred from their
 * packet class or custom-payload identifier.
 */
public final class MultichannelPolicy {

    public static final int STRICT_GAME_CHANNEL = 7;
    public static final int GUARDED_BULK_CHANNEL = 6;
    public static final String PROFILE_PROPERTY = "raknetify.multichannelProfile";

    private static final Profile CONFIGURED_PROFILE = parseProfile(
            System.getProperty(PROFILE_PROPERTY, "compatibility")
    );

    private MultichannelPolicy() {
    }

    public static Profile configuredProfile() {
        return CONFIGURED_PROFILE;
    }

    public static int selectChannel(
            Profile profile,
            DependencyDomain domain,
            int aggressiveChannel,
            boolean independentDomainsReady
    ) {
        return selectChannel(
                profile,
                domain,
                aggressiveChannel,
                independentDomainsReady,
                false
        );
    }

    public static int selectChannel(
            Profile profile,
            DependencyDomain domain,
            int aggressiveChannel,
            boolean independentDomainsReady,
            boolean guardedBulkReady
    ) {
        if (!independentDomainsReady) {
            return STRICT_GAME_CHANNEL;
        }
        if (domain == DependencyDomain.GUARDED_BULK && guardedBulkReady) {
            return GUARDED_BULK_CHANNEL;
        }
        if (profile == Profile.AGGRESSIVE) {
            return aggressiveChannel;
        }
        if (domain == DependencyDomain.GUARDED_BULK) {
            return STRICT_GAME_CHANNEL;
        }
        return domain.orderChannel();
    }

    static Profile parseProfile(String value) {
        if (value != null) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "aggressive" -> Profile.AGGRESSIVE;
                case "compatibility", "compatible", "safe" -> Profile.COMPATIBILITY;
                default -> {
                    System.err.println("Raknetify: Unknown " + PROFILE_PROPERTY + "=" + value
                            + ", using compatibility");
                    yield Profile.COMPATIBILITY;
                }
            };
        }
        return Profile.COMPATIBILITY;
    }

    public enum Profile {
        COMPATIBILITY,
        AGGRESSIVE
    }

}
