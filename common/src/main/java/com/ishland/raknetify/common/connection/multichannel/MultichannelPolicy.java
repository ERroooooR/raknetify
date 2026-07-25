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

import java.util.Locale;
import java.util.function.BooleanSupplier;

/**
 * Connection-wide safety policy for Minecraft's originally single ordered
 * packet stream. Compatibility mode is deliberately the default: arbitrary
 * mod packets may have causal dependencies that cannot be inferred from their
 * packet class or custom-payload identifier.
 */
public final class MultichannelPolicy {

    public static final int STRICT_GAME_CHANNEL = 7;
    public static final String PROFILE_PROPERTY = "raknetify.multichannelProfile";

    private static final Profile CONFIGURED_PROFILE = parseProfile(
            System.getProperty(PROFILE_PROPERTY, "compatibility")
    );

    private MultichannelPolicy() {
    }

    public static Profile configuredProfile() {
        return CONFIGURED_PROFILE;
    }

    public static RakNetSimpleMultiChannelCodec.OverrideHandler configuredProfileHandler() {
        return configuredProfileHandler(() -> false);
    }

    public static RakNetSimpleMultiChannelCodec.OverrideHandler configuredProfileHandler(
            BooleanSupplier independentChannelsReady
    ) {
        return profileHandler(CONFIGURED_PROFILE, independentChannelsReady);
    }

    static RakNetSimpleMultiChannelCodec.OverrideHandler profileHandler(Profile profile) {
        return profileHandler(profile, () -> true);
    }

    static RakNetSimpleMultiChannelCodec.OverrideHandler profileHandler(
            Profile profile,
            BooleanSupplier independentChannelsReady
    ) {
        return (buf, suppressWarning) -> profile == Profile.COMPATIBILITY
                || !independentChannelsReady.getAsBoolean()
                ? RakNetSimpleMultiChannelCodec.OverrideResult.route(STRICT_GAME_CHANNEL)
                : RakNetSimpleMultiChannelCodec.OverrideResult.pass();
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
