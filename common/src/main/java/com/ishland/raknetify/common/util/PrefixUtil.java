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

package com.ishland.raknetify.common.util;

import com.ishland.raknetify.common.Constants;

public class PrefixUtil {

    public static Info getInfo(String address) {
        if (address.startsWith(Constants.RAKNET_GATE_LARGE_MTU_PREFIX)) {
            return new Info(true, address.substring(Constants.RAKNET_GATE_LARGE_MTU_PREFIX.length()), true, true);
        } else if (address.startsWith(Constants.RAKNET_GATE_PREFIX)) {
            return new Info(true, address.substring(Constants.RAKNET_GATE_PREFIX.length()), false, true);
        } else if (address.startsWith(Constants.RAKNET_PREFIX)) {
            return new Info(true, address.substring(Constants.RAKNET_PREFIX.length()), false, false);
        } else if (address.startsWith(Constants.RAKNET_LARGE_MTU_PREFIX)) {
            return new Info(true, address.substring(Constants.RAKNET_LARGE_MTU_PREFIX.length()), true, false);
        } else {
            return new Info(false, address, false, false);
        }
    }

    /**
     * Strips a trailing IPv4 port from a host string. IPv6 addresses enclosed
     * in brackets (e.g., {@code [::1]:25565}) are handled by returning the
     * unbracketed address. IPv6 addresses without brackets are returned as-is.
     */
    public static String stripPort(String host) {
        if (host == null || host.isEmpty()) {
            return host;
        }
        // IPv6 bracketed: [::1]:25565 -> ::1
        if (host.startsWith("[")) {
            int end = host.lastIndexOf(']');
            if (end > 0) {
                return host.substring(1, end);
            }
            return host;
        }
        // Un-bracketed IPv6: multiple colons (e.g. "::1", "2001:db8::1234")
        final int firstColon = host.indexOf(':');
        if (firstColon >= 0 && firstColon != host.lastIndexOf(':')) {
            return host;
        }
        // IPv4/hostname: check last colon for numeric port
        int lastColon = host.lastIndexOf(':');
        if (lastColon > 0) {
            String portPart = host.substring(lastColon + 1);
            if (portPart.chars().allMatch(Character::isDigit)) {
                return host.substring(0, lastColon);
            }
        }
        return host;
    }

    public record Info(boolean useRakNet, String stripped, boolean largeMTU, boolean gateRouteHint) {
    }

}
