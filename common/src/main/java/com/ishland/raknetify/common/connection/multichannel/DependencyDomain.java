/*
 * This file is a part of the Raknetify project, licensed under MIT.
 */

package com.ishland.raknetify.common.connection.multichannel;

/**
 * Application-level dependency domains for Minecraft game packets.
 *
 * <p>The strict and guarded-bulk domains intentionally share one scheduling
 * class and one RakNet order channel. Guarded bulk is classification and
 * observability only until an explicit commit dependency exists.</p>
 */
public enum DependencyDomain {

    STRICT_WORLD(7, SchedulingClass.STRICT),
    INDEPENDENT_CONTROL(1, SchedulingClass.CONTROL),
    EPHEMERAL_EFFECT(4, SchedulingClass.EFFECT),
    GUARDED_BULK(7, SchedulingClass.STRICT);

    private final int orderChannel;
    private final SchedulingClass schedulingClass;

    DependencyDomain(int orderChannel, SchedulingClass schedulingClass) {
        this.orderChannel = orderChannel;
        this.schedulingClass = schedulingClass;
    }

    public int orderChannel() {
        return orderChannel;
    }

    SchedulingClass schedulingClass() {
        return schedulingClass;
    }

    /**
     * Conservatively translates the old channel table. Only the old unordered
     * control bucket and the visual-effect bucket are considered independent.
     */
    public static DependencyDomain fromLegacyChannel(int channel) {
        if (channel == -1) {
            return INDEPENDENT_CONTROL;
        }
        if (channel == 4) {
            return EPHEMERAL_EFFECT;
        }
        return STRICT_WORLD;
    }

    enum SchedulingClass {
        STRICT(16 * 1024),
        CONTROL(8 * 1024),
        EFFECT(4 * 1024);

        private final int quantumBytes;

        SchedulingClass(int quantumBytes) {
            this.quantumBytes = quantumBytes;
        }

        int quantumBytes() {
            return quantumBytes;
        }
    }
}
