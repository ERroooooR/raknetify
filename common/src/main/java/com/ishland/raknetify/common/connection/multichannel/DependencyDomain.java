/*
 * This file is a part of the Raknetify project, licensed under MIT.
 */

package com.ishland.raknetify.common.connection.multichannel;

/**
 * Application-level dependency domains for Minecraft game packets.
 *
 * <p>Guarded bulk shares the strict scheduling class so producer order is
 * retained before the transport split. It may use its dedicated RakNet order
 * channel only after the peer negotiates the bulk-watermark protocol;
 * otherwise callers must send it as strict world state.</p>
 */
public enum DependencyDomain {

    // Strict frames retain the same transport priority as guarded bulk. A
    // strict frame may carry a watermark for an older bulk frame, so letting
    // it consume bandwidth first would only delay the dependency it awaits.
    STRICT_WORLD(7, SchedulingClass.STRICT, 1),
    INDEPENDENT_CONTROL(1, SchedulingClass.CONTROL, 4),
    EPHEMERAL_EFFECT(4, SchedulingClass.EFFECT, 3),
    // The wire channel is separate, but the scheduling class intentionally
    // remains STRICT. This preserves strict-before-bulk order; the reciprocal
    // bulk-before-strict direction is protected by the negotiated watermark.
    GUARDED_BULK(6, SchedulingClass.STRICT, 1);

    private final int orderChannel;
    private final SchedulingClass schedulingClass;
    private final int transportPriority;

    DependencyDomain(
            int orderChannel,
            SchedulingClass schedulingClass,
            int transportPriority
    ) {
        this.orderChannel = orderChannel;
        this.schedulingClass = schedulingClass;
        this.transportPriority = transportPriority;
    }

    public int orderChannel() {
        return orderChannel;
    }

    SchedulingClass schedulingClass() {
        return schedulingClass;
    }

    /**
     * Local-only priority used after a large frame has been fragmented.
     *
     * <p>This is deliberately separate from the wire order channel. The
     * receiver still applies ordinary RakNet reliability and the negotiated
     * causal watermark; priority only controls which already-queued fragment
     * gets admitted to the next datagram.</p>
     */
    public int transportPriority() {
        return transportPriority;
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
