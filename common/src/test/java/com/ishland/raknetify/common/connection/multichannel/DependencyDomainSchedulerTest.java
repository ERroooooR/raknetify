/*
 * This file is a part of the Raknetify project, licensed under MIT.
 */

package com.ishland.raknetify.common.connection.multichannel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DependencyDomainSchedulerTest {

    @Test
    void strictWorldAndGuardedBulkShareOneCausalFifo() {
        final DependencyDomainScheduler<String> scheduler =
                new DependencyDomainScheduler<>();
        scheduler.offer(DependencyDomain.STRICT_WORLD, "spawn", 100);
        scheduler.offer(DependencyDomain.GUARDED_BULK, "chunk", 100);
        scheduler.offer(DependencyDomain.STRICT_WORLD, "metadata", 100);

        assertEquals("spawn", scheduler.poll().value());
        assertEquals("chunk", scheduler.poll().value());
        assertEquals("metadata", scheduler.poll().value());
        assertTrue(scheduler.isEmpty());
    }

    @Test
    void eligiblePollLeavesBackpressuredDomainsQueued() {
        final DependencyDomainScheduler<String> scheduler =
                new DependencyDomainScheduler<>();
        scheduler.offer(DependencyDomain.GUARDED_BULK, "chunk", 100);
        scheduler.offer(DependencyDomain.STRICT_WORLD, "state", 100);
        scheduler.offer(DependencyDomain.INDEPENDENT_CONTROL, "control", 100);
        scheduler.offer(DependencyDomain.EPHEMERAL_EFFECT, "effect", 100);

        assertTrue(scheduler.has(domain ->
                domain == DependencyDomain.INDEPENDENT_CONTROL
                        || domain == DependencyDomain.EPHEMERAL_EFFECT
        ));
        assertEquals(
                "control",
                scheduler.poll(domain ->
                        domain == DependencyDomain.INDEPENDENT_CONTROL
                                || domain == DependencyDomain.EPHEMERAL_EFFECT
                ).value()
        );
        assertEquals(
                "effect",
                scheduler.poll(domain ->
                        domain == DependencyDomain.INDEPENDENT_CONTROL
                                || domain == DependencyDomain.EPHEMERAL_EFFECT
                ).value()
        );
        assertNull(scheduler.poll(domain ->
                domain == DependencyDomain.INDEPENDENT_CONTROL
                        || domain == DependencyDomain.EPHEMERAL_EFFECT
        ));
        assertEquals(2, scheduler.size());
        assertEquals("chunk", scheduler.poll().value());
        assertEquals("state", scheduler.poll().value());
    }

    @Test
    void independentDomainsProgressWithoutBreakingTheirFifo() {
        final DependencyDomainScheduler<String> scheduler =
                new DependencyDomainScheduler<>();
        for (int i = 0; i < 12; i++) {
            scheduler.offer(DependencyDomain.STRICT_WORLD, "world-" + i, 16 * 1024);
        }
        scheduler.offer(DependencyDomain.INDEPENDENT_CONTROL, "control-0", 8 * 1024);
        scheduler.offer(DependencyDomain.INDEPENDENT_CONTROL, "control-1", 8 * 1024);
        scheduler.offer(DependencyDomain.EPHEMERAL_EFFECT, "effect-0", 4 * 1024);

        final List<String> firstSix = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            firstSix.add(scheduler.poll().value());
        }

        assertTrue(firstSix.contains("control-0"));
        assertTrue(firstSix.contains("control-1"));
        assertTrue(firstSix.contains("effect-0"));
        assertTrue(firstSix.indexOf("control-0") < firstSix.indexOf("control-1"));
    }

    @Test
    void capacityFailureDoesNotTakeOwnershipOrCorruptAccounting() {
        final DependencyDomainScheduler<String> scheduler =
                new DependencyDomainScheduler<>(2, 10);

        assertTrue(scheduler.offer(
                DependencyDomain.STRICT_WORLD,
                "world",
                6
        ));
        assertTrue(scheduler.offer(
                DependencyDomain.INDEPENDENT_CONTROL,
                "control",
                4
        ));
        assertFalse(scheduler.offer(
                DependencyDomain.EPHEMERAL_EFFECT,
                "overflow",
                1
        ));
        assertEquals(2, scheduler.size());
        assertEquals(10, scheduler.bytes());

        scheduler.poll();
        assertEquals(1, scheduler.size());
        assertTrue(scheduler.bytes() == 4 || scheduler.bytes() == 6);
        scheduler.poll();
        assertEquals(0, scheduler.size());
        assertEquals(0, scheduler.bytes());
    }
}
