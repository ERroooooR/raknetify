/*
 * This file is a part of the Raknetify project, licensed under MIT.
 */

package com.ishland.raknetify.common.connection.multichannel;

import java.util.ArrayDeque;
import java.util.EnumMap;

/**
 * Work-conserving deficit round-robin scheduler.
 *
 * <p>FIFO is retained inside each scheduling class. Strict world state and
 * guarded bulk deliberately use the same queue, preserving their original
 * relative order.</p>
 */
public final class DependencyDomainScheduler<T> {

    private static final int MAX_ACCOUNTED_BYTES = 64 * 1024;
    private final EnumMap<DependencyDomain.SchedulingClass, ArrayDeque<Entry<T>>> queues =
            new EnumMap<>(DependencyDomain.SchedulingClass.class);
    private final EnumMap<DependencyDomain.SchedulingClass, Integer> deficits =
            new EnumMap<>(DependencyDomain.SchedulingClass.class);
    private int nextClass;
    private int size;

    public DependencyDomainScheduler() {
        for (DependencyDomain.SchedulingClass schedulingClass
                : DependencyDomain.SchedulingClass.values()) {
            queues.put(schedulingClass, new ArrayDeque<>());
            deficits.put(schedulingClass, 0);
        }
    }

    public void offer(DependencyDomain domain, T value, int bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes must be non-negative");
        }
        queues.get(domain.schedulingClass()).addLast(
                new Entry<>(domain, value, Math.max(1, Math.min(bytes, MAX_ACCOUNTED_BYTES)))
        );
        size++;
    }

    public Scheduled<T> poll() {
        if (size == 0) {
            return null;
        }

        final DependencyDomain.SchedulingClass[] classes =
                DependencyDomain.SchedulingClass.values();
        if (nonEmptyClassCount() == 1) {
            for (DependencyDomain.SchedulingClass schedulingClass : classes) {
                final ArrayDeque<Entry<T>> queue = queues.get(schedulingClass);
                if (!queue.isEmpty()) {
                    return removeFirst(schedulingClass, queue);
                }
            }
        }

        while (true) {
            final DependencyDomain.SchedulingClass schedulingClass = classes[nextClass];
            nextClass = (nextClass + 1) % classes.length;
            final ArrayDeque<Entry<T>> queue = queues.get(schedulingClass);
            if (queue.isEmpty()) {
                deficits.put(schedulingClass, 0);
                continue;
            }

            final int deficit = deficits.get(schedulingClass)
                    + schedulingClass.quantumBytes();
            deficits.put(schedulingClass, deficit);
            if (queue.peekFirst().costBytes <= deficit) {
                return removeFirst(schedulingClass, queue);
            }
        }
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    private Scheduled<T> removeFirst(
            DependencyDomain.SchedulingClass schedulingClass,
            ArrayDeque<Entry<T>> queue
    ) {
        final Entry<T> entry = queue.removeFirst();
        deficits.put(schedulingClass, Math.max(
                0,
                deficits.get(schedulingClass) - entry.costBytes
        ));
        size--;
        return new Scheduled<>(entry.domain, entry.value);
    }

    private int nonEmptyClassCount() {
        int count = 0;
        for (ArrayDeque<Entry<T>> queue : queues.values()) {
            if (!queue.isEmpty()) {
                count++;
            }
        }
        return count;
    }

    private record Entry<T>(DependencyDomain domain, T value, int costBytes) {
    }

    public record Scheduled<T>(DependencyDomain domain, T value) {
    }
}
