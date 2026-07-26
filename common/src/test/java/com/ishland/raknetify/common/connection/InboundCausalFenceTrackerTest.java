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

package com.ishland.raknetify.common.connection;

import io.netty.handler.codec.CorruptedFrameException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InboundCausalFenceTrackerTest {

    private static final int ALL_CHANNELS = 0xff;

    @Test
    void commitsOnlyAfterEveryChannelAndAcknowledgesDuplicates() {
        final InboundCausalFenceTracker tracker =
                new InboundCausalFenceTracker(ALL_CHANNELS);
        final CausalFenceProtocol.Request request =
                request(1, 1, ALL_CHANNELS);

        assertAction(
                InboundCausalFenceTracker.Action.PENDING,
                tracker.accept(request, 7)
        );
        assertAction(
                InboundCausalFenceTracker.Action.PENDING,
                tracker.accept(request, 7)
        );
        for (int channel = 6; channel > 0; channel--) {
            assertAction(
                    InboundCausalFenceTracker.Action.PENDING,
                    tracker.accept(request, channel)
            );
        }
        assertEquals(0, tracker.currentEpoch());

        final InboundCausalFenceTracker.Result completed =
                tracker.accept(request, 0);
        assertAction(
                InboundCausalFenceTracker.Action.COMMIT_AND_ACKNOWLEDGE,
                completed
        );
        assertEquals(1, completed.epoch());
        assertEquals(1, tracker.currentEpoch());

        final InboundCausalFenceTracker.Result duplicate =
                tracker.accept(request, 3);
        assertAction(
                InboundCausalFenceTracker.Action.ACKNOWLEDGE,
                duplicate
        );
        assertEquals(1, duplicate.epoch());
    }

    @Test
    void rejectsInterleavingEpochChangesAndCompletedIdReuse() {
        final InboundCausalFenceTracker tracker =
                new InboundCausalFenceTracker(ALL_CHANNELS);

        assertAction(
                InboundCausalFenceTracker.Action.PENDING,
                tracker.accept(request(1, 1, ALL_CHANNELS), 0)
        );
        assertThrows(
                CorruptedFrameException.class,
                () -> tracker.accept(request(2, 1, ALL_CHANNELS), 1)
        );
        assertThrows(
                CorruptedFrameException.class,
                () -> tracker.accept(request(1, 2, ALL_CHANNELS), 1)
        );

        for (int channel = 1; channel < CausalFenceProtocol.ORDER_CHANNEL_COUNT; channel++) {
            tracker.accept(request(1, 1, ALL_CHANNELS), channel);
        }
        assertThrows(
                CorruptedFrameException.class,
                () -> tracker.accept(request(1, 2, ALL_CHANNELS), 0)
        );
    }

    @Test
    void rejectsWrongMasksIgnoredChannelsAndSkippedEpochs() {
        final InboundCausalFenceTracker tracker =
                new InboundCausalFenceTracker(0xfe);

        assertThrows(
                CorruptedFrameException.class,
                () -> tracker.accept(request(1, 1, ALL_CHANNELS), 1)
        );
        assertThrows(
                CorruptedFrameException.class,
                () -> tracker.accept(request(1, 1, 0xfe), 0)
        );
        assertThrows(
                CorruptedFrameException.class,
                () -> tracker.accept(request(1, 2, 0xfe), 1)
        );
    }

    @Test
    void ignoresOlderFenceIdsAndCanClearAnIncompleteFence() {
        final InboundCausalFenceTracker tracker =
                new InboundCausalFenceTracker(ALL_CHANNELS);
        complete(tracker, 2, 1);

        assertAction(
                InboundCausalFenceTracker.Action.IGNORE,
                tracker.accept(request(1, 1, ALL_CHANNELS), 0)
        );

        assertAction(
                InboundCausalFenceTracker.Action.PENDING,
                tracker.accept(request(3, 2, ALL_CHANNELS), 0)
        );
        tracker.clearActive();
        complete(tracker, 4, 2);
        assertEquals(2, tracker.currentEpoch());
    }

    private static void complete(
            InboundCausalFenceTracker tracker,
            long fenceId,
            int epoch
    ) {
        final CausalFenceProtocol.Request request =
                request(fenceId, epoch, ALL_CHANNELS);
        for (int channel = 0;
             channel < CausalFenceProtocol.ORDER_CHANNEL_COUNT - 1;
             channel++) {
            assertAction(
                    InboundCausalFenceTracker.Action.PENDING,
                    tracker.accept(request, channel)
            );
        }
        assertAction(
                InboundCausalFenceTracker.Action.COMMIT_AND_ACKNOWLEDGE,
                tracker.accept(
                        request,
                        CausalFenceProtocol.ORDER_CHANNEL_COUNT - 1
                )
        );
    }

    private static CausalFenceProtocol.Request request(
            long fenceId,
            int epoch,
            int channelMask
    ) {
        return new CausalFenceProtocol.Request(fenceId, epoch, channelMask);
    }

    private static void assertAction(
            InboundCausalFenceTracker.Action expected,
            InboundCausalFenceTracker.Result result
    ) {
        assertEquals(expected, result.action());
    }
}
