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

/**
 * Pure inbound state machine for an eight-channel causal drain fence.
 *
 * <p>The tracker commits an epoch only after one marker for every locally
 * active channel has arrived. Network I/O, acknowledgements, metrics, and
 * Netty events remain the responsibility of {@link SynchronizationLayer}.</p>
 */
final class InboundCausalFenceTracker {

    private final int activeChannelMask;
    private int currentEpoch;
    private long lastCompletedFenceId;
    private long activeFenceId;
    private ActiveFence activeFence;

    InboundCausalFenceTracker(int activeChannelMask) {
        if (activeChannelMask <= 0
                || (activeChannelMask >>> CausalFenceProtocol.ORDER_CHANNEL_COUNT) != 0) {
            throw new IllegalArgumentException(
                    "Invalid active causal fence channel mask: " + activeChannelMask
            );
        }
        this.activeChannelMask = activeChannelMask;
    }

    Result accept(CausalFenceProtocol.Request request, int channel) {
        if (request.channelMask() != activeChannelMask) {
            throw new CorruptedFrameException(
                    "Fence request channel mask does not match the active channels"
            );
        }
        if (channel < 0
                || channel >= CausalFenceProtocol.ORDER_CHANNEL_COUNT
                || (activeChannelMask & 1 << channel) == 0) {
            throw new CorruptedFrameException(
                    "Fence request arrived on an unexpected channel"
            );
        }

        if (request.fenceId() == lastCompletedFenceId) {
            if (request.epoch() != currentEpoch) {
                throw new CorruptedFrameException(
                        "Completed causal fence id was reused for another epoch"
                );
            }
            return new Result(Action.ACKNOWLEDGE, currentEpoch);
        }
        if (request.fenceId() < lastCompletedFenceId) {
            return new Result(Action.IGNORE, currentEpoch);
        }

        if (activeFence == null) {
            if (currentEpoch == Integer.MAX_VALUE
                    || request.epoch() != currentEpoch + 1) {
                throw new CorruptedFrameException(
                        "Unexpected inbound gameplay epoch "
                                + request.epoch()
                                + ", expected "
                                + (currentEpoch + 1)
                );
            }
            activeFenceId = request.fenceId();
            activeFence = new ActiveFence(
                    request.epoch(),
                    request.channelMask()
            );
        } else if (request.fenceId() != activeFenceId) {
            throw new CorruptedFrameException(
                    "Interleaved causal fence ids are not permitted"
            );
        } else if (activeFence.epoch != request.epoch()
                || activeFence.channelMask != request.channelMask()) {
            throw new CorruptedFrameException("Inconsistent causal fence request");
        }

        activeFence.seenMask |= 1 << channel;
        if (activeFence.seenMask != activeFence.channelMask) {
            return new Result(Action.PENDING, currentEpoch);
        }

        currentEpoch = activeFence.epoch;
        lastCompletedFenceId = request.fenceId();
        activeFenceId = 0L;
        activeFence = null;
        return new Result(Action.COMMIT_AND_ACKNOWLEDGE, currentEpoch);
    }

    void clearActive() {
        activeFenceId = 0L;
        activeFence = null;
    }

    int currentEpoch() {
        return currentEpoch;
    }

    enum Action {
        PENDING,
        IGNORE,
        ACKNOWLEDGE,
        COMMIT_AND_ACKNOWLEDGE
    }

    record Result(Action action, int epoch) {
    }

    private static final class ActiveFence {

        private final int epoch;
        private final int channelMask;
        private int seenMask;

        private ActiveFence(int epoch, int channelMask) {
            this.epoch = epoch;
            this.channelMask = channelMask;
        }
    }
}
