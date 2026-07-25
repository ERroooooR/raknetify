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

package com.ishland.raknetify.bungee.connection;

import com.ishland.raknetify.common.connection.PreGatedTransitionScope;
import com.ishland.raknetify.common.connection.RakNetSimpleMultiChannelCodec;
import com.ishland.raknetify.common.connection.SynchronizationLayer;
import io.netty.channel.embedded.EmbeddedChannel;
import net.md_5.bungee.protocol.packet.Commands;
import net.md_5.bungee.protocol.packet.FinishConfiguration;
import net.md_5.bungee.protocol.packet.Login;
import net.md_5.bungee.protocol.packet.Respawn;
import net.md_5.bungee.protocol.packet.StartConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RakNetBungeeClientChannelEventListenerTest {

    @Test
    void onePreGateCoversTheCompleteSyntheticSwitchSequence() {
        final EmbeddedChannel channel =
                new EmbeddedChannel(new RakNetBungeeClientChannelEventListener());
        PreGatedTransitionScope.requestFence(channel);
        channel.runPendingTasks();
        assertSame(SynchronizationLayer.SYNC_REQUEST_OBJECT, channel.readOutbound());

        final Login login = new Login();
        final Respawn firstRespawn = new Respawn();
        final Respawn secondRespawn = new Respawn();
        final StartConfiguration startConfiguration = new StartConfiguration();
        final FinishConfiguration finishConfiguration = new FinishConfiguration();
        final Commands commands = new Commands();

        assertTrue(channel.writeOutbound(
                login,
                firstRespawn,
                secondRespawn,
                startConfiguration,
                finishConfiguration,
                commands
        ));

        assertSame(login, channel.readOutbound());
        assertSame(firstRespawn, channel.readOutbound());
        assertSame(secondRespawn, channel.readOutbound());
        assertSame(startConfiguration, channel.readOutbound());
        assertSame(finishConfiguration, channel.readOutbound());
        assertSame(RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL, channel.readOutbound());
        assertSame(commands, channel.readOutbound());
        assertFalse(PreGatedTransitionScope.isActive(channel));

        final Respawn laterRespawn = new Respawn();
        assertTrue(channel.writeOutbound(laterRespawn));
        assertSame(SynchronizationLayer.SYNC_REQUEST_OBJECT, channel.readOutbound());
        assertSame(laterRespawn, channel.readOutbound());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void nestedSwitchesRestartOnlyAtTheLastCommandBoundary() {
        final EmbeddedChannel channel =
                new EmbeddedChannel(new RakNetBungeeClientChannelEventListener());
        PreGatedTransitionScope.requestFence(channel);
        PreGatedTransitionScope.requestFence(channel);
        channel.runPendingTasks();
        assertSame(SynchronizationLayer.SYNC_REQUEST_OBJECT, channel.readOutbound());
        assertSame(SynchronizationLayer.SYNC_REQUEST_OBJECT, channel.readOutbound());

        final Commands firstCommands = new Commands();
        assertTrue(channel.writeOutbound(firstCommands));
        assertSame(firstCommands, channel.readOutbound());
        assertTrue(PreGatedTransitionScope.isActive(channel));

        final Commands secondCommands = new Commands();
        assertTrue(channel.writeOutbound(secondCommands));
        assertSame(RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL, channel.readOutbound());
        assertSame(secondCommands, channel.readOutbound());
        assertFalse(PreGatedTransitionScope.isActive(channel));
        assertFalse(channel.finishAndReleaseAll());
    }

}
