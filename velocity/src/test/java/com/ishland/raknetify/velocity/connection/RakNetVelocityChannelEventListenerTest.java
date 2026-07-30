/*
 * This file is a part of the Velocity implementation of the Raknetify
 * project, licensed under GPLv3.
 *
 * Copyright (c) 2022-2025 ishland
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.ishland.raknetify.velocity.connection;

import com.ishland.raknetify.common.connection.PreGatedTransitionScope;
import com.ishland.raknetify.common.connection.RakNetSimpleMultiChannelCodec;
import com.ishland.raknetify.common.connection.SynchronizationLayer;
import com.velocitypowered.proxy.protocol.packet.AvailableCommandsPacket;
import com.velocitypowered.proxy.protocol.packet.JoinGamePacket;
import com.velocitypowered.proxy.protocol.packet.RespawnPacket;
import com.velocitypowered.proxy.protocol.packet.config.FinishedUpdatePacket;
import com.velocitypowered.proxy.protocol.packet.config.StartUpdatePacket;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RakNetVelocityChannelEventListenerTest {

    @Test
    void onePreGateCoversFastAndSafeSyntheticSwitchPackets() {
        final EmbeddedChannel channel =
                new EmbeddedChannel(new RakNetVelocityChannelEventListener());
        PreGatedTransitionScope.requestFence(channel);
        channel.runPendingTasks();
        assertSame(SynchronizationLayer.SYNC_REQUEST_OBJECT, channel.readOutbound());

        final JoinGamePacket join = new JoinGamePacket();
        final RespawnPacket firstRespawn = new RespawnPacket();
        final RespawnPacket secondRespawn = new RespawnPacket();
        final AvailableCommandsPacket commands = new AvailableCommandsPacket();

        assertTrue(channel.writeOutbound(
                join,
                firstRespawn,
                secondRespawn,
                StartUpdatePacket.INSTANCE,
                FinishedUpdatePacket.INSTANCE,
                commands
        ));

        assertSame(join, channel.readOutbound());
        assertSame(firstRespawn, channel.readOutbound());
        assertSame(secondRespawn, channel.readOutbound());
        assertSame(StartUpdatePacket.INSTANCE, channel.readOutbound());
        assertSame(FinishedUpdatePacket.INSTANCE, channel.readOutbound());
        assertSame(RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL, channel.readOutbound());
        assertSame(commands, channel.readOutbound());
        assertFalse(PreGatedTransitionScope.isActive(channel));

        final RespawnPacket laterRespawn = new RespawnPacket();
        assertTrue(channel.writeOutbound(laterRespawn));
        assertSame(SynchronizationLayer.SYNC_REQUEST_OBJECT, channel.readOutbound());
        assertSame(laterRespawn, channel.readOutbound());
        assertFalse(channel.finishAndReleaseAll());
    }

    @Test
    void nestedSwitchesRestartOnlyAtTheLastCommandBoundary() {
        final EmbeddedChannel channel =
                new EmbeddedChannel(new RakNetVelocityChannelEventListener());
        PreGatedTransitionScope.requestFence(channel);
        PreGatedTransitionScope.requestFence(channel);
        channel.runPendingTasks();
        assertSame(SynchronizationLayer.SYNC_REQUEST_OBJECT, channel.readOutbound());
        assertSame(SynchronizationLayer.SYNC_REQUEST_OBJECT, channel.readOutbound());

        final AvailableCommandsPacket firstCommands =
                new AvailableCommandsPacket();
        assertTrue(channel.writeOutbound(firstCommands));
        assertSame(firstCommands, channel.readOutbound());
        assertTrue(PreGatedTransitionScope.isActive(channel));

        final AvailableCommandsPacket secondCommands =
                new AvailableCommandsPacket();
        assertTrue(channel.writeOutbound(secondCommands));
        assertSame(RakNetSimpleMultiChannelCodec.SIGNAL_START_MULTICHANNEL, channel.readOutbound());
        assertSame(secondCommands, channel.readOutbound());
        assertFalse(PreGatedTransitionScope.isActive(channel));
        assertFalse(channel.finishAndReleaseAll());
    }

}
