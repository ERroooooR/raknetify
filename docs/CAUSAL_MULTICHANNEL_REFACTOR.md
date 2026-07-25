# Causal multichannel refactor

Minecraft's wire protocol was designed as one ordered byte stream. RakNet order channels are only
independently ordered, so assigning packet classes to different channels is correct only when the
application has proved that no causal dependency crosses those channels.

This refactor changes the default from optimistic packet splitting to compatibility-first delivery.
Each stage must preserve these invariants:

1. A Minecraft bundle is delivered atomically and in its original sub-packet order.
2. A packet that references an entity, world, container or protocol epoch cannot overtake creation
   of that state.
3. Switching profiles or dimensions never reports a dropped reliable packet as successfully sent.
4. Unknown packet classes and custom payloads are strict by default.

## Stage 1: strict causal stream

- Route every Minecraft game packet through one reliable ordered channel by default.
- Preserve bundle delimiters instead of allowing the legacy classifier to discard them.
- Keep the old packet classifier behind the explicit `aggressive` profile for comparison.
- Separate an explicit channel-zero decision from an unmatched override handler.

This stage deliberately gives up application-level stream parallelism, but keeps all RakNet
transport optimizations. It provides a correctness baseline for Create contraptions, NeoForge
complex entity spawning, Touhou Little Maid entity payloads and other unknown mods.

## Stage 2: atomic bundle envelope

- Capture the complete encoded bundle as one logical Raknetify message.
- Reconstruct its delimiter and sub-packets synchronously on the receiver.
- Allow the logical message to fragment at RakNet's transport layer, but never expose a partial
  bundle to Minecraft.

An envelope is required before unrelated channels can be reopened: sending delimiters on one
channel is insufficient because a later packet on another channel can overtake the closing
delimiter and be consumed as part of the wrong bundle.

## Stage 3: lossless fences and epochs

- Replace queue deletion in `SynchronizationLayer` with a drain-and-ack fence.
- Add a negotiated gameplay epoch to Raknetify framing.
- Increment the epoch for join, respawn and reconfiguration transitions.
- Reject stale old-epoch frames and hold new-epoch state until its transition gate is committed.

## Stage 4: dependency domains

- Classify packets as strict world state, independent control, ephemeral effects or guarded bulk.
- Keep unknown packets and payloads in the strict domain.
- Reopen a channel only for an explicit, tested independent domain.
- Use weighted fair scheduling between independent domains while retaining FIFO within each domain.

Candidate bypass traffic includes transport pings and purely visual effects. Chunk bodies require
an epoch-aware commit dependency before entity or block-entity state may pass them.

## Verification

- Inject loss, delay and reordering independently per RakNet channel.
- Repeat dimension transitions and server switches while large fragments are in flight.
- Verify `spawn -> pairing data -> metadata -> passengers` and custom entity payload order.
- Exercise Fabric, Sinytra Connector/NeoForge, Velocity and Bungee paths.
- Run Create and Touhou Little Maid integration loops for at least 100 transitions under 1-5% loss.
