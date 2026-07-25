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

Current implementation status:

- Stage 1: implemented.
- Stage 2: implemented with application-level capability negotiation and bounded envelopes.
- Stage 3: implemented with per-channel drain markers, acknowledgements and gameplay epochs.
- Stage 4: implemented with conservative domain classification, negotiated
  reopening and deficit round-robin scheduling.

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
- Negotiate support through a versioned reliable control frame. Old peers discard that unknown
  Raknetify frame safely, so aggressive routing stays disabled instead of sending an unsupported
  envelope.
- Bound envelopes to 4096 packets and 64 MiB, validate matching delimiters and trailing data, and
  complete every original packet promise from the single envelope write result.

An envelope is required before unrelated channels can be reopened: sending delimiters on one
channel is insufficient because a later packet on another channel can overtake the closing
delimiter and be consumed as part of the wrong bundle.

## Stage 3: lossless fences and epochs

- Replace queue deletion in `SynchronizationLayer` with a drain-and-ack fence.
- Add a negotiated gameplay epoch to Raknetify framing.
- Increment the epoch for join, respawn and reconfiguration transitions.
- Reject stale old-epoch frames and hold new-epoch state until its transition gate is committed.

The sender now appends a reliable ordered marker to all eight order channels and retains every
later message and promise until the receiver acknowledges all markers. The receiver advances the
epoch before sending that acknowledgement, so newly released frames cannot reach Minecraft before
the gate. The legacy reflection path that cleared `ReliabilityHandler` queues, rewrote receive
indices and completed dropped frame promises has been removed. Peers without the negotiated fence
capability keep their existing ordered traffic instead of invoking that lossy fallback.

## Stage 4: dependency domains

- Classify packets as strict world state, independent control, ephemeral effects or guarded bulk.
- Keep unknown packets and payloads in the strict domain.
- Reopen a channel only for an explicit, tested independent domain.
- Use weighted fair scheduling between independent domains while retaining FIFO within each domain.

The compatibility profile now translates only the legacy unordered-control bucket into
`INDEPENDENT_CONTROL` on reliable ordered channel 1 and the legacy sound/particle/vibration bucket
into `EPHEMERAL_EFFECT` on reliable ordered channel 4. These channels remain closed until atomic
bundles, lossless fences and gameplay epochs have all been negotiated. The `aggressive` profile
continues to expose the original channel table for explicit comparison.

Unknown packets, every custom payload, entity/world/container traffic and transition packets use
`STRICT_WORLD` on channel 7. Fabric identifies chunk, light and biome bodies as `GUARDED_BULK`, but
that domain deliberately shares the strict scheduling queue and channel 7: it is observable
separately, yet cannot overtake entity or block-entity state until an epoch-aware chunk commit
dependency is implemented.

A work-conserving deficit round-robin scheduler runs before RakNet fragmentation and reliability.
It uses a 4:2:1 byte quantum for strict, independent-control and effect queues, while retaining FIFO
within each queue. Strict world and guarded bulk share one FIFO. The old reflection-based
replacement of `ReliabilityHandler.frameQueue` has been removed. JSONL metrics now expose queued,
sent and pending frame/byte arrays in `DependencyDomain` enum order.

## Verification

- Inject loss, delay and reordering independently per RakNet channel.
- Repeat dimension transitions and server switches while large fragments are in flight.
- Verify `spawn -> pairing data -> metadata -> passengers` and custom entity payload order.
- Exercise Fabric, Sinytra Connector/NeoForge, Velocity and Bungee paths.
- Run Create and Touhou Little Maid integration loops for at least 100 transitions under 1-5% loss.
