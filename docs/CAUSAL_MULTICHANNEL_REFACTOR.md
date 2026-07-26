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
- Stage 2: implemented with confirmed application-level capability negotiation and bounded
  envelopes.
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
- Require an explicit receipt acknowledgement before the sender uses any advertised capability.
  Advertisement and acknowledgement use reliable ordered channel 7. Merely accepting the local
  advertisement write is insufficient because causal gameplay on channel 1 or 4 could otherwise
  reach the peer before that channel-7 advertisement.
- Bound envelopes to 4096 packets and 64 MiB, validate matching delimiters and trailing data, and
  complete every original packet promise from the single envelope write result.

An envelope is required before unrelated channels can be reopened: sending delimiters on one
channel is insufficient because a later packet on another channel can overtake the closing
delimiter and be consumed as part of the wrong bundle.

Negotiation tracks inbound and outbound readiness independently. Receiving an advertisement makes
the endpoint ready to decode that peer's causal frames; sending causal frames is authorized only
after the peer confirms this endpoint's advertisement. The confirmation extension is itself
capability-gated, so a new endpoint never sends an unknown ACK control type to an older endpoint.
With a pre-confirmation peer, the new endpoint remains strict and unframed outbound while retaining
the ability to decode that peer's legacy causal frames. Lossless-fence authorization uses the same
directional state instead of a single bidirectional boolean.

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

The receiver accepts exactly one inbound fence at a time, requires its channel mask to match the
locally active order channels, and rejects interleaved IDs or reuse of a completed ID for another
epoch. Capability advertisements become immutable after the first valid control frame. A
future-epoch gameplay frame may target only `current + 1`; malformed/truncated headers, unknown
frame types and larger epoch skips are rejected before they can enter a retained queue.

Bungee's backend switch is treated as one platform transition scope. `ServerConnectedEvent` fires
synchronously before Bungee emits its synthetic state-reset sequence, so Raknetify inserts one
fence there and marks the sequence as pre-gated. Synthetic Login, Respawn and configuration packets
do not create additional epochs while that scope is active; the Commands packet closes the scope
at the same boundary that restarts multichannel delivery. Ordinary Respawn packets outside a
server-switch scope still create their own fence.

Velocity uses the same scoped model at its earlier `ServerConnectedEvent`, whose completion precedes
`handleBackendJoinGame` on the client event loop. This covers both Velocity's fast switch
(`JoinGame -> Respawn`) and legacy-Forge safe switch
(`JoinGame -> dummy Respawn -> Respawn`) with one fence instead of two or three serialized RTTs.
`ServerPostConnectEvent` remains responsible only for installing the backend listener after the
join completes.

Both proxy adapters use one Common transition-scope state machine. Opening a scope and writing its
fence execute as one ordered client-event-loop task, rather than mutating a cross-thread boolean
before the corresponding write is scheduled. The scope has a bounded, atomic depth so a second
redirect may begin before the first command tree arrives without letting that first Commands packet
reopen multichannel delivery early. Each Commands packet closes one scope; only the final close
emits the restart barrier. A failed scope-fence write clears the state and closes the connection
instead of leaving future transition packets permanently unfenced.

Inbound gameplay epoch ownership is isolated in a Common gate instead of being interleaved with
packet classification, capability negotiation and outbound scheduling. The gate owns every
future-epoch payload until the matching fence commits, preserves insertion order, rejects any jump
larger than `current + 1`, and applies independent count and byte bounds. Removal or overflow
releases all retained buffers; the codec only decides whether a frame is eligible for epoch
handling and delegates its lifetime to the gate.

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

Strict classification is a safety veto across all registered classifiers. A platform adapter or
external batch/compression detector may match first, but a later custom-payload or unknown-packet
classifier can still force the frame back to `STRICT_WORLD`. Proxy packet-ID maps explicitly set
their copied fastutil map's absent-key value: an unmapped protocol ID can no longer silently inherit
Java's integer zero and be mistaken for legacy order channel 0. ZSTD frames whose original packet
boundaries have already been aggregated are also explicitly strict instead of being expressed as a
legacy channel preference.

A work-conserving deficit round-robin scheduler runs before RakNet fragmentation and reliability.
It uses a 4:2:1 byte quantum for strict, independent-control and effect queues, while retaining FIFO
within each queue. Strict world and guarded bulk share one FIFO. The old reflection-based
replacement of `ReliabilityHandler.frameQueue` has been removed. JSONL metrics now expose queued,
sent and pending frame/byte arrays in `DependencyDomain` enum order.

## Verification

Automated verification now:

- Connects the real `RakNetSimpleMultiChannelCodec` and `SynchronizationLayer` at both ends of a
  deterministic link that independently delays/reorders channels and adds retransmission delay to
  1-5% of first deliveries.
- Runs 100 gameplay-epoch transitions while alternating atomic and non-atomic entity chains and
  placing a 256 KiB guarded-bulk frame before every fence.
- Verifies `spawn -> pairing data -> metadata -> passengers -> custom entity payload` exactly once
  and in order in every epoch.
- Verifies all pre-fence application frames enter the transport before the eight channel markers,
  and that the post-ACK restart barrier enters channel 7 before new-epoch gameplay is released.
- Issues all 100 `fence -> restart -> gameplay` transitions back-to-back before delivering the
  first ACK, then verifies that every fence remains distinct and every epoch/entity chain arrives.
  Transition wait queues preserve the original order of control and gameplay writes. Completed
  ACK state is detached before callbacks can start the next fence, and fence markers/ACKs are
  explicitly flushed so a re-entrant transition cannot stall behind an already completed flush.
- Uses explicit ownership accounting and the same fail-closed bound for all four outbound causal
  hold points: application writes waiting for an epoch ACK, control signals waiting for an atomic
  bundle to close, transport writes waiting behind the eight-channel fence, and frames waiting in
  the dependency-domain DRR scheduler. The first three share one reference-count-safe queue
  implementation; the scheduler preserves its per-domain FIFO while applying identical capacity
  accounting. Each queue is bounded to 16,384 writes and 256 MiB. Overflow fails every held
  promise, releases every retained message and closes the connection instead of allowing a lost
  ACK, delimiter or event-loop stall to create unbounded memory growth. JSONL metrics expose
  aggregate and per-queue counts; the acceptance verifier requires that no queue overflows and
  their aggregate settled depth is zero.
- Exercises byte/count overflow independently at the application, bundle-control, transport-fence
  and dependency-domain scheduler hold points, asserting that every promise fails and every
  reference-counted message is released.
- Exercises the extracted inbound epoch gate directly: future frames remain retained until the
  matching commit, are released to Minecraft in insertion order, and both count and byte overflow
  paths release the rejected payload plus every payload retained at channel removal.
- Injects conflicting capability advertisements, incomplete/unknown epoch headers, skipped future
  epochs, incomplete channel masks, interleaved fence IDs and completed-ID reuse, requiring every
  malformed transition to fail before state is committed or memory is retained.
- Delays the local capability acknowledgement while allowing the remote advertisement to arrive,
  then verifies outbound gameplay remains unframed on strict channel 7 until confirmation. It also
  verifies pre-confirmation peers never receive the new ACK type, remain strict outbound, and can
  still send legacy causal frames inbound.
- Verifies the Bungee synthetic Login/Respawn/configuration switch sequence consumes one pre-gate,
  closes at Commands and leaves later ordinary Respawns free to request their own fence.
- Verifies Velocity fast/safe synthetic JoinGame/Respawn/configuration sequences consume one
  pre-gate, close at AvailableCommands and leave later ordinary Respawns independently fenced.
- Opens nested proxy transition scopes before either Commands boundary, verifies each fence is
  preserved, and requires multichannel restart to remain suppressed until the final scope closes.
- Runs the Common test suite and clean Fabric, Velocity and Bungee builds with license checks.

The deterministic link models the ordered result of RakNet retransmission. The netty-raknet
`manyBufferBadBoth` and `adaptiveV12SurvivesModerateRandomLoss` end-to-end tests separately exercise
real datagram loss, duplication, reordering and fragmentation; both pass against this revision.

Still required in an external Minecraft test environment:

- Exercise Fabric and Sinytra Connector/NeoForge clients through Velocity and Bungee.
- Run Create and Touhou Little Maid integration loops for at least 100 portal/server transitions
  under 1-5% datagram loss, recording crashes, entity invariants and Raknetify JSONL metrics.

The reproducible setup, fault profiles and machine-verifiable acceptance command are documented in
`CAUSAL_MULTICHANNEL_INTEGRATION_TEST.md`.
