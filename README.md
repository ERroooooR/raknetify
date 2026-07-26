# Raknetify
A Fabric mod / Velocity plugin / BungeeCord plugin that uses RakNet to improve multiplayer experience significantly
under unreliable and rate-limited connections.

# Features
- Higher reliability and lower latency under unreliable and rate-limited client connections.
- Uses RakNet's multiple channels with priorities to achieve higher responsiveness. 
- Supports ViaVersion client-side and ViaVersion server-side.

# How to use it?

## Prerequisites
- Raknetify is designed to work on Minecraft 1.17.1+
  Note: On proxies such as Velocity and BungeeCord, **unsupported client version** will cause
  multi-channelling failing to initialize, causing **reduced responsiveness**.  
- You need to have a UDP port opened at the same port number of your normal server port. 

## Installation and usage
- Download the latest release from 
  [GitHub](https://github.com/RelativityMC/raknetify/releases) 
  [Modrinth (Fabric)](https://modrinth.com/mod/raknetify/versions) 
  [CurseForge (Fabric)](https://www.curseforge.com/minecraft/mc-mods/raknetify/files)
  [SpigotMC (BungeeCord)](https://www.spigotmc.org/resources/raknetify-bungeecord.102509/)
  or development builds from [CodeMC](https://ci.codemc.io/job/RelativityMC/job/raknetify/)
- Install the mod on both client and server. (Installation on backend servers are not needed if using on proxies) 
- Prefix your server address with `raknet;` (or `raknetl;` to use high mtu) and save or connect directly. 
  (e.g. `raknet;example.com`)
- Enjoy!

## Adaptive transport options

The bundled transport prefers RakNet protocol v12 and uses a 1400-byte MTU. Clients automatically
fall back to v11 when required. Adaptive pacing, rolling loss classification and safe MTU fallback
are enabled by default. The following
JVM properties are available on both client and server:

```text
-Draknetify.adaptiveTransport=false
-Draknetify.adaptiveDscp=false
-Draknetify.protocolVersion=12
-Draknetify.adaptiveMinPps=50
-Draknetify.adaptiveMaxPps=2000
-Draknetify.smallWriteCoalesceMicros=500
-Draknetify.plpmtudMaxMtu=1452
-Draknetify.metricsJsonl=true
```

`adaptiveTransport` defaults to `true`; no JVM argument is required to enable it. Set the property
to `false` only when comparing against the legacy fixed transport during testing.

Protocol v12 is preferred and automatically falls back to v11 when an older endpoint rejects the
initial request. Version 12 negotiates the RFC 8899-style DPLPMTUD state machine, model-based
congestion window and bounded Reed-Solomon FEC; versions 9-11 never send extension packets.
`plpmtudMaxMtu` is the local UDP payload ceiling and may be larger than the MTU established by the
initial handshake. Its 1452-byte default leaves room for IPv6 and UDP headers on a conventional
1500-byte path; IPv4-only deployments may explicitly test up to 1472. FEC activates only for
measured random loss and remains disabled for burst loss and queue congestion.

`adaptiveDscp` is disabled by default because all players on a server listener share one UDP
socket. When enabled, the transport aggregates connection votes and uses a 2:1 majority plus a
30-second cooldown before switching the shared socket between AF41 and CS0.

`metricsJsonl` is a boolean switch and defaults to `false`. When enabled, each connection appends
one JSON object per second containing RTT, packet/byte rates, queue depth, pacing and delivery rates,
loss classification, congestion-control mode/cwnd/in-flight bytes, ACK aggregation, ECN feedback,
active MTU, Reed-Solomon budget/effectiveness, DPLPMTUD state/outcomes, DSCP and small-write batching.
It also records fragment-reassembly and ordered-queue head-of-line delay, plus actual
external-compressor batch sizes. These fields distinguish a harmless per-tick display peak from
transport queueing or a large compressed batch waiting for all RakNet fragments. The JSON schema
also records byte-pacing and adaptive ACK/NACK recovery policy state. ACK protection remains off
during healthy traffic. Three duplicate reliable FrameSets within one second activate it for at
least two seconds;
ACK ranges are then coalesced and repeated once after an RTT-derived 5-20 ms delay. This protects
the feedback direction without permanently doubling ACK traffic. A sender-local burst drain floor
activates only while its queued plus in-flight data exceeds 48 KiB and uses a four-RTT drain horizon
clamped to 100-500 ms. New connections remain byte-paced at a conservative bootstrap rate while two
small ACK rounds validate the path; their PPS probe remains capped at 600 until useful ACK history is
established. During the protected first minute, healthy samples raise admission geometrically without
bypassing the byte bucket. Later healthy bursts retain at least 80% of recently validated path capacity
and approach the configured 2000 packet-per-second default in steps of at most 2x. Isolated
loss permits only bounded growth up to 600 PPS, while active `RATE_LIMIT`, `QUEUE` or MTU-black-hole
signals disable the backlog floor entirely. After loss becomes quiet, recovery starts near 100 PPS
and doubles every 500 ms up to 600 PPS before normal healthy control resumes. The floor exits below
16 KiB and remains subject to the congestion window and RTT diagnostics.
To avoid treating short carrier-path reordering as congestion, gaps of only one or two FrameSets
receive an RTT/jitter-derived 4-25 ms NACK grace period. A packet arriving during that grace cancels
the NACK and increments `reordered_packets`; gaps of three or more FrameSets bypass the grace and
recover immediately. The receiver evaluates the last 8-32 grace outcomes. When at least 88% are
confirmed losses rather than reordering, it temporarily bypasses the grace and sends NACKs
immediately. After at least two seconds it permits one deferred probe; a true reordered arrival
restores the grace, while another confirmed loss resumes the bypass. `nack_deferred`,
`nack_deferred_expired`, `nack_grace_bypassed` and `nack_grace_bypass` expose this decision.
While either ACK protection or the NACK-grace bypass is active, NACK ranges are also coalesced and
repeated once after an RTT-derived 5-20 ms delay. A missing FrameSet that arrives before the repeat
is removed from the pending range. `nack_repeated_packets` and `nack_repeated_framesets` report the
small amount of added control traffic; remote copies of these counters are synchronized as well.
The adaptive policies can be disabled independently for comparison with
`-Draknetify.adaptiveNackGrace=false`, `-Draknetify.adaptiveNackProtection=false` or
`-Draknetify.adaptiveAckProtection=false`. FEC packet, byte, recovery, expiry, shard-budget and
recovery-ratio metrics are synchronized in both directions so sender effectiveness is not inferred
from receiver-local counters.
`backlog_state` reports `BULK` only while this floor is active; `backlog_probes` counts successful
ACK-driven admission increases. The normal healthy ceiling defaults to 2000 packets per second.
Output is always written to `logs/raknetify-metrics.jsonl` under the game or proxy working directory;
no output path argument is needed. The file and missing `logs` directory are created during
mod/plugin startup. Path, permission and writer errors are printed to the process log, then disable
the recorder without affecting traffic. Records enter a bounded non-blocking queue and a daemon
writer flushes them off the Netty event loop. `export_dropped` reports queue saturation, so slow
storage is visible without stalling players.

## Multichannel compatibility profile

Raknetify defaults to the `compatibility` multichannel profile. Minecraft and mod loaders define
implicit ordering between entity spawns, custom payloads, chunk state and bundle delimiters; those
dependencies cannot be inferred safely from packet IDs. Compatibility mode therefore keeps every
unknown packet, custom payload and entity/world/container packet on one reliable ordered RakNet
channel. Chunk/light/biome bulk currently shares that same strict FIFO. This restores the original
causal order and vanilla bundle atomicity while retaining RakNet congestion control, pacing,
recovery, FEC, MTU discovery and compression.

The previous packet-class-based channel split remains available for controlled comparison with:

```text
-Draknetify.multichannelProfile=aggressive
```

`aggressive` can reduce head-of-line blocking, but is not the default because unknown mod payloads
may overtake the entity, world or inventory state they reference. Compatibility mode reopens only
the explicitly tested independent-control and ephemeral-effect domains. They remain strict until
the peer acknowledges receipt of Raknetify's capability advertisement; accepting a local control
write is not treated as negotiation because channel 1 or 4 could overtake that advertisement on
channel 7. A peer without confirmation support stays strict and unframed outbound. Negotiated
bundles are sent as one logical game frame and reconstructed synchronously on receipt, while still
using RakNet compression, fragmentation and recovery.

Dimension and backend transitions use a negotiated lossless drain fence. Raknetify appends a
reliable marker to every ordered channel, holds later writes until the peer has received all
markers, then advances a gameplay epoch. Old-epoch frames are discarded and future-epoch frames
wait behind the transition gate. This replaces the previous queue deletion and order-index rewrite
behavior; reliable packet promises are never reported successful merely because a transition
occurred. A work-conserving dependency-domain scheduler selectively reopens only the two
proven-independent domains, preserves FIFO within each domain, and leaves mod-defined state on the
strict stream without requiring per-mod compatibility IDs.

## BandwidthOptimizer compatibility

When the mod `bandwidthoptimizer` is present, Raknetify automatically leaves encoded-packet
compression to BandwidthOptimizer. RakNet multi-channel transport remains enabled, while
Raknetify's streaming Deflate and vanilla Minecraft compression are disabled for RakNet
connections to avoid compressing BandwidthOptimizer's Zstd output a second time. TCP connections
are unaffected. BandwidthOptimizer's delayed packet batching is also disabled only on RakNet
connections: a delayed mixed-packet carrier no longer hides the original packet classes that
Raknetify needs to preserve per-packet channel priority. Detection is performed from the shared
runtime class path so it also works when Raknetify is translated for NeoForge by Sinytra Connector.

This compatibility behavior is enabled by default. It can be disabled for comparison testing with
`-Draknetify.bandwidthOptimizerCompatibility=false` on both endpoints.

## ZSTD_Compresser compatibility

When the `zstd_compresser` client mod and the `zstd_velocity` Velocity plugin are present,
Raknetify automatically disables its streaming Deflate layer for RakNet connections. Unlike the
BandwidthOptimizer integration, Raknetify keeps Velocity's `SetCompression` packet intact because
ZSTD_Compresser uses it to switch the client and proxy pipelines to Zstd at the same time. This
prevents compressed bytes from being decoded as a Minecraft packet (for example, `Received unknown
packet id 764`). Raknetify also removes ZSTD_Compresser's redundant TCP frame-length prefix on
Velocity before creating a RakNet frame. RakNet transport, reliability and adaptive networking
optimizations remain active. While ZSTD batching is active, its frames use one ordered RakNet
channel because the original packet boundaries needed for safe multi-channel prioritization no
longer exist. If BandwidthOptimizer is installed as well, ZSTD_Compresser's required compression
negotiation takes precedence and is not suppressed.

Velocity plugin detection uses the registered plugin id so it works across Velocity's isolated
plugin class loaders. Client detection uses the shared runtime class path and therefore also works
when Raknetify runs through Sinytra Connector. Compatibility is enabled by default and can be
disabled for comparison testing with `-Draknetify.zstdCompresserCompatibility=false` on both the
client and Velocity.

For latency-sensitive RakNet links, ZSTD_Compresser's default `batch_max_bytes: 65536` and
`flush_interval_ms: 10` can create visible bursts and make every packet in a compressed batch wait
for the final fragment. A starting point of `batch_max_bytes: 32768` and `flush_interval_ms: 5` on
both the client mod and Velocity plugin reduces that application-level head-of-line delay. This is
an external-compressor setting rather than a Raknetify JVM property; the JSONL `application_*` and
`remote_application_*` fields show the batch sizes actually observed after compression.
