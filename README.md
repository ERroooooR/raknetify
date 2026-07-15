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
still contains byte-pacing and adaptive ACK-policy fields for log compatibility, but those active
experiments were rolled back after public-network testing showed directional feedback mismatch.
They now report neutral values and do not alter packet scheduling. A sender-local burst drain floor
activates only while its queued plus in-flight data exceeds 48 KiB and targets roughly 500 ms of
drain time. New connections remain capped at 600 PPS until an ACK history is established; healthy
bursts then approach the configured 2000 packet-per-second default in steps of at most 2x. Isolated
loss permits only bounded growth up to 600 PPS, while active `RATE_LIMIT`, `QUEUE` or MTU-black-hole
signals disable the backlog floor entirely. After loss becomes quiet, recovery starts near 100 PPS
and doubles every 500 ms up to 600 PPS before normal healthy control resumes. The floor exits below
16 KiB and remains subject to the congestion window and RTT diagnostics.
To avoid treating short carrier-path reordering as congestion, gaps of only one or two FrameSets
receive an RTT/jitter-derived 4-25 ms NACK grace period. A packet arriving during that grace cancels
the NACK and increments `reordered_packets`; gaps of three or more FrameSets bypass the grace and
recover immediately. `nack_deferred` exposes how often this bounded filter is used.
`backlog_state` reports `BULK` only while this floor is active; `backlog_probes` remains zero. The
normal healthy ceiling defaults to 2000 packets per second.
Output is always written to `logs/raknetify-metrics.jsonl` under the game or proxy working directory;
no output path argument is needed. The file and missing `logs` directory are created during
mod/plugin startup. Path, permission and writer errors are printed to the process log, then disable
the recorder without affecting traffic. Records enter a bounded non-blocking queue and a daemon
writer flushes them off the Netty event loop. `export_dropped` reports queue saturation, so slow
storage is visible without stalling players.

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
