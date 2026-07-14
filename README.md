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
-Draknetify.smallWriteCoalesceMicros=250
-Draknetify.plpmtudMaxMtu=1500
-Draknetify.metricsJsonl=/path/to/raknetify-metrics.jsonl
```

`adaptiveTransport` defaults to `true`; no JVM argument is required to enable it. Set the property
to `false` only when comparing against the legacy fixed transport during testing.

Protocol v12 is preferred and automatically falls back to v11 when an older endpoint rejects the
initial request. Version 12 negotiates the RFC 8899-style DPLPMTUD state machine, model-based
congestion window and bounded Reed-Solomon FEC; versions 9-11 never send extension packets.
`plpmtudMaxMtu` is the local UDP payload ceiling and may be larger than the MTU established by the
initial handshake. FEC activates only for measured random loss and remains disabled for burst loss
and queue congestion.

`adaptiveDscp` is disabled by default because all players on a server listener share one UDP
socket. When enabled, the transport aggregates connection votes and uses a 2:1 majority plus a
30-second cooldown before switching the shared socket between AF41 and CS0.

`metricsJsonl` is optional and disabled when unset. When configured, each connection appends one
JSON object per second containing RTT, packet/byte rates, queue depth, pacing and delivery rates,
loss classification, congestion-control mode/cwnd/in-flight bytes, ACK aggregation, ECN feedback,
active MTU, Reed-Solomon budget/effectiveness, DPLPMTUD state/outcomes, DSCP and small-write batching.
Use a separate output file per process. The file and missing parent directories are created during
mod/plugin startup. Path, permission and writer errors are printed to the process log, then disable
the recorder without affecting traffic. Records enter a bounded non-blocking queue and a daemon
writer flushes them off the Netty event loop. `export_dropped` reports queue saturation, so slow
storage is visible without stalling players.

