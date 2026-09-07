# Raknetify — Mainland China Network Fork

[简体中文](README.md) | **English**

This is an unofficial fork of [RelativityMC/raknetify](https://github.com/RelativityMC/raknetify). Thanks to the upstream authors and contributors for the RakNet multi-channel transport foundation. **Report problems with this fork to [this repository's Issues](https://github.com/ERroooooR/raknetify/issues), rather than directly reporting fork-specific problems upstream.**

> **Experimental / AIGC notice:** Commits in this repository's branches contain substantial AI-generated content (AIGC) and AI-assisted code and documentation. The stability of the latest builds cannot be guaranteed. A successful build or the presence of automated tests does not establish stability in real multiplayer environments.

## Goals and support scope

Upstream targets Minecraft multiplayer over unreliable and rate-limited connections. This fork has a different goal: changes and tuning specifically for **QoS, packet-rate limits, loss, jitter and traffic bursts on networks in mainland China**. These strategies are not guaranteed to be universally applicable or to improve performance in other regions, on other carriers or on other paths.

**This fork only guarantees compatibility with Sinytra Connector in a NeoForge environment.** This means loading the Fabric version of Raknetify through Connector; it is not a native NeoForge mod. This compatibility scope does not promise stability for every Minecraft / NeoForge / Connector version, modpack or latest build.

The repository retains upstream Fabric, Velocity and BungeeCord modules and other compatibility code. Their presence does not imply a compatibility guarantee for those standalone environments in this fork.

## Implemented improvements

The following mechanisms are present in the current code and pinned `netty-raknet` submodule. Their implementation does not establish that they improve every network:

- **NeoForge / Connector integration:** Dedicated server binding and channel initialization handling for Connector, encryption injection adjustments, and fixes to some Mixin interactions with Fabric Networking API.
- **Adaptive transport:** Packet-rate and byte pacing, rolling loss classification, congestion-window control and bounded burst draining are enabled by default. Previously validated path capacity is retained across idle periods to reduce unnecessary rate drops when sending resumes.
- **Protocol and MTU:** Prefer RakNet v12, with fallback to v11 when an older endpoint rejects the initial request. Negotiated path MTU discovery (DPLPMTUD), bounded Reed–Solomon FEC and MTU fallback are implemented. Older protocols do not send v12 extension packets.
- **Loss and ordered-queue recovery:** Short-reordering NACK grace, conditional repeated ACK/NACK protection, RACK-style loss inference, PTO probes, extra recovery during application-limited traffic, and targeted repair and peer feedback for blocked ordered queues. Additional recovery is constrained by sending budgets and congestion conditions.
- **Compression compatibility:** Detection and pipeline integration for BandwidthOptimizer and ZSTD_Compresser, addressing duplicate compression, compression negotiation and redundant length prefixes on Velocity. See the limitations below.
- **Observability:** Bidirectional transport metrics, per-channel head-of-line diagnostics, compression-batch metrics and asynchronous JSONL export to help distinguish loss, queueing and compression-batch delays.
- **Connection handling:** Gate route hints, IPv6 address handling fixes, and adjustments to handshake, shutdown and fragment synchronization behavior.

Implementation entry points: [transport configuration](common/src/main/java/com/ishland/raknetify/common/connection/RakNetConnectionUtil.java), [Fabric integration](fabric/src/main/java/com/ishland/raknetify/fabric/mixin), [transport submodule](netty-raknet), and [recovery design and switches](docs/ADAPTIVE_RECOVERY_ROADMAP.md). The design document includes development directions; not every proposal constitutes completed performance validation.

## Installation and connection

1. Obtain the appropriate version from **this repository's** [Releases](https://github.com/ERroooooR/raknetify/releases), if available, or [Actions artifacts](https://github.com/ERroooooR/raknetify/actions/workflows/build.yml). Upstream download channels do not represent builds of this fork.
2. Load the Fabric artifact matching your Minecraft version through Sinytra Connector on NeoForge, with the dependencies required by that Connector version. Do not infer this fork's compatibility solely from upstream's historical version range.
3. Direct connections require installation on both client and server. With a proxy, RakNet terminates at the proxy running the corresponding plugin; backend installation is generally unnecessary. Proxy deployments are outside this fork's guaranteed compatibility scope.
4. Open a **UDP port with the same port number as the Minecraft TCP port** on the server or proxy, and ensure the NAT / forwarding path supports UDP.
5. Connect using `raknet;example.com`. The `raknetl;` prefix requests a high MTU and should not be the default for an unverified path.

## Compatibility and known limitations

| Scenario | Behavior and limitations |
| --- | --- |
| NeoForge + Sinytra Connector | The only compatibility target guaranteed by this fork; game, loader, Connector and dependency versions still need to match. |
| Standalone Fabric / Velocity / BungeeCord / ViaVersion | Upstream implementations or integrations remain, but compatibility is not guaranteed by this fork. Unsupported client versions on proxies may prevent multi-channel initialization and reduce responsiveness. |
| Older RakNet peers | Handshake rejection can trigger fallback to v11. v12 extensions require negotiation; fallback does not retain every optimization. |
| BandwidthOptimizer | Detected by default and takes over compression. Streaming Deflate, vanilla compression and its delayed batching are disabled for RakNet connections to preserve per-packet priority. These adjustments do not affect TCP connections. |
| ZSTD_Compresser | Detects the `zstd_compresser` client mod and `zstd_velocity` plugin, disables streaming Deflate, preserves required `SetCompression` negotiation and removes the redundant TCP length prefix on Velocity. Batched traffic uses one ordered channel, losing original per-packet multi-channel priority; head-of-line blocking remains possible. |
| Both compression integrations installed | The code prioritizes ZSTD_Compresser's required negotiation. This does not guarantee arbitrary combinations or versions are compatible. |
| UDP / QoS / DSCP | Blocked UDP prevents RakNet connections. Carriers may ignore or rewrite DSCP markings; MTU, FEC and rate tuning cannot eliminate all throttling or loss. |

Large ZSTD_Compresser batches can increase fragment waiting time. Try comparing `batch_max_bytes: 32768` and `flush_interval_ms: 5` in the external compressor configuration on both client and Velocity. These are starting points for testing, not universal optimal values or Raknetify JVM properties.

## Configuration and diagnostics

These are the current code defaults. Add properties to the JVM arguments of the relevant client, server or proxy:

| JVM property | Default | Purpose |
| --- | --- | --- |
| `raknetify.adaptiveTransport` | `true` | Adaptive transport master switch |
| `raknetify.protocolVersion` | `12` | Preferred protocol version; accepts 9–12 |
| `raknetify.adaptiveMinPps` | `30` | Adaptive packet-rate floor |
| `raknetify.adaptiveMaxPps` | `2000` | Adaptive packet-rate ceiling |
| `raknetify.smallWriteCoalesceMicros` | `500` | Small-write coalescing time in microseconds |
| `raknetify.plpmtudMaxMtu` | `1452` | Local UDP payload ceiling for probing; normal initial MTU is 1400 |
| `raknetify.adaptiveDscp` | `false` | Adaptive DSCP voting on the shared UDP socket |
| `raknetify.metricsJsonl` | `false` | Export diagnostics once per second per connection |

For example, enable logging with:

```text
-Draknetify.metricsJsonl=true
```

Logs are written to `logs/raknetify-metrics.jsonl` under the game or proxy working directory. Records include RTT, throughput, queues, retransmissions, FEC, MTU, per-channel blocking and compression batches. A full asynchronous writer queue drops metrics and reports `export_dropped`.

`raknetify.adaptiveAckProtection`, `raknetify.adaptiveNackGrace` and `raknetify.adaptiveNackProtection` default to enabled and can individually be set to `false` for comparison tests. See the [recovery document](docs/ADAPTIVE_RECOVERY_ROADMAP.md) for advanced recovery switches. Compression integrations can be disabled on both endpoints using `-Draknetify.bandwidthOptimizerCompatibility=false` or `-Draknetify.zstdCompresserCompatibility=false` for troubleshooting.

When filing an issue, include the build commit, Minecraft / NeoForge / Connector versions, mod and proxy lists, network topology, reproduction steps and relevant logs or metric excerpts. Remove sensitive information such as IP addresses and tokens before sharing.

## License and credits

Licensed under [MIT](LICENSE). Thanks to the authors and contributors of [RelativityMC/raknetify](https://github.com/RelativityMC/raknetify) and netty-raknet. This repository is responsible for the fork's modifications and support commitments.
