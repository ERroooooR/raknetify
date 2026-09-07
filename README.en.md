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

## How the network optimizations work

This explanation follows the pinned transport submodule commit `e7f8627`. These are implementation policies and thresholds, not measurements covering every mainland Chinese network or latency/throughput guarantees. PPS here describes transport datagram pacing; raising its ceiling does not create bandwidth.

### 1. Adapt to feedback instead of treating every loss as congestion

The controller tracks ACK and loss events in ten one-second buckets and combines them with smoothed/minimum RTT, consecutive losses and packet sizes. `QUEUE` represents queueing signals such as inflated RTT with loss. With at least 64 samples, loss of at least 3% and no substantial RTT inflation, it can classify `RATE_LIMIT`; at least three consecutive losses can classify `BURST`; other isolated losses become `RANDOM`. Conditions have precedence. **These are heuristics, not proof that a carrier is applying a particular QoS policy.**

For a new rate response, `RATE_LIMIT` multiplies packet pacing by 0.70, `QUEUE` / `BURST` by 0.75, and other loss branches by 0.85, before limits and burst controls. Repeated feedback within one congestion episode does not repeatedly apply the entire multiplicative reduction, avoiding collapse to the minimum rate. Recovery follows ACK progress; time without feedback is not evidence of recovered capacity.

Source: [AdaptiveTransportController.onLoss / applyLossPacingCeiling](https://github.com/RelativityMC/netty-raknet/blob/e7f8627521798289db32a9ded8e5bfc89c74334b/common/src/main/java/network/ycc/raknet/pipeline/AdaptiveTransportController.java).

### 2. Bound packet rate, in-flight data and bulk starts together

Before sending, the controller checks the congestion window (sent but unacknowledged bytes), then packet tokens. The bucket normally holds at most four datagrams, reduced to one under `BURST` / `QUEUE` / `RATE_LIMIT`. Delivery rate, minimum RTT and ACK aggregation inform the window through `STARTUP`, `DRAIN`, `PROBE_BW` and `PROBE_RTT` states. This is the project's model-based implementation, not a claim of full standard BBR implementation.

Chunk synchronization and compressed batches can produce large queues. Queued plus in-flight bytes determine entry into `BULK`: the entry threshold scales with the congestion window and is capped at 48 KiB; the exit threshold is capped at 16 KiB. The current drain target is **two seconds**. Missing it does not justify unlimited acceleration based on queue age; validated capacity is consulted instead. RTT pressure or active severe-loss signals limit burst acceleration.

The byte-admission path additionally smooths starts with byte tokens. Its initial admission rate is normally constrained by both 384 KiB/s and the packet-rate estimate, with adjustments when retained capacity exists. **Not all BULK traffic always passes this byte gate:** the initial one-minute calibration window or healthy probing conditions can select a `WORK_CONSERVING` path that skips it. Packet pacing and congestion-window checks still apply. The default `2000 PPS` is a configured ceiling, not a constant send rate.

Source: [sendBudget / updateBurstDrain / workConservingBulk](https://github.com/RelativityMC/netty-raknet/blob/e7f8627521798289db32a9ded8e5bfc89c74334b/common/src/main/java/network/ycc/raknet/pipeline/AdaptiveTransportController.java).

### 3. Retain validated capacity across idle periods

Delivery estimation bounds inflated ACK-compression samples. Low samples caused by an application having too little data to send do not directly lower the retained maximum bandwidth. On a healthy path, validated capacity from the last five minutes can seed a restart; older retained capacity is tried at half rate and validated through two rounds of acknowledged bytes. Loss during validation enters `SAFE_RETREAT`. This is history within the current connection, not a bandwidth result persisted across connections.

Source: [updateDeliveryRate / startBurstAdmission / updateResumeValidation](https://github.com/RelativityMC/netty-raknet/blob/e7f8627521798289db32a9ded8e5bfc89c74334b/common/src/main/java/network/ycc/raknet/pipeline/AdaptiveTransportController.java).

### 4. Separate short reordering, lost feedback and data needing retransmission

- **NACK grace:** Gaps of only one or two FrameSets receive an RTT/jitter-derived 4–25 ms wait; larger gaps recover immediately. If at least 88% of the latest 8–32 outcomes expire rather than arrive reordered, grace is temporarily bypassed, with one deferred probe allowed after at least two seconds.
- **ACK/NACK protection:** Three duplicate reliable FrameSets within a second activate ACK protection for at least two seconds. ACK ranges are coalesced and repeated once after 5–20 ms. Active ACK protection or NACK-grace bypass can also coalesce and repeat NACKs; arriving packets remove their pending repeat ranges. Healthy traffic does not permanently double ACKs.
- **RACK-style inference:** Acknowledgment of newer transmissions provides evidence that older data was lost. The RTT-based reordering window starts at 1.25 times RTT and can widen to twice RTT on signs of a mistaken inference, reducing unnecessary retransmissions.
- **PTO probes:** Without new ACKs, the base timeout is `RTT + max(1 ms, 4 × RTT standard deviation) + retryDelay`, followed by bounded exponential backoff. Probes prefer data that may unblock ordered delivery and still require a send budget. A probe timeout does not itself establish loss of the entire flight.

Source: [ReliabilityHandler](https://github.com/RelativityMC/netty-raknet/blob/e7f8627521798289db32a9ded8e5bfc89c74334b/common/src/main/java/network/ycc/raknet/pipeline/ReliabilityHandler.java).

### 5. Target blocked data while bounding repair overhead

**Normal FEC** requires extension negotiation and is considered only for random loss between 0.5% and 3%, without BULK draining or RTT-based queue inflation. After feedback from at least 64 groups, a yield below 0.01 recovered packets per group suppresses normal FEC for ten seconds to avoid continuously sending ineffective redundancy.

**Targeted FEC** follows a separate path: four datagrams selected from a rolling cache of up to eight form a repair group with one Reed–Solomon parity shard, prioritizing a peer-reported blocked target. Candidates need an ordered retransmission history and a score of at least two for “retry count + RTTs elapsed since transmission.” Parity bytes per RTT are capped at the current MTU (with a code floor of 256 bytes), and the congestion window is checked. This is not a global increase in FEC redundancy and does not use normal FEC's random-loss trigger.

When the application queue is empty, additional recovery can resend reliable data already retransmitted but still unacknowledged. Each logical item receives at most one such attempt per application-limited period, sharing a cooldown with PTO. Peer-reported ordered blocking can also trigger an exact probe: the blocking age must reach `max(250 ms, 2 × RTT)`, repeated probes of the same target are at least `max(500 ms, RTT)` apart, and feedback is retained for three seconds.

Implementation boundary: with adaptation enabled, the common eligibility check for additional recovery and targeted FEC excludes BULK, RTT inflation, `QUEUE` and `MTU_BLACK_HOLE`; **it does not independently exclude `RATE_LIMIT` or `BURST`**. Budgets and observed metrics still matter. The design must not be described as disabling repair in every rate-limited scenario.

Sources: [LimitedFecHandler](https://github.com/RelativityMC/netty-raknet/blob/e7f8627521798289db32a9ded8e5bfc89c74334b/common/src/main/java/network/ycc/raknet/pipeline/LimitedFecHandler.java), [ReliabilityHandler](https://github.com/RelativityMC/netty-raknet/blob/e7f8627521798289db32a9ded8e5bfc89c74334b/common/src/main/java/network/ycc/raknet/pipeline/ReliabilityHandler.java), [allowsApplicationLimitedRecovery](https://github.com/RelativityMC/netty-raknet/blob/e7f8627521798289db32a9ded8e5bfc89c74334b/common/src/main/java/network/ycc/raknet/pipeline/AdaptiveTransportController.java).

### 6. MTU, coalescing and DSCP address different problems

DPLPMTUD sends candidate payload probes and waits for confirmation, using a roughly binary search and narrowing the range after up to three timeouts per candidate. A qualifying large-packet black hole returns to a base payload no greater than 1200 bytes. Normal initial MTU is 1400 and the default probe ceiling is 1452. A larger initial MTU selected by `raknetl;` is not forcibly reduced to 1452 by that default ceiling, so it should not be used without validation. MTU probing requires negotiated v12 extensions and cannot bypass blocked UDP.

Small-write coalescing defaults to 500 microseconds, rising to at least 1500 under `RATE_LIMIT` and 750 under `QUEUE` / `BURST`, trading a small wait for fewer packets. This differs from external ZSTD's millisecond-scale batching, which can still hide packet boundaries and cause head-of-line blocking on a single ordered channel.

Static IP TOS defaults to `0xA0` (CS5). Adaptive DSCP is off by default. When enabled, aggregated votes can request AF41 or CS0 with at least 16 votes, one side strictly exceeding twice the other, and a 30-second cooldown. Vote counters and switching state are static within the controller class, not independent per-player markings. Actual effect depends on the socket, operating system and network equipment.

Sources: [DplpmtudController](https://github.com/RelativityMC/netty-raknet/blob/e7f8627521798289db32a9ded8e5bfc89c74334b/common/src/main/java/network/ycc/raknet/pipeline/DplpmtudController.java), [PathMtuDiscoveryHandler](https://github.com/RelativityMC/netty-raknet/blob/e7f8627521798289db32a9ded8e5bfc89c74334b/common/src/main/java/network/ycc/raknet/pipeline/PathMtuDiscoveryHandler.java), [smallWriteCoalesceMicros / applyDscp](https://github.com/RelativityMC/netty-raknet/blob/e7f8627521798289db32a9ded8e5bfc89c74334b/common/src/main/java/network/ycc/raknet/pipeline/AdaptiveTransportController.java).

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
