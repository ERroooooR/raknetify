# Causal multichannel integration test

This runbook produces the external evidence required by
`CAUSAL_MULTICHANNEL_REFACTOR.md`. A successful build or a crash-free login is not sufficient:
the final metrics sample must prove that every causal fence completed and every future-epoch queue
drained.

## Build and instrumentation

1. Build once with `gradlew clean build`.
2. Install the same commit on every RakNet endpoint:
   - `fabric/build/libs/*-all.jar` on the Fabric or Connector/NeoForge client and server.
   - `velocity/build/libs/*-all.jar` on Velocity.
   - `bungee/build/libs/*-all.jar` on Bungee/Waterfall.
3. Remove every older Raknetify jar from those mod/plugin directories.
4. Add `-Draknetify.metricsJsonl=true` to every tested RakNet endpoint.
5. Use protocol v12 and the default `compatibility` multichannel profile. Do not carry tuning
   properties from an earlier performance experiment into the correctness baseline.
6. Archive the exact jars, JVM arguments, mod list and proxy configuration with the result.

Each process writes `logs/raknetify-metrics.jsonl`. Move or delete an older metrics file before
starting a run so connection counters from different builds cannot be combined.

### Reproducible NeoForge/Connector server

The affected client pack can be reduced into an isolated server without modifying either the pack
or an existing server. The preparer reads NeoForge, Forge and Fabric metadata (including embedded
jar-in-jar providers), recursively includes required server-side dependencies and rejects
conflicting providers:

```powershell
$raknetifyFabric = Get-ChildItem fabric\build\libs -Filter '*-all.jar' |
  Sort-Object LastWriteTime | Select-Object -Last 1
python scripts/prepare_causal_testbed.py `
  --server-template D:\path\to\neoforge-server `
  --client-mods D:\path\to\affected-client\mods `
  --raknetify-jar $raknetifyFabric.FullName `
  --output build\causal-testbed\server-a `
  --port 25576
python scripts/prepare_causal_testbed.py `
  --server-template D:\path\to\neoforge-server `
  --client-mods D:\path\to\affected-client\mods `
  --raknetify-jar $raknetifyFabric.FullName `
  --output build\causal-testbed\server-b `
  --port 25579
```

By default it selects Connector, Forgified Fabric API, Connector Extras, BandwidthOptimizer,
Create, the bogie add-on, Touhou Little Maid and MaidUseHandCrank. It binds the copied server to
`127.0.0.1:25576`, enables causal JSONL metrics, pins protocol v12 and deliberately removes old
adaptive-tuning properties. The output contains `.causal-testbed.json` with exact source paths,
hashes, resolved mod IDs and JVM arguments. An existing output is replaced only with `--replace`
and only if that sentinel is present.

The isolated world also contains a datapack-driven client entry point. Any connected player can
run `/trigger rk_causal set 1` to perform exactly 100 alternating Overworld/Nether transitions at
one-second intervals. `/trigger rk_causal set 2` stops it and `/trigger rk_causal set 3` reports
progress. The datapack builds small forced-loaded safety platforms at the destination coordinates.
After the completion message, stay connected for at least ten seconds before collecting metrics.

Velocity and Bungee can be prepared beside it using the exact proxy and plugin jars under test:

```powershell
$raknetifyVelocity = Get-ChildItem velocity\build\libs -Filter '*-all.jar' |
  Sort-Object LastWriteTime | Select-Object -Last 1
$raknetifyBungee = Get-ChildItem bungee\build\libs -Filter '*-all.jar' |
  Sort-Object LastWriteTime | Select-Object -Last 1
python scripts/prepare_causal_proxies.py `
  --velocity-jar D:\path\to\velocity.jar `
  --velocity-plugin $raknetifyVelocity.FullName `
  --bungee-jar D:\path\to\bungeecord.jar `
  --bungee-plugin $raknetifyBungee.FullName `
  --output build\causal-testbed\proxies `
  --backend causal_a=127.0.0.1:25576 `
  --backend causal_b=127.0.0.1:25579
```

The generated proxies bind only to `127.0.0.1` on ports `25577` and `25578`, route to the direct
servers on `25576`/`25579`, and use the same protocol/metrics baseline. Their sentinel records
SHA-256 hashes for both proxy distributions and both Raknetify plugins. Velocity and Bungee expose
the backends as `causal_a` and `causal_b`; use `/server causal_a` and `/server causal_b` while
running the proxy-switch portion of the matrix.

## Network fault profile

Apply faults to the UDP RakNet path, not the backend TCP connection. Run at least one loop for each
loss level: 1%, 3% and 5%. Include variable delay and reordering so independent RakNet order
channels arrive in different relative orders.

Example on an isolated Linux test interface:

```sh
tc qdisc replace dev eth0 root netem loss 3% delay 40ms 20ms distribution normal reorder 10% 50%
```

Do not apply this command to a shared production interface. Remove the rule after the run:

```sh
tc qdisc del dev eth0 root
```

The impairment tool must preserve enough connectivity for reliable retransmission to finish. Record
the exact rule and interface in the result.

## Test matrix

Run all three paths:

1. Fabric or Sinytra Connector/NeoForge client directly to a matching modded server.
2. The same client through Velocity.
3. The same client through Bungee/Waterfall.

The client and gameplay server must include:

- Create plus the contraption/bogie add-ons involved in the original failure.
- Touhou Little Maid and the same maid extensions as the affected pack.
- Sinytra Connector/Connector Extras when testing the NeoForge path.

For each path and loss level:

1. Place a moving Create contraption and a maid near the portal test area.
2. Cross a Nether portal or perform an equivalent dimension change 100 times.
3. For proxy paths, include at least 100 backend server switches in the same run.
4. During the loop, exercise contraption assembly/disassembly, bogie rendering, maid AI,
   passengers and inventory/container interaction.
5. After the last transition, remain connected and idle for at least ten seconds so the metrics
   exporter records a settled sample.
6. Stop cleanly and preserve client, proxy and server `latest.log` plus every JSONL file.

Any crash, void contraption/bogie, missing entity data, abnormal maid behavior, unknown packet ID,
`Message too long`, connection decoder exception or incomplete transition fails the run.

## Machine-verifiable acceptance

Run the verifier against each endpoint that owns the RakNet connection:

```sh
python scripts/verify_causal_integration.py \
  path/to/logs/raknetify-metrics.jsonl \
  --log path/to/logs/latest.log \
  --minimum-transitions 100 \
  --require-bundles
```

The verifier requires:

- at least 100 completed outbound or inbound causal transitions;
- `causal_fences_started == causal_fences_completed`;
- zero failed fences;
- epoch counters equal their completed-fence counters;
- zero stale gameplay frames;
- zero pending future-epoch frames/bytes in the final sample;
- at least one negotiated atomic bundle when `--require-bundles` is used;
- no known fatal compatibility signature in the supplied logs.

A `PASS` result proves the transport invariants recorded by Raknetify. Visual/entity behavior must
still be recorded for the Create and Touhou Little Maid assertions that cannot be inferred from
network counters.
