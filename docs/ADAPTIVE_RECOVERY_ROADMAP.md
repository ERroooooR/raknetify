# Adaptive Loss Recovery Roadmap

## Problem statement

Recent `raknetify-metrics.jsonl` captures show that queue control, pacing, repeated
ACK/NACK protection and bounded FEC prevent the previous multi-megabyte queue
collapse. The remaining failure mode is narrower:

- the path can sustain correlated loss bursts around 20-25%;
- RTT and the sender queue stay comparatively healthy;
- NACK recovery is active, but the retransmission of a reliable ordered frame can
  itself be lost;
- the missing frame then blocks an ordering channel for several seconds while the
  per-FrameSet timeout backs off through 1x, 2x, 4x and 8x intervals.

The objective of this roadmap is to reduce that recovery latency without turning
loss recovery into an unbounded source of traffic on rate-limited paths.

## Design rules

1. Preserve RakNet wire compatibility. All initial stages are sender-side recovery
   or metrics changes and must interoperate with peers that do not implement them.
2. Preserve current NACK, repeated NACK/ACK, pacing, congestion control and FEC.
   New mechanisms complement them; they do not bypass their safety limits.
3. Prefer evidence of forward progress over fixed retry counts.
4. Every speculative transmission is bounded, paced and observable.
5. Do not increase global FEC redundancy solely because loss is high. A rate
   limiter can turn additional redundancy into additional loss.
6. Each stage is independently feature-switchable and covered by deterministic
   EmbeddedChannel tests before the next stage is enabled by default.

## Runtime feature switches

The recovery stages are enabled by default. Operators can disable one mechanism
at JVM startup while leaving the earlier stages and the existing RakNet recovery
path active:

| System property | Stage | Disabled value |
| --- | --- | --- |
| `raknetify.rackLossDetection` | RACK loss inference | `false` |
| `raknetify.ptoProbes` | PTO ordered-data probes | `false` |
| `raknetify.applicationLimitedRecovery` | application-limited extra recovery | `false` |
| `raknetify.targetedFec` | recovery-debt targeted FEC | `false` |

For example, `-Draknetify.targetedFec=false` disables only targeted repair. It
does not disable negotiated bounded FEC or the normal retransmission path.

## Stage 1: RACK-style time-based loss inference

### Rationale

[RFC 8985](https://www.rfc-editor.org/rfc/rfc8985.html) uses the ACK of a
chronologically newer transmission as evidence that an older outstanding
transmission, including a retransmission, is lost after a bounded reordering
window. This is specifically intended to handle application-limited traffic,
lost retransmissions and traffic policers.

### Implementation

- Record the send timestamp of the newest newly acknowledged FrameSet.
- On every useful ACK, inspect outstanding FrameSets that were sent earlier.
- Recall an outstanding FrameSet when both conditions hold:
  - a chronologically newer transmission has been acknowledged;
  - the outstanding transmission is older than the adaptive reordering window.
- Start conservatively with a time threshold derived from the larger of latest
  RTT and smoothed RTT, with a 1.25 multiplier and a small timer granularity.
- Do not apply exponential retry backoff to a loss proven by newer ACK progress.
- Retain the existing sequence-number NACK path as the fastest signal.

### Metrics and gates

- `rack_retransmit_bytes`, `rack_retransmit_framesets`
- `rack_spurious_acks` when an ACK later arrives for a FrameSet already inferred
  lost (requires a short retired-transmission history)
- Default enable only after tests cover reordering, sequence wraparound,
  retransmitted FrameSets and duplicate late ACKs.

## Stage 2: connection-level PTO and ordered-data probe

### Rationale

RACK needs a newer ACK. A tail loss or a small application-limited flight may not
produce one. [RFC 9002](https://www.rfc-editor.org/rfc/rfc9002.html#section-6.2)
uses a Probe Timeout (PTO) to solicit progress without immediately declaring the
entire flight lost.

### Implementation

- Maintain a connection-level progress timer whenever ack-eliciting reliable data
  is outstanding.
- Compute the base PTO from smoothed RTT, RTT variation, timer granularity and the
  configured retry/ACK delay.
- On expiry, send one probe carrying the oldest outstanding reliable data. Prefer
  data that can unblock an ordered channel.
- A second probe is permitted only while the sender is application-limited, the
  queue is not inflated and pacing/congestion budgets allow it.
- PTO expiry does not itself prove loss and does not collapse the congestion
  window. Consecutive PTOs back off exponentially until new ACK progress arrives.
- Probe transmissions count toward bytes in flight and pacing diagnostics.

### Metrics and gates

- `pto_probes`, `pto_probe_bytes`, `pto_probe_acked`
- `pto_count`, `last_ack_progress_age_ns`
- Tests cover tail loss, ACK progress reset, close/removal cancellation and the
  one-probe-per-PTO bound.

## Stage 3: per-ordering-channel HOL observability

### Rationale

Raknetify already maps Minecraft traffic onto RakNet ordering channels, but the
current metrics aggregate ordered pending count and age across the connection.
Before changing channel assignments or priorities, captures must identify which
channel actually blocks.

### Implementation

- Track pending frames, oldest age, released frames and maximum wait separately
  for every RakNet ordering channel.
- Export the active worst channel in the normal metrics row and a compact array of
  per-channel snapshots.
- Include the oldest reliable/order index and retry generation where available.
- Keep the current aggregate fields for dashboard and log compatibility.

### Metrics and gates

- `ordered_worst_channel`
- `ordered_channel_pending[]`, `ordered_channel_oldest_age_ns[]`
- Tests verify independent channels, queue release and metrics synchronization.

## Stage 4: application-limited additional recovery

### Rationale

The experimental IETF proposal
[Enhanced QUIC Recovery for Video Streaming](https://www.ietf.org/archive/id/draft-wu-moq-recovery-for-video-streaming-00.html)
maintains a queue of retransmitted-but-unacknowledged logical data. When the
sender is application-limited, otherwise-unused capacity can perform one bounded
additional recovery while still obeying congestion control.

### Implementation

- Maintain a logical queue of reliable frames that have been retransmitted but
  are not yet acknowledged.
- During an application-limited period, permit at most one additional recovery
  attempt per logical entry.
- Rotate recovered entries to the tail and remove them on success.
- Disable additional recovery while queue inflation, pacing limitation or
  persistent congestion is present.
- Reuse the Stage 2 probe path so speculative sends share one budget.

### Metrics and gates

- `recovery_queue_depth`, `recovery_queue_oldest_age_ns`
- `application_limited_recovery_bytes`
- Tests enforce the once-per-period rule and all suppression conditions.

## Stage 5: debt-controlled targeted FEC/HARQ

### Rationale

Fixed block FEC has measurable but low recovery yield in the current captures.
Research on
[FEC for QUIC](https://arxiv.org/abs/1809.04822) and
[QUIC-FEC](https://arxiv.org/abs/1904.11326) shows that convolutional/sliding
codes can recover short bursts earlier than fixed blocks, while global redundancy
can hurt long transfers or low-delay paths. Application-tailored recovery in
[FlEC](https://arxiv.org/abs/2208.07741) further supports protecting selected
latency-sensitive messages instead of all traffic.

### Implementation

- Derive a recovery-debt score from ordered HOL age in RTTs, retry generation and
  recent unrecovered loss.
- Emit additional repair only for the worst blocked ordering channel or a small
  negotiated critical-channel set.
- Prefer a rolling repair window so a critical FrameSet does not wait for a full
  Reed-Solomon block.
- Keep the existing negotiated bounded block FEC as the compatibility baseline.
- Enforce a strict byte budget and suspend targeted repair under rate-limit or
  queue-inflation signals.

### Metrics and gates

- `recovery_debt`, `targeted_fec_channel`
- `targeted_fec_bytes`, `targeted_fec_recovered`
- Promotion requires loss-simulation tests showing lower ordered completion
  latency without materially increasing queue occupancy or total loss.

## Verification matrix

Every stage must pass:

1. unit tests for timer calculation, wraparound and state transitions;
2. EmbeddedChannel tests for reference counts, promises and duplicate delivery;
3. end-to-end simulated random, burst, ACK-loss and retransmission-loss cases;
4. main project and included `netty-raknet` Gradle test suites;
5. a new real capture comparing RTT, queue, HOL duration, duplicate reliable
   frames, retransmission source and redundancy overhead.

## Rollout order

Implementation proceeds strictly in this order:

1. RACK loss inference;
2. PTO/HOL probe;
3. per-channel HOL metrics;
4. application-limited additional recovery;
5. targeted FEC/HARQ.

Stages remain independently disableable so a regression can be isolated without
discarding the earlier recovery improvements.
