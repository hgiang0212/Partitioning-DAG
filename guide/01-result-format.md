# 01 · Result Format — the normative specification

**This file is the contract.** Everything else in `guide/` either implements it
([02](02-throughput.md)–[04](04-latency.md)) or consumes it
([06](06-visualization.md)–[08](08-build-pipeline.md)). If two projects both conform to
this file, one notebook renders both.

Keywords **MUST**, **SHOULD**, **MAY** are used in the RFC 2119 sense.

---

## 1 · Universal line grammar

Every line in every file MUST match:

```
<ts_ns> [FLAG ...] [key=value ...] [free text]
```

| Element | Rule |
|---|---|
| `ts_ns` | `time.time_ns()` on the **server's** clock. Integer, 19 digits, no separators, never scientific notation. MUST be first. |
| `FLAG` | Bare `UPPERCASE` token, no `=`. Marks a line kind (`SYSTEM`, `ALL`). Zero or more. |
| `key=value` | `[a-z_][a-z0-9_]*` = value with no spaces. Order within a line is not significant to readers but SHOULD be stable. |
| free text | Anything trailing, in parentheses. Informational only; readers MUST ignore it. |

**Value encoding**

| Type | Encoding | Example |
|---|---|---|
| count | bare integer | `done=504` |
| seconds | `{:.3f}` | `busy_s=176.760` |
| milliseconds | `{:.3f}` | `mean_ms=3332.833` |
| rate | `{:.3f}` | `fps=18.131` |
| ratio as percent | `{:.2f}%` — **with** the `%` | `utilization=37.82%` |
| identity | bare string, no spaces | `cluster=intermediate_queue_0` |

A parser MUST strip a trailing `%` before `float()`. Percentages are written `0–100`,
never `0–1`.

This grammar is why one 12-line parser reads every file
([08-build-pipeline.md §2](08-build-pipeline.md)). Do not invent per-file syntaxes.

---

## 2 · File inventory

**Fourteen files: six required, eight optional.** All in one directory. All truncated at
run start. [00-file-inventory.md](00-file-inventory.md) is the long form of this table —
per file, what it measures, what breaks without it, and why a port ends up missing some.

| # | File | Measures | Written | Granularity | Required |
|---|---|---|---|---|---|
| 1 | `batch_done_ns.log` | **when** every unit finished, system-wide | live | one line per completed unit | **MUST** |
| 2 | `group_rate_ns.log` | the same arrivals, **split by group** | live | one line per completed unit, group-tagged | **MUST** |
| 3 | `group_rate.log` | **how fast** the run was | shutdown | one line per group + `SYSTEM` | **MUST** |
| 4 | `utilization.log` | what fraction of its run each device was **busy** | shutdown | one line per device | **MUST** |
| 5 | `utilization_group.log` | the same ratio **rolled up**, pooled and mean | shutdown | per group, per group/role, + `SYSTEM` | **MUST** |
| 6 | `latency_group.log` | **how long** a unit takes, and where the time goes | shutdown | per group/role, per group, + `SYSTEM` | **MUST** |
| 7 | `events_ns.log` | **when** the control plane changed something | live | one line per control event | **MAY** |
| 8 | `free_time.log` | wall clock each device spent **doing nothing** | shutdown | one line per device | **MAY** |
| 9 | `free_time_group.log` | free time rolled up, **and why** | shutdown | per group, per group/role, per machine, + `SYSTEM` | **MAY** |
| 10 | `free_time_series.log` | **when** each device was idle | shutdown | one line per device per time bucket | **MAY** |
| 11 | `broker_ram_ns.log` | the infra host's **memory over the run** | live | one line per RAM sample of the queue host | **MAY** |
| 12 | `broker_ram.log` | what running the system **cost** that host | shutdown | `BROKER` / `USED` / `DELTA` / `RABBIT`, then `PHASE` per phase + `COMPARE` | **MAY** |
| 13 | `message_size.log` | **bytes** a worker puts on the wire | shutdown | one line per measured worker | **MAY** |
| 14 | `message_size_series.log` | payload size **over the run** | shutdown | one line per published message | **MAY** |

The optional files come in **four all-or-none feature groups**: `7` alone, `8–10`,
`11–12`, `13–14`. A group is emitted whole or not at all — half a feature is a summary
whose samples cannot be checked, or samples with no summary to read them by.

Files 13–14 measure the **size of the
payload one worker puts on the wire**, recorded before each publish. Exactly one worker
measures it — the first to register at the first stage, chosen by the server and told via
the dispatch message. See [12-message-size.md](12-message-size.md).

Files 11–12 measure the **RAM of the
machine hosting the message queue**, sampled by the server (nothing of ours runs on that
machine, so it is pulled from outside over SSH). Every line carries `source=`: `ssh` means
host memory from `/proc/meminfo`, `rabbitmq_api` means the management-API fallback, where
`used_mb` is the **broker process**, not the host.

The window opens at **server start** — before any worker registers or anything is
published — and closes a second or two **after** the run, so the series contains the host
at rest as well as under load. Every sample carries `phase=` (`idle` / `run` / `tail`) and
the summary reports each phase plus a `COMPARE` line stating what running the system cost
that host. See [11-broker-ram.md §6](11-broker-ram.md).

Files 8–10 measure **free time**: the
wall clock in which a device did no work of any kind. See
[10-free-time.md](10-free-time.md) for the method and for why free time is neither
utilization nor `1 − utilization`.

> **Naming note.** The reference implementation names files 2, 3, 5, 6, 9
> `fps_cluster_ns.log`, `fps_cluster.log`, `utilization_cluster.log`,
> `latency_cluster.log`, `free_time_cluster.log`, and file 7 `cut_change_ns.log`. Either
> name set is conformant — pick one **per project** and keep it stable. The parsers in
> [08](08-build-pipeline.md) and the validator in §5 take the scheme as a parameter. Do
> not mix the two naming schemes within one project: a directory that mixes them is
> complete and still reads as five missing files. Full cross-reference:
> [00 §4](00-file-inventory.md).

Files MUST be created even when empty. A missing file is a hard error for the reader; an
empty one is a valid "this run had none".

**Not in this inventory, and not results.** Device-side side-car files (each device's own
timing log, its live free-time and message-size records, per-unit metric CSVs) are inputs
to the files above, not outputs of the run — but they have their own startup and archive
rules, and a port that ignores them loses the only per-device record it will ever have.
Project-specific outputs (model accuracy, the pipeline's actual results) are outside this
contract entirely. Both are covered in [00 §6–§7](00-file-inventory.md).

---

## 3 · File-by-file specification

### 3.1 `batch_done_ns.log` — system throughput series

One line per completed unit, appended the instant the completion message arrives. **The
arrival is the event**; nothing in the message body is used for timing.

```
1785127877009606331
1785127877851326519
1785127884523396059 24.13
1785127886561966600 24.87
```

| Column | Meaning |
|---|---|
| 1 | ns-epoch arrival time, server clock |
| 2 | smoothed `window_rate` over the last `W` units — **absent** on the first `W-1` lines |

```
window_rate = (W - 1) * unit_size / (t[-1] - t[-W]) ,  W = 16
```

**Rules**
- `W` MUST be 16 unless the project documents otherwise; charts assume it.
- Column 2 format is exactly `{:.2f}`, space-separated.
- Lines 1..`W-1` have **one** column. Parsers MUST tolerate both arities — this is the
  most common parsing bug.
- This is the authoritative system-wide series. It counts **every** arrival regardless of
  body, so a mis-tagged unit cannot move it.

**Why two arities instead of `0.00` padding:** a zero would be indistinguishable from a
genuine stall. Absence means "no window yet".

---

### 3.2 `group_rate_ns.log` — per-group throughput series

The same arrivals as 3.1, bucketed by the group id carried in the message body.

```
1785127877009606331 cluster=intermediate_queue_0 done=1
1785127884523396059 cluster=intermediate_queue_0 done=16 window_fps=21.44
1785127901123456789 cluster=intermediate_queue_1 done=16 window_fps=11.62
```

| Key | Meaning |
|---|---|
| `cluster` | producing group's id; `unknown` if the producer sent an untagged completion |
| `done` | running count of completions **for that group** |
| `window_fps` | that group's own `W`-unit window rate — absent until it has `W` completions of its own |

**Rules**
- A group reaches its first full window **later** than the system does. That is correct,
  not a bug.
- An unrecognized body MUST be bucketed as `unknown`, never dropped — an old worker
  degrades the breakdown instead of losing units.
- Line count MUST equal `batch_done_ns.log`'s line count. This is a conformance check.

---

### 3.3 `group_rate.log` — throughput summary

Written once at shutdown. One line per group, then one `SYSTEM` line.

```
1785128504858762215 cluster=intermediate_queue_0 fps=18.131 steady_fps=18.491 done=336 frames=10752 share=66.7%
1785128504858762215 cluster=intermediate_queue_1 fps=8.407  steady_fps=8.740  done=168 frames=5376  share=33.3%
1785128504858762215 SYSTEM fps=25.220 done=504 frames=16128 clusters=2
```

| Key | On | Definition |
|---|---|---|
| `fps` | group, SYSTEM | `frames / (START → **that scope's** last completion)`. Shared START, per-scope end |
| `steady_fps` | group only | `(n-1) * unit_size / (first → last completion)` for that group — drops warm-up |
| `done` | both | completed units |
| `frames` | both | `done × unit_size` |
| `share` | group only | that group's percentage of all units |
| `clusters` | SYSTEM only | number of groups |

**Rules**
- `fps` MUST use the **shared** START. Each scope's *end* is its own last completion, so
  the `SYSTEM` end is the overall last completion.
- `steady_fps` MUST use the group's own first completion, or a group that started late is
  unfairly penalised. **This is the fair number for comparing groups.**
- The `SYSTEM` line MUST NOT carry `steady_fps` or `share`.
- `share` is the fastest read on whether work was balanced across groups.

**What is and is not additive — read this before writing a check.**

`done` and `frames` are additive: group values MUST sum exactly to the `SYSTEM` value.

Group `fps` values are **not**. Each group divides by its own span, and a group that
finished early has a smaller denominator, which inflates its rate relative to its
contribution to the system. So:

```
Σ group_fps  ≥  SYSTEM fps          equality only when all groups finish together
```

Measured on the reference runs: `18.131 + 8.407 = 26.538` against a `SYSTEM` of `25.220`
(+5.2%), because one group finished 46 s before the other. That is correct output, not
a bug. A sum *below* the system value would be the bug — it means START was not shared.

The exact, checkable invariant is on the spans:

```
SYSTEM_frames / SYSTEM_fps  ==  max over groups of ( group_frames / group_fps )
```

Both reference runs satisfy this to within 0.005%, which is pure `{:.3f}` rounding. The
validator in §5 checks this form, not the sum.

---

### 3.4 `utilization.log` — per-device busy ratio

One line per device, written as each device's report is drained at shutdown.

```
1785128504862965418 client=7e3dd352-8b9a-4d6b-8173-412705d9bbe3 role=edge  packages=56  busy_s=176.760 total_s=467.394 utilization=37.82%
1785128504872091881 client=c6fdabe4-63e1-45f7-ad21-ef4de628bd3b role=cloud packages=169 busy_s=558.047 total_s=599.152 utilization=93.14%
```

| Key | Meaning |
|---|---|
| col 1 | ns-epoch **arrival** of the report at the server — not a device timestamp |
| `client` | stable device id (UUID) |
| `role` | the device's stage class |
| `packages` | units this device processed |
| `busy_s` | `Σ (output_i − get_input_i)` on the device's own clock |
| `total_s` | `end − start` on the device's own clock |
| `utilization` | `busy_s / total_s` as a percent |

**Rules**
- `busy_s` and `total_s` MUST come from the **same device's** clock. See
  [03-utilization.md](03-utilization.md).
- `utilization` MUST be ≤ 100%. A value above 100% means overlapping intervals were
  summed — the measurement is wrong, not the device.
- A partial collection (not every device reported before the timeout) is acceptable and
  MUST print a warning, but MUST NOT abort the run.

---

### 3.5 `utilization_group.log` — utilization rolled up

Three line kinds, all in one file.

```
1785128504874248492 cluster=intermediate_queue_0 ALL devices=8 utilization=55.06% utilization_mean=53.29% busy_s=2348.297 total_s=4264.621 packages=672
1785128504874248492 cluster=intermediate_queue_0 role=cloud devices=2 utilization=93.44% busy_s=1119.832 total_s=1198.399 packages=336
1785128504874248492 SYSTEM devices=12 clusters=2 utilization=60.75% utilization_mean=59.01% busy_s=4060.923 total_s=6684.796
```

| Line kind | Marked by | Covers |
|---|---|---|
| group total | `cluster=` + `ALL` | every device in that group |
| group × role | `cluster=` + `role=` | devices of one role in one group |
| system | `SYSTEM` | every device |

| Key | Definition |
|---|---|
| `utilization` | **pooled**: `Σbusy / Σtotal`, weighting each device by how long it ran |
| `utilization_mean` | plain mean of per-device ratios. Present on `ALL` and `SYSTEM` only |
| `devices` | device count in that scope |

**Rules**
- Both `utilization` and `utilization_mean` MUST be emitted on `ALL`/`SYSTEM` lines. A
  pooled figure can hide one idle device inside a busy group; **when the two diverge the
  group is imbalanced**, and that divergence is the signal.
- This file groups; it does **not** replace `utilization.log`.

---

### 3.6 `latency_group.log` — latency distributions

```
1785128504874248492 cluster=intermediate_queue_0 role=cloud kind=service  n=336 mean_ms=3332.833 p50_ms=3470.922 p95_ms=3966.246  max_ms=4367.941
1785128504874248492 cluster=intermediate_queue_0 role=cloud kind=pipeline n=336 mean_ms=3410.112 p50_ms=3502.664 p95_ms=4038.901  max_ms=4501.220
1785128504874248492 cluster=intermediate_queue_0 kind=e2e n=336 mean_ms=69655.623 p50_ms=69379.207 p95_ms=102639.355 max_ms=111760.937
1785128504874248492 SYSTEM kind=e2e n=504 mean_ms=75320.528 p50_ms=77109.393 p95_ms=109986.265 max_ms=120801.164
```

| `kind` | Span | Clock | Exact? |
|---|---|---|---|
| `service` | the device's own `get_input → output` | one | **yes** |
| `pipeline` | unit ready → published, including hand-off queue waits | one | yes |
| `e2e` | first stage start → completing stage output | **two machines** | indicative |

**Rules**
- `service` samples MUST sum to the matching `busy_s` in `utilization_group.log`. This
  is a conformance check, and it makes `service` the **only** latency directly comparable
  against utilization.
- `pipeline` additionally contains in-process queue waits, so it scales with queue depth
  rather than device speed. A run with a queue depth of 4 showed `service ≈ 11.6 s` vs
  `pipeline ≈ 57.6 s` on the same devices — a 5× gap that is pure buffering. **If
  `pipeline ≫ service`, reduce the queue depth; throughput does not depend on it.**
- `e2e` has one series per group, emitted only by the completing stage.
- Percentiles MUST be **nearest-rank over pooled raw samples**, no interpolation — every
  number printed is a latency that was actually observed. See
  [04-latency.md](04-latency.md).
- `role=` is absent on `e2e` and `SYSTEM` lines.

---

### 3.7 `events_ns.log` — control-plane events (optional)

One line per control decision worth overlaying on the timeline.

```
1785127901123456789 intermediate_queue_1: cut 4->5 deeper
1785127960221004518 intermediate_queue_1: cut 5->4 shallower
```

Format: `<ts_ns> <scope>: <description>`. Free-form after the colon — this file is for
human reading and for **vertical rules on a time-axis chart**
([07-chart-catalogue.md C9](07-chart-catalogue.md)).

**Rules**
- The timestamp MUST be taken **before** the decision is broadcast, so it marks when the
  decision was made rather than when it landed.
- Written from a background thread in the reference implementation; each append MUST
  open and close the file independently so no handle is shared across threads.
- **Truncation trap.** If this file is truncated only when the feature that writes it is
  enabled, a later run with the feature off inherits the previous run's file — and an
  archiver that copies every non-empty file will archive stale data. **Truncate it
  unconditionally**, or teach the archiver to skip it. The reference implementation has
  this bug; do not reproduce it.

---

### 3.8 `free_time.log` — per-device idle time (optional)

One line per device, written as each device's report is drained at shutdown. **Free
time** is the wall clock in which the device did nothing at all — no input, no compute,
no encode/decode, no transfer, no bookkeeping — computed as the run span minus the
**union** of every one of its lanes' busy intervals.

```
1786024095544125400 client=7e3dd352-8b9a-4d6b-8173-412705d9bbe3 role=edge  machine=machine-2 cluster=intermediate_queue_0 device=cpu  span_s=467.394 busy_s=181.004 free_s=286.390 free=61.28% gaps=57 longest_free_ms=9821.400 host_idle=54.10%
1786024095544125400 client=c6fdabe4-63e1-45f7-ad21-ef4de628bd3b role=cloud machine=machine-7 cluster=intermediate_queue_0 device=cuda span_s=599.152 busy_s=571.930 free_s= 27.222 free= 4.54% gaps=170 longest_free_ms=812.100 host_idle=11.30%
```

| Key | Meaning |
|---|---|
| col 1 | ns-epoch **arrival** of the report at the server — not a device timestamp |
| `client` | stable device id |
| `role` | the device's stage class |
| `machine` | the host this device process runs on. Several devices MAY share one |
| `span_s` | `end − start` on the device's own clock |
| `busy_s` | measure of the **merged** busy intervals across all lanes |
| `free_s` | `span_s − busy_s` |
| `free` | `free_s / span_s` as a percent |
| `gaps` | number of separate free intervals |
| `longest_free_ms` | the longest single one |
| `host_idle` | OS-level idle share of the whole machine over the run. Optional |

**Rules**
- `busy_s` MUST be a **union**, never a sum of per-stage timers. A sum can exceed
  `span_s` whenever two lanes overlap, which is normal for a pipelined device.
- `busy_s + free_s` MUST equal `span_s` exactly.
- `free` MUST be ≤ 100%.
- `free` and `utilization` ([§3.4](#34-utilizationlog--per-device-busy-ratio)) measure
  different things and MUST NOT be expected to sum to 100%.

---

### 3.9 `free_time_group.log` — free time rolled up (optional)

Six line kinds, all in one file.

```
1786024095544125400 cluster=intermediate_queue_0 ALL devices=8 free=48.21% free_mean=46.02% free_s=2055.900 span_s=4264.621
1786024095544125400 cluster=intermediate_queue_0 role=edge devices=6 free=61.28% free_mean=60.11% free_s=1719.200 span_s=2805.700
1786024095544125400 cluster=intermediate_queue_0 FREE reason=input free_s=1802.400 share=87.67%
1786024095544125400 cluster=intermediate_queue_0 KIND kind=inference busy_s=1204.700 share=28.25%
1786024095544125400 MACHINE machine=machine-2 devices=3 free=12.40% free_s=57.900 span_s=467.394 merge_slop_s=0.000 host_idle=54.10%
1786024095544125400 SYSTEM devices=12 clusters=2 machines=12 free=39.55% free_mean=38.10% free_s=2643.700 span_s=6684.796
```

| Line kind | Marked by | Covers |
|---|---|---|
| group total | `cluster=` + `ALL` | every device in that group |
| group × role | `cluster=` + `role=` | devices of one role in one group |
| free breakdown | `FREE` + `reason=` | why that scope was free |
| busy breakdown | `KIND` + `kind=` | where that scope's busy time went |
| machine | `MACHINE` + `machine=` | every device **process** on one host |
| system | `SYSTEM` | every device |

| Key | Definition |
|---|---|
| `free` | **pooled**: `Σfree / Σspan`, weighting each device by how long it ran |
| `free_mean` | plain mean of per-device percentages. Present on `ALL`/`SYSTEM` |
| `share` | this reason's portion of the scope's free time, or this kind's portion of its span |
| `merge_slop_s` | non-busy time swallowed by capping the shipped interval list |
| `host_idle` | mean OS-level idle share reported by the devices on that machine |

**Rules**
- `MACHINE` lines MUST come from the **union of the busy intervals** of the device
  processes on that host, not from their ratios: two devices that are each 50% free can
  keep a machine 100% busy by interleaving. Intervals MUST NOT be unioned across
  machines — that is the one place device timestamps are compared, and it is valid only
  because processes on one host share a clock.
- `FREE reason=` shares MUST sum to 100% of that scope's free time. Overlapping reasons
  MUST be attributed in a fixed published priority so nothing is double counted;
  whatever no reason covers MUST be reported as `unaccounted` rather than dropped.
- `KIND` shares MAY sum to more than 100%: per-kind sums overlap across lanes by
  construction. Only the merged `busy_s` in [§3.8](#38-free_timelog--per-device-idle-time-optional)
  is exclusive.
- A machine that runs no devices (e.g. the controller's own host) MAY appear with
  `devices=0` and only `host_idle`.

---

### 3.10 `free_time_series.log` — free time over the run (optional)

One line per device per fixed-width time bucket. This is the plottable "when was each
device idle" series; it is written at shutdown but describes the whole run.

```
1786024095544125400 client=7e3dd352 role=edge machine=machine-2 cluster=intermediate_queue_0 i=0 t_offset_s=0.000 bucket_s=1.000 free=12.40%
1786024095544125400 client=7e3dd352 role=edge machine=machine-2 cluster=intermediate_queue_0 i=1 t_offset_s=1.000 bucket_s=1.000 free=88.10%
```

| Key | Meaning |
|---|---|
| col 1 | ns-epoch arrival of the report at the server, identical on every line of a report |
| `i` | bucket index within that device's run |
| `t_offset_s` | seconds since **that device's own start** |
| `bucket_s` | bucket width |
| `free` | percent of that bucket the device spent doing nothing |

**Rules**
- The leading timestamp is the report's server-clock arrival, exactly as in
  [§3.4](#34-utilizationlog--per-device-busy-ratio); the position in the run is carried
  by `t_offset_s`, which is on the **device's** clock. Do not conflate them: devices
  start at different moments and their offsets are not directly comparable.
- `bucket_s` MUST be carried on every line rather than assumed, so a long run may widen
  its buckets to bound the file size without breaking readers.

---

### 3.11 `message_size.log` — payload size, per measured worker (optional)

One line per worker that measured, which is normally exactly one: the first worker that
registered at the first stage, selected by the server ([12](12-message-size.md)).

```
1786366279200770600 client=machine-2 role=edge machine=machine-2 cluster=intermediate_queue_0 mode=split splits=5 compress=on num_bit=8 batch_size=32 n=504 total_mb=19657.464 mean_mb=39.003 p50_mb=39.022 p95_mb=39.613 max_mb=40.098 min_mb=37.909 span_s=714.260 rate_mb_s=27.521 per_frame_mb=1.2188
```

| Key | Meaning |
|---|---|
| `n` | messages this worker published |
| `total_mb` | bytes put on the wire over the run (MB = 10⁶) |
| `mean_mb`, `p50_mb`, `p95_mb`, `max_mb`, `min_mb` | per-message size; percentiles nearest-rank over the raw samples |
| `span_s` | first → last publish, that worker's clock |
| `rate_mb_s` | `total_mb / span_s`, this worker's egress |
| `per_frame_mb` | `mean_mb / unit_size` |

**Rules**
- Sizes MUST be the **serialized** bytes handed to the transport, measured **before** the
  publish call.
- Statistics MUST be computed over every sample, even when the shipped series is
  decimated.
- The line MUST carry the context that determines the size (compression, split point,
  mode, unit size). A size without them cannot be reproduced.

---

### 3.12 `message_size_series.log` — payload size over the run (optional)

One line per published message. Written at shutdown, describes the whole run.

```
1786366279200770600 client=machine-2 cluster=intermediate_queue_0 i=0 t_offset_s=0.000 batch_id=0 bytes=38897647 mb=38.898
1786366279200770600 client=machine-2 cluster=intermediate_queue_0 i=1 t_offset_s=1.420 batch_id=1 bytes=39104512 mb=39.105
```

| Key | Meaning |
|---|---|
| col 1 | ns-epoch arrival of the report at the server, identical on every line |
| `i` | sample index |
| `t_offset_s` | seconds since that worker's **own first publish** |
| `bytes` | exact integer, the authoritative value |
| `mb` | same number in MB |

**Rules**
- Same split of clocks as [§3.10](#310-free_time_serieslog--free-time-over-the-run-optional):
  the leading timestamp is the server's, the position in the run is `t_offset_s` on the
  worker's clock. Never conflate them.
- A long run MAY decimate this series evenly to bound its size; it MUST NOT truncate it,
  and `i` MUST stay non-decreasing.

---

## 4 · Truncation and lifecycle

```
startup     truncate every file it emits; purge the measurement queues
   │
START       record the shared start time  (t0 for every rate)
   │
run         batch_done_ns.log, group_rate_ns.log, events_ns.log   ← live append
   │
drain       keep collecting after the first stage finishes (02 §5)
   │
shutdown    group_rate.log
            utilization.log → utilization_group.log, latency_group.log
            free_time.log → free_time_group.log, free_time_series.log   (optional)
            message_size.log → message_size_series.log                  (optional)
   │
archive     copy everything + the config that produced it  (05)
```

**Rules**
- Every file the project emits MUST be truncated at startup, before any worker can
  write — including the optional ones it chooses to emit. Truncating
  per-worker instead of once centrally causes late-starting workers to wipe files that
  earlier workers are already writing.
- Measurement queues MUST be purged at startup so a crashed run's stale messages cannot
  inflate the next run's counts.
- Shutdown collection MUST have a timeout and MUST NOT hang the run.

---

## 5 · Conformance validator

The validator ships beside this file as **`guide/validate_results.py`** — a single
dependency-free script, ~300 lines. It is the executable form of this specification, so
it is the copy to read; do not maintain a second one inline here, or the two drift and
the prose wins arguments it should lose.

It runs in two passes.

**Pass 1 — inventory** ([00](00-file-inventory.md)). Before parsing a single line it
reports all fourteen files as `ok` / `EMPTY` / `MISS`, prints an `N/14 present, M/6
required` tally, and fails on:

| Check | Why it exists |
|---|---|
| a required file is missing or empty | the six are the contract; six of six or the run is not conformant |
| an optional feature is half-present | files 8–10, 11–12 and 13–14 are one feature each ([§2](#2--file-inventory)) |
| `config.yaml` is not beside the results | warning only — but the numbers are unreadable in a month without it ([05](05-archiving.md)) |

It accepts either naming scheme, and either name for file 7, so a conformant project is
never reported as missing files it simply calls something else.

**Pass 2 — grammar and cross-file invariants:**

| Check | Catches |
|---|---|
| every line opens with a 19-digit ns timestamp | a device clock, a float, or a header line leaking into a result file |
| `batch_done_ns.log` has 1 or 2 columns, col 2 a float | the warm-up arity handled wrong — the most common parsing bug |
| line counts match across files 1 and 2 | a stage that stopped reporting mid-run |
| live timestamps are non-decreasing | out-of-order appends, or two writers on one file |
| exactly one `SYSTEM` line, without `steady_fps` | a summary written per group instead of per run |
| group `done` / `frames` sum **exactly** to `SYSTEM` | two stages both publishing completions — a 2× rate error |
| `SYSTEM` span == max group span | a per-group START used where a shared one was required |
| group `fps` never sums **below** `SYSTEM` | the same bug from the other direction |
| `share` sums to ~100% | warning: units bucketed as `unknown` |
| percentages carry `%` and never exceed 100% | overlapping busy intervals summed instead of merged |
| `ALL` / `SYSTEM` carry `utilization_mean` beside `utilization` | a pooled figure hiding one idle device inside a busy group |
| latency lines carry `n`/`mean`/`p50`/`p95`/`max`, ordered, `e2e` without `role` | percentiles computed on pre-averaged data |
| `busy_s + free_s == span_s`, `free` ≤ 100% | free time summed across lanes instead of unioned |
| `FREE reason=` shares total 100% per scope | free time silently dropped instead of reported as `unaccounted` |

Exit code is `0` when there are no errors, `1` otherwise, so it drops straight into CI.

Run it on every run directory before charting:

```bash
python guide/validate_results.py results/2026-07-27/run-a --names cluster
```

**What it catches that code review does not:** two stages both reporting completion
(group `done` sums past `SYSTEM`), a stage that stopped reporting early (line-count
mismatch), overlapping busy intervals (utilization > 100%), a per-group start time used
where a shared one was required (group `fps` does not sum), and percentile computation
done on pre-averaged data (ordering violation).
