# 00 · File inventory — every file a run produces, and what each one measures

**Read this before you port anything.** [01](01-result-format.md) says what a line looks
like; this file says **which files must exist at all**. The single most common porting
failure is not a malformed line — it is a run that quietly produces nine files where the
contract asks for fourteen, because the port implemented the measurements it noticed and
never had a list of the ones it did not.

This is that list.

---

## The count

```
14  result files          6 required  +  8 optional, in 4 all-or-none feature groups
 1  archived config       config.yaml, copied into the run directory
 3  classes of side-car   device-side files the server never writes but a port still needs
 —  project-specific      anything else your project emits is NOT part of this contract
```

A conformant run directory therefore holds **6 files at minimum**, **15 when every
optional feature is on** (14 logs + `config.yaml`), and more only if you archive the
side-car files ([§6](#6--side-car-files--written-by-devices-not-by-the-server)).

Anything not in this inventory is not a result. Anything in it that is absent is a bug —
**except** an optional feature you deliberately did not port, which must be absent as a
whole group, never partially.

---

## 1 · The fourteen, at a glance

| # | File | One line is | Measures | Req |
|---|---|---|---|:--:|
| 1 | `batch_done_ns.log` | one completed unit | **when** every unit finished, system-wide + smoothed rate | ● |
| 2 | `group_rate_ns.log` | one completed unit | the same arrivals, **split by group** | ● |
| 3 | `group_rate.log` | one group, + `SYSTEM` | **how fast** the run was, per group and overall | ● |
| 4 | `utilization.log` | one device | what fraction of its run each device spent **busy** | ● |
| 5 | `utilization_group.log` | one group / group×role / `SYSTEM` | the same ratio **rolled up**, pooled and mean | ● |
| 6 | `latency_group.log` | one (group, role, kind) | **how long** a unit takes: service, pipeline, e2e | ● |
| 7 | `events_ns.log` | one control decision | **when** the control plane changed something | ○ |
| 8 | `free_time.log` | one device | how much wall clock each device spent doing **nothing** | ○ |
| 9 | `free_time_group.log` | one group / role / machine / reason / kind | free time **rolled up, and why** | ○ |
| 10 | `free_time_series.log` | one device × one time bucket | **when** each device was idle | ○ |
| 11 | `broker_ram_ns.log` | one RAM sample | the infrastructure host's memory **over the run** | ○ |
| 12 | `broker_ram.log` | one summary line kind | what running the system **cost** that host | ○ |
| 13 | `message_size.log` | one measured worker | how many **bytes** a worker puts on the wire | ○ |
| 14 | `message_size_series.log` | one published message | payload size **over the run** | ○ |

● required (**MUST**) ○ optional (**MAY**), but all-or-none within its group ([§5](#5--the-four-optional-features-are-all-or-none))

Four axes cover the whole inventory, and it is worth seeing the shape before the detail:

|  | **How fast** | **How busy** | **How idle** | **How big** |
|---|---|---|---|---|
| **series** (over the run) | 1, 2 | — | 10 | 11, 14 |
| **summary** (at shutdown) | 3 | 4, 5, 6 | 8, 9 | 12, 13 |

Every series file has a summary partner and vice versa. When you port one and not the
other you get a chart with no headline number, or a headline number nobody can explain.

---

## 2 · The six required files, one by one

### 1 · `batch_done_ns.log` — required · **live**

- **Question:** how did system throughput move over the run?
- **One line =** one unit completing, timestamped **on arrival at the server**.
- **Measures:** completion instants, plus a `W=16`-unit smoothed rate from line 16 on.
- **Written by:** the server, appended the moment a completion message arrives.
- **Missing ⇒** you have no throughput timeline at all. C3 and C9 cannot be drawn, and
  nothing can be overlaid on a time axis. This is the file everything else is read against.
- **Spec:** [01 §3.1](01-result-format.md) · **Method:** [02](02-throughput.md) ·
  **Charts:** C3, C9

### 2 · `group_rate_ns.log` — required · **live**

- **Question:** which group produced each unit, and how did each group's rate move?
- **One line =** the same arrivals as file 1, tagged with the producing group.
- **Measures:** per-group completion instants and each group's own window rate.
- **Written by:** the server, same append as file 1.
- **Missing ⇒** no per-group timeline (C2, C4), and you lose the ability to see one group
  stalling while the system total looks healthy.
- **Conformance:** its line count **MUST equal** file 1's. A mismatch means a stage stopped
  reporting mid-run.
- **Spec:** [01 §3.2](01-result-format.md) · **Charts:** C2, C4

### 3 · `group_rate.log` — required · **shutdown**

- **Question:** what was the run's throughput, in one number per group and one for the system?
- **One line =** one group, plus exactly one `SYSTEM` line.
- **Measures:** `fps` against the shared start, `steady_fps` against the group's own first
  completion, `done`, `frames`, `share`.
- **Written by:** the server at shutdown, after the final drain.
- **Missing ⇒** no headline number and no run-to-run comparison (C1, C10). Also the file
  the validator uses for the shared-START check, so without it a whole class of
  measurement bug goes undetected.
- **Spec:** [01 §3.3](01-result-format.md) · **Charts:** C1, C10

### 4 · `utilization.log` — required · **shutdown**

- **Question:** was any single device a straggler?
- **One line =** one device, written as its report is drained.
- **Measures:** `busy_s / total_s` on **that device's own clock**.
- **Written by:** the server, from reports the devices ship to a dedicated queue.
- **Missing ⇒** group utilization has no device-level backing, so an imbalance inside a
  group is invisible (C8).
- **Spec:** [01 §3.4](01-result-format.md) · **Method:** [03](03-utilization.md) ·
  **Charts:** C8

### 5 · `utilization_group.log` — required · **shutdown**

- **Question:** is the work split correctly across groups and roles?
- **One line =** a group total (`ALL`), a group×role, or `SYSTEM`.
- **Measures:** pooled `Σbusy / Σtotal` **and** the plain mean of per-device ratios. Both,
  always — their divergence is the imbalance signal.
- **Written by:** the server, from the same reports as file 4.
- **Missing ⇒** no answer to "should the split point move", which is usually the reason
  the run was measured at all (C7, C10).
- **Spec:** [01 §3.5](01-result-format.md) · **Charts:** C7, C10

### 6 · `latency_group.log` — required · **shutdown**

- **Question:** how long does one unit take, and where does the time go?
- **One line =** one `(group, role, kind)` distribution; `kind ∈ {service, pipeline, e2e}`.
- **Measures:** `n`, mean, p50, p95, max in ms — **nearest-rank over pooled raw samples**.
- **Written by:** the server, pooling raw sample arrays shipped by the devices.
- **Missing ⇒** you cannot separate compute time from queueing time, which is the single
  most useful diagnosis this format produces (`pipeline ≫ service` ⇒ reduce queue depth).
- **All three kinds are required** when the file is. Two kinds is a half-port.
- **Spec:** [01 §3.6](01-result-format.md) · **Method:** [04](04-latency.md) ·
  **Charts:** C5, C6, C10

---

## 3 · The eight optional files, one by one

### 7 · `events_ns.log` — optional · **live**

- **Question:** did a control-plane decision change the throughput curve?
- **One line =** `<ts_ns> <scope>: <free text>`, one control decision.
- **Written by:** whichever component makes the decision, **before** it broadcasts it.
- **Missing ⇒** C9 degrades to a plain timeline. Harmless if your system has no control
  plane; skip the file entirely rather than emitting an empty one you never fill.
- **Trap:** truncate it **unconditionally**, or a run with the feature off inherits the
  previous run's events and the archiver stamps them into the wrong run
  ([05 §4](05-archiving.md)).
- **Spec:** [01 §3.7](01-result-format.md) · **Charts:** C9

### 8–10 · free time — optional, **one feature, three files** · **shutdown**

Governed by one flag (`free_time.enable` in the reference config). Emit all three or none.

| File | One line is | Measures |
|---|---|---|
| `free_time.log` | one device | wall clock the device spent doing **nothing at all** — span minus the **union** of every lane's busy intervals |
| `free_time_group.log` | group / group×role / `MACHINE` / `FREE reason=` / `KIND kind=` / `SYSTEM` | the same, rolled up — plus **why** it was free and **where** its busy time went |
| `free_time_series.log` | one device × one time bucket | **when** it was idle — the heat-map input |

- **Written by:** each device writes its own `free_time_<role>_<group>_<id>.log` live and
  ships a report at finish; the server drains those into these three at shutdown.
- **Missing ⇒** you can see that a device was not busy, but not whether it was waiting for
  input, blocked downstream, or genuinely done. Utilization does **not** answer this:
  `free` and `1 − utilization` are different quantities and must not be expected to agree.
- **Hard invariant:** `busy_s + free_s == span_s` exactly, per device, and `busy_s` is a
  **union**, never a sum — a sum exceeds the span on any pipelined device.
- **Spec:** [01 §3.8–3.10](01-result-format.md) · **Method:** [10](10-free-time.md) ·
  **Charts:** none catalogued; see [10 §5](10-free-time.md)

### 11–12 · infrastructure-host RAM — optional, **one feature, two files**

Governed by one flag (`broker_ram.enable`). Emit both or neither.

| File | One line is | Measures |
|---|---|---|
| `broker_ram_ns.log` | one sample (live) | the queue host's used memory through the whole run, each line tagged `phase=idle\|run\|tail` and `source=ssh\|rabbitmq_api` |
| `broker_ram.log` | one summary line kind (shutdown) | `BROKER` / `USED` / `DELTA` / `RABBIT`, then one `PHASE` per phase and a `COMPARE` line |

- **Written by:** the **server**, pulling the host from outside over one long-lived SSH
  session — nothing of yours runs on that machine.
- **Window:** opens at **server start**, before any worker registers, and closes 1–2 s
  after the drain. That leading `idle` stretch is the whole point: without the host at
  rest there is no denominator for "what did running the system cost it".
- **Missing ⇒** a stalled run is unattributable. Devices go quiet and their free time
  lands in `backpressure` with nothing wrong on the device itself; this curve is what
  distinguishes a blocked publisher from a slow one.
- **Spec:** [01 §3.11–3.12 note / §2 files 11–12](01-result-format.md) ·
  **Method:** [11](11-broker-ram.md) · **Charts:** none catalogued; see [11 §7](11-broker-ram.md)

### 13–14 · message size — optional, **one feature, two files** · **shutdown**

Governed by one flag (`message_size.enable`). Emit both or neither.

| File | One line is | Measures |
|---|---|---|
| `message_size.log` | one **measured worker** — normally exactly one line | total/mean/p50/p95/max/min payload size, egress rate, per-item size, plus the context that determines them |
| `message_size_series.log` | one published message | payload size over the run, `bytes` exact + `mb` |

- **Written by:** one worker records sizes **before** each publish and ships a report; the
  server writes both files at shutdown.
- **Exactly one worker measures** — the first to register at the first stage, **chosen by
  the server** and told in the dispatch message. Never self-selected: one stale config and
  either every worker measures or none does, and the summary line looks identical either way.
- **Missing ⇒** utilization tells you a worker was busy but not whether it was busy
  computing or busy shipping, and the RAM curve in 11–12 has no explanation.
- **Spec:** [01 §3.11–3.12](01-result-format.md) · **Method:** [12](12-message-size.md) ·
  **Charts:** none catalogued; see [12 §5](12-message-size.md)

---

## 4 · The two naming schemes — a very common cause of "missing" files

Five files have two conformant names. **Pick one scheme per project and never mix them.**

| Role | `group` scheme | `cluster` scheme |
|---|---|---|
| per-group throughput series | `group_rate_ns.log` | `fps_cluster_ns.log` |
| throughput summary | `group_rate.log` | `fps_cluster.log` |
| utilization rolled up | `utilization_group.log` | `utilization_cluster.log` |
| latency distributions | `latency_group.log` | `latency_cluster.log` |
| free time rolled up | `free_time_group.log` | `free_time_cluster.log` |

The other nine files are named identically in both schemes.

The reference implementation uses the **`cluster`** scheme. This guide is written in the
`group` scheme because it is the neutral term. A port that copies file names out of the
prose while its readers expect the reference names produces a directory that is *complete*
and still reads as five missing files — the validator and the notebook both take the
scheme as a parameter for exactly this reason:

```bash
python guide/validate_results.py <run-dir> --names cluster    # or --names group
```

---

## 5 · The four optional features are all-or-none

| Feature | Files | Governed by | Half-porting it gives you |
|---|---|---|---|
| control events | 7 | the control plane existing at all | — (single file) |
| free time | 8, 9, 10 | `free_time.enable` | per-device idle with no roll-up, or a roll-up nobody can drill into |
| infra-host RAM | 11, 12 | `broker_ram.enable` | a curve with no summary, or a summary with no curve to check it against |
| message size | 13, 14 | `message_size.enable` | statistics that cannot be verified against the samples they came from |

Rules that apply to every one of them:

1. **The flag lives in the server's config** and travels in the dispatch message. No worker
   reads it locally, or a measurement setting has to be changed on N machines and the
   copies drift ([README, invariant 9](README.md)).
2. **Turning the flag off must also skip the server's own collector.** A collector still
   polling a queue nobody will publish to burns its full timeout on every run and then
   warns `0/N` — a stall and a scary message caused by a setting working exactly as
   intended ([README, invariant 10](README.md)).
3. **The files still exist, empty, when the feature is off.** A missing file is a hard
   error for the reader; an empty one is a valid "this run had none".

---

## 6 · Side-car files — written by devices, not by the server

These are **not** results and must not be confused with them, but a port that forgets them
loses the only record of several things, so they belong in the inventory.

| Artifact | Written by | Lives | Why it exists |
|---|---|---|---|
| `free_time_<role>_<group>_<id>.log` | each device, live | the device's own filesystem | the per-device, per-lane, per-bucket breakdown behind files 8–10; survives a server or broker failure |
| `message_size_<group>_<id>.log` | the measured worker, live | that worker's filesystem | every size sample, even if the report never reaches the server |
| per-device timing log (`<ns> <event>`) | each device, live | the device's own filesystem | the raw input to utilization ([03](03-utilization.md)) — a device with no timing log reports nothing |
| per-batch metric CSVs | each device, live | the device's own filesystem | the only per-unit record of size, split point and latency together |

**Lifecycle rules for side-cars — each one has bitten the reference implementation:**

- Delete them **centrally at startup**, in the component that starts **once**. Deleting
  them per worker makes a late-starting worker wipe files an earlier worker is already
  writing.
- Device ids change every run, so last run's file belongs to nothing this run — and an
  archiver that copies by glob will fold it into the new run's results.
- Only devices that share the server's filesystem contribute to the archive. Collecting the
  rest is manual; make that a warning, never a failure.
- Any **write-once cache** keyed by item id must be cleared at startup too. Leftovers win
  forever: run N+1 silently reuses run N's outputs for every key they share, and the
  numbers look entirely plausible.

---

## 7 · What is *not* in this contract

Your project will emit files this guide says nothing about — the reference implementation
writes `map.log` and `map_window.log` for detection accuracy, and `detections_stream.jsonl`
for its actual output. Model-accuracy metrics are deliberately **out of scope**
([README](README.md)): nothing here depends on ground truth or on the pipeline doing
inference at all.

Keep them separate in your head, because the two classes have different rules:

- **Result files** (the 14) — fixed names, fixed grammar, truncated at startup, validated,
  archived, rendered by the shared notebook with zero changes.
- **Project files** — your names, your format, your lifecycle. Archive them if useful.
  Never let one of them fill in for a missing result file: a project-specific accuracy log
  next to nine of fourteen results still reads as an incomplete run.

---

## 8 · Deployment manifest — tick these before calling the port done

Copy into your port's PR description and check every box. Scheme: `group` ☐ / `cluster` ☐
(one, written down).

**Required — all six, or the run is not conformant**

- [ ] 1 `batch_done_ns.log` — live, one append per arrival, **two arities**
- [ ] 2 `group_rate_ns.log` — live, line count **equal** to file 1
- [ ] 3 `group_rate.log` — shutdown, exactly one `SYSTEM` line, shared START
- [ ] 4 `utilization.log` — shutdown, one line per device, never above 100%
- [ ] 5 `utilization_group.log` — shutdown, **both** `utilization` and `utilization_mean`
      on `ALL`/`SYSTEM`
- [ ] 6 `latency_group.log` — shutdown, **all three** kinds, nearest-rank percentiles

**Optional — per feature, all files or none**

- [ ] 7 `events_ns.log`, truncated **unconditionally**
- [ ] 8, 9, 10 free time — `busy_s + free_s == span_s`, busy is a **union**
- [ ] 11, 12 infra RAM — window opens at server start, `phase=` on every sample
- [ ] 13, 14 message size — one worker, chosen by the server, measured **before** publish

**Around the files**

- [ ] `config.yaml` copied into every archived run directory
- [ ] Every file the project emits is truncated at startup, **once, centrally**
- [ ] Measurement queues purged at startup
- [ ] Side-car files ([§6](#6--side-car-files--written-by-devices-not-by-the-server))
      cleared at startup and archived where they share a filesystem
- [ ] Archive skips empty files, is collision-safe, and warns loudly on an empty archive

---

## 9 · Completeness check

The validator reports the inventory before it checks a single line, so a run that is
missing files says so at the top:

```bash
python guide/validate_results.py <run-dir> --names cluster
```

```
inventory  <run-dir>  (naming scheme: cluster)
  [ ok  ]   1  batch_done_ns.log              504 lines
  [ ok  ]   2  fps_cluster_ns.log             504 lines
  ...
  [MISS ]  10  free_time_series.log           -- required by the free-time feature
```

To check only presence — no parsing, no dependencies — while you are still standing up the
port:

```bash
# PowerShell
$req = 'batch_done_ns.log','fps_cluster_ns.log','fps_cluster.log',
       'utilization.log','utilization_cluster.log','latency_cluster.log'
$req | Where-Object { -not (Test-Path (Join-Path $dir $_)) }
```

```bash
# bash
for f in batch_done_ns.log fps_cluster_ns.log fps_cluster.log \
         utilization.log utilization_cluster.log latency_cluster.log; do
  [ -f "$dir/$f" ] || echo "MISSING $f"
done
```

---

## 10 · Port in this order

Do not implement fourteen files at once. Each step below produces something you can
validate and chart before the next one starts.

| Step | Files | You can now |
|---|---|---|
| 1 | 1, 2, 3 | answer "how fast", draw C1–C4, run the validator's throughput checks |
| 2 | 4, 5 | answer "is the work split right", draw C7, C8 |
| 3 | 6 | separate compute from queueing, draw C5, C6 |
| 4 | `config.yaml` + archive | compare two runs at all ([05](05-archiving.md)) |
| 5 | 7 | overlay control decisions, draw C9 |
| 6 | 8–10, 11–12, 13–14 | explain *why* a run looked the way it did |

Steps 1–4 are the port. Steps 5–6 are the features you turn on when a run raises a
question the first four cannot answer.

---

## Why files go missing — ranked by how often it actually happens

| # | Symptom | Cause | Fix |
|---|---|---|---|
| 1 | 5 files "missing", 5 unexpected ones present | naming schemes mixed | pick one scheme, pass `--names` ([§4](#4--the-two-naming-schemes--a-very-common-cause-of-missing-files)) |
| 2 | a whole feature absent | flag off — but nobody wrote down that it was off | flag lives in the server config and is archived with `config.yaml` |
| 3 | one file of a feature group present | half-port | all-or-none ([§5](#5--the-four-optional-features-are-all-or-none)) |
| 4 | file exists, zero bytes, archive skipped it | the collector timed out, or the feature is off | check the shutdown warnings; empty is valid, silently absent from the archive is confusing |
| 5 | files present but from the *previous* run | truncated conditionally instead of unconditionally | truncate everything at startup ([05 §4](05-archiving.md)) |
| 6 | shutdown files missing, live files fine | shutdown collection hung or aborted | collection MUST have a timeout and MUST NOT hang the run |
| 7 | side-car files missing | devices do not share the server's filesystem | collect manually; warn, never fail |
| 8 | archive has fewer files than the log directory | archiver skips empty files by design | expected — confirm against this inventory, not against the folder |
