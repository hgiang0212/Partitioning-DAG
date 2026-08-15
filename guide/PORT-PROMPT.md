# Port prompts — paste one of these into Claude Code in the target project

Two ready-to-use prompts for making **another project** emit results in this project's
format. Both assume you have first copied this `guide/` directory into the target repo:

```bash
cp -r guide/ <target-project>/guide/
```

Both pin the **`cluster` naming scheme** and the exact filenames this project uses, so the
two projects' run directories are byte-comparable and one notebook renders both.

| | Prompt A | Prompt B |
|---|---|---|
| Result files | **14** | **16** (14 + 2 mAP) |
| Measures accuracy | no | yes |
| Timing numbers usable | **yes** | **no** — mAP work lands inside `busy_s` and `e2e` |
| Use it for | throughput / utilization / latency / free time / RAM / message size | model accuracy per cluster, and over time |

**Never use one run for both.** Computing mAP per batch adds a second post-process pass, a
file write per frame and a metric update per frame, all of it inside the `get_input →
output` window — so it inflates `busy_s`, `service`, `pipeline` and `e2e`. Prompt B's run
answers "how accurate", Prompt A's run answers "how fast". A run that tries to do both
answers neither.

---

## Prompt A — results only, no mAP (14 files)

````text
Read the `guide/` directory in this repo, in this order, before writing any code:
README.md → 00-file-inventory.md → 01-result-format.md → 02-throughput.md →
03-utilization.md → 04-latency.md → 05-archiving.md → 09-port-checklist.md.
Then read 10-free-time.md, 11-broker-ram.md and 12-message-size.md only for the
optional features I list below. 01 is normative; where anything else disagrees
with it, 01 wins.

GOAL
Make this project emit run results in exactly that format, so its run directories
are interchangeable with another project's and the same notebook renders both.
Do NOT measure model accuracy — that is out of scope for this port.

NAMING SCHEME — use `cluster`, not `group`. The 14 files, by these exact names:

  required (6)
    batch_done_ns.log          when every unit finished, system-wide + window rate
    fps_cluster_ns.log         the same arrivals, tagged by cluster
    fps_cluster.log            throughput summary, one line per cluster + SYSTEM
    utilization.log            per-device busy ratio, one line per device
    utilization_cluster.log    the same rolled up: cluster / cluster+role / SYSTEM
    latency_cluster.log        service, pipeline and e2e distributions

  optional (8) — each group is ALL its files or NONE
    cut_change_ns.log                                    control-plane events
    free_time.log, free_time_cluster.log,
      free_time_series.log                               free time (guide 10)
    broker_ram_ns.log, broker_ram.log                    infra-host RAM (guide 11)
    message_size.log, message_size_series.log            message size (guide 12)

  plus config.yaml, copied into every archived run directory.

Port ALL of them unless a feature is meaningless here — and if you skip one, skip
the whole group and tell me which and why. Do not invent files, do not rename
these, and do not mix in the `group_*` spellings.

FIRST, before writing code, answer these and wait for my confirmation:
  1. Terminology map (guide/README "Terminology"): what is a unit, a unit size, a
     device, a role, a cluster, the server, the completing stage, in THIS project?
  2. Which component holds the authoritative clock and writes the result files?
  3. Which stage emits the completion signal, and is it exactly one?
  4. Which optional features make sense here, and which do not?

THEN implement in this order (guide 00 §10), validating after each step:
  1. files 1-3 (throughput)  2. files 4-5 (utilization)  3. file 6 (latency)
  4. config archive          5. cut_change_ns.log        6. the optional features

NON-NEGOTIABLE INVARIANTS (guide/README "Design invariants"):
  - One clock. Every timestamp in every shared file is time.time_ns() on the
    server. Never subtract one device's timestamp from another's.
  - The arrival is the event. Completion messages carry an identity, never a
    timing measurement.
  - Ratios stay inside one device: utilization's numerator and denominator both
    come from that device's own clock.
  - Devices ship RAW latency samples; the server pools them and takes nearest-rank
    percentiles. Never average per-device percentiles.
  - Truncate every result file at startup, once, centrally — never per worker.
    Purge the measurement queues too.
  - Aggregate before you divide: throughput is total items / total time, never the
    mean of per-interval rates.
  - Every measurement feature flag lives in the server's config and travels in the
    dispatch message. No worker reads it from its own config file at run time.
    Turning a flag off must also skip the server's collector for it.
  - Telemetry never kills the run: every failure path degrades to a warning.
  - Files must exist even when empty. A missing file is a hard error for the
    reader; an empty one is a valid "this run had none".

ACCEPTANCE — I will check exactly this:
  python guide/validate_results.py <run-dir> --names cluster
  - the inventory header reads `N/14 result files present, 6/6 required`, where N
    matches the features you said you would port
  - exit code 0, zero errors
  - run it on the ARCHIVED directory too, not only the live one
  - line counts of batch_done_ns.log and fps_cluster_ns.log are equal
  - no utilization above 100%; p50 <= p95 <= max everywhere
  - deleting the results directory and re-running reproduces it completely

DELIVERABLES
  1. The implementation.
  2. One real run's directory, validated as above, with its console output.
  3. A short note listing: the terminology map, which optional features you ported
     and which you skipped with the reason, and anything a worker still reads from
     its own local config (guide/README invariant 9 requires this to be explicit).

Work through guide/09-port-checklist.md phase by phase and report which boxes you
could not tick, rather than silently leaving them.
````

---

## Prompt B — results **plus** mAP (16 files)

Use only when you want accuracy. The two extra files are **not** part of the portable
contract — they are this project's own, so their format is pinned below rather than in
`guide/`.

````text
Read the `guide/` directory in this repo, in this order, before writing any code:
README.md → 00-file-inventory.md → 01-result-format.md → 02-throughput.md →
03-utilization.md → 04-latency.md → 05-archiving.md → 09-port-checklist.md, then
10-free-time.md, 11-broker-ram.md, 12-message-size.md. 01 is normative; where
anything else disagrees with it, 01 wins.

GOAL
Make this project emit run results in exactly that format PLUS the two mAP files
specified at the bottom of this prompt — 16 files total.

Note what the guide says and why I am overriding it here: guide/README puts
model-accuracy metrics out of scope, because nothing in the portable format depends
on ground truth. The two mAP files below are therefore MY project's own extension,
not part of the shared contract. They must never stand in for a missing result
file, and the 14 files below must all still be produced.

NAMING SCHEME — use `cluster`, not `group`. The 16 files, by these exact names:

  required (6)
    batch_done_ns.log          when every unit finished, system-wide + window rate
    fps_cluster_ns.log         the same arrivals, tagged by cluster
    fps_cluster.log            throughput summary, one line per cluster + SYSTEM
    utilization.log            per-device busy ratio, one line per device
    utilization_cluster.log    the same rolled up: cluster / cluster+role / SYSTEM
    latency_cluster.log        service, pipeline and e2e distributions

  optional (8) — each group is ALL its files or NONE
    cut_change_ns.log                                    control-plane events
    free_time.log, free_time_cluster.log,
      free_time_series.log                               free time (guide 10)
    broker_ram_ns.log, broker_ram.log                    infra-host RAM (guide 11)
    message_size.log, message_size_series.log            message size (guide 12)

  accuracy (2) — this project's extension, spec at the bottom of this prompt
    map.log                    per-cluster mAP summary, both pipelines
    map_window.log             sliding-window mAP over the run

  plus config.yaml, copied into every archived run directory.

FIRST, before writing code, answer these and wait for my confirmation:
  1. Terminology map (guide/README "Terminology"): what is a unit, a unit size, a
     device, a role, a cluster, the server, the completing stage, in THIS project?
  2. Which component holds the authoritative clock and writes the result files?
  3. Which stage emits the completion signal, and is it exactly one?
  4. Where is the ground truth, and which component holds it?
  5. Which optional features make sense here, and which do not?

THEN implement in this order, validating after each step:
  1. files 1-3 (throughput)  2. files 4-5 (utilization)  3. file 6 (latency)
  4. config archive          5. cut_change_ns.log        6. optional features
  7. the two mAP files LAST — they must not be able to break steps 1-6

NON-NEGOTIABLE INVARIANTS (guide/README "Design invariants"):
  - One clock. Every timestamp in every shared file is time.time_ns() on the
    server. Never subtract one device's timestamp from another's.
  - The arrival is the event. Completion messages carry an identity, never a
    timing measurement.
  - Ratios stay inside one device.
  - Devices ship RAW latency samples; the server pools and takes nearest-rank
    percentiles.
  - Truncate every result file at startup, once, centrally. Purge the measurement
    queues.
  - Every measurement flag lives in the server's config and travels in the dispatch
    message; turning one off also skips the server's collector for it.
  - Telemetry never kills the run — and this applies doubly to mAP: a missing
    ground-truth file, an unreadable prediction, or an absent metrics backend
    degrades to a warning and an omitted line. It must never abort the run or lose
    a single one of the 14 result files.
  - Files exist even when empty.

mAP MEASUREMENT — REQUIRED BEHAVIOUR
  - The whole accuracy path sits behind one server-side flag that travels in the
    dispatch message. With it off, devices do no accuracy work at all and the two
    files are empty, not absent.
  - Per unit, each device writes its low-threshold detections write-once to
    `map/pred/<cluster>/frame_NNNNNN.txt` as
    `class_id x_center y_center width height confidence`, normalized to the network
    input size — mirroring the ground-truth layout `map/label/frame_NNNNNN.txt`
    (`class_id x_center y_center width height`) so the two folders can be diffed
    frame for frame.
  - Write-once means: if a frame index already has a file, SKIP it. Two devices in
    one cluster reprocessing the same input must not overwrite each other.
  - `map/pred/` and any collected scratch directory MUST be deleted at startup by
    the component that starts once. A write-once cache that survives a run wins
    forever: run N+1 silently reuses run N's predictions for every frame they
    share, and the numbers look entirely plausible.
  - At shutdown each device ships its own predictions to the server; the server
    scores them against its local ground truth and runs TWO independent pipelines:
      pipeline 1 (WINDOW): slide a window of 16 consecutive present batches, one
        batch at a time, and compute a full mAP over each window's frames. This is
        the accuracy counterpart of the window rate in batch_done_ns.log — one line
        per window into map_window.log, plus the mean of the windows into map.log.
      pipeline 2 (ALL): one mAP over every matched frame of the cluster, into
        map.log.
  - Collection has a timeout and warns on partial; it never hangs the run.

mAP FILE FORMAT — match these lines exactly

map.log — two lines per cluster (WINDOW then ALL), then two OVERALL lines:

  <ts_ns> cluster=<id> WINDOW mAP50_95=0.1004 mAP50=0.1857 (mean of 14 window(s) x 16 batches, step 1)
  <ts_ns> cluster=<id> ALL    mAP50_95=0.0882 mAP50=0.1632 (905/905 GT frame(s) matched)
  <ts_ns> OVERALL WINDOW      mAP50_95=0.1005 mAP50=0.1858 (avg over 2 cluster(s))
  <ts_ns> OVERALL ALL         mAP50_95=0.0882 mAP50=0.1632 (avg over 2 cluster(s))

map_window.log — one line per sliding window per cluster:

  <ts_ns> cluster=<id> window=0 batches=0-15 frames=512 mAP50_95=0.0787 mAP50=0.1465

Rules for both:
  - Same universal grammar as every other result file (guide 01 §1): a 19-digit
    ns-epoch server timestamp first, then UPPERCASE flags, then key=value, then
    free text in parentheses which readers ignore.
  - WINDOW / ALL / OVERALL are bare uppercase flags, no `=`.
  - mAP values are ratios in 0..1 formatted `{:.4f}` — NOT percentages. This is a
    deliberate exception to guide 01's percent rule; do not append `%` and do not
    scale to 0..100.
  - One timestamp per collection, identical on every line of that collection.
  - A cluster with no matched frames is omitted with a warning, never written as
    zeros — 0.0000 is a real accuracy claim and it would be a false one.

ACCEPTANCE — I will check exactly this:
  python guide/validate_results.py <run-dir> --names cluster
  - the inventory header reads `N/14 result files present, 6/6 required`, where N
    matches the features you said you would port (the validator does not know about
    the 2 mAP files; confirm those separately)
  - exit code 0, zero errors, on the live AND the archived directory
  - map.log has exactly 2 lines per cluster plus 2 OVERALL lines
  - map_window.log's frame counts equal window_batches x unit_size
  - the run still produces all 14 result files with accuracy enabled
  - flipping the accuracy flag off yields empty map files and a run that is
    otherwise identical in shape

DELIVERABLES
  1. The implementation.
  2. One real run's directory, validated as above, with its console output.
  3. A short note listing: the terminology map, features ported vs skipped with
     reasons, anything a worker still reads from its own local config, and an
     explicit statement of how much the accuracy path added to busy_s and e2e
     (compare one run with the flag on against one with it off).

Work through guide/09-port-checklist.md phase by phase and report which boxes you
could not tick, rather than silently leaving them.

FINALLY, warn me in your summary that this run's throughput, utilization and
latency numbers are NOT comparable with a no-mAP run: the second post-process
pass, the per-frame file write and the per-frame metric update all sit inside the
get_input -> output window and inflate busy_s, service, pipeline and e2e.
````
