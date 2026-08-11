# 13 · Bounding the broker — a memory limit that holds, and says so

[11](11-broker-ram.md) measures the infrastructure host. This file **limits** it.

The two are deliberately separate features with separate switches, because a measurement
that also throttles can no longer answer the question the measurement exists for: *what
does this system do when nothing throttles it?* Run 11 alone to find out how much the
broker needs. Run 13 to make sure it never takes more than that.

> **Instance.** In the reference project the infrastructure host is the RabbitMQ box, the
> work queues are `queue_0`–`queue_2`, and the two files are `broker_guard_ns.log` /
> `broker_guard.log`. Substitute your own nouns; nothing else changes.

---

## 1 · Why it earns a file

The hazard is structural, not accidental. In a split pipeline the first stage publishes as
fast as it can read input, every later stage pulls one unit at a time, and **nothing
anywhere connects the two rates**. The moment the producer is faster, the difference does
not queue *somewhere* — it queues in the memory of a machine that runs no code of yours.

The arithmetic is unforgiving. One unit in the reference instance is a 5.8–26.1 MB feature
map ([`layer_profile.json`](../src/layer_profile.json)); at cut layer 11 it is 18.1 MB
uncompressed. A producer 10% faster than its consumer over a 20-minute run leaves roughly
**120 units, ~2 GB**, resident in the broker. Nothing in the pipeline notices, because
nothing in the pipeline is looking.

What happens next is the part worth knowing. A broker at its high-water mark does not
raise an error and does not drop anything: it **blocks every publisher**, silently, at the
TCP level. On the worker that is indistinguishable from "the next stage got slow." You
tune the wrong thing for a week.

So the limit has to do three things, and the third is what most of this file is about:

1. keep the broker under a stated ceiling,
2. lose nothing while doing it,
3. **be visible in the results** — a run that was throttled and a run that was not must
   not look identical.

---

## 2 · Three limits, at three distances from the failure

| # | Where | What it does | When it fires |
|---|---|---|---|
| 1 | producer (`PublishGuard`) | pauses before publishing while its destination queue is over the high mark | normally — this is the working limit |
| 2 | broker policy | `max-length-bytes` + `overflow=reject-publish` per work queue | when layer 1 is off, wrong, or bypassed |
| 3 | broker watermark | `rabbitmqctl set_vm_memory_high_watermark absolute` | last resort; the only *literal* memory cap |

Layer 3 is the only one that actually caps the broker's memory, and it is also the one
whose enforcement is invisible — a blocked publisher looks exactly like a slow consumer.
**Layers 1 and 2 exist so that layer 3 never fires.** Every time either of them acts, it
writes a line.

### Why a policy and not queue arguments

The obvious implementation is `queue_declare(..., arguments={"x-max-length-bytes": N})`.
It does not work here. Queue arguments must match on **every** declarer, and in this
pipeline the workers declare their own work queues in `Scheduler.__init__`, before the
server has told them anything. Change the arguments on one side and the other side gets
`PRECONDITION_FAILED` on a channel it needs — the limit reaches the workers as a crash.

A policy applies to a queue **however it was declared**. One PUT from the server, no
worker change, no declare-time agreement to keep in sync. It also means the limit can be
removed at shutdown, which matters on a shared broker.

### Why `reject-publish` and not `drop-head`

`drop-head` is the RabbitMQ default and it is the wrong default here: it deletes the
**oldest** unit to make room, silently. A run that quietly loses units still prints a
throughput number, and it is a number nobody can tell from a correct one.

`reject-publish` refuses the *new* unit instead — and refusing is only useful if the
producer is told, which is why **publisher confirms are required** whenever the policy is
on. Without confirms a rejected publish is discarded and the publisher never hears about
it: the same silent loss, arrived at from the other direction. With confirms it surfaces
as an exception, which is the only form in which it can be retried.

The cost is one round trip per payload publish, on a dedicated channel so that the
completion signal, the utilization report and the RPC registration keep their unconfirmed
fast path.

---

## 3 · The numbers

One number is configured. The rest are derived, so they cannot drift apart:

```
payload_budget = max_broker_ram_mb - broker_overhead_mb - control_reserve_mb
per_queue_cap  = payload_budget / len(work_queues)
high mark      = per_queue_cap x high_frac      producer pauses above this
resume mark    = per_queue_cap x resume_frac    ...and resumes below this
```

With the shipped defaults (`1024 - 200 - 150 = 674 MB` over three queues):

| | MB |
|---|---|
| cap | 1024.0 |
| payload budget | 674.0 |
| per-queue cap (policy) | 224.7 |
| high mark (producer pauses) | 168.5 |
| resume mark | 112.3 |

Both subtractions are load-bearing:

- **`broker_overhead_mb`** — the Erlang VM, the management database and one connection per
  worker cost 150–250 MB on an *idle* broker. A budget that hands the full cap to payload
  is over the cap before the first unit.
- **`control_reserve_mb`** — this run ships the model itself through the broker:
  `reply_<client>` carries a base64 copy of the weights to every worker, ~7.4 MB × 12 ≈
  90 MB of *control* traffic measured against the same ceiling. It is transient, it is on
  no work queue, and no policy here touches it — so it is reserved, not capped.

The budget is divided **evenly** across the work queues, not by observed load. The
division has to hold when a split moves and a queue that carried nothing last run carries
everything in this one.

The gap between high and resume is hysteresis. Without it a producer sitting exactly on
the threshold pauses and resumes on every unit, and the log fills with events that
describe the threshold rather than the run.

### Sizing it

`per_queue_cap` divided by one unit's size is how many units may be in flight — with the
defaults, 224.7 / 18.1 ≈ **12 units of buffer**, deep enough to absorb jitter and shallow
enough to bound memory. Below about 3 units the buffer stops absorbing anything and the
pipeline runs lockstep; below one unit it cannot work at all, which is handled explicitly
(§5).

---

## 4 · Wiring

The server owns all of it, for the same reason it owns the sampler in
[11](11-broker-ram.md): it is the only component alive for the whole run and the only one
that may write a shared log.

```
Server.__init__       BrokerGuard(...).apply()      before any worker can register:
                                                    a policy applied halfway through a
                                                    run leaves the first half unbounded
                                                    and the two halves incomparable
notify_clients        dispatch["broker_guard"]      the byte budget travels WITH the work
                                                    (README invariant 9) — a cap in each
                                                    worker's config file is twelve caps
Scheduler             PublishGuard.bind(channel)    on the dispatch message, never on the
                                                    first publish: a connection error
                                                    belongs anywhere but the hot path
send_next_layer       guard.publish(...)            the only publish in the system whose
                                                    volume is set by input rate rather
                                                    than by downstream demand
_send_utilization     report rides along            one queue, one drain (README inv. 10)
Server.start          guard.stop() -> summary       stop first: the monitor is what saw
                                                    the peaks, and stopping also hands
                                                    the broker back
```

The producer's pause is booked as a **wait**, not as work (`broker_backpressure` in
[10](10-free-time.md)). Charging it to `send` would report a throttled device as fully
utilized — the exact opposite of what happened, and it would hide the cost of the cap in
the one file built to show it.

---

## 5 · Failure modes, and what each degrades to

Every one of these was reachable in testing. None of them ends a run.

| Situation | Behaviour |
|---|---|
| management API unreachable | no policy, no monitor; the producer guard still works (it uses AMQP, not HTTP). Summary reports `polls=0 within_cap=unknown` |
| user lacks the `policymaker` tag | policy `status=failed` with the HTTP code, loudly; producer guard continues alone |
| depth probe fails 3× | probing gives up, admission control switches off, the policy becomes the only limit — logged in red |
| the guard's own channel dies | falls back to the caller's channel without confirms |
| **one unit larger than the high mark** | admission control turns itself **off** and names both numbers. Left alone this deadlocks: the estimate is over the mark before the first unit and stays there forever |
| the queue is empty | always accepts one unit, whatever the cap says — the same deadlock from the other side |
| producer paused > `max_block_s` | stops waiting, publishes, logs in red. Ending a run is the server's stall detector's call, not a producer's |
| broker rejects for `max_block_s` | raises with a clear message naming the queue. The pipeline really is broken at that point; saying so beats hanging |

The probe loop is also what keeps a paused producer's connection alive. A wait implemented
as a bare `sleep` would sit silent past the heartbeat and be disconnected for idling, in
the middle of behaving correctly.

---

## 6 · The files

Both are written by the server, both are truncated at run start with everything else, both
are archived. As always: present-and-empty is a valid answer, missing is not.

### `broker_guard_ns.log` — one line per **event**, not per sample

Deliberately not a series. [11](11-broker-ram.md) already samples the host every second;
a second per-sample file here would disagree with it by one sampling interval and give no
way to tell which was right. This file records only changes.

```
<ns> GUARD     cap_mb= overhead_mb= control_reserve_mb= payload_budget_mb= queues=
               per_queue_mb= high_mb= resume_mb= policy= watermark= confirms=
<ns> BROKER    node_watermark_mb= mem_used_start_mb= target_cap_mb=
<ns> POLICY    status=applied|failed|removed|kept name= pattern= max_length_bytes= overflow=
<ns> WATERMARK status=set|failed|skipped|restored|restore_failed absolute_bytes= previous_bytes=
<ns> ALARM     state=on|off mem_used_mb= watermark_mb=
<ns> OVER      queue= mb= msgs= high_mb= cap_mb=
<ns> UNDER     queue= mb= msgs=
<ns> BLOCKED   connections=
<ns> NOTE      <free text, spaces as underscores>
```

`ALARM` is the line the whole file is worth having for. Every worker sees a stall with no
local cause; only the broker knows it stopped them. It is written on **both** edges so the
duration is recoverable.

### `broker_guard.log` — the summary

```
<ns> GUARD    ...same fields... overflow=
<ns> BROKER   watermark_mb= mem_start_mb= mem_peak_mb= cap_mb= headroom_mb= polls=
              within_cap=yes|NO|unknown alarms= alarm_s= blocked_peak=
<ns> QUEUE    queue= peak_mb= peak_msgs= cap_mb= high_mb= peak_pct= over_events=
<ns> THROTTLE client= role= cluster= events= wait_s= max_wait_s= published= mb=
              nacks= gave_up= peak_est_mb= probe= active=yes|no off=<reason> span_s=
<ns> THROTTLE client=ALL producers= events= wait_s= max_wait_s= published= nacks=
              throttled_frac=
<ns> NOTE     ...
```

Two fields carry most of the meaning:

- **`within_cap`** — `yes`, `NO`, or `unknown`. The third value is not decoration. A run
  whose management API was unreachable observed a peak of 0 MB, and reporting *that* as
  `yes` would turn "we never looked" into "it held." A limit nobody observed is never
  reported as held.
- **`active` / `off`** — whether the producer was still throttling when the run ended, and
  why it stopped if not (`payload_over_cap`, `probe_dead`). A guard that switched itself
  off and a run that never needed one produce the same `events=0`; only this field
  separates them.
- **`throttled_frac`** — the share of a producer's run spent waiting on the broker. At 0%
  the cap changed nothing and the run is exactly what it would have been uncapped; at 40%
  the throughput number in [02](02-throughput.md) is a *capped* throughput and must be
  read as one.

A queue never observed is **omitted**, never written as zeros — same rule as the phases in
[11 §6](11-broker-ram.md).

### Invariants (checked by `scratch/validate_optional.py`)

1. The per-queue caps **sum** to the payload budget. Otherwise the cap the run enforced is
   not the cap it reports.
2. `resume_mb < high_mb < per_queue_mb`, and `payload_budget_mb < cap_mb`.
3. `polls=0` ⟹ `within_cap=unknown`.
4. `alarms>0` ⟹ the series contains `ALARM` lines. The summary and the series must tell
   the same story.
5. `nacks>0` ⟹ `policy=on`. A rejection can only come from the policy.
6. Per-producer `THROTTLE` lines require the `client=ALL` roll-up.
7. `peak_mb <= cap_mb` on every `QUEUE` line while `policy=on` — if that fails, the
   broker-side cap did not hold and the finding is the point.

---

## 7 · Configuration

```yaml
broker_guard:
  enabled: True
  max_broker_ram_mb: 1024   # the whole point
  broker_overhead_mb: 200
  control_reserve_mb: 150
  work_queues: [queue_0, queue_1, queue_2]
  high_frac: 0.75
  resume_frac: 0.50
  apply_policy: True        # needs a policymaker/administrator user
  keep_policy: False        # remove at shutdown; the broker is shared
  confirm_publish: True     # required for reject-publish to be loss-free
  poll_interval_s: 2.0
  max_block_s: 300
  enforce_watermark: False  # opt-in: a global change to a shared host
  restore_watermark: True
```

### The watermark, deliberately opt-in

Layer 3 is the only limit that is literally the broker's own, and it is the only one that
changes a machine shared with whatever else uses that broker. So it is off by default and
the exact command is printed instead:

```bash
rabbitmqctl set_vm_memory_high_watermark absolute 1073741824
```

Set `enforce_watermark: True` (with `broker_ram.ssh` credentials — **host** login, not
RabbitMQ's application credentials) and the server applies it at startup and restores the
previous value at shutdown. It is a runtime setting either way, so a broker restart also
reverts it: the right default for a change made by a measurement run.

Note what the watermark is measured against — the **broker process**, not the host. If
something else on that box is using memory, capping RabbitMQ at 1 GB does not cap the
host at 1 GB. [11](11-broker-ram.md) is what tells you the difference, and it is why the
two features stay separate.

---

## 8 · Reading a run

Start at `broker_guard.log`:

- `within_cap=yes`, `throttled_frac=0.00%` — the cap was never approached. The run is
  what it would have been with no limit at all, and every other file can be read
  straight.
- `within_cap=yes`, `throttled_frac` high — **the cap is what produced this throughput.**
  The pipeline wanted to run faster than the memory budget allows. Either the budget is
  too small or the stage split is wrong; `QUEUE peak_pct` and the `OVER` lines say which
  queue.
- `alarms>0` — layers 1 and 2 did not hold and the broker stopped publishers itself.
  Everything downstream of the first `ALARM state=on` is a measurement of a blocked
  system. Cross-check the timestamp against `broker_ram_ns.log`.
- `nacks>0` with `events=0` — producers were rejected without ever pausing, which means
  admission control was off or blind (`probe=dead`) while the policy carried the run
  alone.
- `within_cap=unknown` — the guard could not see the broker. The producer-side numbers
  are still true; nothing else in the file is evidence.
