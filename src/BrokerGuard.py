"""Bounding what a run parks in the broker's memory (guide/13-broker-guard.md).

[11](../guide/11-broker-ram.md) measures the queue host; this module *limits* it.
The two are deliberately separate: a measurement that also throttles cannot tell
you what the system does when nothing throttles it.

The hazard is structural, not accidental. The edge publishes one message per
batch as fast as it can decode video, every cloud tier pulls one message at a
time, and nothing anywhere connects the two rates. The moment the edge is
faster, the difference accumulates **in the broker's RAM** — a machine that runs
no code of ours and whose failure mode is not an error but a silent block of
every publisher.

Three limits, at three distances from the failure:

1. **Publisher admission control** (`PublishGuard`, on the worker) — the one
   that normally does the work. Before publishing, the producer estimates what
   its destination queue is already holding and waits while that is over the
   high mark. Costs no round trip while the queue is shallow.
2. **A broker-side policy** (`max-length-bytes` + `overflow=reject-publish`) —
   the backstop for when the estimate is wrong or a producer runs unguarded.
   Applied as a POLICY, never as declare-time queue arguments: arguments have to
   match on every declarer, and this pipeline's workers declare their queues
   before the server ever dispatches them anything, so a change here would
   otherwise reach them as `PRECONDITION_FAILED` on a channel they need.
   `reject-publish` and not the `drop-head` default because drop-head deletes
   the OLDEST batch to make room, silently, and a run that quietly loses units
   still prints a throughput number — a wrong one, indistinguishable from a
   right one.
3. **The broker's own high watermark** (`rabbitmqctl set_vm_memory_high_watermark
   absolute`) — the hard ceiling. Opt-in, because it is a global change to a host
   shared with whatever else uses that broker.

Layer 3 is the only one that literally caps the broker's memory, and it is also
the one whose enforcement is invisible (a blocked publisher looks like a slow
consumer). Layers 1 and 2 exist to keep the run far enough away that layer 3
never fires — and every time they act, they say so in a log.
"""
import json
import threading
import time

import requests
from requests.auth import HTTPBasicAuth

import src.Log

try:
    import paramiko
except Exception:                       # watermark enforcement degrades to a
    paramiko = None                     # printed command; nothing else changes

MB = 1e6                                # decimal MB throughout, so a payload size
                                        # from guide/12 and a memory budget here
                                        # compare without a conversion

POLICY_NAME = "split-inference-guard"


def _token(text):
    """Free text, made safe to put in a log line.

    Three separate hazards, one function. A SPACE splits the value and every
    reader silently discards what follows it (guide/01 §1). An `=` invents a key
    that was never there. And a NON-ASCII character — an em dash out of a
    diagnostic, say — is written in the machine's locale encoding by a plain
    `open(path, "w")`, so the same run produces a different byte for it on a
    Windows controller than on a Linux one, and a UTF-8 reader raises on one of
    them. Sanitising here keeps every log file byte-identical everywhere,
    which is worth more than the dash.
    """
    ascii_text = str(text).encode("ascii", "replace").decode("ascii")
    return "_".join(ascii_text.replace("=", ":").split()) or "none"


# ─────────────────────────────────── budget ──────────────────────────────────

class BrokerBudget:
    """Turns one number a person can reason about — "the broker stays under
    1 GB" — into the per-queue byte caps the mechanism actually enforces.

    Two subtractions stand between the two, and both are the kind of thing that
    is obvious only after it has bitten:

    * **Broker overhead.** The Erlang VM, the management database, and one
      connection per worker cost 150-250 MB on an idle broker. A budget that
      hands the full cap to message payload is over the cap before the first
      batch.
    * **The control plane.** This run ships the model itself through the broker:
      `reply_<client>` carries a base64 copy of the weights to every worker, so
      12 workers put ~90 MB of *control* traffic in the same memory the payload
      budget is measured against. It is transient, it is not on a work queue, and
      no policy here touches it — so it is reserved, not policed.

    What is left is divided evenly across the work queues. Evenly, and not by
    observed load: the division has to hold when a split moves and a queue that
    carried nothing last run carries everything this one.
    """

    def __init__(self, cap_mb, overhead_mb, control_reserve_mb, queues,
                 high_frac, resume_frac):
        self.cap_mb = float(cap_mb)
        self.overhead_mb = float(overhead_mb)
        self.control_reserve_mb = float(control_reserve_mb)
        self.queues = list(queues)
        self.payload_mb = max(self.cap_mb - self.overhead_mb - self.control_reserve_mb, 0.0)
        self.per_queue_mb = self.payload_mb / max(len(self.queues), 1)
        self.high_mb = self.per_queue_mb * float(high_frac)
        self.resume_mb = self.per_queue_mb * float(resume_frac)

    @property
    def usable(self):
        """A cap so low that overhead and reserve eat all of it is a
        configuration error, not a tight budget: enforcing it would throttle the
        first message forever."""
        return self.per_queue_mb > 1.0

    def line(self, t_ns, **extra):
        text = (f"{t_ns} GUARD cap_mb={self.cap_mb:.1f} "
                f"overhead_mb={self.overhead_mb:.1f} "
                f"control_reserve_mb={self.control_reserve_mb:.1f} "
                f"payload_budget_mb={self.payload_mb:.1f} "
                f"queues={len(self.queues)} per_queue_mb={self.per_queue_mb:.1f} "
                f"high_mb={self.high_mb:.1f} resume_mb={self.resume_mb:.1f}")
        for key, value in extra.items():
            text += f" {key}={value}"
        return text


# ──────────────────────────── worker side: admission ─────────────────────────

class PublishGuard:
    """Keeps one producer from putting more into a queue than the budget allows.

    Configured **only** from the dispatch message (guide/README invariant 9), so
    the cap cannot drift across twelve machines and leave a run enforcing two
    different limits at once.

    The estimate is deliberately pessimistic: between probes it assumes the
    consumer took *nothing*. Being wrong in that direction costs an early probe;
    being wrong in the other direction costs the thing this module exists to
    prevent.

    Every failure path here degrades to publishing unguarded and warns. A guard
    that raises has converted a memory problem into a dead run, which is worse
    than the memory problem — layer 2 is still underneath it.
    """

    def __init__(self, enabled=False, high_bytes=0, resume_bytes=0,
                 probe_frac=0.5, probe_interval_s=2.0, max_block_s=300.0,
                 poll_s=0.05, confirm=False, cap_bytes=0):
        # `enabled` is what the configuration asked for; `active` is whether
        # throttling is happening right now. They are separate because the guard
        # can switch itself off mid-run (§5), and a guard that stopped reporting
        # when it stopped throttling would delete the only record of WHY nothing
        # was throttled — the summary would be indistinguishable from a run that
        # never came close to the cap.
        self.enabled = bool(enabled) and high_bytes > 0
        self.active = self.enabled
        self.off_reason = "none" if self.enabled else "disabled"
        self.high_bytes = float(high_bytes)
        self.resume_bytes = float(resume_bytes or high_bytes * 0.66)
        self.cap_bytes = float(cap_bytes)
        self.probe_floor = self.high_bytes * float(probe_frac)
        self.probe_interval_s = float(probe_interval_s)
        self.max_block_s = float(max_block_s)
        self.poll_s = float(poll_s)
        self.confirm = bool(confirm)

        self._state = {}                # queue -> [last_count, since_probe, t_probe]
        self._size_est = 0.0            # EWMA of this producer's own message size
        self._pub_channel = None
        self._probe_channel = None
        self._probe_dead = False
        self._probe_failures = 0
        self._oversize_warned = False

        # Counters, reported at shutdown. A run that was throttled and a run that
        # was not look identical in every other file.
        self.events = 0
        self.wait_s = 0.0
        self.max_wait_s = 0.0
        self.published = 0
        self.bytes_total = 0
        self.nacks = 0
        self.peak_bytes = 0.0
        self.gave_up = 0
        self._t0 = None

    # ─────────────────────────────── plumbing ────────────────────────────────

    def bind(self, channel):
        """Take a second channel on the SAME connection for work payloads.

        Publisher confirms are a channel-wide mode, and the worker's main channel
        also carries the completion signal, the utilization report and the RPC
        registration. Turning confirms on there would put a round trip on the
        DONE of every finished batch — measurable, on the hot path, and bought
        for messages that no policy limits anyway.
        """
        if not self.enabled:
            return
        self._t0 = time.monotonic()
        try:
            self._pub_channel = channel.connection.channel()
            if self.confirm:
                self._pub_channel.confirm_delivery()
            src.Log.print_with_color(
                f"[Guard] admission control on: throttle above "
                f"{self.high_bytes / MB:.0f} MB, resume below "
                f"{self.resume_bytes / MB:.0f} MB, confirms="
                f"{'on' if self.confirm else 'off'}", "green")
        except Exception as e:
            # No dedicated channel → publish on the caller's channel, unguarded
            # by confirms. Still throttled, still capped by the policy.
            self._pub_channel = None
            self.confirm = False
            src.Log.print_with_color(
                f"[Guard] no publish channel ({e}) — using the main channel "
                f"without confirms", "yellow")

    def _probe(self, queue):
        """Depth over AMQP, not the management API: no HTTP plugin, no second
        credential, and it answers even on a broker whose management interface
        is off. A passive declare on a missing queue closes the channel it runs
        on, which is the whole reason it runs on its own."""
        if self._probe_dead:
            return None
        try:
            if self._probe_channel is None or not self._probe_channel.is_open:
                self._probe_channel = self._pub_channel.connection.channel() \
                    if self._pub_channel else None
            if self._probe_channel is None:
                return None
            frame = self._probe_channel.queue_declare(queue=queue, passive=True)
            self._probe_failures = 0
            return int(frame.method.message_count)
        except Exception as e:
            self._probe_channel = None
            self._probe_failures += 1
            if self._probe_failures >= 3:
                self._probe_dead = True
                self.off_reason = "probe_dead"
                src.Log.print_with_color(
                    f"[Guard] depth probe failed 3x ({e}) — admission control "
                    f"off, the broker policy is now the only limit", "red")
            return None

    def _record_probe(self, queue, count):
        self._state[queue] = [count, 0, time.monotonic()]

    def _estimate(self, queue, nbytes):
        """Bytes this queue is holding, refreshing from the broker only when the
        pessimistic local count says it might matter."""
        state = self._state.get(queue)
        now = time.monotonic()
        if state is None:
            count = self._probe(queue)
            if count is None:
                return 0.0
            self._record_probe(queue, count)
            state = self._state[queue]
        estimate = (state[0] + state[1]) * self._size_est + nbytes
        if estimate >= self.probe_floor or (now - state[2]) >= self.probe_interval_s:
            count = self._probe(queue)
            if count is None:
                return 0.0
            self._record_probe(queue, count)
            estimate = count * self._size_est + nbytes
        return estimate

    # ──────────────────────────────── publish ────────────────────────────────

    def publish(self, channel, queue, body):
        """Wait for room, publish, and return the seconds spent waiting.

        The wait is returned rather than swallowed because the caller books it
        against free time (guide/10) as a *wait*, not as work. Counting a
        backpressure stall as busy time would make a throttled device look fully
        utilized — the precise opposite of what happened.
        """
        nbytes = len(body)
        self._size_est = nbytes if not self._size_est else \
            0.5 * nbytes + 0.5 * self._size_est

        waited = self._wait_for_room(queue, nbytes) if self.active else 0.0
        self._publish_with_retry(channel, queue, body)

        state = self._state.get(queue)
        if state is not None:
            state[1] += 1                      # one more in flight since the probe
        self.published += 1
        self.bytes_total += nbytes
        return waited

    def _oversized(self, nbytes):
        """One payload bigger than the whole queue's share of the budget.

        Left alone this deadlocks: the estimate is over the high mark before the
        first message and stays there forever, so the producer waits for room
        that a cap this size can never contain. The cap is wrong, not the
        payload — say so once, with both numbers, and stop throttling. The
        broker-side policy still bounds the queue, and the run produces a result
        instead of a hang nobody can explain.
        """
        if nbytes < self.high_bytes or self._oversize_warned:
            return nbytes >= self.high_bytes
        self._oversize_warned = True
        self.active = False
        self.off_reason = "payload_over_cap"
        src.Log.print_with_color(
            f"[Guard] one payload is {nbytes / MB:.1f} MB but this queue's share "
            f"of the budget is only {self.cap_bytes / MB:.1f} MB (pause mark "
            f"{self.high_bytes / MB:.1f} MB) — admission control OFF. Raise "
            f"max_broker_ram_mb or cut the batch size.", "red")
        return True

    def _wait_for_room(self, queue, nbytes):
        if self._oversized(nbytes):
            return 0.0
        estimate = self._estimate(queue, nbytes)
        self.peak_bytes = max(self.peak_bytes, estimate)
        if estimate < self.high_bytes:
            return 0.0

        # The loop below probes the broker on every pass, which is also what
        # keeps this producer's connection alive while it waits: a wait
        # implemented as a bare sleep would sit silent past the heartbeat and be
        # disconnected for idling, in the middle of behaving correctly.
        t0 = time.monotonic()
        self.events += 1
        src.Log.print_with_color(
            f"[Guard] {queue} holds ~{estimate / MB:.0f} MB (high "
            f"{self.high_bytes / MB:.0f} MB) — pausing this producer", "yellow")
        last_note = t0
        while True:
            count = self._probe(queue)
            if count is None:                  # probing broke mid-wait; the
                break                          # policy still bounds the queue
            estimate = count * self._size_est + nbytes
            self.peak_bytes = max(self.peak_bytes, estimate)
            if count == 0:
                # An empty queue always takes one message, whatever the cap says.
                # Without this rule a budget tighter than a single payload turns
                # into a permanent stall rather than an exceeded budget.
                self._record_probe(queue, count)
                break
            if estimate <= self.resume_bytes:
                self._record_probe(queue, count)
                break
            elapsed = time.monotonic() - t0
            if elapsed >= self.max_block_s:
                # Beyond this, "the consumer is slow" and "the consumer is dead"
                # stop being distinguishable from here, and only one of them is
                # fixed by waiting longer. Proceed, loudly: the server's stall
                # detector owns the decision to end a run, not this producer.
                self.gave_up += 1
                src.Log.print_with_color(
                    f"[Guard] {queue} still ~{estimate / MB:.0f} MB after "
                    f"{elapsed:.0f}s — is the next tier alive? publishing anyway",
                    "red")
                break
            if time.monotonic() - last_note >= 10.0:
                last_note = time.monotonic()
                src.Log.print_with_color(
                    f"[Guard] still waiting on {queue}: ~{estimate / MB:.0f} MB, "
                    f"{elapsed:.0f}s", "yellow")
            time.sleep(self.poll_s)

        waited = time.monotonic() - t0
        self.wait_s += waited
        self.max_wait_s = max(self.max_wait_s, waited)
        return waited

    def _publish_with_retry(self, channel, queue, body):
        """One publish, kept loss-free under `reject-publish`.

        Without confirms a rejected publish is discarded and the producer is
        never told — the batch is simply gone, and every downstream number is
        computed as if it had never existed. With confirms the rejection arrives
        as an exception, which is the only form in which it can be retried.
        """
        target = self._pub_channel if (self._pub_channel is not None
                                       and self._pub_channel.is_open) else channel
        deadline = time.monotonic() + self.max_block_s
        while True:
            try:
                target.basic_publish(exchange="", routing_key=queue, body=body)
                return
            except Exception as e:
                name = type(e).__name__
                if name in ("NackError", "UnroutableError"):
                    # The broker's hard cap, hit. Layer 1 should have prevented
                    # this; that it did not is worth a line of its own.
                    self.nacks += 1
                    if time.monotonic() >= deadline:
                        raise RuntimeError(
                            f"{queue} refused {len(body)} bytes for "
                            f"{self.max_block_s:.0f}s: the queue is at its "
                            f"broker cap and the next tier is not consuming") from e
                    src.Log.print_with_color(
                        f"[Guard] {queue} rejected the publish (at its broker "
                        f"cap) — retrying", "red")
                    time.sleep(max(self.poll_s, 0.2))
                    continue
                if target is not channel:
                    # Channel-level failure on our own channel: fall back to the
                    # caller's rather than lose the batch.
                    src.Log.print_with_color(
                        f"[Guard] publish channel failed ({e}) — falling back to "
                        f"the main channel", "yellow")
                    self._pub_channel = None
                    self.confirm = False
                    target = channel
                    continue
                raise

    # ──────────────────────────────── report ─────────────────────────────────

    def report(self, ident=None):
        """Shipped with the utilization report at shutdown, never written to a
        shared file from here — every timestamp in a shared file is the server's
        (invariant 1). Returns None when the guard is off, so a disabled feature
        reports nothing rather than a convincing row of zeros."""
        if not self.enabled:
            return None
        return {
            **(ident or {}),
            "events": self.events,
            "wait_s": round(self.wait_s, 3),
            "max_wait_s": round(self.max_wait_s, 3),
            "published": self.published,
            "bytes": self.bytes_total,
            "nacks": self.nacks,
            "gave_up": self.gave_up,
            "peak_mb": round(self.peak_bytes / MB, 3),
            "high_mb": round(self.high_bytes / MB, 3),
            "cap_mb": round(self.cap_bytes / MB, 3),
            "probe": "dead" if self._probe_dead else "ok",
            "active": "yes" if self.active else "no",
            "off_reason": self.off_reason,
            "span_s": round(time.monotonic() - self._t0, 3) if self._t0 else 0.0,
        }


# ──────────────────────────── server side: enforcement ───────────────────────

class BrokerGuard:
    """Applies the limits, watches whether they held, and reports both.

    The server owns this for the same reason it owns the RAM sampler: it is the
    only component alive for the whole run, and it is the only one that may
    write a shared log.
    """

    def __init__(self, config, address, username, password, virtual_host,
                 series_path, summary_path, ssh_config=None):
        cfg = config or {}
        self.enabled = bool(cfg.get("enabled", False))
        self.address = address
        self.auth = HTTPBasicAuth(username, password)
        self.virtual_host = virtual_host
        self.series_path = series_path
        self.summary_path = summary_path

        queues = cfg.get("work_queues") or ["queue_0", "queue_1", "queue_2"]
        self.budget = BrokerBudget(
            cap_mb=float(cfg.get("max_broker_ram_mb", 1024)),
            overhead_mb=float(cfg.get("broker_overhead_mb", 200)),
            control_reserve_mb=float(cfg.get("control_reserve_mb", 150)),
            queues=queues,
            high_frac=float(cfg.get("high_frac", 0.75)),
            resume_frac=float(cfg.get("resume_frac", 0.50)))

        self.apply_policy = bool(cfg.get("apply_policy", True))
        self.keep_policy = bool(cfg.get("keep_policy", False))
        self.confirm_publish = bool(cfg.get("confirm_publish", True))
        self.poll_interval_s = float(cfg.get("poll_interval_s", 2.0))
        self.max_block_s = float(cfg.get("max_block_s", 300.0))
        self.probe_frac = float(cfg.get("probe_frac", 0.5))
        self.probe_interval_s = float(cfg.get("probe_interval_s", 2.0))
        self.poll_s = float(cfg.get("resume_poll_s", 0.05))

        self.enforce_watermark = bool(cfg.get("enforce_watermark", False))
        self.restore_watermark = bool(cfg.get("restore_watermark", True))
        ssh = cfg.get("ssh") or ssh_config or {}
        self.ssh_host = ssh.get("host") or address
        self.ssh_port = int(ssh.get("port", 22))
        self.ssh_user = ssh.get("username")
        self.ssh_pass = ssh.get("password")

        self._policy_applied = False
        self._watermark_set = None       # bytes we set, or None
        self._watermark_prev = None      # bytes observed before we touched it
        self._mem_limit_mb = None
        self._mem_start_mb = None
        self._mem_peak_mb = 0.0
        self._alarms = 0
        self._alarm_s = 0.0
        self._alarm_since = None
        self._blocked_peak = 0
        self._polls = 0                  # answered polls; 0 means "we never saw it"
        self._queue_peak = {}            # name -> [peak_bytes, peak_msgs, overs]
        self._over = set()
        self._stop = threading.Event()
        self._thread = None
        self._notes = []                 # human-readable reasons, for the summary

    # ─────────────────────────────── dispatch ────────────────────────────────

    def dispatch(self):
        """What travels to the workers. One home for the configuration: no
        worker reads a byte of this from its own copy of config.yaml."""
        return {
            "enabled": bool(self.enabled and self.budget.usable),
            "high_bytes": self.budget.high_mb * MB,
            "resume_bytes": self.budget.resume_mb * MB,
            "cap_bytes": self.budget.per_queue_mb * MB,
            "probe_frac": self.probe_frac,
            "probe_interval_s": self.probe_interval_s,
            "max_block_s": self.max_block_s,
            "poll_s": self.poll_s,
            # Confirms only earn their round trip where a rejection is possible.
            "confirm": bool(self.confirm_publish and self.apply_policy
                            and self._policy_applied),
        }

    # ────────────────────────────── application ──────────────────────────────

    def apply(self):
        """Preflight, then set the limits. Never raises."""
        if not self.enabled:
            return
        t_ns = time.time_ns()
        try:
            if not self.budget.usable:
                self._note(f"cap {self.budget.cap_mb:.0f} MB leaves "
                           f"{self.budget.payload_mb:.0f} MB for payload — too "
                           f"little to enforce; guard disabled")
                self.enabled = False
                self._emit(self.budget.line(t_ns, status="unusable"))
                src.Log.print_with_color(f"[Guard] {self._notes[-1]}", "red")
                return
            self._preflight(t_ns)
            if self.apply_policy:
                self._apply_policy(t_ns)
            if self.enforce_watermark:
                self._apply_watermark(t_ns)
            else:
                # Say the exact command. The alternative is a user who wants the
                # hard ceiling and has to go find out how to ask for it.
                cap_bytes = int(self.budget.cap_mb * MB)
                self._emit(f"{t_ns} WATERMARK status=skipped "
                           f"reason=enforce_watermark_false "
                           f"command=rabbitmqctl_set_vm_memory_high_watermark_absolute_{cap_bytes}")
            self._emit(self.budget.line(
                t_ns, policy="on" if self._policy_applied else "off",
                watermark=("set" if self._watermark_set else "unchanged"),
                confirms="on" if self.dispatch()["confirm"] else "off"))
            self._thread = threading.Thread(target=self._monitor, daemon=True)
            self._thread.start()
        except Exception as e:
            self._note(f"apply failed: {e}")
            src.Log.print_with_color(f"[Guard] apply failed: {e}", "yellow")

    def _preflight(self, t_ns):
        """What the broker thinks its own ceiling is, before we change anything.

        A cap of 1 GB against a broker whose watermark sits at 40% of a 16 GB
        host means the queue policy is doing all the work and nothing at all
        stops a *different* client of the same broker from blowing past it. That
        is a fine state to run in; it is not a fine state to run in unknowingly.
        """
        nodes = self._get("/api/nodes")
        if not nodes:
            self._note("management API unreachable at start — policy and "
                       "monitoring unavailable")
            return
        self._mem_limit_mb = sum(float(n.get("mem_limit") or 0) for n in nodes) / MB
        self._mem_start_mb = sum(float(n.get("mem_used") or 0) for n in nodes) / MB
        self._mem_peak_mb = self._mem_start_mb
        self._watermark_prev = int(sum(float(n.get("mem_limit") or 0) for n in nodes))
        self._emit(f"{t_ns} BROKER node_watermark_mb={self._mem_limit_mb:.1f} "
                   f"mem_used_start_mb={self._mem_start_mb:.1f} "
                   f"target_cap_mb={self.budget.cap_mb:.1f}")
        if self._mem_start_mb > self.budget.cap_mb:
            self._note(f"broker was ALREADY at {self._mem_start_mb:.0f} MB before "
                       f"the run, over the {self.budget.cap_mb:.0f} MB cap")
            src.Log.print_with_color(f"[Guard] {self._notes[-1]}", "red")
        if self._mem_limit_mb > self.budget.cap_mb and not self.enforce_watermark:
            self._note(f"the broker's own watermark is "
                       f"{self._mem_limit_mb:.0f} MB, above the "
                       f"{self.budget.cap_mb:.0f} MB cap — this run is bounded by "
                       f"the queue policy and the publisher guard, not by the broker")

    def _apply_policy(self, t_ns):
        """`max-length-bytes` per work queue, as a policy.

        A policy applies to a queue however it was declared, which is what makes
        it usable here at all: the workers declare these queues themselves, with
        no arguments, before the server has told them anything.
        """
        pattern = "^(" + "|".join(q.replace(".", r"\.") for q in self.budget.queues) + ")$"
        body = {
            "pattern": pattern,
            "apply-to": "queues",
            "priority": 10,
            "definition": {
                "max-length-bytes": int(self.budget.per_queue_mb * MB),
                "overflow": "reject-publish",
            },
        }
        vhost = requests.utils.quote(self.virtual_host, safe="")
        url = f"http://{self.address}:15672/api/policies/{vhost}/{POLICY_NAME}"
        try:
            response = requests.put(url, auth=self.auth, timeout=5,
                                    headers={"content-type": "application/json"},
                                    data=json.dumps(body))
            if response.status_code in (201, 204):
                self._policy_applied = True
                self._emit(f"{t_ns} POLICY status=applied name={POLICY_NAME} "
                           f"pattern={pattern} "
                           f"max_length_bytes={int(self.budget.per_queue_mb * MB)} "
                           f"overflow=reject-publish")
                src.Log.print_with_color(
                    f"[Guard] policy {POLICY_NAME}: each of "
                    f"{', '.join(self.budget.queues)} capped at "
                    f"{self.budget.per_queue_mb:.0f} MB, overflow rejected "
                    f"(never dropped)", "green")
            else:
                self._note(f"policy rejected: HTTP {response.status_code} "
                           f"{response.text[:120]} — the user likely lacks the "
                           f"policymaker tag")
                self._emit(f"{t_ns} POLICY status=failed "
                           f"http={response.status_code}")
                src.Log.print_with_color(f"[Guard] {self._notes[-1]}", "red")
        except Exception as e:
            self._note(f"policy could not be applied ({e})")
            self._emit(f"{t_ns} POLICY status=failed reason={type(e).__name__}")
            src.Log.print_with_color(f"[Guard] {self._notes[-1]}", "red")

    def _ssh_run(self, command):
        """rabbitmqctl, over the host login. Returns (ok, output)."""
        if paramiko is None or not self.ssh_user:
            return False, ("paramiko not installed" if paramiko is None
                           else "no ssh username configured")
        client = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(self.ssh_host, port=self.ssh_port, username=self.ssh_user,
                           password=self.ssh_pass, timeout=10, banner_timeout=10,
                           auth_timeout=10)
            # rabbitmqctl needs the broker's erlang cookie: readable by root and
            # by the rabbitmq user, and by almost nobody else.
            _in, out, err = client.exec_command(
                f"{command} 2>&1 || sudo -n {command} 2>&1", timeout=20)
            output = out.read().decode(errors="replace").strip()
            status = out.channel.recv_exit_status()
            return status == 0, output or err.read().decode(errors="replace").strip()
        except Exception as e:
            return False, str(e)
        finally:
            try:
                if client is not None:
                    client.close()
            except Exception:
                pass

    def _apply_watermark(self, t_ns):
        """The hard ceiling: the broker stops accepting publishes rather than
        grow past it. Runtime-set and therefore NOT persistent — a broker restart
        returns it to whatever the config file says, which is the right default
        for a change made by a measurement run."""
        cap_bytes = int(self.budget.cap_mb * MB)
        ok, output = self._ssh_run(
            f"rabbitmqctl set_vm_memory_high_watermark absolute {cap_bytes}")
        if ok:
            self._watermark_set = cap_bytes
            self._emit(f"{t_ns} WATERMARK status=set absolute_bytes={cap_bytes} "
                       f"previous_bytes={self._watermark_prev} host={self.ssh_host}")
            src.Log.print_with_color(
                f"[Guard] broker watermark set to {self.budget.cap_mb:.0f} MB "
                f"(was {(self._watermark_prev or 0) / MB:.0f} MB); reverts on "
                f"broker restart", "green")
        else:
            self._note(f"watermark NOT set ({output[:160]}) — run "
                       f"`rabbitmqctl set_vm_memory_high_watermark absolute "
                       f"{cap_bytes}` on {self.ssh_host} for the hard ceiling")
            self._emit(f"{t_ns} WATERMARK status=failed host={self.ssh_host}")
            src.Log.print_with_color(f"[Guard] {self._notes[-1]}", "yellow")

    # ─────────────────────────────── monitoring ──────────────────────────────

    def _monitor(self):
        """Watch the two things that are invisible from a worker: the broker's
        memory alarm, and connections in the `blocked` state.

        Writes on CHANGE, not per poll. The per-sample series is
        [11](../guide/11-broker-ram.md)'s job and duplicating it here would give
        two files that disagree by a sampling interval and no way to tell which
        one was right.
        """
        while not self._stop.is_set():
            try:
                self._poll_once()
            except Exception:
                pass                      # a monitor must never end a run
            self._stop.wait(self.poll_interval_s)

    def _poll_once(self):
        t_ns = time.time_ns()
        nodes = self._get("/api/nodes") or []
        if nodes:
            self._polls += 1
            used_mb = sum(float(n.get("mem_used") or 0) for n in nodes) / MB
            self._mem_peak_mb = max(self._mem_peak_mb, used_mb)
            alarm = any(bool(n.get("mem_alarm")) for n in nodes)
            if alarm and self._alarm_since is None:
                # This is THE line worth having. Every worker sees a stall with
                # no local cause; only the broker knows it stopped them.
                self._alarm_since = time.monotonic()
                self._alarms += 1
                self._emit(f"{t_ns} ALARM state=on mem_used_mb={used_mb:.1f} "
                           f"watermark_mb={self._mem_limit_mb or 0:.1f}")
                src.Log.print_with_color(
                    f"[Guard] BROKER MEMORY ALARM at {used_mb:.0f} MB — every "
                    f"publisher is blocked; a stall now has a cause", "red")
            elif not alarm and self._alarm_since is not None:
                self._alarm_s += time.monotonic() - self._alarm_since
                self._alarm_since = None
                self._emit(f"{t_ns} ALARM state=off mem_used_mb={used_mb:.1f}")
                src.Log.print_with_color(
                    f"[Guard] memory alarm cleared at {used_mb:.0f} MB", "green")

        queues = self._get(f"/api/queues/{requests.utils.quote(self.virtual_host, safe='')}"
                           f"?columns=name,messages,message_bytes") or []
        high_bytes = self.budget.high_mb * MB
        for queue in queues:
            name = queue.get("name")
            if name not in self.budget.queues:
                continue
            n_bytes = float(queue.get("message_bytes") or 0)
            n_msgs = int(queue.get("messages") or 0)
            peak = self._queue_peak.setdefault(name, [0.0, 0, 0])
            peak[0] = max(peak[0], n_bytes)
            peak[1] = max(peak[1], n_msgs)
            if n_bytes >= high_bytes and name not in self._over:
                self._over.add(name)
                peak[2] += 1
                self._emit(f"{t_ns} OVER queue={name} mb={n_bytes / MB:.1f} "
                           f"msgs={n_msgs} high_mb={self.budget.high_mb:.1f} "
                           f"cap_mb={self.budget.per_queue_mb:.1f}")
            elif n_bytes < self.budget.resume_mb * MB and name in self._over:
                self._over.discard(name)
                self._emit(f"{t_ns} UNDER queue={name} mb={n_bytes / MB:.1f} "
                           f"msgs={n_msgs}")

        blocked = self._get("/api/connections?columns=state") or []
        n_blocked = sum(1 for c in blocked
                        if c.get("state") in ("blocked", "blocking"))
        if n_blocked > self._blocked_peak:
            self._blocked_peak = n_blocked
            self._emit(f"{t_ns} BLOCKED connections={n_blocked}")

    # ──────────────────────────────── teardown ───────────────────────────────

    def stop(self):
        """Undo what was done to a host we share. Never raises."""
        if not self.enabled:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        if self._alarm_since is not None:
            self._alarm_s += time.monotonic() - self._alarm_since
            self._alarm_since = None
        t_ns = time.time_ns()

        if self._policy_applied and not self.keep_policy:
            try:
                vhost = requests.utils.quote(self.virtual_host, safe="")
                requests.delete(
                    f"http://{self.address}:15672/api/policies/{vhost}/{POLICY_NAME}",
                    auth=self.auth, timeout=5)
                self._emit(f"{t_ns} POLICY status=removed name={POLICY_NAME}")
            except Exception as e:
                self._emit(f"{t_ns} POLICY status=remove_failed "
                           f"reason={type(e).__name__}")
                self._note(f"policy {POLICY_NAME} left in place ({e})")
        elif self._policy_applied:
            self._emit(f"{t_ns} POLICY status=kept name={POLICY_NAME}")

        if self._watermark_set and self.restore_watermark and self._watermark_prev:
            ok, output = self._ssh_run("rabbitmqctl set_vm_memory_high_watermark "
                                       f"absolute {self._watermark_prev}")
            self._emit(f"{t_ns} WATERMARK status="
                       f"{'restored' if ok else 'restore_failed'} "
                       f"absolute_bytes={self._watermark_prev}")
            if not ok:
                self._note(f"watermark left at {self.budget.cap_mb:.0f} MB on "
                           f"{self.ssh_host} ({output[:120]})")

    # ──────────────────────────────── summary ────────────────────────────────

    def write_summary(self, reports=None):
        """broker_guard.log: GUARD / BROKER / QUEUE / THROTTLE. Never raises."""
        if not self.enabled:
            return
        try:
            self._write_summary(reports or [])
        except Exception as e:
            src.Log.print_with_color(f"[Guard] summary failed: {e}", "yellow")

    def _write_summary(self, reports):
        t_ns = time.time_ns()
        lines = [self.budget.line(
            t_ns,
            policy="on" if self._policy_applied else "off",
            watermark=("set" if self._watermark_set else "unchanged"),
            confirms="on" if self.dispatch()["confirm"] else "off",
            overflow="reject-publish" if self._policy_applied else "none")]

        # `within_cap=yes` on a run that observed NOTHING is the one answer this
        # file must never give: an unreachable management API and a broker that
        # stayed comfortably under its cap would then be written identically,
        # and only one of them is evidence.
        observed = self._polls > 0 and self._mem_peak_mb > 0
        headroom = (self.budget.cap_mb - self._mem_peak_mb) if observed else 0.0
        lines.append(
            f"{t_ns} BROKER watermark_mb={self._mem_limit_mb or 0:.1f} "
            f"mem_start_mb={self._mem_start_mb or 0:.1f} "
            f"mem_peak_mb={self._mem_peak_mb:.1f} cap_mb={self.budget.cap_mb:.1f} "
            f"headroom_mb={headroom:.1f} polls={self._polls} "
            f"within_cap={('yes' if self._mem_peak_mb <= self.budget.cap_mb else 'NO') if observed else 'unknown'} "
            f"alarms={self._alarms} alarm_s={self._alarm_s:.1f} "
            f"blocked_peak={self._blocked_peak}")

        for name in self.budget.queues:
            peak = self._queue_peak.get(name)
            if not peak:
                continue                    # never observed → omitted, not zeroed
            lines.append(
                f"{t_ns} QUEUE queue={name} peak_mb={peak[0] / MB:.1f} "
                f"peak_msgs={peak[1]} cap_mb={self.budget.per_queue_mb:.1f} "
                f"high_mb={self.budget.high_mb:.1f} "
                f"peak_pct={(100.0 * peak[0] / MB / self.budget.per_queue_mb if self.budget.per_queue_mb else 0.0):.1f}% "
                f"over_events={peak[2]}")

        throttled = [r for r in reports if r]
        for report in throttled:
            lines.append(
                f"{t_ns} THROTTLE client={report.get('client', 'unknown')} "
                f"role={report.get('role', 'unknown')} "
                f"cluster={report.get('cluster', 'unknown')} "
                f"events={report.get('events', 0)} "
                f"wait_s={float(report.get('wait_s', 0.0)):.3f} "
                f"max_wait_s={float(report.get('max_wait_s', 0.0)):.3f} "
                f"published={report.get('published', 0)} "
                f"mb={float(report.get('bytes', 0)) / MB:.1f} "
                f"nacks={report.get('nacks', 0)} "
                f"gave_up={report.get('gave_up', 0)} "
                f"peak_est_mb={float(report.get('peak_mb', 0.0)):.1f} "
                f"probe={report.get('probe', 'unknown')} "
                # `active=no off=...` is why a producer that was configured to
                # throttle never did. Without it, a guard that switched itself
                # off reads exactly like a run that never needed it.
                f"active={report.get('active', 'unknown')} "
                f"off={_token(report.get('off_reason', 'unknown'))} "
                f"span_s={float(report.get('span_s', 0.0)):.3f}")
        if throttled:
            wait_total = sum(float(r.get("wait_s", 0.0)) for r in throttled)
            span = max([float(r.get("span_s", 0.0)) for r in throttled] + [0.0])
            lines.append(
                f"{t_ns} THROTTLE client=ALL producers={len(throttled)} "
                f"events={sum(int(r.get('events', 0)) for r in throttled)} "
                f"wait_s={wait_total:.3f} "
                f"max_wait_s={max([float(r.get('max_wait_s', 0.0)) for r in throttled] + [0.0]):.3f} "
                f"published={sum(int(r.get('published', 0)) for r in throttled)} "
                f"nacks={sum(int(r.get('nacks', 0)) for r in throttled)} "
                # Share of one producer's run spent waiting on the broker. This
                # is the number that says whether the cap changed the result: at
                # 0% the run is what it would have been uncapped.
                f"throttled_frac={(100.0 * wait_total / (span * len(throttled)) if span else 0.0):.2f}%")

        for note in self._notes:
            lines.append(f"{t_ns} NOTE {_token(note)}")

        with open(self.summary_path, "w") as f:
            f.write("\n".join(l.encode("ascii", "replace").decode() for l in lines)
                    + "\n")
        for line in lines:
            src.Log.print_with_color(f"[Guard] {line}", "cyan")

    # ──────────────────────────────── plumbing ───────────────────────────────

    def _get(self, path):
        try:
            response = requests.get(f"http://{self.address}:15672{path}",
                                    auth=self.auth, timeout=5)
            return response.json() if response.status_code == 200 else None
        except Exception:
            return None

    def _note(self, text):
        self._notes.append(text)

    def _emit(self, line):
        """Written live, like every other series here: a run that dies still
        leaves behind what it did to the broker."""
        try:
            with open(self.series_path, "a") as f:
                f.write(line.encode("ascii", "replace").decode() + "\n")
        except Exception:
            pass
