"""Device-side optional measurements: free time (guide/10) and message size (guide/12).

Both are telemetry, so every failure path in this module degrades to a warning —
a broken metric loses a number, it must not lose the run (guide/README invariant 7).

Both are also *flag-driven from the dispatch message*: nothing here reads
config.yaml. The server decides whether a feature is on and, for message size,
which single worker measures it (guide/README invariant 9).
"""
import os
import socket
import time
from collections import defaultdict

import src.Log as Log

try:
    import psutil
except Exception:                                  # host_idle degrades to None
    psutil = None


# ───────────────────────────── interval algebra ──────────────────────────────
# Free time is defined by the UNION of busy intervals, never their sum: a
# pipelined device runs lanes concurrently, so summing durations can exceed the
# wall clock outright and silently misses the gaps between stages, which is
# precisely where free time lives (guide/10 §1).

def merge_intervals(intervals):
    """Sorted, disjoint union of [start, end) pairs."""
    if not intervals:
        return []
    ordered = sorted(intervals)
    out = [list(ordered[0])]
    for start, end in ordered[1:]:
        if start <= out[-1][1]:
            if end > out[-1][1]:
                out[-1][1] = end
        else:
            out.append([start, end])
    return out


def total_length(intervals):
    return sum(end - start for start, end in intervals)


def clip_intervals(intervals, lo, hi):
    """Trim to [lo, hi] and drop anything that falls outside it entirely.
    Without this, a span opened before `start` makes busy exceed the run window
    and `busy + free == span` stops holding."""
    out = []
    for start, end in intervals:
        start, end = max(start, lo), min(end, hi)
        if end > start:
            out.append([start, end])
    return out


def complement(intervals, lo, hi):
    """The gaps: [lo, hi] minus a merged interval list."""
    out, cursor = [], lo
    for start, end in intervals:
        if start > cursor:
            out.append([cursor, start])
        cursor = max(cursor, end)
    if cursor < hi:
        out.append([cursor, hi])
    return out


def intersect(a, b):
    """Intersection of two merged interval lists, by a two-pointer sweep."""
    out, i, j = [], 0, 0
    while i < len(a) and j < len(b):
        lo, hi = max(a[i][0], b[j][0]), min(a[i][1], b[j][1])
        if hi > lo:
            out.append([lo, hi])
        if a[i][1] < b[j][1]:
            i += 1
        else:
            j += 1
    return out


def subtract(a, b):
    """a minus b, both merged."""
    out = []
    for start, end in a:
        cursor = start
        for bs, be in b:
            if be <= cursor or bs >= end:
                continue
            if bs > cursor:
                out.append([cursor, bs])
            cursor = max(cursor, be)
            if cursor >= end:
                break
        if cursor < end:
            out.append([cursor, end])
    return out


def cap_intervals(intervals, max_n):
    """Shrink a merged list to `max_n` entries by closing the SMALLEST gaps first.

    Returns (capped, slop_ns) where slop is the non-busy time swallowed. Biasing
    toward *less* free time is the safe direction, and reporting the slop gives
    the answer its own error bar (guide/10 §4)."""
    if max_n <= 0 or len(intervals) <= max_n:
        return intervals, 0
    gaps = sorted(
        ((intervals[i + 1][0] - intervals[i][1], i) for i in range(len(intervals) - 1)),
        key=lambda g: g[0],
    )
    closed = set(idx for _, idx in gaps[: len(intervals) - max_n])
    slop = sum(size for size, idx in gaps[: len(intervals) - max_n])
    out = [list(intervals[0])]
    for i in range(1, len(intervals)):
        if (i - 1) in closed:
            out[-1][1] = intervals[i][1]
        else:
            out.append(list(intervals[i]))
    return out, slop


# ───────────────────────────── free time (guide/10) ──────────────────────────

class FreeTimeTracker:
    """Records what every lane of this device was doing, so the run's leftover
    wall clock can be reported as free time.

    A **lane** is a thread, identified automatically — a lane that had to be
    named by the caller would rot the moment a stage moved to another thread.
    Work spans define the answer; wait spans only *explain* it, so a moment that
    no wait span covers is still free and lands in `unaccounted`.
    """

    # Overlapping reasons are attributed in this fixed order, each one excluding
    # what busy time and earlier reasons already claimed, so the parts sum to the
    # whole with no double counting. The order is part of the definition.
    REASON_PRIORITY = ("input", "backpressure", "downstream", "stop", "control")

    def __init__(self, enabled=True, bucket_s=1.0, max_intervals=4000,
                 coalesce_ns=1_000_000, merge_every=2000):
        self.enabled = bool(enabled)
        self.bucket_s = float(bucket_s or 1.0)
        self.max_intervals = int(max_intervals or 4000)
        self.coalesce_ns = int(coalesce_ns)
        self.merge_every = int(merge_every)

        self._merged = []                       # merged work intervals (mono ns)
        self._pending = []                      # work spans not yet folded in
        self._waits = defaultdict(list)         # reason -> [[start, end], ...]
        self._open_wait = {}                    # (lane, reason) -> index of open span
        self._kind_ns = defaultdict(int)        # per-kind sums; these OVERLAP
        self._lane_ns = defaultdict(int)
        self._n_spans = 0

        self._t0 = self._t1 = None              # monotonic run window
        self._epoch0 = None                     # epoch at _t0, for the machine union
        self._cpu0 = None

    # A monotonic clock, so a wall-clock step mid-run can never produce a
    # negative interval. Converted to epoch only on export.
    @staticmethod
    def now():
        return time.perf_counter_ns()

    def start(self):
        if not self.enabled:
            return
        self._t0 = self.now()
        self._epoch0 = time.time_ns()
        if psutil is not None:
            try:
                self._cpu0 = psutil.cpu_times()
            except Exception:
                self._cpu0 = None

    def stop(self):
        if self.enabled and self._t1 is None:
            self._t1 = self.now()

    def add_work(self, kind, t0, t1=None):
        """A real operation: read input, compute, encode, send, receive, decode,
        postprocess, bookkeeping. Never coalesced — busy has to stay exact."""
        if not self.enabled or self._t0 is None:
            return
        t1 = self.now() if t1 is None else t1
        if t1 <= t0:
            return
        self._pending.append([t0, t1])
        self._kind_ns[kind] += t1 - t0
        self._lane_ns[self._lane()] += t1 - t0
        self._bump()

    def add_wait(self, reason, t0, t1=None):
        """A block: waiting for input, a full downstream queue, a back-pressure
        stall, an idle poll. Coalesced across a sub-millisecond gap, or a 2 ms
        poll loop produces ~500 intervals per idle second."""
        if not self.enabled or self._t0 is None:
            return
        t1 = self.now() if t1 is None else t1
        if t1 <= t0:
            return
        key = (self._lane(), reason)
        spans = self._waits[reason]
        idx = self._open_wait.get(key)
        if idx is not None and idx < len(spans) and t0 - spans[idx][1] <= self.coalesce_ns:
            spans[idx][1] = max(spans[idx][1], t1)
        else:
            spans.append([t0, t1])
            self._open_wait[key] = len(spans) - 1
        self._bump()

    def _lane(self):
        import threading
        return threading.get_ident()

    def _bump(self):
        self._n_spans += 1
        if len(self._pending) >= self.merge_every:
            self._fold()

    def _fold(self):
        """Fold pending work into the merged list, so memory is bounded by the
        number of DISJOINT busy periods rather than by operations recorded."""
        if self._pending:
            self._merged = merge_intervals(self._merged + self._pending)
            self._pending = []

    def _host_idle(self):
        """OS-level idle share of the whole machine over the run, across every
        process — not just ours. Answers a different question from pipeline free
        time, and the two disagreeing is itself informative (guide/10 §4)."""
        if psutil is None or self._cpu0 is None:
            return None
        try:
            now = psutil.cpu_times()
            fields = [f for f in now._fields if hasattr(self._cpu0, f)]
            deltas = {f: getattr(now, f) - getattr(self._cpu0, f) for f in fields}
            total = sum(v for v in deltas.values() if v > 0)
            idle = max(deltas.get("idle", 0.0), 0.0)
            return 100.0 * idle / total if total > 0 else None
        except Exception:
            return None

    def finish(self):
        """One report for the whole run, or None when the tracker is off.

        A disabled tracker reports NOTHING rather than reporting zeros — zeros
        would read as a device that was never idle."""
        if not self.enabled or self._t0 is None:
            return None
        try:
            return self._finish()
        except Exception as e:                     # telemetry never kills the run
            Log.print_with_color(f"[FreeTime] report failed: {e}", "yellow")
            return None

    def _finish(self):
        self.stop()
        self._fold()
        t0, t1 = self._t0, self._t1
        span_ns = max(t1 - t0, 0)
        if span_ns <= 0:
            return None

        busy = clip_intervals(self._merged, t0, t1)
        busy = merge_intervals(busy)
        busy_ns = total_length(busy)
        free = complement(busy, t0, t1)            # busy_ns + free_ns == span_ns, exactly
        free_ns = total_length(free)

        # Priority-ordered attribution: each reason claims only what busy time
        # and earlier reasons left, so the shares sum to free_ns exactly and
        # nothing is counted twice. Whatever no reason covers is `unaccounted`.
        remaining = free
        reasons = {}
        ordered = list(self.REASON_PRIORITY) + sorted(
            set(self._waits) - set(self.REASON_PRIORITY))
        for reason in ordered:
            if not self._waits.get(reason) or not remaining:
                continue
            claimed = intersect(merge_intervals(
                clip_intervals(self._waits[reason], t0, t1)), remaining)
            if claimed:
                reasons[reason] = total_length(claimed)
                remaining = subtract(remaining, claimed)
        leftover = total_length(remaining)
        if leftover > 0:
            reasons["unaccounted"] = leftover

        # Buckets: percent of each fixed-width slice the device spent doing
        # nothing. bucket_s travels on every line, so a long run may widen it.
        bucket_ns = max(int(self.bucket_s * 1e9), 1)
        n_buckets = max(int((span_ns + bucket_ns - 1) // bucket_ns), 1)
        series = []
        for i in range(n_buckets):
            lo = t0 + i * bucket_ns
            hi = min(lo + bucket_ns, t1)
            if hi <= lo:
                break
            width = hi - lo
            busy_here = total_length(intersect(busy, [[lo, hi]]))
            series.append((i, (lo - t0) / 1e9, width / 1e9,
                           100.0 * (width - busy_here) / width))

        # Merged busy intervals in EPOCH ns, so the server can union the device
        # processes sharing one host (guide/10 §4). Valid only within a machine.
        capped, slop_ns = cap_intervals(busy, self.max_intervals)
        epoch_busy = [[self._epoch0 + (s - t0), self._epoch0 + (e - t0)]
                      for s, e in capped]

        return {
            "span_ns": span_ns,
            "busy_ns": busy_ns,
            "free_ns": free_ns,
            "gaps": len(free),
            "longest_free_ns": max((e - s for s, e in free), default=0),
            "reasons_ns": dict(reasons),
            "kinds_ns": dict(self._kind_ns),
            "lanes_ns": {str(k): v for k, v in self._lane_ns.items()},
            "busy_epoch_ns": epoch_busy,
            "merge_slop_ns": slop_ns,
            "series": series,
            "bucket_s": bucket_ns / 1e9,
            "host_idle": self._host_idle(),
            "machine": socket.gethostname(),
            "epoch_start_ns": self._epoch0,
            "epoch_end_ns": self._epoch0 + span_ns,
        }


# ─────────────────────────── message size (guide/12) ─────────────────────────

class MessageSizeRecorder:
    """Records the size of every payload this worker hands to the transport.

    Exactly one worker in a run has `enabled=True`, and the SERVER decides which
    (the first worker registered at the first stage) and says so in the dispatch
    message. A worker that read "am I the one?" from its own config would be one
    stale file away from every edge measuring, or none — and the summary line
    looks identical either way (guide/12 §1).
    """

    def __init__(self, enabled=False, path="message_size_local.log", max_series=2000):
        self.enabled = bool(enabled)
        self.path = path
        self.max_series = int(max_series or 2000)
        self._samples = []            # (t_offset_s, batch_id, n_bytes) — ALL of them
        self._t0_ns = None
        self._file_ok = self.enabled
        if self.enabled:
            try:
                open(self.path, "w").close()       # never mix two runs
            except Exception as e:
                self._warn_once(f"cannot truncate {self.path}: {e}")

    def _warn_once(self, msg):
        """One warning, then the recorder stays quiet — a per-message warning on
        a 500-batch run buries the console (guide/12 §6.7)."""
        if self._file_ok:
            Log.print_with_color(f"[MsgSize] {msg} — recorder disabled", "yellow")
        self._file_ok = False

    def record(self, n_bytes, batch_id):
        """Call this BEFORE the publish, never after. Both orderings look
        equivalent until the transport blocks — and a broker at its high-water
        mark is exactly the run this measurement exists to explain."""
        if not self.enabled:
            return
        now = time.time_ns()
        if self._t0_ns is None:
            self._t0_ns = now
        offset_s = (now - self._t0_ns) / 1e9
        self._samples.append((offset_s, int(batch_id), int(n_bytes)))
        if self._file_ok:
            try:
                with open(self.path, "a") as f:    # live: a run that dies keeps its samples
                    f.write(f"{now} i={len(self._samples) - 1} t_offset_s={offset_s:.3f} "
                            f"batch_id={batch_id} bytes={int(n_bytes)} "
                            f"mb={n_bytes / 1e6:.3f}\n")
            except Exception as e:
                self._warn_once(f"write failed: {e}")

    @staticmethod
    def _percentile(sorted_samples, q):
        """Nearest-rank, no interpolation — same rule as latency, so every size
        printed is one that actually occurred."""
        import math
        if not sorted_samples:
            return 0.0
        k = max(1, math.ceil(q / 100.0 * len(sorted_samples)))
        return sorted_samples[k - 1]

    def report(self, context):
        """Summary over EVERY sample, plus an evenly decimated series.
        Decimation may coarsen a plot; it must never coarsen a number."""
        if not self.enabled or not self._samples:
            return None
        sizes = sorted(s[2] for s in self._samples)
        n = len(sizes)
        total = sum(sizes)
        span_s = self._samples[-1][0] - self._samples[0][0]

        # Decimate EVENLY rather than truncating, so the series still spans the
        # whole run instead of stopping partway through it.
        step = max(1, (n + self.max_series - 1) // self.max_series)
        series = self._samples[::step]

        return {
            "n": n,
            "total_bytes": total,
            "mean_bytes": total / n,
            "p50_bytes": self._percentile(sizes, 50),
            "p95_bytes": self._percentile(sizes, 95),
            "max_bytes": sizes[-1],
            "min_bytes": sizes[0],
            "span_s": span_s,
            "series": series,
            **context,
        }
