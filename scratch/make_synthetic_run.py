"""Generate a synthetic run directory by driving the REAL emitters.

This exists to exercise Phases 6-8 of guide/09-port-checklist.md without a
RabbitMQ cluster: `validate_results.py` needs a run directory, the notebook needs
one, and there is no point proving that a hand-written log file is conformant.

So nothing here writes a log line itself. It fabricates a plausible run — arrival
times, per-device timing, payload sizes, host memory — and pushes it through
`Server.on_fps`, `Server._write_group_rate`, `_write_utilization_group`,
`_write_latency_group`, `_write_free_time`, `_write_message_size`, the real
`FreeTimeTracker` / `MessageSizeRecorder`, and `BrokerRamSampler.write_summary`.
What lands on disk is what a real run would land, so if the validator passes here
it passes on the pipeline.

    python scratch/make_synthetic_run.py [--out results/results_0101_1200_synthetic]

The output is a SYNTHETIC run and must never be committed as a result.
"""
import argparse
import random
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# src.Server pulls in the broker client and the model runtime at import time, and
# neither is needed to exercise the emitters — `Server.__init__` is never called
# here. Stub whatever will not import so this script runs on a plain checkout
# with no RabbitMQ and no working CUDA. A stub that shadowed a package which
# imports FINE would be a trap, so the real one is tried first and kept.
for _name, _attrs in (("pika", ("PlainCredentials", "BlockingConnection",
                                "ConnectionParameters")),
                      ("ultralytics", ("YOLO",))):
    try:
        __import__(_name)
    except Exception:            # ImportError, or a broken native dependency
        _stub = types.ModuleType(_name)
        for _attr in _attrs:
            setattr(_stub, _attr, object)
        sys.modules[_name] = _stub

import src.Log                                        # noqa: E402
from src.BrokerRam import BrokerRamSampler            # noqa: E402
from src.Measure import FreeTimeTracker, MessageSizeRecorder   # noqa: E402
from src.Server import Server                         # noqa: E402

BATCH_SIZE = 32
START_EPOCH_NS = 1_785_000_000_000_000_000
RNG = random.Random(20260811)


class VirtualClock:
    """Stands in for the `time` module inside src.Server, so a run that takes
    ten minutes of wall clock can be generated in a second while every timestamp
    the emitters write stays realistic."""

    def __init__(self, epoch_ns):
        self.epoch_ns = epoch_ns

    def time_ns(self):
        return int(self.epoch_ns)

    def time(self):
        return self.epoch_ns / 1e9

    def sleep(self, _seconds):
        pass

    def strftime(self, fmt):
        return "0000"


class VirtualTracker(FreeTimeTracker):
    """FreeTimeTracker on a virtual monotonic clock. Every merge, attribution
    and bucketing path below is the real one — only `now()` is fabricated."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.t = 0

    def now(self):
        return self.t


# ─────────────────────────── the fabricated fleet ────────────────────────────
# Two clusters, so the group roll-ups and the `share` column have something to
# say. machine-3 hosts TWO device processes, which is what makes the MACHINE
# union line in free_time_group.log a real test: two devices that are each half
# free can keep one machine fully busy by interleaving.
DEVICES = [
    # (client_id, role, cluster,   machine,     device, rel_start_s, span_s)
    (1, "edge",  "queue_2", "machine-1", "cpu",   0.0, 560.0),
    (2, "edge",  "queue_2", "machine-2", "cpu",   1.4, 558.0),
    (3, "edge",  "queue_5", "machine-3", "cpu",   0.8, 549.0),
    (4, "cloud", "queue_2", "machine-4", "cuda",  2.1, 604.0),
    (5, "cloud", "queue_2", "machine-5", "cuda",  2.6, 602.0),
    (6, "cloud", "queue_5", "machine-3", "cuda",  1.9, 601.0),
    (7, "cloud", "queue_5", "machine-6", "cuda",  3.0, 599.0),
]
CLUSTER_SHARE = {"queue_2": 0.62, "queue_5": 0.38}
N_BATCHES = 214


def build_server(run_dir):
    """A Server with every attribute the emitters touch, and no broker."""
    server = object.__new__(Server)
    server.log_path = str(run_dir)
    server.batch_log_path        = str(run_dir / "batch_done_ns.log")
    server.group_rate_ns_path    = str(run_dir / "group_rate_ns.log")
    server.group_rate_path       = str(run_dir / "group_rate.log")
    server.util_log_path         = str(run_dir / "utilization.log")
    server.util_group_path       = str(run_dir / "utilization_group.log")
    server.latency_group_path    = str(run_dir / "latency_group.log")
    server.events_path           = str(run_dir / "events_ns.log")
    server.free_time_path        = str(run_dir / "free_time.log")
    server.free_time_group_path  = str(run_dir / "free_time_group.log")
    server.free_time_series_path = str(run_dir / "free_time_series.log")
    server.broker_ram_ns_path    = str(run_dir / "broker_ram_ns.log")
    server.broker_ram_path       = str(run_dir / "broker_ram.log")
    server.msg_size_path         = str(run_dir / "message_size.log")
    server.msg_size_series_path  = str(run_dir / "message_size_series.log")
    server.result_files = [
        server.batch_log_path, server.group_rate_ns_path, server.group_rate_path,
        server.util_log_path, server.util_group_path, server.latency_group_path,
        server.events_path, server.free_time_path, server.free_time_group_path,
        server.free_time_series_path, server.broker_ram_ns_path,
        server.broker_ram_path, server.msg_size_path, server.msg_size_series_path,
    ]
    for path in server.result_files:               # truncate, as a real run does
        open(path, "w").close()

    server.batch_size = BATCH_SIZE
    server._fps_times, server._group_times = [], {}
    server._fps_window = 16
    server._fps_start_t = START_EPOCH_NS / 1e9
    server._free_time_enabled = True
    server._msg_size_enabled = True
    return server


class _Ack:
    delivery_tag = 0


class _Channel:
    def basic_ack(self, delivery_tag=None):
        pass


def drive_arrivals(server, clock):
    """Push every completion through the real `on_fps`, which writes both live
    series and computes the rolling window exactly as it does in production.

    Arrivals are bursty on purpose — that is what makes total-items/total-time
    differ from the mean of per-gap rates, and the summary prints both."""
    order = []
    for cluster, share in CLUSTER_SHARE.items():
        order += [cluster] * int(round(N_BATCHES * share))
    RNG.shuffle(order)

    t_ns = START_EPOCH_NS + int(18.0 * 1e9)          # pipeline fill-up before #1
    channel, method = _Channel(), _Ack()
    for i, cluster in enumerate(order):
        base = 2.35 if cluster == "queue_2" else 3.9
        gap = base * RNG.uniform(0.45, 1.7)
        if RNG.random() < 0.05:                      # occasional stall
            gap += RNG.uniform(3.0, 9.0)
        t_ns += int(gap * 1e9)
        clock.epoch_ns = t_ns
        server.on_fps(channel, method, None, cluster.encode())
        if i == 40:
            server._log_event("queue_2", "cut 11 (pdd)")
        if i == 150:
            server._log_event("queue_5", "cut 11->13 deeper")
    return t_ns


def free_time_report(rel_start_s, span_s, busy_fraction, host_idle, machine):
    """A real FreeTimeTracker report, built from fabricated overlapping lanes.

    Two lanes are used deliberately: their per-kind sums overlap, so the merged
    busy total is strictly smaller than the sum of the timers — which is the one
    property that proves the merge is a merge (guide/10 §7)."""
    span_ns = int(span_s * 1e9)
    tracker = VirtualTracker(enabled=True, bucket_s=10.0, max_intervals=4000)
    tracker.t = 0
    tracker.start()
    tracker._epoch0 = START_EPOCH_NS + int(rel_start_s * 1e9)
    tracker._cpu0 = None                             # host_idle stamped below

    cursor = 0
    while cursor < span_ns:
        work = max(min(int(RNG.uniform(0.6, 1.6) * 3.0e9), span_ns - cursor), 1)
        # Lane A computes; lane B overlaps the tail of the SAME chunk with the
        # transfer. Staying inside the chunk is what keeps the intended busy
        # fraction honest while still making the two lanes genuinely overlap.
        tracker.add_work("inference", cursor, cursor + work)
        tracker.add_work("send", cursor + int(work * 0.45), cursor + work)
        cursor += work
        idle = int(work * (1.0 - busy_fraction) / max(busy_fraction, 0.05)
                   * RNG.uniform(0.4, 1.8))
        idle = min(idle, span_ns - cursor)
        if idle > 0:
            reason = "input" if RNG.random() < 0.85 else "backpressure"
            # A sliver at the head of each gap is left unattributed on purpose,
            # so `unaccounted` is exercised rather than always being zero.
            tracker.add_wait(reason, cursor + int(idle * 0.06), cursor + idle)
        cursor += max(idle, 0)
    tracker.t = span_ns
    tracker.stop()
    report = tracker.finish()
    report["host_idle"] = host_idle
    # In production this is socket.gethostname() on the device itself; here the
    # fleet is fabricated, so the fabricated host has to be stamped on — without
    # it every synthetic device claims THIS machine and the MACHINE union
    # collapses to a single meaningless line.
    report["machine"] = machine
    return report


def message_size_report(cluster, machine, n_messages, span_s):
    """A real MessageSizeRecorder report. Sizes vary with scene content, which is
    what makes p95/p50 spread meaningful rather than noise."""
    recorder = MessageSizeRecorder(enabled=True, max_series=2000,
                                   path=str(Path(ROOT) / "scratch" /
                                            "_synthetic_message_size.log"))
    for batch_id in range(n_messages):
        recorder.record(int(RNG.gauss(39_000_000, 620_000)), batch_id)
    # The recorder stamps offsets from its own first publish using the real
    # clock, so rewrite them onto the fabricated timeline.
    recorder._samples = [(i * span_s / max(n_messages - 1, 1), b, n)
                         for i, (_, b, n) in enumerate(recorder._samples)]
    return recorder.report({
        "client": "1", "role": "edge", "machine": machine, "cluster": cluster,
        "mode": "split", "compress": "on", "num_bit": 8,
        # Space-free, exactly as Scheduler._kv_safe renders the (11, 15) tuple:
        # a `key=value` value with a space in it is silently truncated by every
        # reader of the universal grammar (01 §1).
        "batch_size": BATCH_SIZE, "splits": "11-15",
    })


def build_reports(total_done):
    """One UTILIZATION report per device — the same dict a worker publishes."""
    per_cluster = {}
    for cluster, share in CLUSTER_SHARE.items():
        per_cluster[cluster] = int(round(total_done * share))

    reports = []
    for client_id, role, cluster, machine, device, rel_start, span_s in DEVICES:
        peers = [d for d in DEVICES if d[1] == role and d[2] == cluster]
        packages = max(per_cluster[cluster] // len(peers), 1)

        # `service` samples ARE the busy intervals utilization sums, so busy_ns
        # is derived from them rather than drawn independently. That identity is
        # the conformance check tying latency_group.log to utilization_group.log
        # (guide/04 §2.1) — faking them separately would fake the check away.
        centre = 2.6 if role == "edge" else 3.4
        service_ms = [max(RNG.gauss(centre * 1000, centre * 175), 90.0)
                      for _ in range(packages)]
        busy_ns = int(sum(service_ms) * 1e6)
        total_ns = int(span_s * 1e9)
        if busy_ns >= total_ns:                     # utilization must stay <= 100%
            scale = 0.93 * total_ns / busy_ns
            service_ms = [s * scale for s in service_ms]
            busy_ns = int(sum(service_ms) * 1e6)

        pipeline_ms = [s + max(RNG.gauss(900, 300), 0.0) for s in service_ms]
        e2e_ms = ([max(RNG.gauss(58_000, 14_000), 1200.0) for _ in range(packages)]
                  if role == "cloud" else [])       # ONLY the completing tier

        busy_fraction = 0.93 if role == "cloud" else 0.42
        report = {
            "action": "UTILIZATION", "client_id": client_id, "layer_id":
                1 if role == "edge" else 2,
            "cluster": cluster, "role": role, "device": device,
            "packages": packages, "busy_ns": busy_ns, "total_ns": total_ns,
            "utilization": busy_ns / total_ns,
            "service_ms": service_ms, "pipeline_ms": pipeline_ms, "e2e_ms": e2e_ms,
            "free_time": free_time_report(rel_start, span_s, busy_fraction,
                                          RNG.uniform(8.0, 61.0), machine),
            "message_size": None,
        }
        reports.append(report)

    # Exactly ONE worker measures payload size: the first registered at tier 1.
    first_edge = next(r for r in reports if r["role"] == "edge")
    first_edge["message_size"] = message_size_report(
        first_edge["cluster"], "machine-1", first_edge["packages"], 545.0)
    return reports


def write_utilization_lines(server, reports, clock, last_ns):
    """utilization.log is normally appended as each report is DRAINED, so the
    leading timestamp is the server-clock arrival — not a device timestamp."""
    lines = []
    for i, report in enumerate(reports):
        clock.epoch_ns = last_ns + int((i + 1) * 0.31 * 1e9)
        lines.append(
            f"{clock.time_ns()} client={report['client_id']} role={report['role']} "
            f"packages={report['packages']} "
            f"busy_s={report['busy_ns'] / 1e9:.3f} "
            f"total_s={report['total_ns'] / 1e9:.3f} "
            f"utilization={report['utilization'] * 100:.2f}%")
    with open(server.util_log_path, "w") as f:
        f.write("\n".join(lines) + "\n")


def write_broker_ram(server, run_dir, last_ns):
    """Drive the real summary writer over a fabricated memory curve: a flat idle
    stretch, a run that climbs as the queues fill, and a tail that gives it back.

    The `run` phase is sized from the ACTUAL last completion, because the window
    has to span the run it describes — a curve that stops early reads on the
    chart as memory being released while throughput carries on, which is a
    conclusion the data never supported."""
    sampler = BrokerRamSampler(
        {"enabled": True, "interval_s": 1.0}, "192.168.101.91", "machine-1",
        "123456", "/", server.broker_ram_ns_path, server.broker_ram_path)
    sampler._source = "ssh"
    total_mb = 5921.5
    idle_s = 31
    run_s = int((last_ns - START_EPOCH_NS) / 1e9) + 4   # past the drain
    t_ns = START_EPOCH_NS - int(idle_s * 1e9)        # window opens BEFORE dispatch
    for phase, count, base, climb in (("idle", idle_s, 1586.0, 0.0),
                                      ("run", run_s, 1601.0, 470.0),
                                      ("tail", 3, 1660.0, -40.0)):
        sampler._phase = phase
        for i in range(count):
            frac = i / max(count - 1, 1)
            used = base + climb * (frac ** 0.6) + RNG.gauss(0, 7.5)
            sampler._append(dict(
                ts_ns=t_ns, source="ssh", total_mb=total_mb, used_mb=used,
                avail_mb=total_mb - used, free_mb=total_mb - used - 740.0,
                cached_mb=747.0 + 30 * frac, swap_used_mb=1032.3,
                rss_mb=87.8 + 390.0 * (frac ** 0.6 if phase == "run" else 0.0)))
            t_ns += int(1.0 * 1e9)
    sampler.write_summary()


def main():
    ap = argparse.ArgumentParser()
    # Lands under scratch/, never in results/. results/<run-id>/ means "a real
    # run happened", and a fabricated directory sitting there is indistinguishable
    # from one months later.
    ap.add_argument("--out", default=str(ROOT / "scratch" / "synthetic" /
                                         "results_0101_1200_synthetic"))
    args = ap.parse_args()

    run_dir = Path(args.out).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    import src.BrokerRam as broker_module
    import src.Server as server_module
    clock = VirtualClock(START_EPOCH_NS)
    real_time, real_print = server_module.time, src.Log.print_with_color
    real_broker_time = broker_module.time
    # Both modules stamp their own `time.time_ns()`, so both need the virtual
    # clock or the RAM summary lands ~a year after the run it summarises.
    server_module.time = clock
    broker_module.time = clock
    src.Log.print_with_color = lambda *a, **k: None      # 200 DONE lines is noise
    try:
        server = build_server(run_dir)
        last_ns = drive_arrivals(server, clock)
        clock.epoch_ns = last_ns + int(0.4 * 1e9)
        server._write_group_rate()

        reports = build_reports(len(server._fps_times))
        write_utilization_lines(server, reports, clock, last_ns)
        clock.epoch_ns = last_ns + int(3.1 * 1e9)
        server._write_utilization_group(reports)
        server._write_latency_group(reports)
        server._write_free_time(reports)
        server._write_message_size(reports)
        write_broker_ram(server, run_dir, last_ns)
    finally:
        server_module.time = real_time
        broker_module.time = real_broker_time
        src.Log.print_with_color = real_print
        scratch_copy = ROOT / "scratch" / "_synthetic_message_size.log"
        if scratch_copy.exists():
            scratch_copy.unlink()

    # Archive the config beside the numbers: without it these are unreadable in
    # a month — you will not remember the batch size or the split point (05 §3).
    import shutil
    shutil.copy2(ROOT / "config.yaml", run_dir / "config.yaml")

    print(f"synthetic run -> {run_dir}\n")
    for path in sorted(run_dir.iterdir()):
        n = sum(1 for _ in open(path, encoding="utf-8", errors="ignore"))
        print(f"  {path.name:<28} {n:>6} line(s)  {path.stat().st_size:>9,} B")
    print(f"\n  {len(server._fps_times)} completions x {BATCH_SIZE} frames "
          f"across {len(server._group_times)} clusters, {len(DEVICES)} devices")
    return 0


if __name__ == "__main__":
    sys.exit(main())
