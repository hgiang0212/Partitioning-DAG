"""guide/10 §7 — the four free-time invariants, on a synthetic two-lane device.

Run two threads doing known-length work that OVERLAPS in time. The sum of the
per-kind timers must come out ~2x the merged busy interval, and the merged value
is the one that must appear in the report.

A run where those two numbers agree is a run where the merge is silently a sum —
and free time then reads far too low with nothing else looking wrong. That is the
whole reason this check exists.

    python scratch/selfcheck_free_time.py
"""
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.Measure import FreeTimeTracker      # noqa: E402

WORK_S = 0.05
ROUNDS = 8


def lane(tracker, kind, barrier):
    """One thread doing ROUNDS x WORK_S of `kind`, in lockstep with the other so
    the two lanes genuinely overlap rather than interleaving."""
    for _ in range(ROUNDS):
        barrier.wait()
        t0 = tracker.now()
        end = time.perf_counter() + WORK_S
        while time.perf_counter() < end:      # busy-wait: a sleep would idle the
            pass                              # CPU but still occupy the lane
        tracker.add_work(kind, t0)
        t0 = tracker.now()
        time.sleep(WORK_S)                    # both lanes idle together
        tracker.add_wait("input", t0)


def main():
    tracker = FreeTimeTracker(enabled=True, bucket_s=0.1)
    tracker.start()
    barrier = threading.Barrier(2)
    threads = [threading.Thread(target=lane, args=(tracker, kind, barrier))
               for kind in ("inference", "send")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    tracker.stop()
    report = tracker.finish()

    span, busy, free = report["span_ns"], report["busy_ns"], report["free_ns"]
    kind_sum = sum(report["kinds_ns"].values())
    reason_sum = sum(report["reasons_ns"].values())

    print(f"\nsynthetic device: 2 lanes x {ROUNDS} x {WORK_S * 1000:.0f} ms overlapping work\n")
    print(f"  span_s          {span / 1e9:9.4f}")
    print(f"  busy_s (merged) {busy / 1e9:9.4f}")
    print(f"  free_s          {free / 1e9:9.4f}   ({100.0 * free / span:.2f}%)")
    print(f"  Sum per-kind    {kind_sum / 1e9:9.4f}   "
          f"({kind_sum / busy:.2f}x the merged busy)")
    print(f"  reasons         {', '.join(f'{k}={v / 1e9:.4f}s' for k, v in report['reasons_ns'].items())}")
    print(f"  buckets         {len(report['series'])} x {report['bucket_s']}s")
    print(f"  host_idle       {report['host_idle']}\n")

    checks = [
        ("busy + free == span, exactly", busy + free == span,
         "intervals escaped the run window, or the clip step is missing"),
        ("Sum free reasons == free, exactly", reason_sum == free,
         "attribution double-counts, or `unaccounted` is not emitted"),
        ("Sum per-kind > merged busy", kind_sum > busy,
         "LANES ARE BEING SUMMED, NOT MERGED — the error this method exists to prevent"),
        ("busy <= span", busy <= span,
         "same bug, caught from the other side"),
        ("per-kind sum is ~2x merged (2 overlapping lanes)", 1.6 <= kind_sum / busy <= 2.4,
         "the lanes did not actually overlap; the test is not testing the merge"),
    ]
    failed = 0
    for name, ok, why in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
        if not ok:
            failed += 1
            print(f"         -> {why}")

    # A disabled tracker must report NOTHING rather than reporting zeros: zeros
    # would read as a device that was never idle (guide/09 Phase 4b).
    off = FreeTimeTracker(enabled=False)
    off.start()
    off.add_work("inference", off.now())
    off.stop()
    disabled_ok = off.finish() is None
    print(f"  [{'PASS' if disabled_ok else 'FAIL'}] disabled tracker reports nothing, not zeros")
    failed += 0 if disabled_ok else 1

    print(f"\n  -> {failed} failure(s)\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
