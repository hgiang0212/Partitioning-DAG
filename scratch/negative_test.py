"""Negative test — guide/09 Phase 6: "corrupt a copy and confirm the validator
catches it".

A validator that has only ever been run on good data is a validator nobody has
tested. Each case below reproduces one entry from the "Common porting failures"
table in guide/09, applies it to a throwaway copy of a conformant run, and fails
if the validator does NOT complain.

    python scratch/negative_test.py [--run results/results_0101_1200_synthetic]
"""
import argparse
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


# ─────────────────────────── the corruptions ─────────────────────────────────

def _sub(path, pattern, repl, count=1):
    text = path.read_text(encoding="utf-8")
    new = re.sub(pattern, repl, text, count=count)
    assert new != text, f"corruption did not apply to {path.name}: {pattern}"
    path.write_text(new, encoding="utf-8")


def double_counted_units(d):
    """Two stages publish for the same unit -> group `done` sums past SYSTEM."""
    _sub(d / "group_rate.log", r"(cluster=queue_2 .*?done=)(\d+)",
         lambda m: f"{m.group(1)}{int(m.group(2)) + 40}")


def stage_stopped_early(d):
    """No final drain after the compute thread joins -> line-count mismatch."""
    lines = (d / "group_rate_ns.log").read_text(encoding="utf-8").splitlines()
    (d / "group_rate_ns.log").write_text("\n".join(lines[:-4]) + "\n", encoding="utf-8")


def overlapping_busy_intervals(d):
    """Utilization above 100% -> intervals were summed, not merged."""
    _sub(d / "utilization.log", r"utilization=\d+\.\d+%", "utilization=137.42%")


def per_group_start(d):
    """A per-group START where a shared one was required -> SYSTEM span no longer
    equals the max group span."""
    _sub(d / "group_rate.log", r"(SYSTEM fps=)\d+\.\d+", r"\g<1>31.400")


def averaged_percentiles(d):
    """Percentiles computed on pre-averaged data -> p50 > p95."""
    _sub(d / "latency_group.log", r"p50_ms=\d+\.\d+", "p50_ms=999999.000")


def role_on_e2e(d):
    """`role=` on an e2e line -> double-counts when charts group by role."""
    _sub(d / "latency_group.log", r"(kind=e2e)", "role=cloud kind=e2e")


def system_carries_steady(d):
    """The SYSTEM line must carry neither steady_fps nor share."""
    _sub(d / "group_rate.log", r"(SYSTEM fps=\d+\.\d+)", r"\1 steady_fps=9.900")


def missing_required_file(d):
    """A missing file is a hard error for the reader; an EMPTY one is valid."""
    (d / "utilization_group.log").unlink()


def missing_utilization_mean(d):
    """A pooled figure alone can hide one idle device inside a busy group."""
    _sub(d / "utilization_group.log", r" utilization_mean=\d+\.\d+%", "", count=99)


def non_monotonic_arrivals(d):
    """The live series is the server's own arrival clock — it cannot go back."""
    lines = (d / "batch_done_ns.log").read_text(encoding="utf-8").splitlines()
    lines[60], lines[20] = lines[20], lines[60]
    (d / "batch_done_ns.log").write_text("\n".join(lines) + "\n", encoding="utf-8")


# ── optional families ────────────────────────────────────────────────────────

def busy_plus_free_not_span(d):
    """Intervals escaped the run window, or the clip step is missing."""
    _sub(d / "free_time.log", r"free_s=\d+\.\d+", "free_s=12.000")


def lanes_summed_not_merged(d):
    """The error the whole free-time method exists to prevent: per-kind sums
    equal to merged busy means the lanes were added instead of unioned."""
    text = (d / "free_time_group.log").read_text(encoding="utf-8")
    kept = [l for l in text.splitlines() if " KIND " not in l]
    kept.append("1785000762468320838 cluster=queue_2 KIND kind=inference "
                "busy_s=0.100 share=0.01%")
    (d / "free_time_group.log").write_text("\n".join(kept) + "\n", encoding="utf-8")


def reasons_do_not_sum(d):
    """Attribution double-counts, or `unaccounted` was dropped."""
    _sub(d / "free_time_group.log", r"(FREE reason=input .*?share=)\d+\.\d+%",
         r"\g<1>40.00%")


def message_sizes_out_of_order(d):
    _sub(d / "message_size.log", r"p95_mb=\d+\.\d+", "p95_mb=1.000")


def unlabelled_ram_source(d):
    """A fallback that is not labelled is worse than no fallback."""
    _sub(d / "broker_ram_ns.log", r"source=\w+ ", "", count=99)


def space_inside_a_value(d):
    """A value with a space in it — the failure that looks like well-formed
    output right up until a chart label reads `splits=(11,`."""
    _sub(d / "message_size.log", r"splits=\S+", "splits=(11, 15)")


def phase_written_as_zeros(d):
    """A phase with no samples must be omitted, never written as zeros."""
    text = (d / "broker_ram.log").read_text(encoding="utf-8")
    text += ("1785000762468320838 PHASE phase=warmup samples=0 span_s=0.000 "
             "min_mb=0.0 mean_mb=0.0 p50_mb=0.0 p95_mb=0.0 max_mb=0.0 "
             "mean=0.00% max=0.00% mean_rss_mb=0.0 max_rss_mb=0.0 "
             "t_start_ns=0 t_end_ns=0\n")
    (d / "broker_ram.log").write_text(text, encoding="utf-8")


REQUIRED_CASES = [
    ("two stages publish per unit",        double_counted_units,   "sums to"),
    ("stage stopped reporting early",      stage_stopped_early,    "line-count mismatch"),
    ("overlapping busy intervals",         overlapping_busy_intervals, "exceeds 100%"),
    ("per-group START, not shared",        per_group_start,        "span"),
    ("percentiles on pre-averaged data",   averaged_percentiles,   "percentiles out of order"),
    ("role= on an e2e line",               role_on_e2e,            "must not carry role"),
    ("SYSTEM carries steady_fps",          system_carries_steady,  "steady_fps"),
    ("required file missing",              missing_required_file,  "MISSING"),
    ("ALL line without utilization_mean",  missing_utilization_mean, "utilization_mean"),
    ("arrivals not monotonic",             non_monotonic_arrivals, "monotonic"),
]

OPTIONAL_CASES = [
    ("busy + free != span",                busy_plus_free_not_span, "!= span_s"),
    ("lanes summed instead of merged",     lanes_summed_not_merged, "SUMMED instead of merged"),
    ("free reasons do not sum to 100%",    reasons_do_not_sum,      "sum to"),
    ("message sizes out of order",         message_sizes_out_of_order, "out of order"),
    ("a value containing a space",          space_inside_a_value,    "stray token"),
    ("RAM source not labelled",            unlabelled_ram_source,   "source="),
    ("empty phase written as zeros",       phase_written_as_zeros,  "OMITTED"),
]


def run_case(source, name, corrupt, expect, validator, extra_args):
    with tempfile.TemporaryDirectory(prefix="negtest_") as tmp:
        d = Path(tmp) / "run"
        shutil.copytree(source, d)
        corrupt(d)
        proc = subprocess.run(
            [sys.executable, str(validator), str(d), *extra_args],
            capture_output=True, text=True)
        out = proc.stdout + proc.stderr
        caught = proc.returncode != 0
        matched = expect.lower() in out.lower()
        ok = caught and matched
        detail = ""
        if not caught:
            detail = "validator exited 0 — the corruption was NOT caught"
        elif not matched:
            detail = f"caught, but no message matching {expect!r}"
        return ok, detail, out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default=str(ROOT / "scratch" / "synthetic" /
                                         "results_0101_1200_synthetic"))
    args = ap.parse_args()
    source = Path(args.run).resolve()
    if not source.exists():
        sys.exit(f"no run directory at {source} — run scratch/make_synthetic_run.py first")

    suites = [
        ("guide/validate_results.py", ROOT / "guide" / "validate_results.py",
         ["--names", "group"], REQUIRED_CASES),
        ("scratch/validate_optional.py", ROOT / "scratch" / "validate_optional.py",
         [], OPTIONAL_CASES),
    ]

    failed = 0
    for title, validator, extra, cases in suites:
        print(f"\n{title} — {len(cases)} corruption(s)")
        for name, corrupt, expect in cases:
            ok, detail, out = run_case(source, name, corrupt, expect, validator, extra)
            print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
            if not ok:
                failed += 1
                print(f"         -> {detail}")
                print("         " + "\n         ".join(out.strip().splitlines()[-6:]))

    # The control: the uncorrupted run must still pass both, or the whole suite
    # is measuring the copy step rather than the corruptions.
    print("\ncontrol — the uncorrupted run")
    for title, validator, extra, _ in suites:
        proc = subprocess.run([sys.executable, str(validator), str(source), *extra],
                              capture_output=True, text=True)
        ok = proc.returncode == 0
        print(f"  [{'PASS' if ok else 'FAIL'}] {title} exits 0 on clean input")
        failed += 0 if ok else 1

    print(f"\n  -> {failed} failure(s)\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
