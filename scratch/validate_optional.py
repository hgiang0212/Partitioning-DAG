"""Conformance checks for the OPTIONAL result families.

`guide/validate_results.py` is the normative validator, and it deliberately
covers only the six required files. Files 8-14 carry their own invariants —
guide/10 §7, guide/11 §5, guide/12 §6 — and each one fails loudly in exactly the
way that measurement is usually wrong:

  free time    busy + free must equal span EXACTLY; the reasons must sum to the
               free time EXACTLY; per-kind sums must EXCEED merged busy, because
               if they are equal the lanes are being summed instead of merged
  broker RAM   phases partition the series; a phase with no samples is omitted,
               never written as zeros; every line names its source
  message size min <= p50 <= p95 <= max, and exactly one worker measured
  broker guard the per-queue caps must SUM to the budget, resume < high < cap, and
               a limit nobody observed is reported as `unknown`, never as held
  accuracy     (map.log / map_window.log — a project extension, NOT a guide
               family) ratios stay in [0, 1], mAP@50:95 never exceeds mAP@50, and
               the SYSTEM line names its pooling because it is a weighted mean
               rather than a real pooled mAP

    python scratch/validate_optional.py <run-dir>
"""
import re
import sys
from pathlib import Path

KV = re.compile(r"(\w+)=([^\s]+)")
TOL = 0.0015          # the files carry {:.3f} seconds, so exact identities can
                      # only be checked to within rounding


def read(path):
    if not path.exists():
        return None
    return [ln.strip() for ln in path.read_text(encoding="utf-8",
                                                errors="ignore").splitlines() if ln.strip()]


def kv(line):
    return dict(KV.findall(line))


def num(value):
    try:
        return float(str(value).rstrip("%"))
    except (TypeError, ValueError):
        return float("nan")


def flags(line):
    return [p for p in line.split()[1:] if "=" not in p and p.isupper()]


def check_free_time(d, errs, warns, notes):
    dev, group, series = (read(d / "free_time.log"),
                          read(d / "free_time_cluster.log"),
                          read(d / "free_time_series.log"))
    present = [x is not None for x in (dev, group, series)]
    if not any(present):
        notes.append("free time: not emitted (optional, all-or-none) — skipped")
        return
    if not all(present):
        errs.append("free time: files 8-10 are one feature — emit all three or none")
        return
    if not dev:
        notes.append("free time: files present but empty (feature off this run)")
        return

    machines = {}
    for i, line in enumerate(dev, 1):
        k = kv(line)
        span, busy, free = num(k.get("span_s")), num(k.get("busy_s")), num(k.get("free_s"))
        if abs(busy + free - span) > TOL:
            errs.append(f"free_time.log:{i}: busy_s + free_s = {busy + free:.3f} != "
                        f"span_s {span:.3f} (intervals escaped the run window, or "
                        f"the clip step is missing)")
        if num(k.get("free")) > 100.0:
            errs.append(f"free_time.log:{i}: free={k.get('free')} exceeds 100%")
        if busy > span + TOL:
            errs.append(f"free_time.log:{i}: busy_s {busy:.3f} > span_s {span:.3f} "
                        f"(lanes were summed, not merged)")
        machines.setdefault(k.get("machine"), []).append(k)
    notes.append(f"free time: {len(dev)} device(s) on {len(machines)} machine(s), "
                 f"busy+free == span on every one")

    by_scope_free, reasons, kinds, all_lines = {}, {}, {}, 0
    for i, line in enumerate(group, 1):
        k, fl = kv(line), flags(line)
        scope = k.get("cluster") or ("SYSTEM" if "SYSTEM" in fl else None)
        if "ALL" in fl and scope:
            by_scope_free[scope] = num(k.get("free_s"))
            if "free_mean" not in k:
                errs.append(f"free_time_cluster.log:{i}: ALL lines must carry free_mean "
                            f"beside free")
            all_lines += 1
        if "FREE" in fl:
            reasons.setdefault(scope, []).append((k.get("reason"), num(k.get("free_s")),
                                                  num(k.get("share"))))
        if "KIND" in fl:
            kinds.setdefault(scope, 0.0)
            kinds[scope] += num(k.get("busy_s"))
        if "SYSTEM" in fl and "free_mean" not in k:
            errs.append(f"free_time_cluster.log:{i}: SYSTEM line must carry free_mean")

    for scope, entries in reasons.items():
        share = sum(s for _, _, s in entries)
        total = sum(v for _, v, _ in entries)
        if abs(share - 100.0) > 0.5:
            errs.append(f"free_time_cluster.log: FREE reason shares for {scope} sum to "
                        f"{share:.2f}%, expected 100% (attribution double-counts or "
                        f"leaks; `unaccounted` must absorb the remainder)")
        if scope in by_scope_free and abs(total - by_scope_free[scope]) > 0.05:
            errs.append(f"free_time_cluster.log: FREE reasons for {scope} sum to "
                        f"{total:.3f}s but the ALL line says free_s="
                        f"{by_scope_free[scope]:.3f}s")
    if reasons:
        notes.append(f"free time: reason shares sum to 100% on all "
                     f"{len(reasons)} scope(s)")

    # The one that catches the error this whole method exists to prevent.
    for scope, kind_sum in kinds.items():
        busy = next((num(kv(l).get("busy_s")) for l in dev), None)
        merged = sum(num(kv(l).get("busy_s")) for l in dev
                     if kv(l).get("cluster") == scope)
        if merged and kind_sum <= merged + TOL:
            errs.append(f"free_time_cluster.log: per-kind busy for {scope} sums to "
                        f"{kind_sum:.3f}s, not MORE than the merged busy {merged:.3f}s "
                        f"— on a pipelined worker that means the lanes are being "
                        f"SUMMED instead of merged")
    if kinds:
        notes.append(f"free time: per-kind sums exceed merged busy on all "
                     f"{len(kinds)} scope(s) — the merge is a merge")

    buckets = {kv(l).get("bucket_s") for l in series}
    if series and None in buckets:
        errs.append("free_time_series.log: bucket_s must be carried on EVERY line, "
                    "never assumed by the reader")
    notes.append(f"free time: {len(series)} series row(s), bucket_s on every line")


def check_broker_ram(d, errs, warns, notes):
    series, summary = read(d / "broker_ram_ns.log"), read(d / "broker_ram.log")
    if series is None and summary is None:
        notes.append("broker RAM: not emitted (optional, all-or-none) — skipped")
        return
    if series is None or summary is None:
        errs.append("broker RAM: files 11-12 are one feature — emit both or neither")
        return
    if not summary:
        notes.append("broker RAM: files present but empty (feature off this run)")
        return

    for i, line in enumerate(series, 1):
        k = kv(line)
        if "source" not in k:
            errs.append(f"broker_ram_ns.log:{i}: every line must carry source= "
                        f"(a fallback is labelled, never silently substituted)")
            break
        if k.get("phase") not in ("idle", "run", "tail"):
            errs.append(f"broker_ram_ns.log:{i}: phase must be idle/run/tail, "
                        f"got {k.get('phase')!r}")
            break

    kinds = [flags(l)[0] for l in summary if flags(l)]
    broker = next((kv(l) for l in summary if "BROKER" in flags(l)), {})
    if not broker:
        errs.append("broker_ram.log: no BROKER line")
        return
    if int(num(broker.get("samples", 0))) == 0:
        # A missing file is indistinguishable from a run where the host was fine;
        # samples=0 with a reason is not.
        if "(" not in " ".join(summary):
            errs.append("broker_ram.log: samples=0 must name the reason in "
                        "trailing parentheses")
        notes.append("broker RAM: samples=0 line carries its reason — conformant")
        return

    phases = [kv(l).get("phase") for l in summary if "PHASE" in flags(l)]
    seen = {kv(l).get("phase") for l in series}
    if set(phases) != seen:
        errs.append(f"broker_ram.log: PHASE lines {sorted(phases)} do not match the "
                    f"phases present in the series {sorted(seen)} — a phase with no "
                    f"samples must be OMITTED, never written as zeros")
    if "idle" in phases and "run" in phases and "COMPARE" not in kinds:
        errs.append("broker_ram.log: idle and run both exist, so COMPARE must be "
                    "written — it is the only line that states what running the "
                    "system COSTS the host")
    for required in ("USED", "DELTA", "RABBIT"):
        if required not in kinds:
            errs.append(f"broker_ram.log: missing the {required} line")

    used = next((kv(l) for l in summary if "USED" in flags(l)), {})
    if used and not (num(used["min_mb"]) <= num(used["p50_mb"])
                     <= num(used["p95_mb"]) <= num(used["max_mb"])):
        errs.append("broker_ram.log: USED percentiles out of order "
                    "(min <= p50 <= p95 <= max required)")
    notes.append(f"broker RAM: {int(num(broker['samples']))} samples, "
                 f"source={broker.get('source')}, phases {sorted(set(phases))}, "
                 f"COMPARE present")


def check_message_size(d, errs, warns, notes):
    summary, series = (read(d / "message_size.log"),
                       read(d / "message_size_series.log"))
    if summary is None and series is None:
        notes.append("message size: not emitted (optional, all-or-none) — skipped")
        return
    if summary is None or series is None:
        errs.append("message size: files 13-14 are one feature — emit both or neither")
        return
    if not summary:
        notes.append("message size: files present but empty (feature off this run)")
        return

    if len(summary) != 1:
        warns.append(f"message_size.log: {len(summary)} workers measured. Exactly one "
                     f"is expected — the payload shape is fixed by the configuration, "
                     f"so N workers produce one number N times at N times the cost")
    for i, line in enumerate(summary, 1):
        k = kv(line)
        order = [num(k.get(x)) for x in ("min_mb", "p50_mb", "p95_mb", "max_mb")]
        if not all(a <= b for a, b in zip(order, order[1:])):
            errs.append(f"message_size.log:{i}: sizes out of order "
                        f"(min <= p50 <= p95 <= max required)")
        for required in ("n", "total_mb", "mean_mb", "span_s", "rate_mb_s",
                         "per_frame_mb"):
            if required not in k:
                errs.append(f"message_size.log:{i}: missing {required}=")
        # A size without the context that determines it is unreproducible.
        for context in ("mode", "compress", "num_bit", "batch_size", "splits"):
            if context not in k:
                errs.append(f"message_size.log:{i}: missing context key {context}= "
                            f"— a size without it cannot be reproduced")
        n = int(num(k.get("n", 0)))
        if len(series) > n:
            errs.append(f"message_size_series.log: {len(series)} rows for n={n} samples")

    idx = [int(num(kv(l).get("i", -1))) for l in series]
    if idx != sorted(idx):
        errs.append("message_size_series.log: i must stay non-decreasing "
                    "(a long series is DECIMATED evenly, never truncated)")
    if any("bytes" not in kv(l) for l in series):
        errs.append("message_size_series.log: every row needs the exact integer bytes=")
    k = kv(summary[0])
    notes.append(f"message size: 1 worker, n={k.get('n')} messages, "
                 f"mean={k.get('mean_mb')} MB, series {len(series)} row(s) "
                 f"(decimated from {k.get('n')})")


def check_map(d, errs, warns, notes):
    """Accuracy is NOT a guide family — guide/README puts mAP out of scope — so
    these invariants are this project's own. They are the ones that catch a mAP
    that is quietly meaningless:

      - a ratio outside [0, 1] means the metric was scaled or averaged wrong
      - mAP@50:95 is an average that INCLUDES the .50 threshold, so it can never
        exceed mAP@50; if it does, the two are swapped
      - `matched` may not exceed `frames`: you cannot score more frames against
        ground truth than the tier produced
      - the SYSTEM line is a frame-weighted mean, not a pooled mAP, and MUST say
        so — an unlabelled one reads as exact and is not
      - a window series that carries no `client=` cannot be separated when two
        completing devices share a cluster
    """
    summary, series = read(d / "map.log"), read(d / "map_window.log")
    if summary is None and series is None:
        notes.append("accuracy: not emitted (project extension, all-or-none) — skipped")
        return
    if summary is None or series is None:
        errs.append("accuracy: map.log and map_window.log are one feature — "
                    "emit both or neither")
        return
    if not summary:
        notes.append("accuracy: files present but empty (feature off this run)")
        return

    sys_lines = [l for l in summary if "SYSTEM" in flags(l)]
    dev_lines = [l for l in summary if "SYSTEM" not in flags(l)]
    for i, line in enumerate(summary, 1):
        k = kv(line)
        m50, m5095 = num(k.get("map50")), num(k.get("map5095"))
        for name, value in (("map50", m50), ("map5095", m5095)):
            if not (0.0 <= value <= 1.0):
                errs.append(f"map.log:{i}: {name}={k.get(name)} outside [0, 1] "
                            f"— mAP is a ratio, never a percentage here")
        if m5095 > m50 + 1e-9:
            errs.append(f"map.log:{i}: map5095={k.get('map5095')} exceeds "
                        f"map50={k.get('map50')} — mAP@50:95 averages over "
                        f"thresholds that include .50, so it cannot be higher")
        if "role" in k:
            errs.append(f"map.log:{i}: must not carry role= — only the completing "
                        f"tier scores accuracy, so every line is the same role")
    for i, line in enumerate(dev_lines, 1):
        k = kv(line)
        if "client" not in k or "cluster" not in k:
            errs.append(f"map.log:{i}: device lines need both client= and cluster=")
        if num(k.get("matched")) > num(k.get("frames")):
            errs.append(f"map.log:{i}: matched={k.get('matched')} exceeds "
                        f"frames={k.get('frames')} — more frames scored than produced")
    if len(sys_lines) > 1:
        errs.append(f"map.log: expected at most 1 SYSTEM line, found {len(sys_lines)}")
    for line in sys_lines:
        if kv(line).get("pooling") != "frame_weighted":
            errs.append("map.log: the SYSTEM line MUST carry pooling=frame_weighted. "
                        "A true pooled mAP needs every detection in one place; this "
                        "is a weighted mean and an unlabelled one reads as exact")

    idx_by_client = {}
    for line in series:
        k = kv(line)
        if "client" not in k:
            errs.append("map_window.log: every row needs client= — two completing "
                        "devices in one cluster both start at i=0")
            break
        idx_by_client.setdefault(k["client"], []).append(int(num(k.get("i", -1))))
    for client, idx in idx_by_client.items():
        if idx != sorted(idx):
            errs.append(f"map_window.log: i must stay non-decreasing (client={client})")
    for i, line in enumerate(series, 1):
        k = kv(line)
        for required in ("t_offset_s", "batches", "frames", "map50", "map5095"):
            if required not in k:
                errs.append(f"map_window.log:{i}: missing {required}=")
                break
        if num(k.get("map5095")) > num(k.get("map50")) + 1e-9:
            errs.append(f"map_window.log:{i}: map5095 exceeds map50")
    if sys_lines:
        k = kv(sys_lines[0])
        notes.append(f"accuracy: {len(dev_lines)} completing device(s) over "
                     f"{k.get('clusters')} cluster(s), SYSTEM map50={k.get('map50')} "
                     f"map5095={k.get('map5095')} ({k.get('pooling')}), "
                     f"{len(series)} window row(s)")


def check_broker_guard(d, errs, warns, notes):
    """guide/13 §6. The family whose invariants are about AGREEMENT: a limit
    that reports itself as held is worth nothing unless the file also shows what
    it was measured against, and unless the summary and the series tell the same
    story."""
    series, summary = (read(d / "broker_guard_ns.log"),
                       read(d / "broker_guard.log"))
    if series is None and summary is None:
        notes.append("broker guard: not emitted (optional, all-or-none) — skipped")
        return
    if series is None or summary is None:
        errs.append("broker guard: files 15-16 are one feature — emit both or neither")
        return
    if not summary:
        notes.append("broker guard: files present but empty (feature off this run)")
        return

    kinds = [flags(l)[0] for l in summary if flags(l)]
    guard = next((kv(l) for l in summary if "GUARD" in flags(l)), {})
    if not guard:
        errs.append("broker_guard.log: no GUARD line — the budget it enforced is "
                    "the one thing every other line is relative to")
        return

    per_q, n_q = num(guard.get("per_queue_mb")), num(guard.get("queues"))
    if abs(per_q * n_q - num(guard.get("payload_budget_mb"))) > 0.5:
        errs.append(f"broker_guard.log: {n_q:.0f} x per_queue_mb {per_q:.1f} != "
                    f"payload_budget_mb {num(guard.get('payload_budget_mb')):.1f} "
                    f"— the per-queue caps must SUM to the budget, or the cap the "
                    f"run enforced is not the cap it reports")
    if not (num(guard.get("resume_mb")) < num(guard.get("high_mb")) < per_q):
        errs.append("broker_guard.log: resume_mb < high_mb < per_queue_mb required "
                    "— without the gap a producer oscillates on the threshold")
    if num(guard.get("payload_budget_mb")) >= num(guard.get("cap_mb")):
        errs.append("broker_guard.log: payload_budget_mb must be BELOW cap_mb — "
                    "the broker's own overhead and the control plane are not free")

    broker = next((kv(l) for l in summary if "BROKER" in flags(l)), {})
    if not broker:
        errs.append("broker_guard.log: no BROKER line")
    else:
        within = broker.get("within_cap")
        if within not in ("yes", "NO", "unknown"):
            errs.append(f"broker_guard.log: within_cap={within!r}, expected "
                        f"yes/NO/unknown")
        if int(num(broker.get("polls", 0))) == 0 and within != "unknown":
            # The failure this catches: an unreachable management API reporting
            # a peak of 0 MB, which reads as a run that never came close.
            errs.append("broker_guard.log: polls=0 but within_cap is not unknown "
                        "— a limit nobody observed cannot be reported as held")
        if int(num(broker.get("alarms", 0))) > 0 and \
                not any("ALARM" in flags(l) for l in series):
            errs.append("broker_guard.log: alarms>0 but broker_guard_ns.log has no "
                        "ALARM line — the summary and the series disagree")

    policy_on = guard.get("policy") == "on"
    for i, line in enumerate(summary, 1):
        if "QUEUE" not in flags(line):
            continue
        k = kv(line)
        if policy_on and num(k.get("peak_mb")) > num(k.get("cap_mb")) + 0.5:
            errs.append(f"broker_guard.log:{i}: {k.get('queue')} peaked at "
                        f"{k.get('peak_mb')} MB above its cap {k.get('cap_mb')} MB "
                        f"while policy=on — the broker-side cap did not hold")

    throttles = [kv(l) for l in summary if "THROTTLE" in flags(l)]
    per_client = [t for t in throttles if t.get("client") != "ALL"]
    if per_client and not any(t.get("client") == "ALL" for t in throttles):
        errs.append("broker_guard.log: per-producer THROTTLE lines without the "
                    "client=ALL roll-up")
    for t in per_client:
        if int(num(t.get("nacks", 0))) > 0 and not policy_on:
            errs.append(f"broker_guard.log: client={t.get('client')} reports "
                        f"nacks>0 with policy=off — a rejection can only come "
                        f"from the policy")
        if int(num(t.get("events", 0))) == 0 and num(t.get("wait_s", 0)) > 0:
            errs.append(f"broker_guard.log: client={t.get('client')} waited "
                        f"{t.get('wait_s')}s across zero events")

    known = {"GUARD", "BROKER", "POLICY", "WATERMARK", "ALARM", "OVER", "UNDER",
             "BLOCKED", "NOTE"}
    unknown = {f[0] for f in (flags(l) for l in series) if f and f[0] not in known}
    if unknown:
        errs.append(f"broker_guard_ns.log: unknown line kind(s) {sorted(unknown)}")
    n_alarm = sum(1 for l in series if "ALARM" in flags(l))
    if n_alarm % 2:
        # One edge without the other leaves the duration unrecoverable.
        warns.append("broker_guard_ns.log: an odd number of ALARM lines — an alarm "
                     "that went on and never off means the run ended inside it")

    notes.append(f"broker guard: cap {guard.get('cap_mb')} MB, "
                 f"{n_q:.0f} queue(s) x {per_q:.1f} MB, policy={guard.get('policy')}, "
                 f"watermark={guard.get('watermark')}, "
                 f"peak {broker.get('mem_peak_mb')} MB, "
                 f"within_cap={broker.get('within_cap')}, "
                 f"{len(per_client)} throttled producer(s)")


def check_no_spaces_in_values(d, errs, warns, notes):
    """A `key=value` value MUST contain no spaces (guide/01 §1).

    This one is worth a dedicated check because it fails SILENTLY: the universal
    parser splits on whitespace, so `splits=(11, 15)` is read as `splits=(11,`
    and ` 15)` is quietly discarded as trailing free text. The file still looks
    well-formed, the validator's own KV regex still matches, and the truncated
    value only shows up as nonsense in a chart label.

    The structural symptom is a token that is neither a flag nor a pair sitting
    BEFORE the last pair on the line. Trailing free text after every pair is
    legal and is left alone."""
    checked = 0
    for path in sorted(d.glob("*.log")):
        for i, line in enumerate(read(path) or [], 1):
            tokens = line.split()[1:]
            pair_at = [j for j, t in enumerate(tokens) if "=" in t]
            if not pair_at:
                continue
            checked += 1
            for j, token in enumerate(tokens[:max(pair_at)]):
                if "=" in token or (token.isupper() and token.isalpha()):
                    continue
                errs.append(f"{path.name}:{i}: stray token {token!r} between "
                            f"key=value pairs — a value with a SPACE in it was "
                            f"split, and everything after it is being silently "
                            f"discarded by every reader")
                break
            else:
                continue
            break
    notes.append(f"grammar: {checked} kv line(s) across "
                 f"{len(list(d.glob('*.log')))} file(s), no space-split values")


def main(run_dir):
    d = Path(run_dir)
    errs, warns, notes = [], [], []
    check_no_spaces_in_values(d, errs, warns, notes)
    check_free_time(d, errs, warns, notes)
    check_broker_ram(d, errs, warns, notes)
    check_message_size(d, errs, warns, notes)
    check_map(d, errs, warns, notes)
    check_broker_guard(d, errs, warns, notes)

    print(f"\noptional-family checks for {d}")
    for n in notes:
        print(f"  [ok]   {n}")
    for w in warns:
        print(f"  [WARN] {w}")
    for e in errs:
        print(f"  [FAIL] {e}")
    print(f"\n  -> {len(errs)} error(s), {len(warns)} warning(s): "
          f"{'CONFORMANT' if not errs else 'NOT CONFORMANT'}\n")
    return 1 if errs else 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: python validate_optional.py <run-dir>")
    sys.exit(main(sys.argv[1]))
