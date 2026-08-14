"""build_nb.py — emit the result-visualization notebook.

Follows guide/08-build-pipeline.md §5: the notebook is generated, never
hand-authored, so a style fix is one edit here instead of twenty in JSON.

    python scratch/build_nb.py                       # charts results/
    python scratch/build_nb.py --results <dir> --out <file.ipynb>

Then execute it with scratch/run_nb.py.
"""
import argparse
from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parent.parent

ap = argparse.ArgumentParser()
ap.add_argument("--results", default=str(ROOT / "results"),
                help="directory holding the archived run directories")
ap.add_argument("--out", default=str(ROOT / "results" / "visual" /
                                    "Split Inference Result Visualization.ipynb"))
args = ap.parse_args()

RESULTS = Path(args.results).resolve()
OUT = Path(args.out).resolve()

nb = nbf.v4.new_notebook()
cells = []
md = lambda s: cells.append(nbf.v4.new_markdown_cell(s.strip("\n")))
code = lambda s: cells.append(nbf.v4.new_code_cell(s.strip("\n")))


def guarded_code(condition, reason, body):
    """Emit a chart cell that skips itself, with a reason, when its source frame
    is empty.

    A chart drawn from no rows is not an empty chart — it is a titled axis with a
    legend and a `0.0 … 1.2` scale, which reads as a measured zero rather than as
    missing data, and `finish()` writes it to `imgs/` and counts it in the
    manifest all the same. The optional families (10-14) already refuse to draw
    on absent input; the six required ones (01-08) must refuse on the same terms
    (guide/07 C9: skip it rather than ship an empty one, and say so).

    The guard is applied here, at build time, so the chart bodies below stay
    exactly as they read in guide/07 — one wrapper instead of eight hand-indented
    copies of the same `if/else`.
    """
    indented = "\n".join("    " + line if line.strip() else line
                         for line in body.strip("\n").splitlines())
    code(f"if {condition}:\n    print({reason!r})\nelse:\n{indented}")


# ─────────────────────────────── cell 0 ──────────────────────────────────────
md(rf"""
# Split Inference — Distributed Run Results

Charts are generated from the result format in `guide/01-result-format.md`
(naming scheme: the **`*_cluster` set** — `fps_cluster*`, `utilization_cluster`,
`latency_cluster`, `free_time_cluster` — carrying `cluster=` keys; one scheme per
project, never mixed). Every run directory under `{RESULTS}` that contains a
`batch_done_ns.log` is picked up automatically, so adding a run needs no edit here.

Images are written to `results/imgs/`.

Each chart's second series is a second **statistic**, not a second run — a run
measures one configuration, and the comparison that matters inside it is whole-run
vs steady-state, mean vs p95, cloud vs edge. Load two run directories and the
charts facet by run and 09 appears.

| Input file | Feeds |
|---|---|
| `batch_done_ns.log` | 02 |
| `fps_cluster_ns.log` | 03, 04 |
| `fps_cluster.log` | 01, 09, 00 |
| `utilization.log` | 08 |
| `utilization_cluster.log` | 07, 09 |
| `latency_cluster.log` | 05, 06, 09 |
| `events_ns.log` | 02 (vertical rules) |
| `free_time*.log` | 10, 11, 12 |
| `broker_ram*.log` | 13, 14 (subtitle) |
| `message_size*.log` | 14 |
| `map.log`, `map_window.log` | 15 |

`map.log` and `map_window.log` are **not** a guide family: `guide/README` puts
model-accuracy metrics out of scope, since nothing else measured here depends on
ground truth or on the work being a detection task. They are written to the same
universal grammar anyway, so the one parser core reads them too.

Before charting, confirm the inputs are conformant:

```
python guide/validate_results.py <run-dir> --names cluster    # the six required files
python scratch/validate_optional.py <run-dir>                 # files 8-14 + accuracy
```

**No results yet?** `python scratch/make_synthetic_run.py` fabricates a run and
pushes it through the real emitters, so the whole pipeline can be exercised
before the cluster is available:

```
python scratch/make_synthetic_run.py
python scratch/build_nb.py --results scratch/synthetic --out <this file>
python scratch/run_nb.py
```

It writes to `scratch/`, never to `results/` — a directory under `results/` means
a real run happened, and a fabricated one sitting there is indistinguishable from
a real one months later.
""")


# ─────────────────────────────── setup ───────────────────────────────────────
md("## 0 · Setup — paths, palette, chart style")
code(rf'''
import re
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from pathlib import Path

RESULTS = Path(r"{RESULTS}")
IMG_DIR = RESULTS / "imgs"
IMG_DIR.mkdir(parents=True, exist_ok=True)

# ---- tokens (guide/06 §3) ------------------------------------------------
SURFACE = "#fcfcfb"; PAGE  = "#f9f9f7"
INK     = "#0b0b0b"; INK_2 = "#52514e"
MUTED   = "#898781"; GRID  = "#e1e0d9"; AXIS = "#c3c2b7"

# Categorical slots, taken IN ORDER — the ordering is the colorblind-safety
# mechanism, not cosmetic. Validated with:
#   python guide/validate_palette.py "#2a78d6,#eb6834,#1baf7a" light all
SLOTS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100",
         "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
S1, S2, S3 = SLOTS[0], SLOTS[1], SLOTS[2]
GOOD, BAD, NEUTRAL = "#0ca30c", "#d03b3b", MUTED
# Slots below 3:1 against the surface: using one OBLIGATES visible direct
# labels (guide/06 §3, relief rule). Every chart below labels its marks.
LOW_CONTRAST = {{"#1baf7a", "#eda100", "#e87ba4"}}

mpl.rcParams.update({{
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,        # else the PNG is transparent -> black in dark mode
    "font.family": ["Segoe UI", "DejaVu Sans", "sans-serif"],
    "font.size": 10,
    "axes.titlesize": 13, "axes.titleweight": "semibold",
    "axes.titlecolor": INK, "axes.titlepad": 12,
    "axes.labelsize": 10.5, "axes.labelcolor": INK_2,
    "axes.edgecolor": AXIS, "axes.linewidth": 0.8,
    "axes.grid": True, "axes.axisbelow": True,
    "grid.color": GRID, "grid.linestyle": "-", "grid.linewidth": 0.8,   # solid hairline
    "xtick.color": MUTED, "ytick.color": MUTED,
    "xtick.labelsize": 9.5, "ytick.labelsize": 9.5,
    "xtick.major.size": 0, "ytick.major.size": 0,
    "legend.frameon": False, "legend.fontsize": 9.5, "legend.labelcolor": INK_2,
    "figure.dpi": 110, "savefig.dpi": 300, "savefig.bbox": "tight",
}})

# The surface-coloured edge IS the 2px gap between adjacent fills.
# It is not a contrasting border drawn to separate marks — never black.
BAR_KW  = dict(edgecolor=SURFACE, linewidth=1.2)
LINE_KW = dict(linewidth=2.0, solid_capstyle="round")
MARK_KW = dict(markersize=6, markeredgecolor=SURFACE, markeredgewidth=1.4)

SAVED = []   # running manifest

def finish(fig, filename, hide_spines=("top", "right")):
    """Tidy spines, save at 300 dpi, record in the manifest, show."""
    for ax in fig.get_axes():
        for side in hide_spines:
            ax.spines[side].set_visible(False)
        ax.set_axisbelow(True)
    out = IMG_DIR / filename
    fig.savefig(out, dpi=300, bbox_inches="tight", facecolor=SURFACE)
    SAVED.append(filename)
    print(f"saved -> {{out}}")
    plt.show()

def best_corner(per_category_max):
    """Top corner OPPOSITE the tallest bar, so the legend can never cover a mark.
    A hard-coded corner is right until the data reorders."""
    v = np.asarray(per_category_max, dtype=float)
    if not v.size or np.isnan(v).all():
        return "upper right"
    return "upper left" if int(np.nanargmax(v)) >= len(v) / 2 else "upper right"

def label_bars(ax, bars, fmt="{{:.2f}}", dy=3, fontsize=9, color=INK_2):
    """Direct value labels above bars — the relief for sub-3:1 fills."""
    for bar in bars:
        h = bar.get_height()
        if h is None or np.isnan(h):
            continue
        ax.annotate(fmt.format(h),
                    xy=(bar.get_x() + bar.get_width() / 2, h),
                    xytext=(0, dy), textcoords="offset points",
                    ha="center", va="bottom", fontsize=fontsize, color=color)

# ---- the house style: legend above, finding underneath -------------------
_ANCHOR = {{"center": ("lower center", (0.5, 1.0)),
           "right":  ("lower right",  (1.0, 1.0)),
           "left":   ("lower left",   (0.0, 1.0))}}

def legend_above(ax, ncol, side="center", handles=None, labels=None):
    """Horizontal legend anchored ABOVE the axes. Structurally cannot cover a
    mark, which is what a corner placement can never promise once data moves."""
    loc, bbox = _ANCHOR[side]
    kw = dict(ncol=ncol, frameon=False, loc=loc, bbox_to_anchor=bbox,
              borderaxespad=0.5, columnspacing=2.2, handlelength=1.6)
    return ax.legend(handles, labels, **kw) if handles is not None else ax.legend(**kw)

def subtitle(ax, text, dy=-0.17):
    """The finding, in words, under the axes. A chart that states its own
    conclusion cannot be misread as the opposite one."""
    ax.annotate(text, xy=(0.5, dy), xycoords="axes fraction",
                ha="center", fontsize=9.5, color=MUTED)

def endpoint(ax, x, y, color, fmt="{{:.1f}}"):
    """Dot + bold value at the end of a series: the one label a dense line gets."""
    ax.plot([x], [y], "o", color=color, **MARK_KW)
    ax.annotate(fmt.format(y), xy=(x, y), xytext=(11, 0),
                textcoords="offset points", va="center",
                fontsize=10.5, fontweight="semibold", color=color)

def seconds_axis(df_run, ts_col="ts"):
    """ns epochs -> seconds since this run's first completion. Subtract the int
    baseline FIRST: ns epochs overflow float precision."""
    t0 = int(df_run[ts_col].iloc[0])
    return (df_run[ts_col].astype("int64") - t0) / 1e9

# ---- discover runs -------------------------------------------------------
# Any directory (depth 1 or 2) holding a batch_done_ns.log is a run, so both
# archive layouts in guide/05 §1 work: results_<MMDD>_<HHMM>_<tag>/ and
# <date>/<variant>/.
RUN_ID = re.compile(r"results_\d{{4}}_\d{{4}}_(.+)$")

def discover_runs(root):
    found = {{}}
    for path in sorted(root.rglob("batch_done_ns.log")):
        d = path.parent
        if d.name == "imgs" or "visual" in d.parts:
            continue
        label = d.name if d.parent == root else f"{{d.parent.name}}/{{d.name}}"
        found[label] = d
    # Display labels at discovery time, so no chart below carries a raw run id:
    # results_0802_1010_pdd -> pdd. Falls back to the full name on a collision,
    # because two runs must never share a label.
    short = {{k: (RUN_ID.match(k).group(1) if RUN_ID.match(k) else k) for k in found}}
    if len(set(short.values())) == len(found):
        return {{short[k]: v for k, v in found.items()}}
    return found

RUNS = discover_runs(RESULTS)
RUN_ORDER = list(RUNS)
HAS_PAIR = len(RUN_ORDER) == 2       # A-vs-B charts need exactly two
RUN_COLOR = {{run: SLOTS[i % len(SLOTS)] for i, run in enumerate(RUN_ORDER)}}
ROLE_COLOR = {{"cloud": S1, "edge": S2, "unknown": MUTED}}

print(f"results root : {{RESULTS}}")
print(f"runs found   : {{len(RUN_ORDER)}}")
for r, d in RUNS.items():
    print(f"  {{r:<34}} {{d}}")
if not RUN_ORDER:
    print("\n!! No run directories found. Execute the pipeline first, then re-run "
          "scratch/build_nb.py && scratch/run_nb.py.")
''')


# ─────────────────────────────── parsers ─────────────────────────────────────
md("""
## 1 · Log parsers

The universal line grammar (`<ts_ns> [FLAG ...] [key=value ...]`) means **one**
parser core reads every file — no bespoke regex per file.
""")
code(r'''
KV = re.compile(r"(\w+)=([^\s]+)")

def parse_kv_line(line):
    """-> (timestamp, [UPPERCASE flags], {key: value})"""
    parts = line.split()
    if not parts:
        return None
    ts    = int(parts[0]) if parts[0].isdigit() else None
    kv    = {k: v for k, v in KV.findall(line)}
    flags = [p for p in parts[1:] if "=" not in p and p.isupper()]
    return ts, flags, kv

def num(v):
    """'55.06%' -> 55.06 ; '336' -> 336.0 ; junk -> nan"""
    if v is None:
        return np.nan
    try:
        return float(str(v).rstrip("%"))
    except ValueError:
        return np.nan

def read_lines(path):
    if not Path(path).exists():
        print(f"!! missing: {path}")
        return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return [ln.rstrip("\n") for ln in f if ln.strip()]

# Raw ids -> display labels AT PARSE TIME, so no chart code below ever contains
# a raw queue name. A group here is the cloud chain an edge feeds.
def group_label(raw):
    if raw is None:
        return None
    if raw == "unknown":
        return "Untagged"
    # The id is the QUEUE an edge publishes to, and this project names those
    # `intermediate_queue_2` where the guide's examples say `queue_2`. Match on
    # the trailing index so both land on the same display label — anchoring the
    # whole string put a raw queue name on every axis of every chart.
    m = re.search(r"queue_(\d+)$", raw)
    return f"Cluster {m.group(1)}" if m else raw

def short_label(scope):
    """Compact form for two-line tick labels: 'Cluster 2' -> 'C2'."""
    m = re.fullmatch(r"Cluster (\d+)", scope or "")
    return f"C{m.group(1)}" if m else (scope or "")

def scope_of(flags, kv):
    """Group label, or 'System' on a SYSTEM line, or None for lines we skip."""
    if "cluster" in kv:
        return group_label(kv["cluster"])
    return "System" if "SYSTEM" in flags else None
''')

code(r'''
def parse_rate_summary(run, path):                    # fps_cluster.log
    rows = []
    for ln in read_lines(path):
        ts, flags, kv = parse_kv_line(ln)
        scope = scope_of(flags, kv)
        if scope is None:
            continue                                  # skip lines this parser doesn't own
        rows.append(dict(run=run, scope=scope, ts=ts,
                         fps=num(kv.get("fps")),
                         # SYSTEM carries neither steady_fps nor share (01 §3.3).
                         # Leave them NaN — do NOT default steady_fps to fps, or the
                         # System row silently looks like it has a steady-state number.
                         steady_fps=num(kv.get("steady_fps")),
                         done=num(kv.get("done")), frames=num(kv.get("frames")),
                         share=num(kv.get("share"))))
    return rows

def parse_rate_timeline(run, path):                   # fps_cluster_ns.log
    rows = []
    for ln in read_lines(path):
        ts, _, kv = parse_kv_line(ln)
        if "window_fps" not in kv:
            continue                                  # warm-up rows, before the window fills
        rows.append(dict(run=run, cluster=group_label(kv["cluster"]),
                         ts=ts, done=int(num(kv["done"])),
                         window_fps=num(kv["window_fps"])))
    return rows

def parse_batch_done(run, path):                      # batch_done_ns.log — TWO arities
    rows, idx = [], 0
    for ln in read_lines(path):
        parts = ln.split()
        idx += 1                                      # increments on EVERY line, so the
        if len(parts) == 2:                           # batch index stays true even though
            rows.append(dict(run=run, batch=idx,      # warm-up rows carry no rate yet
                             ts=int(parts[0]),
                             window_fps=float(parts[1])))
    return rows

def parse_latency(run, path):                         # latency_cluster.log
    rows = []
    for ln in read_lines(path):
        ts, flags, kv = parse_kv_line(ln)
        scope = scope_of(flags, kv)
        if scope is None:
            continue
        rows.append(dict(run=run, scope=scope,
                         role=kv.get("role", "all"), kind=kv.get("kind"),
                         n=num(kv.get("n")), mean_ms=num(kv.get("mean_ms")),
                         p50_ms=num(kv.get("p50_ms")), p95_ms=num(kv.get("p95_ms")),
                         max_ms=num(kv.get("max_ms"))))
    return rows

def parse_util_group(run, path):                      # utilization_cluster.log
    rows = []
    for ln in read_lines(path):
        ts, flags, kv = parse_kv_line(ln)
        scope = scope_of(flags, kv)
        if scope is None:
            continue
        rows.append(dict(run=run, scope=scope, role=kv.get("role", "all"),
                         devices=num(kv.get("devices")),
                         utilization=num(kv.get("utilization")),
                         utilization_mean=num(kv.get("utilization_mean")),
                         busy_s=num(kv.get("busy_s")), total_s=num(kv.get("total_s")),
                         packages=num(kv.get("packages"))))
    return rows

def parse_util_device(run, path):                     # utilization.log
    rows = []
    for ln in read_lines(path):
        ts, _, kv = parse_kv_line(ln)
        if "client" not in kv:
            continue
        rows.append(dict(run=run, client=kv["client"], role=kv.get("role"),
                         packages=num(kv.get("packages")),
                         busy_s=num(kv.get("busy_s")), total_s=num(kv.get("total_s")),
                         utilization=num(kv.get("utilization"))))
    return rows

def parse_events(run, path):                          # events_ns.log
    rows = []
    for ln in read_lines(path):
        parts = ln.split(None, 1)                     # split ONCE: description has spaces
        if len(parts) != 2 or not parts[0].isdigit():
            continue
        rows.append(dict(run=run, ts=int(parts[0]), description=parts[1]))
    return rows
''')

md("""
### 1.1 · Optional families — free time, queue-host RAM, payload size

Files 8–14 (guide/01 §2). Each family is all-its-files-or-none, and a run with a
feature switched off leaves its files **present but empty** — so these parsers
return nothing rather than failing, and the charts below skip themselves.
""")
code(r'''
def parse_free_time_device(run, path):                # free_time.log
    rows = []
    for ln in read_lines(path):
        ts, _, kv = parse_kv_line(ln)
        if "client" not in kv:
            continue
        rows.append(dict(run=run, client=kv["client"], role=kv.get("role"),
                         machine=kv.get("machine"), cluster=group_label(kv.get("cluster")),
                         dev=kv.get("device"), span_s=num(kv.get("span_s")),
                         busy_s=num(kv.get("busy_s")), free_s=num(kv.get("free_s")),
                         free=num(kv.get("free")), gaps=num(kv.get("gaps")),
                         longest_free_ms=num(kv.get("longest_free_ms")),
                         host_idle=num(kv.get("host_idle"))))
    return rows

def parse_free_time_cluster(run, path):                 # free_time_cluster.log
    """Six line kinds in one file, told apart by their FLAGS. `line_kind` is a
    deliberate name: a column called `kind` collides with the KIND lines' own
    `kind=` key, and one named `agg` would be shadowed by DataFrame.agg."""
    rows = []
    for ln in read_lines(path):
        ts, flags, kv = parse_kv_line(ln)
        if "FREE" in flags:
            line_kind = "reason"
        elif "KIND" in flags:
            line_kind = "kind"
        elif "MACHINE" in flags:
            line_kind = "machine"
        elif "SYSTEM" in flags:
            line_kind = "system"
        elif "ALL" in flags:
            line_kind = "all"
        elif "role" in kv:
            line_kind = "role"
        else:
            continue
        rows.append(dict(run=run, line_kind=line_kind,
                         scope=scope_of(flags, kv) or "System",
                         role=kv.get("role"), reason=kv.get("reason"),
                         kind=kv.get("kind"), machine=kv.get("machine"),
                         devices=num(kv.get("devices")), free=num(kv.get("free")),
                         free_mean=num(kv.get("free_mean")),
                         free_s=num(kv.get("free_s")), span_s=num(kv.get("span_s")),
                         busy_s=num(kv.get("busy_s")), share=num(kv.get("share")),
                         host_idle=num(kv.get("host_idle"))))
    return rows

def parse_free_time_series(run, path):                # free_time_series.log
    rows = []
    for ln in read_lines(path):
        ts, _, kv = parse_kv_line(ln)
        if "client" not in kv:
            continue
        rows.append(dict(run=run, client=kv["client"], role=kv.get("role"),
                         machine=kv.get("machine"), i=int(num(kv.get("i"))),
                         # t_offset_s is on the DEVICE's clock, ts is the server's.
                         # Devices start at different moments, so offsets are not
                         # directly comparable — never conflate the two.
                         t_offset_s=num(kv.get("t_offset_s")),
                         bucket_s=num(kv.get("bucket_s")), free=num(kv.get("free"))))
    return rows

def parse_broker_ram(run, path):                      # broker_ram_ns.log
    rows = []
    for ln in read_lines(path):
        ts, _, kv = parse_kv_line(ln)
        if "used_mb" not in kv:
            continue
        rows.append(dict(run=run, ts=ts, host=kv.get("host"), source=kv.get("source"),
                         phase=kv.get("phase"), total_mb=num(kv.get("total_mb")),
                         used_mb=num(kv.get("used_mb")), used=num(kv.get("used")),
                         rss_mb=num(kv.get("rabbit_rss_mb")),
                         swap_used_mb=num(kv.get("swap_used_mb"))))
    return rows

def parse_broker_summary(run, path):                  # broker_ram.log
    """BROKER / USED / DELTA / RABBIT / PHASE / COMPARE, one dict per line."""
    rows = []
    for ln in read_lines(path):
        ts, flags, kv = parse_kv_line(ln)
        if not flags:
            continue
        rows.append(dict(run=run, line_kind=flags[0].lower(), phase=kv.get("phase"),
                         **{k: num(v) for k, v in kv.items() if k != "phase"}))
    return rows

def parse_message_size(run, path):                    # message_size.log
    rows = []
    for ln in read_lines(path):
        ts, _, kv = parse_kv_line(ln)
        if "n" not in kv:
            continue
        rows.append(dict(run=run, client=kv.get("client"), role=kv.get("role"),
                         machine=kv.get("machine"),
                         cluster=group_label(kv.get("cluster")),
                         # `mode` is a DataFrame ATTRIBUTE: a column with that
                         # name is shadowed, and `df.mode == "split"` compares a
                         # bound method to a string and yields all-False WITHOUT
                         # raising (guide/08 §8). Renamed at parse time.
                         run_mode=kv.get("mode"), splits=kv.get("splits"),
                         compress=kv.get("compress"), num_bit=kv.get("num_bit"),
                         batch_size=num(kv.get("batch_size")), n=num(kv.get("n")),
                         total_mb=num(kv.get("total_mb")), mean_mb=num(kv.get("mean_mb")),
                         p50_mb=num(kv.get("p50_mb")), p95_mb=num(kv.get("p95_mb")),
                         max_mb=num(kv.get("max_mb")), min_mb=num(kv.get("min_mb")),
                         span_s=num(kv.get("span_s")),
                         rate_mb_s=num(kv.get("rate_mb_s")),
                         per_frame_mb=num(kv.get("per_frame_mb"))))
    return rows

def parse_message_series(run, path):                  # message_size_series.log
    rows = []
    for ln in read_lines(path):
        ts, _, kv = parse_kv_line(ln)
        if "bytes" not in kv:
            continue
        rows.append(dict(run=run, client=kv.get("client"),
                         cluster=group_label(kv.get("cluster")),
                         i=int(num(kv.get("i"))), t_offset_s=num(kv.get("t_offset_s")),
                         batch_id=num(kv.get("batch_id")),
                         n_bytes=num(kv.get("bytes")), mb=num(kv.get("mb"))))
    return rows

# Accuracy is this project's OWN extension, not a guide family, so its earlier
# spelling is this project's to keep reading. Runs archived before the rename
# carry `mAP50=` / `mAP50_95=` — uppercase, which guide/01 §1 does not allow —
# and index their windows with `window=` instead of `i=` plus offsets. Aliasing
# the two spellings at the parser is four lines; the alternative is chart 15
# silently vanishing on every archive older than the rename, which is exactly
# how a missing chart gets read as "this run had no accuracy".
MAP_KEYS = {"map50": ("map50", "mAP50"), "map5095": ("map5095", "mAP50_95")}

def map_num(kv, name):
    return next((num(kv[k]) for k in MAP_KEYS[name] if k in kv), np.nan)

def parse_map(run, path):                             # map.log
    rows = []
    for ln in read_lines(path):
        ts, flg, kv = parse_kv_line(ln)
        # A pre-rename file carries TWO lines per scope: WINDOW (the mean over
        # windows) and ALL (the whole run). The whole-run number is the one the
        # reference lines and the subtitle mean, and the only one the current
        # format writes at all — so the restatement is dropped, not averaged in.
        if np.isnan(map_num(kv, "map50")) or "WINDOW" in flg:
            continue
        rows.append(dict(run=run,
                         scope=("System" if {"SYSTEM", "OVERALL"} & set(flg)
                                else group_label(kv.get("cluster"))),
                         client=kv.get("client"), pooling=kv.get("pooling"),
                         frames=num(kv.get("frames")),
                         matched=num(kv.get("matched")),
                         map50=map_num(kv, "map50"),
                         map5095=map_num(kv, "map5095")))
    return rows

def parse_map_window(run, path):                      # map_window.log
    rows = []
    for ln in read_lines(path):
        ts, _, kv = parse_kv_line(ln)
        if np.isnan(map_num(kv, "map50")):
            continue
        idx = num(kv.get("i", kv.get("window")))
        rows.append(dict(run=run, client=kv.get("client"),
                         cluster=group_label(kv.get("cluster")),
                         i=int(idx) if np.isfinite(idx) else len(rows),
                         t_offset_s=num(kv.get("t_offset_s")),
                         batches=num(kv.get("batches")), frames=num(kv.get("frames")),
                         map50=map_num(kv, "map50"),
                         map5095=map_num(kv, "map5095")))
    return rows
''')


# ─────────────────────────────── load ────────────────────────────────────────
md("## 2 · Load, and print what was loaded")
code(r'''
# An EMPTY list becomes a DataFrame with NO columns, so any later `df.run` raises
# KeyError instead of returning an empty frame. Always declare the columns.
COLS = {
    "rate":  ["run", "scope", "ts", "fps", "steady_fps", "done", "frames", "share"],
    "tl":    ["run", "cluster", "ts", "done", "window_fps"],
    "batch": ["run", "batch", "ts", "window_fps"],
    "lat":   ["run", "scope", "role", "kind", "n",
              "mean_ms", "p50_ms", "p95_ms", "max_ms"],
    "utg":   ["run", "scope", "role", "devices", "utilization",
              "utilization_mean", "busy_s", "total_s", "packages"],
    "utd":   ["run", "client", "role", "packages", "busy_s", "total_s", "utilization"],
    "ev":    ["run", "ts", "description"],          # events_ns.log is optional (01 §2)
    # Optional families (files 8-14). Absent or empty -> a 0-row frame WITH
    # columns, so the charts below can filter it without raising.
    "ftd":   ["run", "client", "role", "machine", "cluster", "dev", "span_s",
              "busy_s", "free_s", "free", "gaps", "longest_free_ms", "host_idle"],
    "ftg":   ["run", "line_kind", "scope", "role", "reason", "kind", "machine",
              "devices", "free", "free_mean", "free_s", "span_s", "busy_s",
              "share", "host_idle"],
    "fts":   ["run", "client", "role", "machine", "i", "t_offset_s",
              "bucket_s", "free"],
    "ram":   ["run", "ts", "host", "source", "phase", "total_mb", "used_mb",
              "used", "rss_mb", "swap_used_mb"],
    "msz":   ["run", "client", "role", "machine", "cluster", "run_mode", "splits",
              "compress", "num_bit", "batch_size", "n", "total_mb", "mean_mb",
              "p50_mb", "p95_mb", "max_mb", "min_mb", "span_s", "rate_mb_s",
              "per_frame_mb"],
    "mss":   ["run", "client", "cluster", "i", "t_offset_s", "batch_id",
              "n_bytes", "mb"],
    # Accuracy — a project extension, not a guide family (guide/README, Scope).
    "map":   ["run", "scope", "client", "pooling", "frames", "matched",
              "map50", "map5095"],
    "mapw":  ["run", "client", "cluster", "i", "t_offset_s", "batches",
              "frames", "map50", "map5095"],
}

def load_all():
    rows = {k: [] for k in COLS}
    ram_sum = []
    for run, d in RUNS.items():
        rows["rate"]  += parse_rate_summary(run,  d / "fps_cluster.log")
        rows["tl"]    += parse_rate_timeline(run, d / "fps_cluster_ns.log")
        rows["batch"] += parse_batch_done(run,    d / "batch_done_ns.log")
        rows["lat"]   += parse_latency(run,       d / "latency_cluster.log")
        rows["utg"]   += parse_util_group(run,    d / "utilization_cluster.log")
        rows["utd"]   += parse_util_device(run,   d / "utilization.log")
        rows["ev"]    += parse_events(run,        d / "events_ns.log")
        rows["ftd"]   += parse_free_time_device(run, d / "free_time.log")
        rows["ftg"]   += parse_free_time_cluster(run,  d / "free_time_cluster.log")
        rows["fts"]   += parse_free_time_series(run, d / "free_time_series.log")
        rows["ram"]   += parse_broker_ram(run,       d / "broker_ram_ns.log")
        rows["msz"]   += parse_message_size(run,     d / "message_size.log")
        rows["mss"]   += parse_message_series(run,   d / "message_size_series.log")
        rows["map"]   += parse_map(run,              d / "map.log")
        rows["mapw"]  += parse_map_window(run,       d / "map_window.log")
        ram_sum       += parse_broker_summary(run,   d / "broker_ram.log")
    frames = {k: pd.DataFrame(v, columns=COLS[k]) for k, v in rows.items()}
    frames["ram_sum"] = pd.DataFrame(ram_sum)
    return frames

F = load_all()
df_rate, df_tl, df_batch = F["rate"], F["tl"], F["batch"]
df_lat, df_utg, df_utd, df_events = F["lat"], F["utg"], F["utd"], F["ev"]
df_ftd, df_ftg, df_fts = F["ftd"], F["ftg"], F["fts"]
df_ram, df_ram_sum, df_msz, df_mss = F["ram"], F["ram_sum"], F["msz"], F["mss"]
df_map, df_mapw = F["map"], F["mapw"]

for name, df in [("rate summary", df_rate), ("rate timeline", df_tl),
                 ("batch timeline", df_batch), ("latency", df_lat),
                 ("utilization/group", df_utg), ("utilization/device", df_utd),
                 ("events", df_events), ("free time/device", df_ftd),
                 ("free time/group", df_ftg), ("free time/series", df_fts),
                 ("broker ram", df_ram), ("message size", df_msz),
                 ("message size/series", df_mss), ("accuracy", df_map),
                 ("accuracy/window", df_mapw)]:
    print(f"{name:<22} {df.shape}")

# Which optional families this run actually carries. The charts consume these
# flags instead of guessing, so a run with a feature off skips its chart and
# SAYS SO rather than shipping an empty axes.
HAS_FREE_TIME = len(df_ftd) > 0
HAS_RAM       = len(df_ram) > 0
HAS_MSG_SIZE  = len(df_msz) > 0
HAS_MAP       = len(df_map) > 0
print(f"\noptional families: free_time={HAS_FREE_TIME}  broker_ram={HAS_RAM}  "
      f"message_size={HAS_MSG_SIZE}  map={HAS_MAP}")

# Entity colour dicts, built ONCE from every group seen across all runs, so a
# chart that filters to one run never repaints the survivors.
GROUPS = sorted({g for g in df_rate.scope.unique() if g != "System"})
SCOPES = GROUPS + ["System"]
GROUP_COLOR = {g: SLOTS[i % len(SLOTS)] for i, g in enumerate(GROUPS)}
GROUP_COLOR["System"] = SLOTS[len(GROUPS) % len(SLOTS)]
ROLES = [r for r in ["cloud", "edge"] if (df_utd.role == r).any()]
print(f"\ngroups: {GROUPS}    roles: {ROLES}    runs: {RUN_ORDER}")
''')


# ───────────────────────── verify assumptions ────────────────────────────────
md("""
## 3 · Verify the assumptions before charting them

Compute what is about to be asserted visually, and **branch on the result**.
Plotting two identical series draws two perfectly overlapping lines: the reader
sees one line and cannot tell whether the other is hidden or missing.
""")
code(r'''
# Same workload? Otherwise the runs are not comparable and no chart will say so.
print("Workload per run (must match to compare):")
print(df_rate[df_rate.scope == "System"][["run", "done", "frames"]].to_string(index=False))
sysw = df_rate[df_rate.scope == "System"]
SAME_WORKLOAD = sysw.done.nunique() <= 1 and sysw.frames.nunique() <= 1
print("=> identical workload." if SAME_WORKLOAD else
      "=> WORKLOAD DIFFERS: these runs are NOT directly comparable (guide/05 §6).")

# Is mean e2e actually configuration-dependent, or identical across runs?
IDENTICAL_E2E = False
if HAS_PAIR:
    piv = df_lat[df_lat.kind == "e2e"].pivot_table(index="scope", columns="run",
                                                   values="mean_ms")
    if set(RUN_ORDER).issubset(piv.columns):
        delta = (piv[RUN_ORDER[0]] - piv[RUN_ORDER[1]]).abs().max()
        IDENTICAL_E2E = bool(delta == 0)
        print(f"\nmax |A - B| mean e2e = {delta:.6f} ms")
        print("=> identical across runs; chart as ONE series."
              if IDENTICAL_E2E else "=> differs; both runs plotted separately.")

# A utilization above 100% is a measurement bug, not a fast device (03 §2.1).
over = df_utd[df_utd.utilization > 100]
print(f"\ndevices reporting >100% utilization: {len(over)}  (must be 0)")
if len(over):
    print(over.to_string(index=False))

# service samples must sum to busy_s for the matching scope/role (04 §2.1).
# Both come from the same device's clock, so this is an exact identity.
chk = (df_lat[df_lat.kind == "service"]
       .assign(sum_s=lambda d: d.n * d.mean_ms / 1000.0)
       .merge(df_utg[df_utg.role != "all"], on=["run", "scope", "role"],
              suffixes=("", "_utg")))
if len(chk):
    chk = chk.assign(delta_s=(chk.sum_s - chk.busy_s).abs())
    worst = chk.delta_s.max()
    print(f"\nmax |sum(service) - busy_s| = {worst:.4f}s over {len(chk)} scope/role pairs"
          f"  ({'OK' if worst < 0.5 else 'MISMATCH — the two are instrumented at different points'})")

# ---- optional families: the invariants that hold BY CONSTRUCTION -----------
if HAS_FREE_TIME:
    # busy + free == span exactly. A failure means intervals escaped the run
    # window, or the clip step is missing.
    worst = (df_ftd.busy_s + df_ftd.free_s - df_ftd.span_s).abs().max()
    print(f"\nfree time: max |busy + free - span| = {worst:.4f}s over "
          f"{len(df_ftd)} devices  ({'OK' if worst < 0.002 else 'BROKEN'})")

    # Reason shares must sum to 100% of each scope's free time: attribution is
    # priority-ordered, so nothing is claimed twice and `unaccounted` absorbs
    # whatever no reason covers.
    shares = (df_ftg[df_ftg.line_kind == "reason"]
              .groupby(["run", "scope"]).share.sum())
    print(f"free time: reason shares per scope -> "
          f"{', '.join(f'{s:.1f}%' for s in shares)}  "
          f"({'OK' if (shares - 100.0).abs().max() < 0.5 else 'ATTRIBUTION LEAKS'})")

    # Per-kind sums OVERLAP across lanes by construction, so they must come out
    # ABOVE the merged busy total. Equal means the lanes were summed instead of
    # merged — the one error the whole free-time method exists to prevent, and
    # it makes free time read far too low with nothing else looking wrong.
    for (run, scope), grp in df_ftg[df_ftg.line_kind == "kind"].groupby(["run", "scope"]):
        # System spans every device in the run: no DEVICE row carries
        # cluster="System", so matching on the cluster leaves the denominator at
        # 0 and prints "nanx ... SUMMED, NOT MERGED" — a false alarm on the one
        # scope that covers the whole fleet.
        of_run = df_ftd[df_ftd.run == run]
        merged = (of_run if scope == "System" else of_run[of_run.cluster == scope]).busy_s.sum()
        if not merged:
            print(f"free time: {scope} has no device busy total to check per-kind against")
            continue
        ratio = grp.busy_s.sum() / merged
        print(f"free time: {scope} per-kind sum is {ratio:.2f}x the merged busy  "
              f"({'OK — merged' if ratio > 1.001 else 'SUMMED, NOT MERGED'})")

if HAS_RAM:
    src_kinds = sorted(df_ram.source.unique())
    print(f"\nbroker RAM: {len(df_ram)} samples, source(s) {src_kinds}, "
          f"phases {sorted(df_ram.phase.unique())}")
    if "rabbitmq_api" in src_kinds:
        print("  note: source=rabbitmq_api reports the BROKER PROCESS, not the host — "
              "a different quantity from source=ssh, never silently substituted.")

if HAS_MSG_SIZE:
    bad = df_msz[~((df_msz.min_mb <= df_msz.p50_mb) & (df_msz.p50_mb <= df_msz.p95_mb)
                   & (df_msz.p95_mb <= df_msz.max_mb))]
    print(f"\nmessage size: {len(df_msz)} measuring worker(s) "
          f"(exactly 1 expected), sizes out of order: {len(bad)} (must be 0)")
''')


# ─────────────────────────────── C1 ──────────────────────────────────────────
md("""
## 01 · Throughput by cluster

`fps_cluster.log`. The two series are the two throughput questions: **whole run**
(`fps`, warm-up included — what the user experienced) and **steady state**
(`steady_fps`, warm-up dropped — what you compare configurations with).

`SYSTEM` carries no `steady_fps` by spec (01 §3.3), so the System category
correctly shows one bar. **Cluster bars do not sum to the System bar** and should
not be expected to — each cluster divides by its own span.
""")
guarded_code("not len(df_rate)",
             "fps_cluster.log has no rows — 01 skipped.", r'''
n_panels = max(len(RUN_ORDER), 1)
fig, axes = plt.subplots(1, n_panels, figsize=(7.6 * n_panels, 4.9), sharey=True)
axes = np.atleast_1d(axes)
SERIES = [("fps", "Whole run", S1), ("steady_fps", "Steady state", S2)]
x, width = np.arange(len(SCOPES)), 0.36

ymax = np.nanmax(df_rate[["fps", "steady_fps"]].to_numpy()) if len(df_rate) else 1.0
for ax, run in zip(axes, RUN_ORDER or [None]):
    sub = df_rate[df_rate.run == run].set_index("scope") if run else df_rate.set_index("scope")
    for i, (col, lbl, colour) in enumerate(SERIES):
        vals = [float(sub.loc[s, col]) if s in sub.index else np.nan for s in SCOPES]
        b = ax.bar(x + (i - 0.5) * (width + 0.03), vals, width,
                   label=lbl, color=colour, **BAR_KW)
        label_bars(ax, b, fmt="{:.1f}")
    ax.set_xticks(x, SCOPES)
    ax.set_ylim(0, ymax * 1.22)
    ax.grid(axis="x", visible=False)
    if len(RUN_ORDER) > 1:
        ax.set_title(run)

axes[0].set_ylabel("Throughput (FPS)")
legend_above(axes[0], ncol=2, side="left")
if RUN_ORDER:
    sysrow = df_rate[(df_rate.run == RUN_ORDER[0]) & (df_rate.scope == "System")]
    if len(sysrow):
        subtitle(axes[0], f"system throughput {float(sysrow.fps.iloc[0]):.1f} FPS")
fig.suptitle("Throughput by cluster", fontsize=14,
             fontweight="semibold", color=INK, y=1.04)
fig.tight_layout()
finish(fig, "01_throughput_by_cluster.png")
''')


# ─────────────────────────────── C2 ──────────────────────────────────────────
md("""
## 02 · System throughput over the run

`batch_done_ns.log` — the authoritative system-wide series — against **seconds
into the run**, with `events_ns.log` overlaid as vertical rules. Both files are on
the server clock, which is the entire reason for the one-clock rule (01 §1).

The raw reading is noisy, so a 31-reading centred mean carries the trend and the
raw series recedes behind it. The first `W-1 = 15` batches carry no `window_fps`,
so the line legitimately starts a little after t=0 (01 §3.1).
""")
guarded_code("not len(df_batch)",
             "batch_done_ns.log has no windowed rows — 02 skipped.", r'''
n_panels = max(len(RUN_ORDER), 1)
fig, axes = plt.subplots(n_panels, 1, figsize=(13, 4.4 * n_panels), sharex=False)
axes = np.atleast_1d(axes)                      # each run has its own duration
ymax = (df_batch.window_fps.max() * 1.30) if len(df_batch) else 1.0

for ax, run in zip(axes, RUN_ORDER):
    s = df_batch[df_batch.run == run].sort_values("ts")
    if s.empty:
        continue
    secs = seconds_axis(s)
    colour = RUN_COLOR[run]
    m = s.window_fps.mean()
    ax.plot(secs, s.window_fps, color=colour, alpha=0.42, linewidth=1.2, label="reading")
    roll = s.window_fps.rolling(31, center=True, min_periods=1).mean()
    # The statistic lives IN the legend label, not in a floating annotation: it
    # stays attached to the series and can never collide with the curve, which
    # is exactly what an anchored `mean 9.4` at the left edge does.
    ax.plot(secs, roll, color=colour, label=f"31-reading mean  (run mean {m:.1f})",
            **LINE_KW)
    ax.axhline(m, color=colour, linewidth=1.0, alpha=0.45)     # reference: must recede
    endpoint(ax, secs.iloc[-1], float(roll.iloc[-1]), colour)

    ax.set_ylim(0, ymax)                        # BEFORE annotating, so the rules
    top = ax.get_ylim()[1]                      # anchor to the real top, not the data
    ev = df_events[df_events.run == run]
    t0 = int(s.ts.iloc[0])
    for j, (_, e) in enumerate(ev.iterrows()):
        ax.axvline((int(e.ts) - t0) / 1e9, color=MUTED, linewidth=1.0)
        ax.annotate(e.description, xy=((int(e.ts) - t0) / 1e9, top),
                    xytext=(4, -9 - 12 * (j % 3)),   # stagger: events cluster in time
                    textcoords="offset points", fontsize=8, color=MUTED, va="top")

    ax.set_ylabel("Rolling window FPS")
    ax.grid(axis="both", visible=True)
    legend_above(ax, ncol=2, side="right")
    if len(RUN_ORDER) > 1:
        ax.set_title(f"{run}  —  {len(ev)} control event(s)")

axes[-1].set_xlabel("seconds into the run")
fig.suptitle("System throughput over the run", fontsize=14,
             fontweight="semibold", color=INK, y=1.04)
fig.tight_layout()
finish(fig, "02_system_window_fps.png")
''')


# ─────────────────────────────── C3 ──────────────────────────────────────────
md("""
## 03 · Throughput per cluster over the run

`fps_cluster_ns.log`, same seconds axis as 02. A cluster reaches its first full
window later than the system does, and clusters end at different times — both are
correct, not truncated series.
""")
guarded_code("not len(df_tl)",
             "fps_cluster_ns.log has no windowed rows — 03 skipped.", r'''
n_panels = max(len(RUN_ORDER), 1)
fig, axes = plt.subplots(n_panels, 1, figsize=(13, 3.9 * n_panels), sharex=False)
axes = np.atleast_1d(axes)
ymax = (df_tl.window_fps.max() * 1.28) if len(df_tl) else 1.0

for ax, run in zip(axes, RUN_ORDER):
    sub_run = df_tl[df_tl.run == run].sort_values("ts")
    if sub_run.empty:
        continue
    t0 = int(sub_run.ts.iloc[0])                # shared baseline across clusters
    for group in GROUPS:
        s = sub_run[sub_run.cluster == group]
        if not len(s):
            continue
        secs = (s.ts.astype("int64") - t0) / 1e9
        ax.plot(secs, s.window_fps, color=GROUP_COLOR[group], label=group, **LINE_KW)
        endpoint(ax, secs.iloc[-1], float(s.window_fps.iloc[-1]), GROUP_COLOR[group])
    ax.set_ylabel("Rolling window FPS")
    ax.set_ylim(0, ymax)
    ax.grid(axis="both", visible=True)
    # One series needs no legend — the endpoint label carries the identity.
    if len(GROUPS) >= 2:
        legend_above(ax, ncol=min(len(GROUPS), 4), side="right")
    if len(RUN_ORDER) > 1:
        ax.set_title(run)

axes[-1].set_xlabel("seconds into the run")
fig.suptitle("Throughput per cluster over the run", fontsize=14,
             fontweight="semibold", color=INK, y=1.04)
fig.tight_layout()
finish(fig, "03_cluster_window_fps.png")
''')


# ─────────────────────────────── C4 ──────────────────────────────────────────
md("""
## C4 · Window rate distribution

A high mean with a wide box is a worse result than a slightly lower mean with a
tight one.
""")
guarded_code("not len(df_tl)",
             "fps_cluster_ns.log has no windowed rows — 04 skipped.", r'''
fig, ax = plt.subplots(figsize=(9.0, 4.9))

positions, data, colors = [], [], []
for ci, group in enumerate(GROUPS):
    for ri, run in enumerate(RUN_ORDER):
        vals = df_tl[(df_tl.cluster == group) & (df_tl.run == run)].window_fps.values
        positions.append(ci + (ri - (len(RUN_ORDER) - 1) / 2) * 0.34)
        data.append(vals)
        colors.append(RUN_COLOR[run])

if any(len(v) for v in data):
    bp = ax.boxplot(data, positions=positions, widths=0.28, patch_artist=True,
                    showfliers=False,
                    medianprops=dict(color=SURFACE, linewidth=1.8),  # reads on the fill
                    whiskerprops=dict(color=AXIS, linewidth=1.0),
                    capprops=dict(color=AXIS, linewidth=1.0))
    for patch, c in zip(bp["boxes"], colors):
        patch.set_facecolor(c); patch.set_edgecolor(SURFACE); patch.set_linewidth(1.2)

    # Label the mean ABOVE THE WHISKER CAP — p75 sits inside the whisker.
    for pos, vals in zip(positions, data):
        if not len(vals):
            continue
        q1, q3 = np.percentile(vals, [25, 75])
        whisker_top = vals[vals <= q3 + 1.5 * (q3 - q1)].max()
        ax.annotate(f"{vals.mean():.1f}", xy=(pos, whisker_top),
                    xytext=(0, 7), textcoords="offset points",
                    ha="center", fontsize=9, color=INK_2)

    # Headroom for the mean labels, which sit above the whisker cap and would
    # otherwise be clipped by the top spine (boxplot does not autoscale for them).
    lo, hi = ax.get_ylim()
    ax.set_ylim(lo, hi + (hi - lo) * 0.12)

    # boxplot produces NO legend handles — build Rectangle proxies.
    if len(RUN_ORDER) >= 2:
        handles = [plt.Rectangle((0, 0), 1, 1, color=RUN_COLOR[r]) for r in RUN_ORDER]
        legend_above(ax, ncol=len(RUN_ORDER), side="right",
                     handles=handles, labels=list(RUN_ORDER))

ax.set_xticks(range(len(GROUPS)), GROUPS)
ax.set_ylabel("Rolling window FPS"); ax.set_xlabel("Cluster")
ax.set_title("Window FPS distribution  (box = IQR, whiskers = 1.5×IQR)")
ax.grid(axis="x", visible=False)
finish(fig, "04_window_fps_distribution.png")
''')


# ─────────────────────────────── C5 ──────────────────────────────────────────
md("""
## C5 · Service latency by role — two panels

**This is the dual-axis replacement.** `sharey` is deliberately omitted: each
panel is honestly its own scale and the panel titles say which is which.

`kind=service` is charted, not `pipeline` — `pipeline` measures buffering, not
device speed (04 §2.2).
""")
guarded_code('not (df_lat.kind == "service").any()',
             "latency_cluster.log has no kind=service rows — 05 skipped.", r'''
svc = df_lat[df_lat.kind == "service"]
panel_roles = ROLES or ["all"]
# One x slot per cluster, or per cluster x run when several runs are loaded.
cols = ([(g, r) for g in GROUPS for r in RUN_ORDER] if len(RUN_ORDER) > 1
        else [(g, RUN_ORDER[0] if RUN_ORDER else None) for g in GROUPS])
ticks = [g if len(RUN_ORDER) <= 1 else f"{short_label(g)}\n{r}" for g, r in cols]

fig, axes = plt.subplots(1, len(panel_roles), figsize=(6.2 * len(panel_roles), 4.8))
axes = np.atleast_1d(axes)                       # NOT sharey — that is the point
SERIES = [("mean_ms", "Mean", S1), ("p95_ms", "p95", S2)]
x, width = np.arange(len(cols)), 0.36

for ax, role in zip(axes, panel_roles):
    sub = svc[svc.role == role]
    peak = 0.0
    for i, (col, lbl, colour) in enumerate(SERIES):
        vals = []
        for g, r in cols:
            m = sub[(sub.scope == g) & ((sub.run == r) if r else True)]
            vals.append(float(m[col].iloc[0]) if len(m) else np.nan)
        b = ax.bar(x + (i - 0.5) * (width + 0.03), vals, width,
                   label=lbl, color=colour, **BAR_KW)
        # Thousands separators: 18194 ms is unreadable without them.
        label_bars(ax, b, fmt="{:,.0f}")
        peak = max(peak, np.nanmax(vals) if not np.isnan(vals).all() else 0.0)
    ax.set_xticks(x, ticks)
    ax.set_title(f"{role.capitalize()} devices")
    if peak:
        ax.set_ylim(0, peak * 1.20)
    ax.grid(axis="x", visible=False)

axes[0].set_ylabel("Service latency (ms)")
fig.suptitle("Service latency by device role  (lower is better)", fontsize=14,
             fontweight="semibold", color=INK, y=1.13)
fig.tight_layout()
# Figure-level legend: one legend for both panels, sitting ABOVE the panel
# titles. Anchored to an axes it lands on the first panel's title instead.
fig.legend([plt.Rectangle((0, 0), 1, 1, color=c) for _, _, c in SERIES],
           [lbl for _, lbl, _ in SERIES], loc="upper center",
           bbox_to_anchor=(0.5, 1.06), ncol=2, frameon=False, columnspacing=2.2)
finish(fig, "05_service_latency_by_role.png")
''')


# ─────────────────────────────── C6 ──────────────────────────────────────────
md("""
## C6 · End-to-end latency profile

`kind=e2e`. **Indicative, not exact** — `e2e` spans two machines by definition and
inherits any offset between their clocks (04 §2.3).
""")
guarded_code('not (df_lat.kind == "e2e").any()',
             "latency_cluster.log has no kind=e2e rows — 06 skipped.", r'''
e2e = df_lat[df_lat.kind == "e2e"]
stats  = [("mean_ms", "Mean"), ("p50_ms", "p50"), ("p95_ms", "p95"), ("max_ms", "Max")]
scopes = [s for s in SCOPES if (e2e.scope == s).any()]

fig, axes = plt.subplots(1, max(len(scopes), 1),
                         figsize=(4.9 * max(len(scopes), 1), 4.8), sharey=True)
axes = np.atleast_1d(axes)                       # sharey: same measure across scopes
x, width = np.arange(len(stats)), 0.36 if HAS_PAIR else 0.55
ymax = (np.nanmax(e2e[[c for c, _ in stats]].to_numpy()) / 1000.0) if len(e2e) else 1.0

for ax, scope in zip(axes, scopes):
    sub = e2e[e2e.scope == scope].set_index("run")
    for i, run in enumerate(RUN_ORDER):
        if run not in sub.index:
            continue
        vals = [float(np.ravel(sub.loc[run, col])[0]) / 1000.0 for col, _ in stats]
        # Single run -> one blue series: the profile IS the story, and a second
        # hue would imply a comparison that isn't there.
        b = ax.bar(x + (i - (len(RUN_ORDER) - 1) / 2) * (width + 0.03), vals, width,
                   label=run, color=RUN_COLOR[run], **BAR_KW)
        label_bars(ax, b, fmt="{:.1f}", fontsize=9 if len(RUN_ORDER) == 1 else 8.5)
    ax.set_xticks(x, [lbl for _, lbl in stats])
    ax.set_title(scope)
    ax.set_ylim(0, ymax * 1.16)
    ax.grid(axis="x", visible=False)

axes[0].set_ylabel("End-to-end latency (s)")
if len(RUN_ORDER) >= 2:                      # one series needs no legend
    legend_above(axes[0], ncol=len(RUN_ORDER), side="left")
fig.suptitle("End-to-end latency profile  (lower is better)",
             fontsize=14, fontweight="semibold", color=INK,
             y=1.05 if len(RUN_ORDER) >= 2 else 1.01)
fig.tight_layout()
subtitle(axes[0], "indicative — e2e spans two machine clocks", dy=-0.16)
finish(fig, "06_e2e_latency_profile.png")
''')


# ─────────────────────────────── C7 ──────────────────────────────────────────
md("""
## C7 · Utilization by group and role

Pooled `utilization` (Σbusy / Σtotal). Where it diverges from `utilization_mean`,
the group is imbalanced (03 §6) — both are annotated on the `ALL` rows.
""")
guarded_code("not len(df_utg)",
             "utilization_cluster.log has no rows — 07 skipped.", r'''
rows, labels = [], []
for g in GROUPS:
    for r in ROLES:
        if ((df_utg.scope == g) & (df_utg.role == r)).any():
            rows.append((g, r)); labels.append(f"{short_label(g)}\n{r}")
rows.append(("System", "all")); labels.append("System\nall")

idx = df_utg.set_index(["run", "scope", "role"]).sort_index()
n_panels = max(len(RUN_ORDER), 1)
fig, axes = plt.subplots(1, n_panels, figsize=(max(8.4, 1.7 * len(rows)) * n_panels, 5.0),
                         sharey=True)
axes = np.atleast_1d(axes)
x = np.arange(len(rows))

def cell(run, scope, role, col):
    try:
        v = np.ravel(idx.loc[(run, scope, role), col])   # guards a Series on dup keys
        return float(v[0]) if len(v) else np.nan
    except KeyError:
        return np.nan

for ax, run in zip(axes, RUN_ORDER or [None]):
    vals = [cell(run, s, r, "utilization") for s, r in rows]
    # Colour follows the ENTITY (the role), never the bar's position or rank.
    colours = [ROLE_COLOR.get(r, MUTED) if r != "all" else NEUTRAL for _, r in rows]
    b = ax.bar(x, vals, 0.62, color=colours, **BAR_KW)
    label_bars(ax, b, fmt="{:.1f}%", fontsize=9.5)

    # Where pooled and mean diverge, the cluster is imbalanced (03 §6).
    for xi, (s, r) in enumerate(rows):
        if r != "all" and s != "System":
            continue
        pooled, mean = cell(run, s, r, "utilization"), cell(run, s, r, "utilization_mean")
        if np.isnan(pooled) or np.isnan(mean) or abs(pooled - mean) < 2.0:
            continue
        ax.annotate(f"mean {mean:.1f}%", xy=(xi, pooled), xytext=(0, 26),
                    textcoords="offset points", ha="center", fontsize=8, color=MUTED)

    ax.set_xticks(x, labels)             # two-line ticks beat rotation
    ax.set_ylim(0, 118)                  # percentages: fix the ceiling, never autoscale
    ax.grid(axis="x", visible=False)
    if len(RUN_ORDER) > 1:
        ax.set_title(run)

axes[0].set_ylabel("Utilization (%)")
handles = ([plt.Rectangle((0, 0), 1, 1, color=ROLE_COLOR[r]) for r in ROLES]
           + [plt.Rectangle((0, 0), 1, 1, color=NEUTRAL)])
legend_above(axes[0], ncol=len(ROLES) + 1, side="right",
             handles=handles, labels=[r.capitalize() for r in ROLES] + ["All devices"])

# State the verdict in words: colour alone must never carry the finding.
# Pool across clusters rather than reading one — "averages" has to mean it.
if RUN_ORDER and len(ROLES) == 2:
    head_rows = df_utg[(df_utg.run == RUN_ORDER[0]) & (df_utg.scope != "System")]
    def role_pooled(role):
        r = head_rows[head_rows.role == role]
        return 100.0 * r.busy_s.sum() / r.total_s.sum() if r.total_s.sum() else np.nan
    a, c = role_pooled(ROLES[0]), role_pooled(ROLES[1])
    if not (np.isnan(a) or np.isnan(c)):
        slow = ROLES[0] if a > c else ROLES[1]
        verdict = ("the split is balanced" if abs(a - c) < 20 else
                   f"the {slow} side is the bottleneck")
        subtitle(axes[0], f"{ROLES[0]} averages {a:.0f}% against {c:.0f}% "
                          f"on the {ROLES[1]} — {verdict}")
fig.suptitle("Device utilization by cluster and role", fontsize=14,
             fontweight="semibold", color=INK, y=1.04)
fig.tight_layout()
finish(fig, "07_utilization_by_role.png")
''')


# ─────────────────────────────── C8 ──────────────────────────────────────────
md("""
## C8 · Per-device utilization

Hunting stragglers. A bar above 100% would be a **measurement bug**, not a fast
device (03 §2.1) — it is deliberately not clipped.
""")
guarded_code("not len(df_utd)",
             "utilization.log has no rows — 08 skipped.", r'''
n_panels = max(len(RUN_ORDER), 1)
fig, axes = plt.subplots(1, n_panels, figsize=(max(11, 0.9 * len(df_utd)) if n_panels == 1
                                              else 7.4 * n_panels, 4.9), sharey=True)
axes = np.atleast_1d(axes)

for ax, run in zip(axes, RUN_ORDER):
    sub = (df_utd[df_utd.run == run]
           .sort_values(["role", "utilization"], ascending=[True, False])
           .reset_index(drop=True))
    if not len(sub):
        continue
    pos = np.arange(len(sub))
    b = ax.bar(pos, sub.utilization, width=0.72,
               color=[ROLE_COLOR.get(r, MUTED) for r in sub.role], **BAR_KW)
    label_bars(ax, b, fmt="{:.0f}", fontsize=9, color=MUTED)
    # cumcount numbers WITHIN each role -> C1 C2 E1 E2. A running enumerate
    # gives C1 C2 E3 E4, which reads as missing devices.
    ticks = sub.groupby("role").cumcount() + 1
    ax.set_xticks(pos, [f"{r[0].upper()}{n}" for r, n in zip(sub.role, ticks)],
                  fontsize=9)
    ax.set_xlabel("Device  (C = Cloud, E = Edge)")
    ax.set_ylim(0, 118)
    ax.grid(axis="x", visible=False)
    if len(RUN_ORDER) > 1:
        ax.set_title(f"{run}  —  mean {sub.utilization.mean():.1f}%")

axes[0].set_ylabel("Utilization (%)")
if ROLES:
    handles = [plt.Rectangle((0, 0), 1, 1, color=ROLE_COLOR[r]) for r in ROLES]
    legend_above(axes[0], ncol=len(ROLES), side="right",
                 handles=handles, labels=[r.capitalize() for r in ROLES])

# The aggregate lives in the title: it is the reference the bars are read against.
head = f" — {df_utd[df_utd.run == RUN_ORDER[0]].utilization.mean():.1f}% mean across " \
       f"{len(df_utd[df_utd.run == RUN_ORDER[0]])} devices" if len(RUN_ORDER) == 1 else ""
fig.suptitle(f"Per-device utilization{head}", fontsize=14,
             fontweight="semibold", color=INK, y=1.04)
fig.tight_layout()
finish(fig, "08_device_utilization.png")
''')


# ─────────────────────────────── C9 ──────────────────────────────────────────
# ─────────────────────────────── 09 ──────────────────────────────────────────
md("""
## 09 · Run comparison — verdict bar

Only drawn when two runs are loaded. `events_ns.log` needs no chart of its own —
its rules are overlaid on 02, where they can be read against the throughput they
were supposed to change.

**Colour keys to the verdict, never the sign.** A `+29%` latency move is a
regression; painting it like a `+21%` throughput gain is a lie the reader cannot
detect. Colour therefore carries status semantics, so the verdict is also spelled
out in text.
""")
code(r'''
if not HAS_PAIR:
    print(f"09 needs exactly two runs to compare; found {len(RUN_ORDER)}. Skipped.")
else:
    A, B = RUN_ORDER[0], RUN_ORDER[1]

    def rate_of(run):
        s = df_rate[(df_rate.run == run) & (df_rate.scope == "System")]
        return float(s.fps.iloc[0]) if len(s) else np.nan

    def e2e_of(run, col):
        s = df_lat[(df_lat.run == run) & (df_lat.scope == "System") &
                   (df_lat.kind == "e2e")]
        return float(s[col].iloc[0]) / 1000.0 if len(s) else np.nan

    def util_of(run):
        s = df_utg[(df_utg.run == run) & (df_utg.scope == "System")]
        return float(s.utilization.iloc[0]) if len(s) else np.nan

    # goal: +1 higher is better, -1 lower is better, 0 direction-neutral
    metrics = [
        ("System throughput",  rate_of(A),          rate_of(B),
         "{:.2f} f/s", "higher is better", +1),
        ("Mean E2E latency",   e2e_of(A, "mean_ms"), e2e_of(B, "mean_ms"),
         "{:.1f} s",   "lower is better",  -1),
        ("p95 E2E latency",    e2e_of(A, "p95_ms"),  e2e_of(B, "p95_ms"),
         "{:.1f} s",   "lower is better",  -1),
        ("System utilization", util_of(A),          util_of(B),
         "{:.1f} %",   "workload dependent", 0),
    ]
    metrics = [m for m in metrics if not (np.isnan(m[1]) or np.isnan(m[2]) or m[2] == 0)]

    pct, colors, verdicts = [], [], []
    for _, a, b, _, _, goal in metrics:
        p = (a / b - 1) * 100
        score = goal * np.sign(p)
        pct.append(p)
        colors.append(GOOD if score > 0 else BAD if score < 0 else NEUTRAL)
        verdicts.append("better" if score > 0 else "worse" if score < 0
                        else ("no change" if p == 0 else "neutral"))

    y = np.arange(len(metrics))[::-1]
    fig, ax = plt.subplots(figsize=(11, 1.15 * len(metrics) + 1.6))
    ax.barh(y, pct, height=0.5, color=colors, **BAR_KW)
    ax.axvline(0, color=AXIS, linewidth=1.0)

    for yi, p, v in zip(y, pct, verdicts):
        ax.annotate(f"{p:+.1f}%  {v}", xy=(p, yi), xytext=(6 if p >= 0 else -6, 0),
                    textcoords="offset points", va="center",
                    ha="left" if p >= 0 else "right",
                    fontsize=10, fontweight="semibold", color=INK)

    # Absolute values live in the tick label — no floating text to collide.
    ax.set_yticks(y, [f"{n}\nA {f.format(a)}  ·  B {f.format(b)}\n({h})"
                      for n, a, b, f, h, _ in metrics], fontsize=9.5)
    ax.tick_params(axis="y", colors=INK_2)
    lim = max(abs(p) for p in pct) * 1.9 + 4
    ax.set_xlim(-lim, lim)                 # symmetric, or bar lengths mislead
    ax.set_xlabel("Change of A relative to B (%)")
    ax.set_title(f"A = {A}   vs   B = {B}  (B = baseline)")
    ax.annotate("Colour marks the verdict (green better / red worse), not the sign "
                "of the change",
                xy=(0.5, -0.30), xycoords="axes fraction", ha="center",
                fontsize=9, color=MUTED)
    ax.grid(axis="y", visible=False)
    finish(fig, "09_run_comparison.png", hide_spines=("top", "right", "left"))
''')


# ─────────────────────────────── 10 ──────────────────────────────────────────
md("""
## 10 · Free time per device

`free_time.log`. **Free time is not `1 − utilization`.** Utilization measures one
lane's `get input → output` window, so a back-pressure wait *inside* that window
counts as busy there and free here, while work on another lane (capture, encode,
publish) counts as busy here and as nothing there. The two answer different
questions and neither derives from the other (guide/10 §1) — which is why 07 and
this chart legitimately disagree.

Every bar is the same height because every bar is that device's whole run.
""")
code(r'''
if not HAS_FREE_TIME:
    print("free_time.log absent or empty — 10 skipped (optional family, guide/01 §2).")
else:
    n_panels = max(len(RUN_ORDER), 1)
    fig, axes = plt.subplots(1, n_panels, figsize=(max(10, 1.15 * len(df_ftd)), 5.0),
                             sharey=True)
    axes = np.atleast_1d(axes)

    for ax, run in zip(axes, RUN_ORDER):
        sub = (df_ftd[df_ftd.run == run]
               .sort_values(["role", "free"], ascending=[True, False])
               .reset_index(drop=True))
        if not sub.empty:
            pos = np.arange(len(sub))
            busy_pct = 100.0 - sub.free
            ax.bar(pos, busy_pct, width=0.72, color=S1, label="Busy", **BAR_KW)
            b = ax.bar(pos, sub.free, width=0.72, bottom=busy_pct,
                       color=S2, label="Free", **BAR_KW)
            # Direct labels: the free share IS the finding, and stacked segments
            # cannot be read off a shared axis.
            for xi, value in zip(pos, sub.free):
                ax.annotate(f"{value:.0f}%", xy=(xi, 100.0), xytext=(0, 4),
                            textcoords="offset points", ha="center",
                            fontsize=9, color=INK_2)
            # host_idle is the OS's own view across ALL processes on the box.
            # Where it disagrees with pipeline free time, something else is on
            # the machine (or the pipeline is blocked on I/O, not compute).
            ok = sub.host_idle.notna()
            if ok.any():
                ax.plot(pos[ok.values], sub.host_idle[ok], "D", color=MUTED,
                        markersize=5, linestyle="none", label="Host idle (OS)",
                        **{k: v for k, v in MARK_KW.items() if k != "markersize"})
            ticks = sub.groupby("role").cumcount() + 1
            ax.set_xticks(pos, [f"{r[0].upper()}{n}" for r, n in zip(sub.role, ticks)],
                          fontsize=9)
            ax.set_xlabel("Device  (C = Cloud, E = Edge)")
        ax.set_ylim(0, 118)          # percentages: fix the ceiling, never autoscale
        ax.grid(axis="x", visible=False)
        if len(RUN_ORDER) > 1:
            ax.set_title(run)

    axes[0].set_ylabel("Share of the device's run (%)")
    legend_above(axes[0], ncol=3, side="right")
    head = df_ftd[df_ftd.run == RUN_ORDER[0]] if RUN_ORDER else df_ftd
    pooled = 100.0 * head.free_s.sum() / head.span_s.sum() if head.span_s.sum() else 0.0
    subtitle(axes[0], f"the fleet was idle {pooled:.0f}% of its wall clock — "
                      f"free time is a CAPACITY measure, not a performance one")
    fig.suptitle("Free time per device", fontsize=14,
                 fontweight="semibold", color=INK, y=1.05)
    fig.tight_layout()
    finish(fig, "10_free_time_by_device.png")
''')


# ─────────────────────────────── 11 ──────────────────────────────────────────
md("""
## 11 · Why the fleet was free, and where the busy time went

`free_time_cluster.log`. Two panels because the two breakdowns have **different
totals**, and putting them on one axis would imply they are comparable:

- **`FREE reason=` sums to exactly 100%** of the scope's free time. Attribution is
  priority-ordered on the device, so no moment is claimed twice and `unaccounted`
  absorbs whatever no reason covers.
- **`KIND` may sum to MORE than 100%** — per-kind durations overlap across lanes
  by construction. Only the merged `busy_s` is exclusive.
""")
code(r'''
if not HAS_FREE_TIME:
    print("free_time_cluster.log absent or empty — 11 skipped.")
else:
    reasons = df_ftg[df_ftg.line_kind == "reason"]
    kinds   = df_ftg[df_ftg.line_kind == "kind"]
    scopes  = sorted(set(reasons.scope) | set(kinds.scope))

    fig, axes = plt.subplots(1, 2, figsize=(13, 4.9))     # NOT sharey: different totals
    for ax, (src, title, ylab) in zip(axes, [
            (reasons, "Why it was free  (sums to 100%)", "Share of free time (%)"),
            (kinds,   "Where busy time went  (may exceed 100%)", "Share of the run (%)")]):
        col = "reason" if src is reasons else "kind"
        names = sorted(src[col].dropna().unique(),
                       key=lambda v: -src[src[col] == v].share.sum())
        # guide/06 §2: a 9th series is never a generated hue — fold it into
        # "Other". This run reports NINE busy kinds, and `SLOTS[i % 8]` handed
        # the ninth slot 0's blue, so `metrics` and `inference` came out the
        # same colour in one legend. The tail is folded, not cycled, and the
        # names are already ordered by share so the tail is the small ones.
        if len(names) > len(SLOTS):
            tail = set(names[len(SLOTS) - 1:])
            src = src.assign(**{col: src[col].where(~src[col].isin(tail), "Other")})
            names = names[:len(SLOTS) - 1] + ["Other"]
        # Colour follows the ENTITY, bound once, so filtering never repaints it.
        # "Other" is an aggregate rather than an entity, so it takes the muted
        # chrome grey instead of a categorical slot.
        colour_of = {name: (MUTED if name == "Other" else SLOTS[i])
                     for i, name in enumerate(names)}
        x, width = np.arange(len(scopes)), 0.8 / max(len(names), 1)
        for i, name in enumerate(names):
            vals = [float(src[(src.scope == s) & (src[col] == name)].share.sum())
                    for s in scopes]
            b = ax.bar(x + (i - (len(names) - 1) / 2) * width, vals, width * 0.92,
                       label=name, color=colour_of[name], **BAR_KW)
            label_bars(ax, b, fmt="{:.0f}", fontsize=8)
        ax.set_xticks(x, scopes)
        # legend_above anchors the legend's BOTTOM to the axes top, so the title
        # must clear the WHOLE legend block. A fixed pad clears one row; the two
        # panels here carry different numbers of series, and the busy panel's
        # three rows were drawn straight through its own title. Scale the pad by
        # the row count instead.
        ncol = min(len(names), 4)
        rows = -(-len(names) // ncol)          # ceil: legend rows this panel needs
        ax.set_title(title, pad=17 * (rows + 1))
        ax.set_ylabel(ylab)
        ax.grid(axis="x", visible=False)
        ax.set_ylim(0, max(105, ax.get_ylim()[1] * 1.12))
        legend_above(ax, ncol=ncol, side="center")

    fig.suptitle("Free-time attribution and busy breakdown", fontsize=14,
                 fontweight="semibold", color=INK, y=1.10)
    fig.tight_layout()
    finish(fig, "11_free_time_attribution.png")

    # MACHINE lines come from the UNION of the intervals of the device processes
    # on that host, never from averaging their ratios: two devices each 50% free
    # can keep one machine 100% busy by interleaving.
    machines = df_ftg[df_ftg.line_kind == "machine"]
    if len(machines):
        print("\nPer machine (union of the device processes on that host):")
        print(machines[["machine", "devices", "free", "free_s", "span_s",
                        "host_idle"]].to_string(index=False))
''')


# ─────────────────────────────── 12 ──────────────────────────────────────────
md("""
## 12 · When each device was idle

`free_time_series.log` as a heat map — one row per device, time across, free share
as colour on a **single-hue sequential ramp** (light → dark; a rainbow would imply
categories where there is a magnitude).

`t_offset_s` is on each **device's own** clock, because devices start at different
moments. The rows are therefore aligned to each device's own start, not to a
shared wall clock — read a row against itself, not a column across rows.
""")
code(r'''
if not HAS_FREE_TIME or df_fts.empty:
    print("free_time_series.log absent or empty — 12 skipped.")
else:
    from matplotlib.colors import LinearSegmentedColormap
    # Sequential: ONE hue, light -> dark (guide/06 §3).
    SEQ = LinearSegmentedColormap.from_list(
        "seq_blue", ["#cde2fb", "#86b6ef", "#3987e5", "#256abf", "#104281"])

    run = RUN_ORDER[0] if RUN_ORDER else None
    sub = df_fts[df_fts.run == run] if run else df_fts
    devices = (df_ftd[df_ftd.run == run] if run else df_ftd) \
        .sort_values(["role", "free"], ascending=[True, False]).reset_index(drop=True)
    # Build the label WITH the client id it belongs to, then filter — deriving
    # labels and row order from two separate passes lets them drift apart the
    # moment one device is missing from the series.
    ticks = devices.groupby("role").cumcount() + 1
    label_of = {c: f"{r[0].upper()}{n}"
                for c, r, n in zip(devices.client, devices.role, ticks)}
    order = [c for c in devices.client if (sub.client == c).any()]
    labels = [label_of[c] for c in order]

    grid = (sub.pivot_table(index="client", columns="i", values="free")
            .reindex(order))
    fig, ax = plt.subplots(figsize=(13, 0.52 * len(order) + 2.6))
    bucket = float(sub.bucket_s.iloc[0])
    im = ax.imshow(grid.to_numpy(), aspect="auto", cmap=SEQ, vmin=0, vmax=100,
                   extent=[0, grid.shape[1] * bucket, len(order) - 0.5, -0.5],
                   interpolation="nearest")
    ax.set_yticks(np.arange(len(order)), labels, fontsize=9)
    ax.set_xlabel(f"seconds since that device's own start  (bucket = {bucket:g}s)")
    ax.set_ylabel("Device")
    ax.grid(visible=False)                # a grid over a heat map is only noise
    cbar = fig.colorbar(im, ax=ax, pad=0.015, fraction=0.026)
    cbar.set_label("Free (% of bucket)", color=INK_2, fontsize=10)
    cbar.outline.set_visible(False)
    ax.set_title("When each device was idle" +
                 (f"  —  {run}" if len(RUN_ORDER) > 1 else ""))
    subtitle(ax, "a band of free time that lines up with a throughput dip in 02 "
                 "names the stage that stalled", dy=-0.20)
    finish(fig, "12_free_time_over_run.png", hide_spines=("top", "right"))
''')


# ─────────────────────────────── 13 ──────────────────────────────────────────
md("""
## 13 · Queue-host memory against throughput

`broker_ram_ns.log` over `batch_done_ns.log`, **two panels on one shared x axis —
never a dual axis**. Memory climbing while throughput falls is the backpressure
signature, and it is unmistakable once the two curves are stacked (guide/11 §7).

The window opens at **controller start**, before anything is published, so the
shaded `idle` band on the left is the same host measured while the system is not
running. That band is the denominator that turns "this host was using N MB" into
"running the system costs this host N MB".
""")
code(r'''
if not HAS_RAM:
    print("broker_ram_ns.log absent or empty — 13 skipped (needs broker_ram.enabled).")
else:
    run = RUN_ORDER[0] if RUN_ORDER else None
    ram = df_ram[df_ram.run == run].sort_values("ts") if run else df_ram.sort_values("ts")
    batch = df_batch[df_batch.run == run].sort_values("ts") if run else df_batch

    # ns epochs overflow float precision: subtract the int baseline FIRST. Both
    # files are on the SERVER's clock, which is what makes the overlay mean
    # anything at all (guide/01 §1).
    t0 = int(ram.ts.iloc[0])
    ram_s = (ram.ts.astype("int64") - t0) / 1e9

    fig, axes = plt.subplots(2, 1, figsize=(13, 7.4), sharex=True,
                             gridspec_kw=dict(height_ratios=[1.25, 1]))
    ax = axes[0]
    PHASE_TINT = {"idle": "#eef3fa", "run": SURFACE, "tail": "#f4f1ea"}
    phase_spans = []
    for phase, grp in ram.groupby("phase", sort=False):
        span = (grp.ts.astype("int64") - t0) / 1e9
        ax.axvspan(span.min(), span.max(), color=PHASE_TINT.get(phase, SURFACE),
                   zorder=0)
        phase_spans.append((phase, span.min(), span.max()))
    ax.plot(ram_s, ram.used_mb, color=S1, label="Host used", **LINE_KW)
    ax.plot(ram_s, ram.rss_mb, color=S3, linewidth=1.6, label="Broker process RSS")
    endpoint(ax, ram_s.iloc[-1], float(ram.used_mb.iloc[-1]), S1, fmt="{:.0f}")
    ax.set_ylabel("Memory (MB)")
    ax.set_ylim(0, ram.used_mb.max() * 1.28)
    # Label the bands only AFTER set_ylim: annotating against get_ylim() while
    # the axes is still autoscaled anchors the text to a limit that then moves,
    # and the labels end up outside the panel entirely.
    top = ax.get_ylim()[1]
    for phase, lo, hi in phase_spans:
        ax.annotate(phase, xy=((lo + hi) / 2, top), xytext=(0, -10),
                    textcoords="offset points", ha="center", va="top",
                    fontsize=9, color=MUTED)
    legend_above(ax, ncol=2, side="right")
    ax.set_title(f"Queue host {ram.host.iloc[0]}  (source={ram.source.iloc[0]})",
                 pad=30)                       # clears the legend row above it

    ax2 = axes[1]
    if len(batch):
        secs = (batch.ts.astype("int64") - t0) / 1e9
        ax2.plot(secs, batch.window_fps, color=S2, alpha=0.42, linewidth=1.2)
        ax2.plot(secs, batch.window_fps.rolling(31, center=True, min_periods=1).mean(),
                 color=S2, **LINE_KW)
        ax2.set_ylim(0, batch.window_fps.max() * 1.20)
    ax2.set_ylabel("Rolling window FPS")
    ax2.set_xlabel("seconds since the RAM window opened (controller start)")
    for a in axes:
        a.grid(axis="both", visible=True)

    # The one number in the file that is a property of the SYSTEM rather than of
    # the machine: what running it costs this host, against the host at rest.
    cmp_row = df_ram_sum[df_ram_sum.line_kind == "compare"] if len(df_ram_sum) else []
    if len(cmp_row):
        c = cmp_row.iloc[0]
        subtitle(ax2, f"running the system costs this host "
                      f"{c.run_minus_idle_mb:+.0f} MB on average and "
                      f"{c.run_peak_over_idle_mb:+.0f} MB at peak, against the same "
                      f"host at rest; the tail gave back to "
                      f"{c.get('tail_minus_idle_mb', float('nan')):+.0f} MB",
                 dy=-0.24)
    fig.suptitle("Queue-host memory against system throughput", fontsize=14,
                 fontweight="semibold", color=INK, y=1.01)
    fig.tight_layout()
    finish(fig, "13_broker_ram_vs_throughput.png")
''')


# ─────────────────────────────── 14 ──────────────────────────────────────────
md("""
## 14 · Payload size on the wire

`message_size_series.log` — every message one worker published, sized **before**
the publish call so a stalled transport still leaves its sample behind.

Exactly one worker measures: the payload shape is fixed by the configuration, so
every worker in a group publishes the same shape and nine measuring would produce
one number nine times. The context that determines the size is in the subtitle —
a size without it cannot be reproduced.
""")
code(r'''
if not HAS_MSG_SIZE:
    print("message_size.log absent or empty — 14 skipped (needs message_size.enabled).")
else:
    run = RUN_ORDER[0] if RUN_ORDER else None
    summary = (df_msz[df_msz.run == run] if run else df_msz).iloc[0]
    series = (df_mss[df_mss.run == run] if run else df_mss).sort_values("i")

    fig, ax = plt.subplots(figsize=(12.4, 4.6))
    if len(series):
        ax.plot(series.t_offset_s, series.mb, color=S1, alpha=0.5, linewidth=1.1,
                label="per message")
        ax.plot(series.t_offset_s,
                series.mb.rolling(15, center=True, min_periods=1).mean(),
                color=S1, label="15-message mean", **LINE_KW)
    # The question this chart answers is VARIANCE, not magnitude — a payload that
    # tracks scene content makes any single-number bandwidth estimate optimistic.
    # A zero baseline compresses a 37-41 MB spread into one flat line and answers
    # nothing, so the axis is framed on the data and the caption says so.
    lo, hi = summary.min_mb, summary.max_mb
    pad = max((hi - lo) * 0.35, hi * 0.01)
    ax.set_ylim(lo - pad, hi + pad * 1.6)

    # Reference lines: same hue, thinner, receding. The value lives in the label,
    # placed on opposite sides so the two never sit on top of each other.
    for value, name, alpha, dy in ((summary.mean_mb, "mean", 0.5, 5),
                                   (summary.p95_mb, "p95", 0.32, -13)):
        ax.axhline(value, color=S1, linewidth=1.0, alpha=alpha)
        ax.annotate(f"{name} {value:.1f} MB", xy=(1.0, value), xytext=(-4, dy),
                    xycoords=("axes fraction", "data"), textcoords="offset points",
                    ha="right", fontsize=9, color=INK_2)
    ax.set_xlabel("seconds since that worker's own first publish")
    ax.set_ylabel("Payload size (MB)")
    ax.grid(axis="x", visible=False)
    legend_above(ax, ncol=2, side="right")
    ax.set_title(f"Payload published by {summary.client} "
                 f"({summary.role}, {summary.cluster})", pad=30)
    ax.annotate("y-axis is framed on the data, not zero — the reading is the "
                f"spread (p95/p50 = {summary.p95_mb / summary.p50_mb:.3f})",
                xy=(0.5, -0.17), xycoords="axes fraction", ha="center",
                fontsize=9, color=MUTED)
    subtitle(ax, f"mode={summary.run_mode} · cut {summary.splits} · compress="
                 f"{summary.compress}/{summary.num_bit}-bit · batch="
                 f"{summary.batch_size:.0f}  ·  {summary.n:.0f} messages, "
                 f"{summary.total_mb / 1000:.1f} GB total, "
                 f"{summary.rate_mb_s:.1f} MB/s egress, "
                 f"{summary.per_frame_mb:.3f} MB per frame", dy=-0.26)
    finish(fig, "14_message_size_over_run.png")

    # mean_mb x the queue depth cap is the RAM the queue host must hold. Read it
    # against DELTA peak_over_start_mb in 13: when the host's peak is far larger,
    # something is buffering that this run did not account for.
    if HAS_RAM and len(df_ram_sum):
        delta = df_ram_sum[df_ram_sum.line_kind == "delta"]
        if len(delta):
            print(f"\npayload mean {summary.mean_mb:.1f} MB  ·  queue-host peak over "
                  f"start {float(delta.iloc[0].peak_over_start_mb):.0f} MB  "
                  f"=> {float(delta.iloc[0].peak_over_start_mb) / summary.mean_mb:.1f} "
                  f"messages' worth of buffering at peak")
''')


# ─────────────────────────────── 15 ──────────────────────────────────────────
md("""
## 15 · Detection accuracy over the run

`map.log` + `map_window.log`. **Not a guide family** — `guide/README` puts mAP
out of scope, because nothing else measured here depends on ground truth or on
the work being a detection task. It is charted last for that reason.

The series is what the whole-run number cannot tell you: whether accuracy
**drifted**. Each window is scored by its own metric over only its own frames, so
a late collapse shows as a falling line instead of being averaged away into the
headline. Two lines because mAP@50 and mAP@50:95 answer different questions —
the second averages over stricter IoU thresholds and is always the lower of the
two, which is why a crossing would be a measurement bug rather than a result.

The **System** value is a frame-weighted mean of the per-device numbers, not a
pooled mAP: a true pooling needs every detection in one place. It is labelled
`pooling=frame_weighted` in the file and stated in the subtitle here, the same
way `e2e` latency is labelled indicative rather than exact.
""")
code(r'''
if not HAS_MAP:
    # "Absent" and "present but unreadable" are different failures and must not
    # print the same sentence: the first is a run without accuracy, the second is
    # a format the parser above does not cover, and only one of them is a bug.
    raw = sum(len(read_lines(d / "map.log")) for d in RUNS.values())
    print("map.log absent or empty — 15 skipped (needs map.enabled and ground truth)."
          if not raw else
          f"map.log holds {raw} line(s) but none carried a readable mAP — 15 skipped. "
          f"A present file that charts nothing is a FORMAT MISMATCH, not a run "
          f"without accuracy; extend MAP_KEYS in the parser cell.")
else:
    run = RUN_ORDER[0] if RUN_ORDER else None
    window = (df_mapw[df_mapw.run == run] if run else df_mapw)
    summary = (df_map[df_map.run == run] if run else df_map)
    system = summary[summary.scope == "System"]

    fig, ax = plt.subplots(figsize=(12.4, 4.7))
    SERIES = [("map50", "mAP@50", S1), ("map5095", "mAP@50:95", S2)]
    xcol = "t_offset_s"
    if len(window):
        # One line per metric, pooled across completing devices at each window
        # index: the question is "did accuracy move over the run", and one line
        # per device per metric is four lines answering it twice.
        pooled = (window.groupby("i")
                        .agg(t_offset_s=("t_offset_s", "mean"),
                             map50=("map50", "mean"), map5095=("map5095", "mean"))
                        .reset_index().sort_values("i"))
        # A pre-rename file indexes windows and ships no offsets. Fall back to
        # the index and SAY SO on the axis — a silent swap would put window
        # numbers under a label that reads seconds.
        xcol = "t_offset_s" if pooled.t_offset_s.notna().any() else "i"
        for col, lbl, colour in SERIES:
            ax.plot(pooled[xcol], pooled[col], color=colour, label=lbl, **LINE_KW)
            endpoint(ax, pooled[xcol].iloc[-1], float(pooled[col].iloc[-1]),
                     colour, fmt="{:.3f}")
        # Whole-run reference for each metric: same hue, thinner, receding.
        if len(system):
            for col, _lbl, colour in SERIES:
                ax.axhline(float(system.iloc[0][col]), color=colour,
                           linewidth=1.0, alpha=0.45)

    # mAP is a ratio in [0, 1]. Fix the ceiling the way the utilization charts
    # do — autoscaling a 0.59-0.62 band to fill the panel turns ordinary window
    # noise into a dramatic collapse.
    ax.set_ylim(0, 1.0)
    ax.set_xlabel("seconds since that device's own first scored frame"
                  if xcol == "t_offset_s" else "window index")
    ax.set_ylabel("mAP (0-1, higher is better)")
    ax.grid(axis="x", visible=False)
    legend_above(ax, ncol=2, side="right")
    ax.set_title("Detection accuracy over the run", pad=30)
    if len(system):
        s = system.iloc[0]
        # An archive that carries neither the frame count nor the pooling label
        # states what it has and omits what it does not. Printing `nan frames`
        # or an unlabelled mean would both read as measurements.
        scored = f" over {s.frames:,.0f} frames" if np.isfinite(s.frames) else ""
        pooling = (s.pooling or "pooling unlabelled").replace("_", "-")
        subtitle(ax, f"whole run: mAP@50 {s.map50:.3f} · mAP@50:95 "
                     f"{s.map5095:.3f}{scored} — {pooling} across "
                     f"{len(summary[summary.scope != 'System'])} completing "
                     f"device(s), indicative rather than pooled")
    finish(fig, "15_map_over_run.png")
''')


# ─────────────────────────────── 00 ──────────────────────────────────────────
md("""
## 00 · Hero stat tile

When the answer is one number, the number **is** the chart. A one-bar bar chart is
always wrong.
""")
code(r'''
if not RUN_ORDER:
    print("No runs loaded — 00 skipped.")
else:
    head = RUN_ORDER[0]
    s = df_rate[(df_rate.run == head) & (df_rate.scope == "System")]
    value = float(s.fps.iloc[0]) if len(s) else np.nan

    fig, ax = plt.subplots(figsize=(4.6, 2.4))
    ax.axis("off")
    ax.text(0, 0.72, "System throughput", fontsize=11, color=INK_2)
    ax.text(0, 0.30, f"{value:.2f}", fontsize=40, color=INK, fontweight="semibold")
    ax.text(0.58, 0.36, "frames/s", fontsize=13, color=MUTED, transform=ax.transAxes)
    if HAS_PAIR:
        base = df_rate[(df_rate.run == RUN_ORDER[1]) & (df_rate.scope == "System")]
        if len(base) and float(base.fps.iloc[0]):
            d = (value / float(base.fps.iloc[0]) - 1) * 100
            ax.text(0, 0.06, f"{d:+.1f}% vs baseline", fontsize=10,
                    color=GOOD if d >= 0 else BAD)
    else:
        ax.text(0, 0.06, head, fontsize=10, color=MUTED)
    finish(fig, "00_hero_throughput.png", hide_spines=())
''')


# ─────────────────────────────── manifest ────────────────────────────────────
md("## 4 · Manifest & coverage check")
code(r'''
print(f"{len(SAVED)} image(s) written to {IMG_DIR}\n")
for f in SAVED:
    print("  ", f)

COVERAGE = {
    "batch_done_ns.log":       ("required", ["02", "13"]),
    "fps_cluster_ns.log":       ("required", ["03", "04"]),
    "fps_cluster.log":          ("required", ["01", "09", "00"]),
    "utilization.log":         ("required", ["08"]),
    "utilization_cluster.log":   ("required", ["07", "09"]),
    "latency_cluster.log":       ("required", ["05", "06", "09"]),
    "events_ns.log":           ("optional", ["02 (rules; nothing drawn if absent)"]),
    "free_time.log":           ("optional", ["10", "12"]),
    "free_time_cluster.log":     ("optional", ["11"]),
    "free_time_series.log":    ("optional", ["12"]),
    "broker_ram_ns.log":       ("optional", ["13"]),
    "broker_ram.log":          ("optional", ["13 (COMPARE subtitle)", "14"]),
    "message_size.log":        ("optional", ["14"]),
    "message_size_series.log": ("optional", ["14"]),
    # Not a guide family — accuracy is out of scope in guide/README, and these
    # two are this project's own extension. Listed here so the coverage check
    # still accounts for every file the run directory holds.
    "map.log":                 ("extra",    ["15"]),
    "map_window.log":          ("extra",    ["15"]),
}
print("\nCoverage — every log that this run produced feeds at least one chart:")
for log, (need, charts) in COVERAGE.items():
    present = (RESULTS / RUN_ORDER[0] / log).exists() if RUN_ORDER else False
    # Fall back to a search: display labels do not always equal directory names.
    if not present and RUN_ORDER:
        present = any((d / log).exists() and (d / log).stat().st_size > 0
                      for d in RUNS.values())
    state = "  " if present else "--"
    print(f"  {state} {log:<26} {need:<9} -> {', '.join(charts)}")
print("\n  '--' = this run did not produce that file. Required files must never be "
      "missing;\n  optional ones are all-or-none per family and their charts skip "
      "themselves.")
''')


nb["cells"] = cells
nb.metadata = {                      # required, or nbclient cannot pick an executor
    "kernelspec":    {"display_name": "Python 3", "language": "python",
                      "name": "python3"},
    "language_info": {"name": "python", "version": "3"},
}

OUT.parent.mkdir(parents=True, exist_ok=True)
nbf.write(nb, str(OUT))
print("wrote", OUT, f"({len(cells)} cells)")
