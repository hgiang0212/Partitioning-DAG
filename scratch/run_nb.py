"""run_nb.py — execute the notebook in place and report every failure.

guide/08-build-pipeline.md §6. `allow_errors=True` is the important flag: it
collects every failing cell in one pass instead of one round-trip per bug.

    python scratch/run_nb.py [path/to/notebook.ipynb]
"""
import sys
from pathlib import Path

import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parent.parent
DEFAULT = ROOT / "results" / "visual" / "Split Inference Result Visualization.ipynb"

p = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT
if not p.exists():
    sys.exit(f"notebook not found: {p}\nrun scratch/build_nb.py first")

nb = nbformat.read(str(p), as_version=4)

NotebookClient(nb, timeout=600, kernel_name="python3",
               resources={"metadata": {"path": str(p.parent)}},
               allow_errors=True).execute()      # collect, don't stop at the first
nbformat.write(nb, str(p))

fail = 0
for i, c in enumerate(nb.cells):
    for o in c.get("outputs", []):
        if o.get("output_type") == "error":
            fail += 1
            print(f"\n### ERROR in cell {i} ###")
            print(c.source[:300], "\n---")
            print("\n".join(o.get("traceback", []))[-2500:])
        elif o.get("output_type") == "stream" and o.get("text", "").strip():
            print(f"[cell {i}] {o['text'].rstrip()[:1200]}")

print(f"\n=== {fail} cell error(s) ===")
sys.exit(1 if fail else 0)
