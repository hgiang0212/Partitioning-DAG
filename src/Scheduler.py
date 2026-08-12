import torch
import cv2
import pickle
from tqdm import tqdm
import copy
import socket
import time
import csv
import os
import psutil
import numpy as np

from src.BrokerGuard import PublishGuard
from src.Compress import Encoder, Decoder
import src.Log as Log
from src.Measure import FreeTimeTracker, MessageSizeRecorder
from src.Model import inference, postprocess_yolo


class Scheduler:
    def __init__(self, client_id, layer_id, channel, device):
        self.client_id = client_id
        self.layer_id = layer_id
        self.channel = channel
        self.device = device
        self.channel.queue_declare("queue_0", durable=False)
        self.channel.queue_declare("queue_1", durable=False)
        self.channel.queue_declare("queue_2", durable=False)
        self.fps_queue = "fps_queue"
        self.channel.queue_declare(queue=self.fps_queue, durable=False)
        self.utilization_queue = "utilization_queue"
        self.channel.queue_declare(queue=self.utilization_queue, durable=False)

        import glob as _glob
        for f in _glob.glob("metrics_raw_*.csv") + ["metrics_pivoted.csv", "metrics_pivot.lock"]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except PermissionError:
                    Log.print_with_color(f"[!] Cannot delete {f} (file is open). Close it and retry.", "red")

        cid_short = str(client_id).replace('-', '')[:12]
        self._timing_log_edge = f"timing_edge_{cid_short}.log"
        self._timing_log_cloud = f"timing_cloud_{cid_short}.log"
        for tlog in [self._timing_log_edge, self._timing_log_cloud]:
            if os.path.exists(tlog):
                try:
                    os.remove(tlog)
                except Exception:
                    pass
        # Group id (01 §3.2): the cloud chain this device belongs to, named after
        # the queue the edge publishes into. The edge derives it from its own
        # next hop; every downstream tier reads it off the incoming message, so
        # the completing tier can tag its DONE with the group that produced it.
        self.cluster = "unknown"
        # Raw per-batch latency samples, shipped unreduced at shutdown (04 §1) —
        # percentiles cannot validly be averaged across devices, so the server
        # pools these and reduces once.
        self._pipeline_ms = []
        self._e2e_ms = []
        # Optional measurements (guide/10, guide/12). Both are configured ONLY
        # from the dispatch message in `configure_measurements` — a worker that
        # read a measurement flag from its own config file would be one stale
        # copy away from a run that silently mixes two configurations
        # (guide/README invariant 9). Until then they are inert.
        self.free_time = FreeTimeTracker(enabled=False)
        self.msg_size = MessageSizeRecorder(enabled=False)
        # Admission control (guide/13). Inert until the dispatch message arrives,
        # for the same reason: the byte budget is the server's to divide, and a
        # worker that capped itself from its own config file would be one stale
        # copy away from a run where twelve producers enforced two limits.
        self.publish_guard = PublishGuard(enabled=False)
        self.size_message = None
        self.splits = None
        # Accuracy (map.log / map_window.log). Inert until the dispatch message
        # arrives — the ground-truth path is the server's to choose, exactly
        # like every measurement flag above.
        self.map_metric = None
        self.gt_dict = {}
        self._det_results = {}
        self._map_enabled = False
        self._map_window_batches = 16
        self._map_frames = 0          # frames actually scored against ground truth
        self._map_window_metric = None
        self._map_window_frames = 0
        self._map_window_batch0 = None
        self._map_series = []         # (i, t_offset_s, batches, frames, m50, m5095)
        self._map_t0_ns = None

    # ──────────────────────────── Measurement helpers ────────────────────────

    def configure_measurements(self, response):
        """Arm the optional measurements from the DISPATCH MESSAGE, never from a
        local file (guide/README invariant 9).

        `measure_message_size` is true on exactly one worker per run — the first
        registered at the first stage — and the server is what decides that. A
        worker that self-selected could leave a run where every edge measured or
        none did, and the summary line looks identical either way (guide/12 §1).
        """
        ft_cfg = response.get("free_time") or {}
        self.free_time = FreeTimeTracker(
            enabled=bool(ft_cfg.get("enabled", False)),
            bucket_s=float(ft_cfg.get("bucket_s", 1.0)),
            max_intervals=int(ft_cfg.get("max_intervals", 4000)))

        ms_cfg = response.get("message_size") or {}
        cid_short = str(self.client_id).replace('-', '')[:12]
        self.msg_size = MessageSizeRecorder(
            enabled=bool(response.get("measure_message_size", False)),
            path=f"message_size_{cid_short}.log",
            max_series=int(ms_cfg.get("max_series", 2000)))
        if self.msg_size.enabled:
            Log.print_with_color(
                f"[MsgSize] this worker is the one measuring payload size", "green")
        # The share of the broker's memory this producer may occupy (13 §3).
        # Bound to the connection here, not at first publish: opening a channel
        # inside the hot path would put a connection error on the batch that
        # happens to be first through it.
        guard_cfg = response.get("broker_guard") or {}
        self.publish_guard = PublishGuard(
            enabled=bool(guard_cfg.get("enabled", False)),
            high_bytes=float(guard_cfg.get("high_bytes", 0)),
            resume_bytes=float(guard_cfg.get("resume_bytes", 0)),
            cap_bytes=float(guard_cfg.get("cap_bytes", 0)),
            probe_frac=float(guard_cfg.get("probe_frac", 0.5)),
            probe_interval_s=float(guard_cfg.get("probe_interval_s", 2.0)),
            max_block_s=float(guard_cfg.get("max_block_s", 300.0)),
            poll_s=float(guard_cfg.get("poll_s", 0.05)),
            confirm=bool(guard_cfg.get("confirm", False)))
        self.publish_guard.bind(self.channel)

        # Accuracy. The ground-truth directory arrives in the dispatch message
        # too: a worker reading it from its own config file is one stale copy
        # away from two machines scoring against two different ground truths
        # and the server averaging the result into one meaningless number
        # (guide/README invariant 9). Loading is deferred to here for the same
        # reason — at __init__ the dispatch has not arrived yet.
        map_cfg = response.get("map") or {}
        self._map_enabled = bool(map_cfg.get("enabled", False))
        self._map_window_batches = max(int(map_cfg.get("window_batches", 16)), 1)
        if self._map_enabled:
            self._load_gt_dict(map_cfg.get("gt_dir", "datasets/groundtruth"))

        # Context that DETERMINES the size — a size without it is unreproducible.
        self._ms_context = {
            "mode": ms_cfg.get("mode", "split"),
            "compress": "on" if (response.get("compress") or {}).get("enable") else "off",
            "num_bit": (response.get("compress") or {}).get("num_bit", 0),
            "batch_size": response.get("batch_size", 0),
        }

    def _send_fps_done(self):
        """Publish exactly one completion per finished batch, tagged with the
        group id (02 §3-4). The body is an IDENTITY, never a measurement or a
        timestamp — a garbled body can mis-bucket one batch but can never distort
        a rate, and the server's arrival clock does all the timing.
        MUST run on the thread that owns the channel (pika is not thread-safe)."""
        try:
            self.channel.basic_publish(exchange="", routing_key=self.fps_queue,
                                       body=str(self.cluster).encode())
        except Exception as e:
            Log.print_with_color(f"[FPS] send DONE failed: {e}", "red")

    def _compute_utilization(self, log_path, role):
        """Parse this device's own timing log into ONE whole-run utilization
        ratio = sum(output - get input) / (end - start)  (03 §3).
        Pure file-parsing, called once after the `end` line; None on a bad log.

        The same intervals are also returned as raw `service_ms` samples, so
        `Σ service == busy_s` holds by construction — that identity is the
        conformance check tying latency_cluster.log to utilization_cluster.log."""
        try:
            with open(log_path) as f:
                lines = f.readlines()
        except Exception as e:
            Log.print_with_color(f"[Utilization] cannot read {log_path}: {e}", "yellow")
            return None
        t_start = t_end = t_input = None
        busy_ns = 0
        n_packages = 0
        service_ms = []
        for line in lines:
            parts = line.strip().split(" ", 1)   # event names contain spaces
            if len(parts) != 2 or not parts[0].isdigit():
                continue
            ts, event = int(parts[0]), parts[1]
            if event == "start":
                t_start = ts
            elif event == "end":
                t_end = ts
            elif event == "get input":
                t_input = ts
            elif event == "output":
                if t_input is not None:   # unmatched "get input" is dropped
                    busy_ns += ts - t_input
                    service_ms.append((ts - t_input) / 1e6)
                    n_packages += 1
                    t_input = None
            # any other event (e.g. queue_wait_*) is ignored → forward-extensible
        if t_start is None or t_end is None or t_end <= t_start:
            Log.print_with_color(f"[Utilization] incomplete timing log {log_path}", "yellow")
            return None
        total_ns = t_end - t_start
        utilization = busy_ns / total_ns
        Log.print_with_color(
            f"[Utilization][{role}] packages={n_packages} busy={busy_ns / 1e9:.3f}s "
            f"total={total_ns / 1e9:.3f}s utilization={utilization * 100:.2f}%", "green")
        return {
            "role": role,
            "packages": n_packages,
            "busy_ns": busy_ns,
            "total_ns": total_ns,
            "utilization": utilization,
            "service_ms": service_ms,
        }

    def _send_utilization(self, stats):
        """Publish the report to utilization_queue (03 §4, 04 §3). Raw latency
        arrays, the free-time report (10 §3) and the message-size report (12 §3)
        ride along with it — never pre-reduced percentiles.

        One queue, one drain: the reports sit on the broker until the server's
        shutdown collection picks them up, so publisher and consumer never need
        to be alive at the same moment. A second queue would buy nothing and
        cost the server a second timeout.

        Telemetry only — every failure degrades to a warning, never an exception."""
        if stats is None:
            stats = {}
        free_time = self.free_time.finish()      # None when the tracker is off:
        message = {"action": "UTILIZATION", "client_id": self.client_id,   # a
                   "layer_id": self.layer_id, "cluster": self.cluster,     # disabled
                   "device": str(self.device),                             # tracker
                   "pipeline_ms": self._pipeline_ms, "e2e_ms": self._e2e_ms,
                   "free_time": free_time,       # reports NOTHING, never zeros
                   "message_size": self._message_size_report(),
                   # Accuracy rides the same report and the same drain as every
                   # other measurement, so turning it on costs the shutdown no
                   # extra queue and no extra timeout (invariant 10).
                   "map": self._map_report(),
                   # Whether the cap actually bit, and for how long. Without it a
                   # throttled run and an unthrottled one differ only by a
                   # throughput number neither of them explains.
                   "broker_guard": self.publish_guard.report({
                       "client": self._kv_safe(self.client_id),
                       "role": "edge" if self.layer_id == 1 else "cloud",
                       "cluster": self._kv_safe(self.cluster)}),
                   **stats}
        if not stats and free_time is None and message["message_size"] is None \
                and message["broker_guard"] is None and message["map"] is None:
            return                               # nothing worth a publish
        body = pickle.dumps(message)
        try:
            self.channel.queue_declare(queue=self.utilization_queue, durable=False)
            self.channel.basic_publish(exchange="", routing_key=self.utilization_queue, body=body)
        except Exception:
            # The channel may already be broken this late in the run → retry
            # once on a fresh connection built from config.yaml, then give up.
            try:
                import pika
                import yaml
                with open("config.yaml") as f:
                    rabbit = yaml.safe_load(f)["rabbit"]
                credentials = pika.PlainCredentials(rabbit["username"], rabbit["password"])
                connection = pika.BlockingConnection(pika.ConnectionParameters(
                    rabbit["address"], 5672, rabbit["virtual-host"], credentials))
                channel = connection.channel()
                channel.queue_declare(queue=self.utilization_queue, durable=False)
                channel.basic_publish(exchange="", routing_key=self.utilization_queue, body=body)
                connection.close()
            except Exception as e:
                Log.print_with_color(f"[Utilization] send failed: {e}", "red")

    @staticmethod
    def _kv_safe(value):
        """A `key=value` value MUST contain no spaces (01 §1) — the universal
        parser splits on whitespace, so `splits=(11, 15)` silently truncates to
        `splits=(11,` and the rest of the line's keys shift. Tuples are the easy
        way to trip over this."""
        text = "-".join(str(v) for v in value) if isinstance(value, (tuple, list)) \
            else str(value)
        return "_".join(text.split()) or "none"

    def _message_size_report(self):
        """Summary over every sample this worker took, with the context that
        determines the size (12 §4). Returns None on every non-measuring worker."""
        context = getattr(self, "_ms_context", None) or {}
        return self.msg_size.report({
            "client": self._kv_safe(self.client_id),
            "role": "edge" if self.layer_id == 1 else "cloud",
            "machine": self._kv_safe(socket.gethostname()),
            "cluster": self._kv_safe(self.cluster),
            "splits": self._kv_safe(self.splits),
            **{k: self._kv_safe(v) for k, v in context.items()},
        })

    def get_ram_mb(self):
        try:
            import subprocess, re
            result = subprocess.run(
                ['tegrastats', '--once'],
                capture_output=True, text=True, timeout=2
            )
            m = re.search(r'RAM (\d+)/\d+MB', result.stdout)
            if m:
                return int(m.group(1))
        except Exception:
            pass
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)

    def write_metrics(self, mode, role, best_cut, batch_id, batch_size, latency_ms, fps, ram_mb, message_size_bytes=0, e2e_latency_ms=0, edge_start_time=None):
        file_path = f"metrics_raw_{str(self.client_id).replace('-', '')}.csv"
        file_exists = os.path.exists(file_path)

        with open(file_path, "a", newline="") as f:
            writer = csv.writer(f)

            if not file_exists:
                writer.writerow([
                    "mode",
                    "role",
                    "best_cut",
                    "batch_id",
                    "batch_size",
                    "latency_ms",
                    "fps",
                    "ram_mb",
                    "message_size_bytes",
                    "e2e_latency_ms",
                    "edge_start_time",
                ])

            writer.writerow([
                mode,
                role,
                best_cut,
                batch_id,
                batch_size,
                round(latency_ms, 3),
                round(fps, 3) if fps > 0 else "",  # fps=0 (first batch) → empty
                round(ram_mb, 3),
                message_size_bytes,
                round(e2e_latency_ms, 3),
                edge_start_time if edge_start_time is not None else "",
            ])


    # ──────────────────────────── mAP helpers ────────────────────────────────

    def _load_gt_dict(self, gt_dir="datasets/groundtruth"):
        if not os.path.isdir(gt_dir):
            Log.print_with_color(
                f"[mAP] ground truth '{gt_dir}' not found — accuracy not scored", "yellow")
            return
        try:
            from torchmetrics.detection import MeanAveragePrecision
            self.map_metric = MeanAveragePrecision(iou_type="bbox")
        except ImportError:
            Log.print_with_color("[!] torchmetrics not installed, mAP disabled", "red")
            return
        for fname in sorted(os.listdir(gt_dir)):
            if not fname.endswith(".txt"):
                continue
            try:
                num = int(os.path.splitext(fname)[0].split("_")[-1])
            except ValueError:
                continue
            boxes, labels = [], []
            with open(os.path.join(gt_dir, fname)) as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) < 5:
                        continue
                    cls, cx, cy, bw, bh = map(float, parts[:5])
                    boxes.append([(cx - bw / 2) * 640, (cy - bh / 2) * 640,
                                  (cx + bw / 2) * 640, (cy + bh / 2) * 640])
                    labels.append(int(cls))
            self.gt_dict[num] = {
                "boxes":  torch.tensor(boxes,  dtype=torch.float32) if boxes  else torch.zeros((0, 4)),
                "labels": torch.tensor(labels, dtype=torch.int64)   if labels else torch.zeros(0, dtype=torch.int64),
            }
        Log.print_with_color(f"[mAP] Loaded GT for {len(self.gt_dict)} frames from '{gt_dir}'", "green")

    def _update_map(self, batch_results, batch_id, batch_size):
        import json
        for img_idx, r in enumerate(batch_results):
            frame_num = batch_id * batch_size + img_idx + 1
            dets = [
                {
                    "box":   r["boxes"][i].cpu().tolist(),
                    "score": round(float(r["scores"][i]), 4),
                    "class": int(r["classes"][i]),
                }
                for i in range(len(r["boxes"]))
            ]
            self._det_results[frame_num] = dets
            with open("detections_stream.jsonl", "a") as f:
                f.write(json.dumps({"frame": frame_num, "dets": dets}) + "\n")
            if self.map_metric is None or frame_num not in self.gt_dict:
                continue
            pred = [{"boxes":  r["boxes"].cpu().float(),
                     "scores": r["scores"].cpu().float(),
                     "labels": r["classes"].cpu().long()}]
            target = [self.gt_dict[frame_num]]
            self.map_metric.update(pred, target)     # whole-run, for map.log
            self._map_window_update(pred, target)    # this window, for map_window.log
            self._map_frames += 1
        self._map_window_close(batch_id)

    # ── windowed accuracy (map_window.log) ───────────────────────────────────
    # A whole-run mAP says how good the run was; it cannot say whether accuracy
    # DRIFTED while it ran. Each window is scored by its own metric instance
    # over only that window's frames, so a late collapse shows up as a falling
    # series instead of being averaged into the headline number.

    def _map_window_update(self, pred, target):
        if self._map_window_metric is None:
            try:
                from torchmetrics.detection import MeanAveragePrecision
                self._map_window_metric = MeanAveragePrecision(iou_type="bbox")
            except Exception:
                return
            if self._map_t0_ns is None:
                self._map_t0_ns = time.time_ns()
        self._map_window_metric.update(pred, target)
        self._map_window_frames += 1

    def _map_window_close(self, batch_id):
        """Close a tumbling window every `window_batches` batches. Windows are
        counted in BATCHES, not seconds: frames only exist in batch units, so a
        time bucket would have to split one, and the split half would be scored
        against nothing."""
        if self._map_window_batch0 is None:
            self._map_window_batch0 = batch_id
        if batch_id - self._map_window_batch0 + 1 < self._map_window_batches:
            return
        batches = batch_id - self._map_window_batch0 + 1
        self._map_window_batch0 = batch_id + 1
        if self._map_window_metric is None or not self._map_window_frames:
            return
        try:
            result = self._map_window_metric.compute()
            # Offset from this worker's OWN first scored frame. It never crosses
            # a machine boundary, so it is exact, and it locates the window in
            # the run without a device timestamp reaching a shared file.
            offset_s = (time.time_ns() - self._map_t0_ns) / 1e9
            self._map_series.append((len(self._map_series), offset_s, batches,
                                     self._map_window_frames,
                                     float(result["map_50"]), float(result["map"])))
        except Exception as e:                       # telemetry never kills the run
            Log.print_with_color(f"[mAP] window compute failed: {e}", "yellow")
        self._map_window_metric = None               # fresh metric per window
        self._map_window_frames = 0

    def _map_report(self):
        """Whole-run accuracy plus the window series, or None when this worker
        scored nothing — a completing tier with no ground truth reports NOTHING
        rather than reporting 0.0, which would read as a model that detected
        nothing at all."""
        if not self._map_enabled or self.map_metric is None or not self._map_frames:
            return None
        try:
            self._map_window_flush()
            result = self.map_metric.compute()
            return {
                "client": self._kv_safe(self.client_id),
                "cluster": self._kv_safe(self.cluster),
                "frames": len(self._det_results),
                "matched": self._map_frames,   # frames that had ground truth
                "map50": float(result["map_50"]),
                "map5095": float(result["map"]),
                "series": self._map_series,
            }
        except Exception as e:
            Log.print_with_color(f"[mAP] report failed: {e}", "yellow")
            return None

    def _map_window_flush(self):
        """Close a partial trailing window, so the last frames of the run are in
        the series instead of being silently dropped with it."""
        if self._map_window_metric is None or not self._map_window_frames:
            return
        self._map_window_batches = 1        # force the next close to fire
        self._map_window_close(self._map_window_batch0 or 0)

    def _print_map(self):
        if self.map_metric is None:
            return
        try:
            result = self.map_metric.compute()
            print("=" * 55)
            print(f"  [mAP]   mAP@50={result['map_50']:.4f}  mAP@50:95={result['map']:.4f}")
            print("=" * 55)
        except Exception as e:
            Log.print_with_color(f"[mAP] compute failed: {e}", "red")

    def _write_detections_json(self):
        import json
        out = "detections.json"
        with open(out, "w") as f:
            json.dump({str(k): v for k, v in sorted(self._det_results.items())}, f)
        Log.print_with_color(f"[Tracker] Saved {out} ({len(self._det_results)} frames)", "green")

    # ──────────────────────────── Summary report ─────────────────────────────

    def _print_summary(self):
        import glob as _glob

        lock_path = "metrics_pivot.lock"
        out_path = "metrics_pivoted.csv"

        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
        except FileExistsError:
            return

        try:
            self._do_summary(lock_path, out_path)
        finally:
            try:
                os.remove(lock_path)
            except (FileNotFoundError, PermissionError):
                pass

    def _do_summary(self, lock_path, out_path):
        import glob as _glob
        import datetime
        time.sleep(0.5)

        # Collect rows per role, assign device sequence number per CSV file
        edge_rows = []
        cloud_rows = []
        edge_seq = 0
        cloud_seq = 0

        for fpath in sorted(_glob.glob("metrics_raw_*.csv")):
            with open(fpath, newline="") as f:
                rows_in_file = list(csv.DictReader(f))
            if not rows_in_file:
                continue
            role = rows_in_file[0]["role"]
            if role == "edge":
                edge_seq += 1
                for row in rows_in_file:
                    row["device_seq"] = edge_seq
                    edge_rows.append(row)
            elif role == "cloud":
                cloud_seq += 1
                for row in rows_in_file:
                    row["device_seq"] = cloud_seq
                    cloud_rows.append(row)

        # Join edge ↔ cloud by the edge_start_time embedded in each message
        edge_by_time = {
            row["edge_start_time"]: row
            for row in edge_rows if row.get("edge_start_time")
        }
        matched_pairs = []
        matched_edge_times = set()
        for c in cloud_rows:
            t = c.get("edge_start_time", "")
            e = edge_by_time.get(t, {})
            matched_pairs.append((e, c))
            if t:
                matched_edge_times.add(t)
        # Edge rows with no matching cloud (e.g. edge-only runs)
        for e in edge_rows:
            if e.get("edge_start_time", "") not in matched_edge_times:
                matched_pairs.append((e, {}))
        matched_pairs.sort(
            key=lambda p: float(p[0].get("edge_start_time") or p[1].get("edge_start_time") or 0)
        )
        n_rows = len(matched_pairs)

        # Console summary
        def avg(rows, key, skip_zero=False):
            vals = [float(r[key]) for r in rows if r.get(key)
                    and (not skip_zero or float(r[key]) > 0)]
            return round(sum(vals) / len(vals), 3) if vals else None

        def mb(val):
            return round(val / 1024 / 1024, 3) if val is not None else "N/A"

        def total_fps(rows):
            by_device = {}
            for r in rows:
                seq = r.get("device_seq")
                val = float(r.get("fps") or 0)
                if val > 0 and seq is not None:
                    by_device.setdefault(seq, []).append(val)
            device_avgs = [sum(v) / len(v) for v in by_device.values() if v]
            return round(sum(device_avgs), 3) if device_avgs else None

        all_data_rows = cloud_rows if cloud_rows else edge_rows
        valid_batches = len([r for r in all_data_rows if float(r.get("fps") or 0) > 0])
        cuts = set(r.get("best_cut", "N/A") for r in (edge_rows + cloud_rows))
        cut_str = "/".join(sorted(str(c) for c in cuts))
        sys_fps = total_fps(all_data_rows)
        n_final_devices = len(set(r.get("device_seq") for r in all_data_rows))

        print("=" * 50)
        print(f"  SUMMARY  |  batches={n_rows} (valid={valid_batches})  cut={cut_str}")
        print("=" * 50)
        if edge_rows:
            print(f"  [EDGE]  latency={avg(edge_rows,'latency_ms',True)} ms  "
                  f"fps={avg(edge_rows,'fps',True)}  "
                  f"ram={avg(edge_rows,'ram_mb',True)} MB  "
                  f"msg={mb(avg(edge_rows,'message_size_bytes'))} MB")
        if cloud_rows:
            print(f"  [CLOUD] latency={avg(cloud_rows,'latency_ms',True)} ms  "
                  f"fps={avg(cloud_rows,'fps',True)}  "
                  f"ram={avg(cloud_rows,'ram_mb',True)} MB  "
                  f"msg={mb(avg(cloud_rows,'message_size_bytes'))} MB")
        print(f"  [E2E]   latency={avg(all_data_rows,'e2e_latency_ms',True)} ms")
        print(f"  [SYSTEM TOTAL FPS] {sys_fps} fps  "
              f"(sum of avg fps across {n_final_devices} final device(s))")
        print("=" * 50)

        # Save pivoted CSV — one row per batch, edge and cloud side by side
        fieldnames = [
            "batch_id", "batch_size", "best_cut",
            "edge_device", "edge_latency_ms", "edge_fps", "edge_ram_mb", "edge_message_size_bytes",
            "cloud_device", "cloud_latency_ms", "cloud_fps", "cloud_ram_mb", "cloud_message_size_bytes",
            "e2e_latency_ms",
        ]
        candidates = [out_path,
                      f"metrics_pivoted_{datetime.datetime.now().strftime('%H%M%S')}.csv"]
        saved_path = None
        for path in candidates:
            try:
                with open(path, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for i, (e, c) in enumerate(matched_pairs):
                        writer.writerow({
                            "batch_id":                 i,
                            "batch_size":               e.get("batch_size") or c.get("batch_size", ""),
                            "best_cut":                 e.get("best_cut") or c.get("best_cut", ""),
                            "edge_device":              e.get("device_seq", ""),
                            "edge_latency_ms":          e.get("latency_ms", ""),
                            "edge_fps":                 e.get("fps", ""),
                            "edge_ram_mb":              e.get("ram_mb", ""),
                            "edge_message_size_bytes":  e.get("message_size_bytes", ""),
                            "cloud_device":             c.get("device_seq", ""),
                            "cloud_latency_ms":         c.get("latency_ms", ""),
                            "cloud_fps":                c.get("fps", ""),
                            "cloud_ram_mb":             c.get("ram_mb", ""),
                            "cloud_message_size_bytes": c.get("message_size_bytes", ""),
                            "e2e_latency_ms":           c.get("e2e_latency_ms", ""),
                        })
                saved_path = path
                break
            except PermissionError:
                Log.print_with_color(f"[Metrics] {path} is locked, trying next name...", "yellow")

        for fpath in _glob.glob("metrics_raw_*.csv"):
            try:
                os.remove(fpath)
            except (FileNotFoundError, PermissionError):
                pass

        if saved_path:
            Log.print_with_color(f"[Metrics] Saved {saved_path} ({n_rows} batches)", "green")
        else:
            Log.print_with_color("[Metrics] Could not save CSV — close all open metrics files and re-run.", "red")

    # ──────────────────────────── Pipeline ───────────────────────────────────

    def send_next_layer(self, intermediate_queue, data, compress, batch_id=0):
        t_encode = self.free_time.now()
        if compress["enable"]:
            data["data"] = [t.cpu().numpy() if isinstance(t, torch.Tensor) else None for t in
                            data["data"]]
            data["data"], data["shape"] = Encoder(data_output=data["data"], num_bits=compress["num_bit"])
        else:
            data["data"] = [t.cpu() if isinstance(t, torch.Tensor) else None for t in
                            data["data"]]
        message = pickle.dumps({
            "action": "OUTPUT",
            "data": data
        })
        self.size_message = len(message)
        self.free_time.add_work("encode", t_encode)

        # The SERIALIZED byte count handed to the transport, recorded BEFORE the
        # publish call (12 §2). Both orderings look equivalent until the broker
        # hits its high-water mark and stops accepting — which is exactly the run
        # this measurement exists to explain, and the one where measuring after
        # the call writes the sample late or, if it raises, never.
        self.msg_size.record(self.size_message, batch_id)

        # Log message size
        Log.print_with_color(f"[>>>] Sending message to {intermediate_queue}: {self.size_message} bytes", "yellow")

        # Admission control sits HERE and nowhere else: this is the only publish
        # in the system whose volume is set by how fast video decodes rather
        # than by how fast anything downstream consumes, and it is therefore the
        # only one that can grow the broker without bound (13 §1).
        t_send = self.free_time.now()
        waited_s = self.publish_guard.publish(self.channel, intermediate_queue, message)
        t_done = self.free_time.now()
        if waited_s > 0:
            # Booked as a WAIT, not as work. A producer paused by backpressure
            # was idle; charging that to "send" would report a throttled device
            # as fully utilized, which is the exact opposite of what happened
            # and would hide the cost of the cap in the one file built to show
            # it (guide/10 §2).
            t_split = min(t_send + int(waited_s * 1e9), t_done)
            self.free_time.add_wait("broker_backpressure", t_send, t_split)
            self.free_time.add_work("send", t_split, t_done)
        else:
            self.free_time.add_work("send", t_send, t_done)

    def send_to_server(self, message):
        self.channel.queue_declare('rpc_queue', durable=False)
        self.channel.basic_publish(exchange='',
                                   routing_key='rpc_queue',
                                   body=pickle.dumps(message))

    def first_layer(self, model, data, batch_size, logger, compress, next_client_id, mode = "split"):
        orig_images = []
        input_image = []

        model.eval()
        model.to(self.device)

        video_path = data
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            Log.print_with_color(f"Not open video", "red")
            return False

        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

        # This edge's group = the cloud chain it feeds (01 §3.2). Every batch it
        # emits is tagged with it, and every tier downstream forwards the tag.
        self.cluster = f"queue_{next_client_id}"

        with open(self._timing_log_edge, "w") as _tf:
            print(str(time.time_ns()) + " start", file=_tf)
        # Free time opens on the SAME window as utilization's start/end, so the
        # two describe the same run span and can be read against each other.
        self.free_time.start()
        pbar = tqdm(desc="Processing video (while loop)", unit="frame")
        batch_id = 0
        prev_batch_end = None
        batch_first_frame_ns = None
        while True:
            t_capture = self.free_time.now()
            ret, frame = cap.read()
            if not ret:
                self.free_time.add_work("capture", t_capture)
                break
            if not input_image:
                # `pipeline` starts here, not at `get input`: the first frame of
                # a batch sits in the buffer until the batch fills, and that wait
                # is latency the frame really experienced (04 §2.2). The gap
                # between pipeline and service is the cost of the batch size.
                batch_first_frame_ns = time.time_ns()
            frame = cv2.resize(frame, (640, 640))
            orig_images.append(copy.deepcopy(frame))
            frame = frame.astype('float32') / 255.0
            tensor = torch.from_numpy(frame).permute(2, 0, 1)  # shape: (3, 640, 640)
            input_image.append(tensor)
            self.free_time.add_work("capture", t_capture)

            if len(input_image) == batch_size:
                with open(self._timing_log_edge, "a") as _tf:
                    print(str(time.time_ns()) + " get input", file=_tf)
                batch_start = time.perf_counter()
                edge_start_wall = time.time()

                t_infer = self.free_time.now()
                input_image = torch.stack(input_image)
                input_image = input_image.to(self.device)

                y = []
                x, y = inference(model, input_image, y, 0)
                y[-1] = x
                self.free_time.add_work("inference", t_infer)

                y_msg = {
                    "data": y,
                    "width": width,
                    "height": height,
                    "edge_start_time": edge_start_wall,
                    "cluster": self.cluster,
                }
                self.send_next_layer(f"queue_{next_client_id}", y_msg, compress, batch_id)

                if mode == "only_edge":      # edge completes the batch only in this mode
                    self._send_fps_done()

                batch_end = time.perf_counter()
                output_ns = time.time_ns()
                t_book = self.free_time.now()
                with open(self._timing_log_edge, "a") as _tf:
                    print(str(output_ns) + " output", file=_tf)
                if batch_first_frame_ns is not None:
                    self._pipeline_ms.append((output_ns - batch_first_frame_ns) / 1e6)
                if mode == "only_edge":      # only the completing tier reports e2e
                    self._e2e_ms.append((output_ns / 1e9 - edge_start_wall) * 1000)
                latency_ms = (batch_end - batch_start) * 1000
                fps = batch_size / (batch_end - prev_batch_end) if prev_batch_end is not None else 0.0
                e2e_latency_ms = latency_ms if mode == "only_edge" else 0.0
                ram_mb = self.get_ram_mb()
                msg_size = self.size_message if self.size_message is not None else 0

                print(f"[Batch {batch_id:4d}] EDGE | latency={latency_ms:.1f}ms | "
                      f"fps={fps:.1f} | ram={ram_mb:.1f}MB | msg={msg_size}B")

                self.write_metrics(
                    mode=mode,
                    role="edge_sender" if mode == "only_cloud" else "edge",
                    best_cut=self.splits,
                    batch_id=batch_id,
                    batch_size=batch_size,
                    latency_ms=latency_ms,
                    fps=fps,
                    ram_mb=ram_mb,
                    message_size_bytes=msg_size,
                    e2e_latency_ms=e2e_latency_ms,
                    edge_start_time=edge_start_wall,
                )

                batch_id += 1
                prev_batch_end = batch_end
                input_image = []
                orig_images = []
                pbar.update(batch_size)
                self.free_time.add_work("bookkeeping", t_book)
            else:
                continue

        print(f'size message: {self.size_message} bytes.')
        with open(self._timing_log_edge, "a") as _tf:
            print(str(time.time_ns()) + " end", file=_tf)
        self.free_time.stop()          # same window as `end` in the timing log
        print(f'size message: {self.size_message} bytes.')

        cap.release()
        pbar.close()

        # utilization: computed from own log, sent BEFORE NOTIFY (guide §4).
        # The free-time and message-size reports ride along with it.
        self._send_utilization(self._compute_utilization(self._timing_log_edge, "edge"))

        notify_data = {"action": "NOTIFY", "client_id": self.client_id, "layer_id": self.layer_id,
                       "message": "Finish training!"}
        self.send_to_server(notify_data)

        broadcast_queue_name = f'reply_{self.client_id}'
        while True:
            method_frame, header_frame, body = self.channel.basic_get(queue=broadcast_queue_name, auto_ack=True)
            if body:
                received_data = pickle.loads(body)
                Log.print_with_color(f"[<<<] Received message from server {received_data}", "blue")
                if received_data["action"] == "STOP":
                    Log.print_with_color("[>>>] Finish!", "red")
                    break
            time.sleep(0.5)

    def last_layer(self, model, batch_size, splits, logger, compress,mode = "split"):
        model.eval()
        model.to(self.device)

        self.channel.basic_qos(prefetch_count=10)
        pbar = tqdm(desc="Processing video (while loop)", unit="frame")
        batch_id = 0
        prev_batch_end = None
        with open(self._timing_log_cloud, "w") as _tf:
            print(str(time.time_ns()) + " start", file=_tf)
        self.free_time.start()
        while True:
            # Retroactive classification (10 §2): a non-blocking get is WORK when
            # it yields a batch and FREE when the queue was empty, and which one
            # it was is only known after it returns. Take the timestamp before
            # the call, decide after.
            t_poll = self.free_time.now()
            method_frame, header_frame, body = self.channel.basic_get(queue=f"queue_{self.client_id}", auto_ack=True)
            if method_frame and body:
                self.free_time.add_work("recv", t_poll)
                # `pipeline` starts the moment the batch became available to this
                # device. With no in-process hand-off queue that is also when
                # compute starts, so pipeline == service here; add a hand-off
                # thread and the two diverge on their own (04 §2.2).
                ready_ns = time.time_ns()
                with open(self._timing_log_cloud, "a") as _tf:
                    print(str(ready_ns) + " get input", file=_tf)
                batch_start = time.perf_counter()
                received_message_size = len(body)
                t_decode = self.free_time.now()
                received_data = pickle.loads(body)
                y = received_data["data"]
                edge_start_time = y.get("edge_start_time", time.time())
                self.cluster = y.get("cluster") or self.cluster   # group tag rides the batch

                if compress["enable"]:
                    y["data"] = Decoder(y["data"], y["shape"])
                    y["data"] = [torch.from_numpy(t) if t is not None else None for t in y["data"]]

                y["data"] = [t.to(self.device) if t is not None else None for t in y["data"]]
                self.free_time.add_work("decode", t_decode)

                t_infer = self.free_time.now()
                list_output = y["data"]
                x = list_output[-1]
                x, _ = inference(model, x, list_output, splits)
                self.free_time.add_work("inference", t_infer)

                t_post = self.free_time.now()
                results = postprocess_yolo(x)
                self._update_map(results, batch_id, batch_size)
                self.free_time.add_work("postprocess", t_post)

                if mode != "only_edge":      # cloud completes the batch in split / only_cloud
                    t_done = self.free_time.now()
                    self._send_fps_done()
                    self.free_time.add_work("send", t_done)

                batch_end = time.perf_counter()
                output_ns = time.time_ns()
                t_book = self.free_time.now()
                with open(self._timing_log_cloud, "a") as _tf:
                    print(str(output_ns) + " output", file=_tf)
                cloud_end_wall = output_ns / 1e9
                self._pipeline_ms.append((output_ns - ready_ns) / 1e6)
                latency_ms = (batch_end - batch_start) * 1000
                fps = batch_size / (batch_end - prev_batch_end) if prev_batch_end is not None else 0.0
                # e2e spans two machines by definition, so it inherits any offset
                # between their clocks — report it, but treat it as indicative.
                # Only the COMPLETING tier reports it: one series per group.
                e2e_latency_ms = (cloud_end_wall - edge_start_time) * 1000
                self._e2e_ms.append(e2e_latency_ms)
                ram_mb = self.get_ram_mb()

                print(f"[Batch {batch_id:4d}] CLOUD | latency={latency_ms:.1f}ms | "
                      f"fps={fps:.1f} | e2e={e2e_latency_ms:.1f}ms | "
                      f"ram={ram_mb:.1f}MB | recv={received_message_size}B")

                self.write_metrics(
                    mode=mode,
                    role="cloud",
                    best_cut=self.splits,
                    batch_id=batch_id,
                    batch_size=batch_size,
                    latency_ms=latency_ms,
                    fps=fps,
                    ram_mb=ram_mb,
                    message_size_bytes=received_message_size,
                    e2e_latency_ms=e2e_latency_ms,
                    edge_start_time=edge_start_time,
                )

                batch_id += 1
                prev_batch_end = batch_end
                pbar.update(batch_size)
                self.free_time.add_work("bookkeeping", t_book)

            else:
                broadcast_queue_name = f'reply_{self.client_id}'
                method_frame, header_frame, body = self.channel.basic_get(queue=broadcast_queue_name, auto_ack=True)
                if body:
                    received_data = pickle.loads(body)
                    Log.print_with_color(f"[<<<] Received message from server {received_data}", "blue")
                    if received_data["action"] == "STOP":
                        Log.print_with_color("[>>>] Finish!", "red")
                        # The stretch since the empty get was spent waiting for a
                        # STOP that has now arrived — free, not busy.
                        self.free_time.add_wait("stop", t_poll)
                        break
                else:
                    time.sleep(0.5)
                # The empty get, the control poll and the sleep are ONE idle
                # stretch, recorded as a single interval rather than three.
                self.free_time.add_wait("input", t_poll)
        with open(self._timing_log_cloud, "a") as _tf:
            print(str(time.time_ns()) + " end", file=_tf)
        self.free_time.stop()
        self._send_utilization(self._compute_utilization(self._timing_log_cloud, "cloud"))
        try:
            cv2.destroyAllWindows()
        except Exception:
            pass
        pbar.close()

    def middle_layer(self, model, batch_size, splits, logger, compress, next_client_id, mode="split"):
        model.eval()
        model.to(self.device)

        self.channel.basic_qos(prefetch_count=10)
        with open(self._timing_log_cloud, "w") as _tf:
            print(str(time.time_ns()) + " start", file=_tf)
        self.free_time.start()
        pbar = tqdm(desc="Processing video (while loop)", unit="frame")
        batch_id = 0
        prev_batch_end = None
        while True:
            t_poll = self.free_time.now()          # see last_layer: retroactive
            method_frame, header_frame, body = self.channel.basic_get(queue=f"queue_{self.client_id}", auto_ack=True)
            if method_frame and body:
                self.free_time.add_work("recv", t_poll)
                ready_ns = time.time_ns()          # see last_layer: pipeline start
                with open(self._timing_log_cloud, "a") as _tf:
                    print(str(ready_ns) + " get input", file=_tf)
                batch_start = time.perf_counter()
                received_message_size = len(body)
                t_decode = self.free_time.now()
                received_data = pickle.loads(body)
                y = received_data["data"]
                self.cluster = y.get("cluster") or self.cluster   # group tag rides the batch

                if compress["enable"]:
                    y["data"] = Decoder(y["data"], y["shape"])
                    y["data"] = [torch.from_numpy(t) if t is not None else None for t in y["data"]]

                y["data"] = [t.to(self.device) if t is not None else None for t in y["data"]]
                self.free_time.add_work("decode", t_decode)

                t_infer = self.free_time.now()
                list_output = y["data"]
                x = list_output[-1]
                x, y = inference(model, x, list_output, splits)
                y[-1] = x
                self.free_time.add_work("inference", t_infer)

                y_msg = {
                    "data": y,
                    # "width": received_data["width"],
                    # "height": received_data["height"],
                    "edge_start_time": received_data["data"]["edge_start_time"],
                    "cluster": self.cluster,
                }
                print(f"[DEBUG] middle_layer sending to queue: {next_client_id}")
                self.send_next_layer(f"queue_{next_client_id}", y_msg, compress, batch_id)
                batch_end = time.perf_counter()
                output_ns = time.time_ns()
                t_book = self.free_time.now()
                with open(self._timing_log_cloud, "a") as _tf:
                    print(str(output_ns) + " output", file=_tf)
                cloud_end_wall = output_ns / 1e9
                # A middle tier does not complete the batch, so it reports no
                # e2e series — that belongs to the completing tier alone.
                self._pipeline_ms.append((output_ns - ready_ns) / 1e6)
                latency_ms = (batch_end - batch_start) * 1000
                fps = batch_size / (batch_end - prev_batch_end) if prev_batch_end is not None else 0.0
                e2e_latency_ms = latency_ms if mode == "only_edge" else 0.0
                ram_mb = self.get_ram_mb()

                print(f"[Batch {batch_id:4d}] CLOUD | latency={latency_ms:.1f}ms | "
                      f"fps={fps:.1f} |"
                      f"ram={ram_mb:.1f}MB | recv={received_message_size}B")

                self.write_metrics(
                    mode=mode,
                    role="cloud",
                    best_cut=self.splits,
                    batch_id=batch_id,
                    batch_size=batch_size,
                    latency_ms=latency_ms,
                    fps=fps,
                    ram_mb=ram_mb,
                    message_size_bytes=received_message_size,
                    e2e_latency_ms=e2e_latency_ms,
                    edge_start_time=received_data["data"]["edge_start_time"],
                )
                batch_id += 1
                prev_batch_end = batch_end
                pbar.update(batch_size)
                self.free_time.add_work("bookkeeping", t_book)
            else:
                broadcast_queue_name = f'reply_{self.client_id}'
                method_frame, header_frame, body = self.channel.basic_get(queue=broadcast_queue_name, auto_ack=True)
                if body:
                    received_data = pickle.loads(body)
                    Log.print_with_color(f"[<<<] Received message from server {received_data}", "blue")
                    if received_data["action"] == "STOP":
                        Log.print_with_color("[>>>] Finish!", "red")
                        self.free_time.add_wait("stop", t_poll)
                        break
                else:
                    time.sleep(0.5)
                self.free_time.add_wait("input", t_poll)

        with open(self._timing_log_cloud, "a") as _tf:
            print(str(time.time_ns()) + " end", file=_tf)
        self.free_time.stop()
        self._send_utilization(self._compute_utilization(self._timing_log_cloud, "cloud"))
        cv2.destroyAllWindows()
        pbar.close()

    def inference_func(self, model, data, num_layers, next_client_id, num_layers_model, splits, batch_size, logger, compress):
        if len(splits) == 3 :
            self.splits = (splits[0],splits[1])
        else:
            self.splits = splits[0]
        if os.path.exists("detections_stream.jsonl"):
            os.remove("detections_stream.jsonl")
        if self.layer_id == 1:
            self.first_layer(model, data, batch_size, logger, compress, next_client_id)
        elif self.layer_id == num_layers:
            if splits[1] == num_layers_model:
                self.last_layer(model, batch_size, splits[0], logger, compress)
                self._print_summary()
                self._print_map()
                if self._det_results:
                    self._write_detections_json()
            else:
                self.middle_layer(model, batch_size, splits[0], logger, compress,next_client_id)
