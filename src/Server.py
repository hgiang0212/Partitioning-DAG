import os
import sys
import json
import math
import time
import base64
import shutil
import threading
import pickle
import urllib.parse

import numpy as np
import pika
import requests
from requests.auth import HTTPBasicAuth
from ultralytics import YOLO

import src.Log
from src.BrokerGuard import BrokerGuard
from src.BrokerRam import BrokerRamSampler
from src.Measure import clip_intervals, merge_intervals, total_length
from src.PDD import evaluate_pdd_for_one_client


class Server:
    def __init__(self, config):
        self.address      = config["rabbit"]["address"]
        self.username     = config["rabbit"]["username"]
        self.password     = config["rabbit"]["password"]
        self.virtual_host = config["rabbit"]["virtual-host"]

        self.model_name = config["server"]["model"]
        self.total_clients = config["server"]["clients"]
        self.cut_layer = config["server"]["cut-layer"]
        self.batch_size = config["server"]["batch-size"]
        self.data = config["data"]
        self.compress = config["compress"]

        credentials = pika.PlainCredentials(self.username, self.password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.address, 5672, f'{self.virtual_host}', credentials))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='rpc_queue')

        self.register_clients = [0 for _ in range(len(self.total_clients))]
        self.list_clients = []
        self.count_clients = 0
        self.notify_counts = [0 for _ in range(len(self.total_clients))]

        self.channel.basic_qos(prefetch_count=1)
        self.reply_channel = self.connection.channel()
        self.channel.basic_consume(queue="rpc_queue", on_message_callback=self.on_request)


        # PDD config
        self.pdd = config["pdd"]["enabled"]
        self.inter_cloud_bw = config["pdd"]["inter_cloud_bandwidth_MBps"]
        self.devices_path = config["profile"]["devices_path"]
        self.layer_profile_path = config["profile"]["layer_profile_path"]

        log_path = config["log-path"]
        self.logger = src.Log.Logger(f"{log_path}/app.log", config["debug-mode"])
        self.logger.log_info(f"Application start. Server is waiting for {self.total_clients} clients.")
        src.Log.print_with_color(f"Application start. Server is waiting for {self.total_clients} clients.", "green")

        # ── Result files (guide/01-result-format.md §2) ────────────────────
        # Naming scheme for THIS project: `group_*` filenames carrying `cluster=`
        # keys. One scheme per project — never mix in the `fps_cluster_*` set.
        # A group here is the cloud chain an edge feeds, named after the queue it
        # publishes into (`queue_2`); the completing tier tags every DONE with it.
        self.log_path = log_path
        self.batch_log_path     = f"{log_path}/batch_done_ns.log"
        self.group_rate_ns_path = f"{log_path}/group_rate_ns.log"
        self.group_rate_path    = f"{log_path}/group_rate.log"
        self.util_log_path      = f"{log_path}/utilization.log"
        self.util_group_path    = f"{log_path}/utilization_group.log"
        self.latency_group_path = f"{log_path}/latency_group.log"
        self.events_path        = f"{log_path}/events_ns.log"
        # Optional families, each all-its-files-or-none (01 §2):
        self.free_time_path        = f"{log_path}/free_time.log"          # 10
        self.free_time_group_path  = f"{log_path}/free_time_group.log"
        self.free_time_series_path = f"{log_path}/free_time_series.log"
        self.broker_ram_ns_path    = f"{log_path}/broker_ram_ns.log"      # 11
        self.broker_ram_path       = f"{log_path}/broker_ram.log"
        self.msg_size_path         = f"{log_path}/message_size.log"       # 12
        self.msg_size_series_path  = f"{log_path}/message_size_series.log"
        self.broker_guard_ns_path  = f"{log_path}/broker_guard_ns.log"    # 13
        self.broker_guard_path     = f"{log_path}/broker_guard.log"
        self.result_files = [self.batch_log_path, self.group_rate_ns_path,
                             self.group_rate_path, self.util_log_path,
                             self.util_group_path, self.latency_group_path,
                             self.events_path,
                             self.free_time_path, self.free_time_group_path,
                             self.free_time_series_path,
                             self.broker_ram_ns_path, self.broker_ram_path,
                             self.msg_size_path, self.msg_size_series_path,
                             self.broker_guard_ns_path, self.broker_guard_path]
        # Truncate ALL of them UNCONDITIONALLY, once, centrally, before any
        # worker can write (01 §4) — including the optional ones, and including
        # the ones this run's flags turn off. Conditional truncation is how a
        # previous run's file leaks into this run's archive (05 §4), and an
        # empty-but-present file is a valid "this run had none".
        for path in self.result_files:
            open(path, "w").close()

        # ── FPS measurement (guide/02-throughput.md) ───────────────────────
        fps_cfg = config.get("fps") or {}
        self._fps_grace_s = float(fps_cfg.get("grace_s", 10))          # keep collecting after work queues drain
        self._fps_hardcap_s = float(fps_cfg.get("shutdown_timeout_s", 300))  # stall detector: give up after this long with no completed batch

        self.channel.queue_declare(queue="fps_queue", durable=False)
        self.channel.queue_purge(queue="fps_queue")   # start every run from empty
        self.channel.basic_consume(queue="fps_queue", on_message_callback=self.on_fps)

        self._fps_times = []          # arrival time of every DONE (seconds, server clock)
        self._group_times = {}        # cluster -> that group's arrival times, same clock
        self._fps_start_t = None      # set when START is dispatched to the workers
        self._fps_window = 16         # DONEs per live smoothed sample (W; charts assume 16)
        self._fps_stop_bcast_t = None # time the first tier finished
        self._fps_empty_since = None  # time work queues were first seen empty
        self._fps_work_queues = set() # pipeline queues to watch while draining
        self._fps_printed = False
        self._fps_shutdown = False    # set by _finish_fps; start()'s loop exits on it

        # ── Device utilization + latency (guide/03, guide/04) ──────────────
        self.channel.queue_declare(queue="utilization_queue", durable=False)
        self.channel.queue_purge(queue="utilization_queue")   # drop stale reports from a crashed run

        # ── Optional measurements (guide/10, 11, 12) ───────────────────────
        # Every flag lives HERE, in the server's config, and travels to the
        # workers in the dispatch message. Nothing below is ever read by a
        # worker from its own config file (guide/README invariant 9), so a
        # measurement setting can never drift across N machines and silently
        # mix two configurations into one run.
        ft_cfg = config.get("free_time") or {}
        self._free_time_enabled = bool(ft_cfg.get("enabled", False))
        self._free_time_dispatch = {
            "enabled": self._free_time_enabled,
            "bucket_s": float(ft_cfg.get("bucket_s", 1.0)),
            "max_intervals": int(ft_cfg.get("max_intervals", 4000)),
        }
        ms_cfg = config.get("message_size") or {}
        self._msg_size_enabled = bool(ms_cfg.get("enabled", False))
        self._msg_size_dispatch = {
            "enabled": self._msg_size_enabled,
            "max_series": int(ms_cfg.get("max_series", 2000)),
            "mode": ms_cfg.get("mode", "split"),
        }
        # Chosen by registration order, which the server already knows before it
        # dispatches anything and which needs no configuration (12 §1).
        self._msg_size_client = None

        # The infra host's window opens HERE, at controller start — before any
        # worker registers or anything is published — so the series contains the
        # host at rest, which is the only thing that turns "this host was using
        # N MB" into "running the system costs this host N MB" (11 §6).
        self._broker_ram = BrokerRamSampler(
            config.get("broker_ram"), self.address, self.username, self.password,
            self.virtual_host, self.broker_ram_ns_path, self.broker_ram_path)
        self._broker_ram.start()

        # ── Bounding that host's memory (guide/13-broker-guard.md) ─────────
        # Applied HERE, before a single worker can register: the limits have to
        # be in force before anything is published, and a queue policy applied
        # halfway through a run leaves the first half unbounded and the two
        # halves incomparable. The ssh block from broker_ram is reused — the
        # watermark lives on the same host, behind the same login — so the
        # credentials are configured once.
        guard_cfg = config.get("broker_guard") or {}
        self._broker_guard = BrokerGuard(
            guard_cfg, self.address, self.username, self.password,
            self.virtual_host, self.broker_guard_ns_path, self.broker_guard_path,
            ssh_config=(config.get("broker_ram") or {}).get("ssh"))
        self._broker_guard.apply()
        self._guard_reports = []

        # ── Archiving (guide/05-archiving.md) ──────────────────────────────
        archive_cfg = config.get("archive") or {}
        self._archive_enabled = bool(archive_cfg.get("enabled", True))
        self._archive_root = archive_cfg.get("path", "results")
        self._config_path = archive_cfg.get("config_path", "config.yaml")

    def on_request(self, ch, method, _, body):
        message    = pickle.loads(body)
        action = message["action"]

        if action == "REGISTER":
            client_id = message["client_id"]
            layer_id  = message["layer_id"]

            if (int(client_id), layer_id) not in self.list_clients:
                self.list_clients.append((int(client_id), layer_id))

            # First worker to register at the FIRST tier measures payload size
            # (12 §1). The first tier is the one whose output crosses the
            # network, and every worker in a group publishes the same payload
            # shape from the same split point — so nine workers measuring would
            # produce one number nine times, at nine times the cost.
            if layer_id == 1 and self._msg_size_client is None:
                self._msg_size_client = int(client_id)

            src.Log.print_with_color(f"[<<<] Received message from client: {message}", "blue")
            self.register_clients[layer_id - 1] += 1

            if self.register_clients == self.total_clients:
                src.Log.print_with_color("All clients are connected. Sending notifications.", "green")
                self.notify_clients()


        elif action == "NOTIFY":

            layer_id = message["layer_id"]
            self.notify_counts[layer_id - 1] += 1

            src.Log.print_with_color(
                f"[FPS] NOTIFY: {self.notify_counts[0]}/{self.total_clients[0]} edges finished", "yellow")

            # First tier (edges) finished → STOP the edges only, and keep
            # consuming fps_queue until the work queues drain (§8). Clouds get
            # their STOP from _finish_fps once every DONE is counted — sending
            # it now lets a momentarily-starved downstream cloud exit early
            # and lose the batches still flowing through the chain.
            if self._fps_stop_bcast_t is None and self.notify_counts[0] >= self.total_clients[0]:
                self.logger.log_info("Stop Inference !!!")
                self._log_event("system", "all edges finished, STOP broadcast to layer 1")
                self.notify_clients(start=False, layers={1})
                self._fps_stop_bcast_t = time.time()
                self._fps_work_queues = self._fps_discover_work_queues()
                src.Log.print_with_color(
                    f"[FPS] drain-watch started: watching {sorted(self._fps_work_queues)}", "yellow")
                self.connection.call_later(1.0, self._fps_drain_check)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    # ─────────────────── Control-plane events (01 §3.7) ──────────────────────

    def _log_event(self, scope, description):
        """One control decision per line: `<ts_ns> <scope>: <description>`.
        The timestamp is taken BEFORE the decision is broadcast, so it marks when
        the decision was made rather than when it landed. The file is opened and
        closed per append, so no handle is ever shared across threads."""
        try:
            with open(self.events_path, "a") as f:
                f.write(f"{time.time_ns()} {scope}: {description}\n")
        except Exception as e:
            src.Log.print_with_color(f"[Events] write failed: {e}", "yellow")

    # ──────────────────────── FPS (guide/02-throughput.md) ───────────────────

    def on_fps(self, ch, method, _props, body):
        # The ARRIVAL is the event. The body carries an identity (the producing
        # group) and never a measurement, so a garbled body can mis-bucket one
        # batch but can never distort a rate.
        t_ns = time.time_ns()          # ONE clock reading, used for both the FPS math and the log
        t_s = t_ns / 1e9
        self._fps_times.append(t_s)
        n = len(self._fps_times)
        W = self._fps_window
        window_fps = None
        if n >= W:                                   # live smoothed view
            span = self._fps_times[-1] - self._fps_times[-W]
            if span > 0:
                window_fps = (W - 1) * self.batch_size / span
                src.Log.print_with_color(
                    f"[FPS] DONE #{n}  window_fps={window_fps:6.2f} (last {W} batches)", "cyan")
        with open(self.batch_log_path, "a") as f:    # authoritative system series
            if window_fps is None:
                f.write(f"{t_ns}\n")
            else:
                f.write(f"{t_ns} {window_fps:.2f}\n")

        # Same arrival, bucketed by group (01 §3.2). An unrecognised body is
        # bucketed as `unknown`, never dropped — an old worker degrades the
        # breakdown instead of losing batches, so the line counts still match.
        try:
            cluster = body.decode("utf-8", "ignore").strip()
        except Exception:
            cluster = ""
        if not cluster or " " in cluster or cluster == "DONE":
            cluster = "unknown"
        group = self._group_times.setdefault(cluster, [])
        group.append(t_s)
        gn = len(group)
        group_line = f"{t_ns} cluster={cluster} done={gn}"
        if gn >= W:                 # a group reaches its first full window LATER
            gspan = group[-1] - group[-W]        # than the system does — correct, not a bug
            if gspan > 0:
                group_line += f" window_fps={(W - 1) * self.batch_size / gspan:.2f}"
        with open(self.group_rate_ns_path, "a") as f:
            f.write(group_line + "\n")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _fps_discover_work_queues(self):
        """Names of the queues batches flow through, via the management HTTP API."""
        try:
            url = f'http://{self.address}:15672/api/queues'
            response = requests.get(url, auth=HTTPBasicAuth(self.username, self.password), timeout=5)
            if response.status_code == 200:
                return {q["name"] for q in response.json() if q["name"].startswith("queue_")}
        except Exception:
            pass
        return {"queue_0", "queue_1", "queue_2"}   # fallback: queues Scheduler declares

    def _fps_total_work_depth(self):
        """Sum of messages across the work queues, or None if the API is unreachable."""
        try:
            vhost = urllib.parse.quote(self.virtual_host, safe="")
            total = 0
            for queue_name in self._fps_work_queues:
                url = f'http://{self.address}:15672/api/queues/{vhost}/{queue_name}'
                response = requests.get(url, auth=HTTPBasicAuth(self.username, self.password), timeout=5)
                if response.status_code == 404:      # queue already deleted → empty
                    continue
                if response.status_code != 200:
                    return None
                total += int(response.json().get("messages", 0))
            return total
        except Exception:
            return None

    def _fps_drain_check(self):
        if self._fps_printed:
            return
        now = time.time()
        # Hard cap = stall detector: fires only after `shutdown_timeout_s` with
        # ZERO completed batches. Each arriving DONE slides the window, so a
        # slow-but-alive drain is never truncated — but a dead worker whose
        # queue never moves still can't hang the server.
        last_progress = max(
            self._fps_times[-1] if self._fps_times else 0.0,
            self._fps_stop_bcast_t or 0.0,
        )
        if last_progress and (now - last_progress) >= self._fps_hardcap_s:
            return self._finish_fps(
                f"hard cap reached (no completed batch for {self._fps_hardcap_s:.0f}s)")

        depth = self._fps_total_work_depth()
        self._fps_drain_ticks = getattr(self, "_fps_drain_ticks", 0) + 1
        if self._fps_drain_ticks % 5 == 0:   # heartbeat so a long drain is visibly alive
            src.Log.print_with_color(
                f"[FPS] draining: work depth={depth}  DONEs={len(self._fps_times)}  "
                f"(finish {self._fps_grace_s:.0f}s after depth stays 0)", "yellow")
        if depth is None:
            # can't query queues → fall back to "no DONE for grace seconds"
            last = self._fps_times[-1] if self._fps_times else self._fps_stop_bcast_t
            if last and (now - last) >= self._fps_grace_s:
                return self._finish_fps("grace elapsed (no queue stats)")
        elif depth > 0:
            self._fps_empty_since = None          # still draining → reset grace
        else:
            if self._fps_empty_since is None:
                self._fps_empty_since = now
            elif (now - self._fps_empty_since) >= self._fps_grace_s:
                return self._finish_fps("work queues drained + grace")

        self.connection.call_later(1.0, self._fps_drain_check)  # re-arm

    def _finish_fps(self, reason=""):
        if self._fps_printed:
            return
        self._fps_printed = True
        t, n, bs = self._fps_times, len(self._fps_times), self.batch_size
        print("=" * 60)
        if n >= 1 and self._fps_start_t is not None and t[-1] > self._fps_start_t:
            total_time = t[-1] - self._fps_start_t
            system_fps = n * bs / total_time
            print(f"  [SYSTEM FPS]      {system_fps:8.3f} fps   "
                  f"= {n} DONE x {bs} / {total_time:.2f}s  (START -> last DONE)")
            if n >= 2 and t[-1] > t[0]:
                span = t[-1] - t[0]
                steady = (n - 1) * bs / span
                print(f"  [steady-state]    {steady:8.3f} fps   "
                      f"= {n - 1} x {bs} / {span:.2f}s  (first -> last DONE)")
            if n >= 2:
                gaps = [t[i] - t[i - 1] for i in range(1, n) if t[i] > t[i - 1]]
                if gaps:
                    ref_mean = sum(bs / g for g in gaps) / len(gaps)
                    print(f"  [ref mean, N/U]   {ref_mean:8.3f} fps   "
                          f"(arithmetic mean of 1/dt — reference only, biased high)")
        else:
            print("  [SYSTEM FPS]      no DONEs received — nothing to report")
        print(f"  batches counted: {n}   stop reason: {reason}")
        print("=" * 60)
        self._write_group_rate()
        # Every DONE is counted — now release the tiers kept alive to drain
        # the backlog (layers >= 2; the edges got their STOP at first-tier-done).
        self._log_event("system", f"drain complete, releasing clouds ({reason})")
        try:
            self.notify_clients(start=False, layers=set(range(2, len(self.total_clients) + 1)))
        except Exception as e:
            src.Log.print_with_color(f"[FPS] releasing clouds failed: {e}", "red")
        # No stop_consuming() here — it would have to unwind pika's nested
        # dispatch from inside this timer callback. Just flag the main loop
        # in start(), which closes the connection from the main frame.
        self._fps_shutdown = True

    def _write_group_rate(self):
        """Throughput summary: one line per group + one SYSTEM line (01 §3.3).

        `fps` uses the SHARED START for every scope, with each scope's own last
        completion as the end — that is what makes `SYSTEM span == max group
        span` hold. `steady_fps` uses the group's OWN first completion, so a
        group that started late is not penalised; it is the fair number for
        comparing groups. Group `fps` values do NOT sum to SYSTEM and are not
        meant to: each divides by its own span, so Σ group_fps >= SYSTEM fps."""
        t_ns = time.time_ns()
        bs, start = self.batch_size, self._fps_start_t
        total_done = len(self._fps_times)
        lines = []
        for cluster, times in sorted(self._group_times.items()):
            n = len(times)
            frames = n * bs
            fps = frames / (times[-1] - start) if start and times[-1] > start else 0.0
            steady = ((n - 1) * bs / (times[-1] - times[0])
                      if n >= 2 and times[-1] > times[0] else 0.0)
            share = 100.0 * n / total_done if total_done else 0.0
            lines.append(f"{t_ns} cluster={cluster} fps={fps:.3f} steady_fps={steady:.3f} "
                         f"done={n} frames={frames} share={share:.1f}%")
        sys_frames = total_done * bs
        sys_fps = (sys_frames / (self._fps_times[-1] - start)
                   if start and total_done and self._fps_times[-1] > start else 0.0)
        # The SYSTEM line carries neither steady_fps nor share, by spec.
        lines.append(f"{t_ns} SYSTEM fps={sys_fps:.3f} done={total_done} "
                     f"frames={sys_frames} clusters={len(self._group_times)}")
        try:
            with open(self.group_rate_path, "w") as f:
                f.write("\n".join(lines) + "\n")
            for line in lines:
                src.Log.print_with_color(f"[FPS] {line}", "cyan")
        except Exception as e:
            src.Log.print_with_color(f"[FPS] writing {self.group_rate_path} failed: {e}", "red")

    # ─────────────── Utilization + latency collection (03, 04) ───────────────

    def _collect_utilization(self, timeout_s=30.0):
        """Drain utilization_queue into utilization.log, then roll the same
        reports up into utilization_group.log and latency_group.log (03 §5).

        Runs after the FPS summary — every device has (or is about to have)
        published its report, and the reports sit on the broker until fetched
        here, so publisher and consumer never need to be alive at once."""
        expected = {cid for cid, _ in self.list_clients}
        reported = set()
        reports = []
        deadline = time.time() + timeout_s
        while len(reported) < len(expected) and time.time() < deadline:
            method_frame, _props, body = self.channel.basic_get(
                queue="utilization_queue", auto_ack=True)
            if not body:
                time.sleep(0.2)
                continue
            try:
                message = pickle.loads(body)
            except Exception:
                continue
            if not isinstance(message, dict) or message.get("action") != "UTILIZATION":
                continue
            t_ns = time.time_ns()   # server-clock arrival prefix
            reported.add(message.get("client_id"))
            reports.append(message)
            line = (f"{t_ns} client={message.get('client_id')} role={message.get('role')} "
                    f"packages={message.get('packages')} "
                    f"busy_s={message.get('busy_ns', 0) / 1e9:.3f} "
                    f"total_s={message.get('total_ns', 0) / 1e9:.3f} "
                    f"utilization={message.get('utilization', 0.0) * 100:.2f}%")
            with open(self.util_log_path, "a") as f:
                f.write(line + "\n")
            src.Log.print_with_color(f"[Utilization] {line}", "green")
        if len(reported) < len(expected):
            # A partial collection warns but never aborts: telemetry loses a
            # number, it must not lose the run.
            src.Log.print_with_color(
                f"[Utilization] Collected {len(reported)}/{len(expected)} reports before timeout",
                "yellow")
        self._write_utilization_group(reports)
        self._write_latency_group(reports)
        # Both optional families ride on the same reports and the same drain, so
        # turning one off costs the shutdown nothing: there is no extra queue to
        # poll and no extra timeout to burn (guide/README invariant 10).
        self._write_free_time(reports)
        self._write_message_size(reports)
        # Held, not written: the guard's own peaks are only final once its
        # monitor has stopped, and that happens after this drain (13 §4).
        self._guard_reports = [r["broker_guard"] for r in reports
                               if r.get("broker_guard")]

    @staticmethod
    def _cluster_of(report):
        return report.get("cluster") or "unknown"

    def _write_utilization_group(self, reports):
        """Roll the per-device reports up per group and per group/role (01 §3.5).

        `utilization` is POOLED (Σbusy / Σtotal), weighting each device by how
        long it ran; `utilization_mean` is the plain mean of the per-device
        ratios. Both are emitted on ALL/SYSTEM lines because a pooled figure can
        hide one idle device inside a busy group — when the two diverge, the
        group is imbalanced, and that divergence is the signal."""
        def ratios(devs):
            busy = sum(d.get("busy_ns", 0) for d in devs)
            total = sum(d.get("total_ns", 0) for d in devs)
            pooled = busy / total if total else 0.0
            per_device = [d["busy_ns"] / d["total_ns"] for d in devs
                          if d.get("total_ns")]
            mean = sum(per_device) / len(per_device) if per_device else 0.0
            packages = sum(d.get("packages", 0) for d in devs)
            return busy, total, pooled, mean, packages

        by_cluster = {}
        for report in reports:
            by_cluster.setdefault(self._cluster_of(report), []).append(report)

        t_ns = time.time_ns()
        lines = []
        for cluster, devs in sorted(by_cluster.items()):
            busy, total, pooled, mean, packages = ratios(devs)
            lines.append(f"{t_ns} cluster={cluster} ALL devices={len(devs)} "
                         f"utilization={pooled * 100:.2f}% utilization_mean={mean * 100:.2f}% "
                         f"busy_s={busy / 1e9:.3f} total_s={total / 1e9:.3f} packages={packages}")
            for role in sorted({d.get("role") or "unknown" for d in devs}):
                role_devs = [d for d in devs if (d.get("role") or "unknown") == role]
                busy, total, pooled, _mean, packages = ratios(role_devs)
                lines.append(f"{t_ns} cluster={cluster} role={role} devices={len(role_devs)} "
                             f"utilization={pooled * 100:.2f}% busy_s={busy / 1e9:.3f} "
                             f"total_s={total / 1e9:.3f} packages={packages}")
        busy, total, pooled, mean, _packages = ratios(reports)
        lines.append(f"{t_ns} SYSTEM devices={len(reports)} clusters={len(by_cluster)} "
                     f"utilization={pooled * 100:.2f}% utilization_mean={mean * 100:.2f}% "
                     f"busy_s={busy / 1e9:.3f} total_s={total / 1e9:.3f}")
        try:
            with open(self.util_group_path, "w") as f:
                f.write("\n".join(lines) + "\n")
            src.Log.print_with_color(f"[Utilization] {lines[-1]}", "green")
        except Exception as e:
            src.Log.print_with_color(
                f"[Utilization] writing {self.util_group_path} failed: {e}", "yellow")

    @staticmethod
    def _percentile(sorted_samples, q):
        """Nearest-rank percentile, no interpolation — every number printed is a
        latency that was actually observed on some batch. q in [0, 100]."""
        if not sorted_samples:
            return None
        k = max(1, math.ceil(q / 100.0 * len(sorted_samples)))
        return sorted_samples[k - 1]

    def _latency_stats_line(self, t_ns, scope, kind, samples):
        """One `latency_group.log` line from POOLED raw samples. Percentiles are
        never averaged across devices — that has no statistical meaning — so the
        devices ship raw arrays and the reduction happens once, here."""
        s = sorted(samples)
        return (f"{t_ns} {scope} kind={kind} n={len(s)} "
                f"mean_ms={sum(s) / len(s):.3f} "
                f"p50_ms={self._percentile(s, 50):.3f} "
                f"p95_ms={self._percentile(s, 95):.3f} "
                f"max_ms={s[-1]:.3f}")

    def _write_latency_group(self, reports):
        """Latency distributions per group/role, per group, and SYSTEM (01 §3.6).

        `service` spans the device's own get input -> output, so its samples sum
        to that scope's `busy_s` in utilization_group.log — the conformance check
        that ties the two files together. `pipeline` adds any local buffering
        before the batch was ready. `e2e` spans two machines by definition and is
        reported only by the completing tier, so it carries no `role=`."""
        by_role = {}     # (cluster, role) -> {kind: samples}
        by_cluster = {}  # cluster -> e2e samples
        system_e2e = []
        for report in reports:
            cluster = self._cluster_of(report)
            role = report.get("role") or "unknown"
            for kind in ("service", "pipeline"):
                samples = report.get(f"{kind}_ms") or []
                if samples:
                    by_role.setdefault((cluster, role), {}).setdefault(kind, []).extend(samples)
            e2e = report.get("e2e_ms") or []
            if e2e:
                by_cluster.setdefault(cluster, []).extend(e2e)
                system_e2e.extend(e2e)

        t_ns = time.time_ns()
        lines = []
        for (cluster, role), kinds in sorted(by_role.items()):
            for kind in ("service", "pipeline"):
                if kinds.get(kind):
                    lines.append(self._latency_stats_line(
                        t_ns, f"cluster={cluster} role={role}", kind, kinds[kind]))
        for cluster, samples in sorted(by_cluster.items()):
            lines.append(self._latency_stats_line(t_ns, f"cluster={cluster}", "e2e", samples))
        if system_e2e:
            lines.append(self._latency_stats_line(t_ns, "SYSTEM", "e2e", system_e2e))
        try:
            with open(self.latency_group_path, "w") as f:
                f.write(("\n".join(lines) + "\n") if lines else "")
            for line in lines:
                src.Log.print_with_color(f"[Latency] {line}", "cyan")
        except Exception as e:
            src.Log.print_with_color(
                f"[Latency] writing {self.latency_group_path} failed: {e}", "yellow")

    # ──────────────────── Free time (guide/10-free-time.md) ──────────────────

    def _write_free_time(self, reports):
        """free_time.log, free_time_group.log and free_time_series.log (01 §3.8-3.10).

        Free time is the wall clock in which a device did nothing AT ALL — no
        input, no compute, no encode, no transfer, no bookkeeping. It is neither
        utilization nor `1 - utilization`: utilization measures one lane's
        `get input -> output` window, so a back-pressure wait inside that window
        counts as busy there and free here, while work on another lane counts as
        busy here and as nothing there. Both are emitted; neither derives from
        the other."""
        if not self._free_time_enabled:
            return
        devices = [(r, r.get("free_time")) for r in reports if r.get("free_time")]
        if not devices:
            src.Log.print_with_color(
                "[FreeTime] enabled but no device reported — nothing written", "yellow")
            return
        t_ns = time.time_ns()
        try:
            self._write_free_time_devices(t_ns, devices)
            self._write_free_time_group(t_ns, devices)
            self._write_free_time_series(t_ns, devices)
        except Exception as e:
            src.Log.print_with_color(f"[FreeTime] writing failed: {e}", "yellow")

    @staticmethod
    def _kv_safe(value, default="unknown"):
        """A `key=value` value MUST contain no spaces (01 §1): the universal
        parser splits on whitespace, so one space shifts every key after it on
        that line. Everything below crosses the wire from a worker, so it is
        sanitized here as well as there — a shared file is the wrong place to
        find out that a hostname had a space in it."""
        text = "_".join(str(value).split()) if value is not None else ""
        return text or default

    @classmethod
    def _ft_ident(cls, report, free_time):
        return (cls._kv_safe(report.get("client_id")),
                cls._kv_safe(report.get("role")),
                cls._kv_safe(free_time.get("machine")),
                cls._kv_safe(report.get("cluster")),
                cls._kv_safe(report.get("device")))

    def _write_free_time_devices(self, t_ns, devices):
        lines = []
        for report, ft in devices:
            client, role, machine, cluster, device = self._ft_ident(report, ft)
            span_s, free_s = ft["span_ns"] / 1e9, ft["free_ns"] / 1e9
            host_idle = ft.get("host_idle")
            # busy_s is the measure of the MERGED intervals, never a sum of
            # per-stage timers: a sum can exceed span_s whenever two lanes
            # overlap, which is normal for a pipelined device.
            line = (f"{t_ns} client={client} role={role} machine={machine} "
                    f"cluster={cluster} device={device} span_s={span_s:.3f} "
                    f"busy_s={ft['busy_ns'] / 1e9:.3f} free_s={free_s:.3f} "
                    f"free={100.0 * ft['free_ns'] / ft['span_ns']:.2f}% "
                    f"gaps={ft['gaps']} "
                    f"longest_free_ms={ft['longest_free_ns'] / 1e6:.3f}")
            if host_idle is not None:
                line += f" host_idle={host_idle:.2f}%"
            lines.append(line)
        with open(self.free_time_path, "w") as f:
            f.write("\n".join(lines) + "\n")
        src.Log.print_with_color(f"[FreeTime] {len(lines)} device line(s)", "green")

    def _write_free_time_group(self, t_ns, devices):
        def pooled(group):
            free = sum(ft["free_ns"] for _, ft in group)
            span = sum(ft["span_ns"] for _, ft in group)
            mean = (sum(100.0 * ft["free_ns"] / ft["span_ns"] for _, ft in group)
                    / len(group)) if group else 0.0
            return free, span, (100.0 * free / span if span else 0.0), mean

        by_cluster, by_machine = {}, {}
        for report, ft in devices:
            _, _, machine, cluster, _ = self._ft_ident(report, ft)
            by_cluster.setdefault(cluster, []).append((report, ft))
            by_machine.setdefault(machine, []).append((report, ft))

        lines = []
        for cluster, group in sorted(by_cluster.items()):
            free, span, ratio, mean = pooled(group)
            lines.append(f"{t_ns} cluster={cluster} ALL devices={len(group)} "
                         f"free={ratio:.2f}% free_mean={mean:.2f}% "
                         f"free_s={free / 1e9:.3f} span_s={span / 1e9:.3f}")
            for role in sorted({r.get("role") or "unknown" for r, _ in group}):
                sub = [(r, ft) for r, ft in group if (r.get("role") or "unknown") == role]
                free_r, span_r, ratio_r, _ = pooled(sub)
                lines.append(f"{t_ns} cluster={cluster} role={role} devices={len(sub)} "
                             f"free={ratio_r:.2f}% free_s={free_r / 1e9:.3f} "
                             f"span_s={span_r / 1e9:.3f}")
            # Reasons sum to EXACTLY the scope's free time: attribution is
            # priority-ordered on the device, so no moment is claimed twice, and
            # whatever no reason covers is reported as `unaccounted` rather than
            # quietly dropped.
            reasons = {}
            for _, ft in group:
                for reason, value in (ft.get("reasons_ns") or {}).items():
                    reasons[reason] = reasons.get(reason, 0) + value
            for reason, value in sorted(reasons.items(), key=lambda kv: -kv[1]):
                lines.append(f"{t_ns} cluster={cluster} FREE reason={reason} "
                             f"free_s={value / 1e9:.3f} "
                             f"share={(100.0 * value / free if free else 0.0):.2f}%")
            # KIND shares MAY sum to more than 100%: per-kind sums overlap across
            # lanes by construction. Only the merged busy_s above is exclusive.
            kinds = {}
            for _, ft in group:
                for kind, value in (ft.get("kinds_ns") or {}).items():
                    kinds[kind] = kinds.get(kind, 0) + value
            for kind, value in sorted(kinds.items(), key=lambda kv: -kv[1]):
                lines.append(f"{t_ns} cluster={cluster} KIND kind={kind} "
                             f"busy_s={value / 1e9:.3f} "
                             f"share={(100.0 * value / span if span else 0.0):.2f}%")

        # MACHINE lines come from the UNION of the busy intervals of the device
        # processes on that host, never from their ratios: two devices that are
        # each 50% free can keep a machine 100% busy by interleaving. This is the
        # one place device timestamps are compared, and it is valid for exactly
        # one reason — processes on one host share a clock. Never across hosts.
        for machine, group in sorted(by_machine.items()):
            lo = min(ft["epoch_start_ns"] for _, ft in group)
            hi = max(ft["epoch_end_ns"] for _, ft in group)
            busy = merge_intervals(clip_intervals(
                [iv for _, ft in group for iv in (ft.get("busy_epoch_ns") or [])], lo, hi))
            span_ns = max(hi - lo, 1)
            free_ns = max(span_ns - total_length(busy), 0)
            idles = [ft["host_idle"] for _, ft in group if ft.get("host_idle") is not None]
            line = (f"{t_ns} MACHINE machine={machine} devices={len(group)} "
                    f"free={100.0 * free_ns / span_ns:.2f}% free_s={free_ns / 1e9:.3f} "
                    f"span_s={span_ns / 1e9:.3f} "
                    f"merge_slop_s={sum(ft.get('merge_slop_ns', 0) for _, ft in group) / 1e9:.3f}")
            if idles:
                line += f" host_idle={sum(idles) / len(idles):.2f}%"
            lines.append(line)

        free, span, ratio, mean = pooled(devices)
        lines.append(f"{t_ns} SYSTEM devices={len(devices)} clusters={len(by_cluster)} "
                     f"machines={len(by_machine)} free={ratio:.2f}% free_mean={mean:.2f}% "
                     f"free_s={free / 1e9:.3f} span_s={span / 1e9:.3f}")
        with open(self.free_time_group_path, "w") as f:
            f.write("\n".join(lines) + "\n")
        src.Log.print_with_color(f"[FreeTime] {lines[-1]}", "green")

    def _write_free_time_series(self, t_ns, devices):
        """One line per device per bucket. The leading timestamp is the report's
        server-clock arrival; the position in the run is carried by t_offset_s,
        which is on the DEVICE's clock. Devices start at different moments, so
        their offsets are not directly comparable — do not conflate the two."""
        lines = []
        for report, ft in devices:
            client, role, machine, cluster, _ = self._ft_ident(report, ft)
            for i, offset_s, bucket_s, free_pct in ft.get("series") or []:
                # bucket_s travels on EVERY line rather than being assumed, so a
                # long run may widen its buckets without breaking readers.
                lines.append(f"{t_ns} client={client} role={role} machine={machine} "
                             f"cluster={cluster} i={i} t_offset_s={offset_s:.3f} "
                             f"bucket_s={bucket_s:.3f} free={free_pct:.2f}%")
        with open(self.free_time_series_path, "w") as f:
            f.write(("\n".join(lines) + "\n") if lines else "")

    # ─────────────────── Message size (guide/12-message-size.md) ─────────────

    def _write_message_size(self, reports):
        """message_size.log + message_size_series.log (01 §3.11-3.12).

        Normally exactly one line in the summary: the payload shape is fixed by
        the configuration, so it is the same on every worker in a group and one
        worker measuring is one worker too few only if it fails."""
        if not self._msg_size_enabled:
            return
        measured = [r["message_size"] for r in reports if r.get("message_size")]
        if not measured:
            src.Log.print_with_color(
                "[MsgSize] enabled but the selected worker reported nothing", "yellow")
            return
        t_ns = time.time_ns()
        try:
            summary, series = [], []
            for report in measured:
                n, span_s = report["n"], report["span_s"]
                total_mb = report["total_bytes"] / 1e6
                mean_mb = report["mean_bytes"] / 1e6
                safe = {k: self._kv_safe(report.get(k)) for k in
                        ("client", "role", "machine", "cluster", "mode", "splits",
                         "compress", "num_bit", "batch_size")}
                try:
                    batch_size = float(report.get("batch_size") or 0)
                except (TypeError, ValueError):
                    batch_size = 0.0
                summary.append(
                    f"{t_ns} client={safe['client']} role={safe['role']} "
                    f"machine={safe['machine']} cluster={safe['cluster']} "
                    # The context that DETERMINES the size. A size without it
                    # cannot be reproduced.
                    f"mode={safe['mode']} splits={safe['splits']} "
                    f"compress={safe['compress']} num_bit={safe['num_bit']} "
                    f"batch_size={safe['batch_size']} n={n} "
                    f"total_mb={total_mb:.3f} mean_mb={mean_mb:.3f} "
                    f"p50_mb={report['p50_bytes'] / 1e6:.3f} "
                    f"p95_mb={report['p95_bytes'] / 1e6:.3f} "
                    f"max_mb={report['max_bytes'] / 1e6:.3f} "
                    f"min_mb={report['min_bytes'] / 1e6:.3f} "
                    f"span_s={span_s:.3f} "
                    f"rate_mb_s={(total_mb / span_s if span_s else 0.0):.3f} "
                    f"per_frame_mb={(mean_mb / batch_size if batch_size else 0.0):.4f}")
                # Offsets, never device timestamps: every timestamp in a shared
                # file is the server's own clock (invariant 1), and an offset
                # locates a sample without ever crossing machines.
                for i, (offset_s, batch_id, n_bytes) in enumerate(report.get("series") or []):
                    series.append(f"{t_ns} client={safe['client']} "
                                  f"cluster={safe['cluster']} i={i} "
                                  f"t_offset_s={offset_s:.3f} batch_id={batch_id} "
                                  f"bytes={n_bytes} mb={n_bytes / 1e6:.3f}")
            with open(self.msg_size_path, "w") as f:
                f.write("\n".join(summary) + "\n")
            with open(self.msg_size_series_path, "w") as f:
                f.write(("\n".join(series) + "\n") if series else "")
            for line in summary:
                src.Log.print_with_color(f"[MsgSize] {line}", "cyan")
        except Exception as e:
            src.Log.print_with_color(f"[MsgSize] writing failed: {e}", "yellow")

    # ─────────────────── Archiving (guide/05-archiving.md) ───────────────────

    def _archive_results(self):
        """Copy this run's logs plus the config that produced them into
        results/<run-id>/. Copy, never move — the live log directory keeps its
        own copies where every existing reader expects them, and the next run
        truncates them itself. Failure is non-fatal."""
        if not self._archive_enabled:
            return
        try:
            tag = "pdd" if self.pdd else f"static_{self.cut_layer}"
            run_id = f"results_{time.strftime('%m%d')}_{time.strftime('%H%M')}_{tag}"
            dest = os.path.join(self._archive_root, run_id)
            suffix = 1
            while os.path.exists(dest):        # collision-safe: …-2, …-3
                suffix += 1
                dest = os.path.join(self._archive_root, f"{run_id}-{suffix}")
            os.makedirs(dest, exist_ok=True)

            copied = []
            for path in self.result_files:
                # Skip empty files: a zero-length log must never be archived as
                # a misleading result. Every file here was truncated at startup,
                # so nothing non-empty can be a leftover from a previous run.
                if os.path.exists(path) and os.path.getsize(path) > 0:
                    shutil.copy2(path, os.path.join(dest, os.path.basename(path)))
                    copied.append(os.path.basename(path))
            if os.path.exists(self._config_path):
                shutil.copy2(self._config_path, os.path.join(dest, "config.yaml"))
                copied.append("config.yaml")

            if len(copied) <= 1:   # config only, or nothing at all
                src.Log.print_with_color(
                    f"[Archive] {dest} is EMPTY — this run produced no results", "red")
            else:
                src.Log.print_with_color(
                    f"[Archive] {dest}  ({len(copied)} files: {', '.join(copied)})", "green")
        except Exception as e:
            src.Log.print_with_color(f"[Archive] failed: {e}", "yellow")

    def send_to_response(self, client_id, message):
        reply_queue_name = f"reply_{client_id}"
        self.reply_channel.queue_declare(reply_queue_name, durable=False)
        src.Log.print_with_color(f"[>>>] Sent notification to client {client_id}", "red")
        self.reply_channel.basic_publish(
            exchange='',
            routing_key=reply_queue_name,
            body=message
        )

    def start(self):
        # Drive the ioloop from the main frame instead of start_consuming():
        # consumer + timer callbacks are dispatched by process_data_events, and
        # the exit condition is checked OUT here, outside any callback — so
        # shutdown never depends on stop_consuming() unwinding pika's nested
        # dispatch (which is what left the server hanging after the summary).
        while not self._fps_shutdown:
            self.connection.process_data_events(time_limit=1.0)
        self._collect_utilization()   # after FPS summary, before close (03 §5)
        # The last collector finishing is not the system being idle: the drain is
        # still settling, and a curve stopped there ends on the busiest moment of
        # the shutdown. Sample a short tail past it (11 §6).
        self._broker_ram.stop()
        self._broker_ram.write_summary()
        # Stop before summarising: the monitor is what observed the peaks, and
        # stopping it is also what hands the broker back — the policy comes off
        # here whether the run ended cleanly or on the stall detector.
        self._broker_guard.stop()
        self._broker_guard.write_summary(self._guard_reports)
        self._archive_results()       # after the last shutdown pipeline has written (05 §3)
        try:
            self.connection.close()
        except Exception:
            pass
        # Belt-and-braces: a stray non-daemon thread (torch/ultralytics) must
        # not keep a finished server process alive.
        sys.stdout.flush()
        os._exit(0)

    def notify_clients(self, start=True, layers=None):
        """start=False sends STOP; `layers` limits it to those layer_ids (None = all)."""
        if start:
            if os.path.exists(f"{self.model_name}.pt"):
                src.Log.print_with_color(f"Exist {self.model_name}", "green")
            else:
                src.Log.print_with_color(f"Download {self.model_name}", "yellow")
                _ = YOLO(f"{self.model_name}.pt")

            optimal_cut, clouds_split, num_layers_model = self._compute_splits_from_profiles()

            file_path = f"{self.model_name}.pt"
            if os.path.exists(file_path):
                src.Log.print_with_color(f"Send model {self.model_name} to devices.", "green")
                with open(f"{self.model_name}.pt", "rb") as f:
                    file_bytes = f.read()
                    encoded = base64.b64encode(file_bytes).decode('utf-8')
            else:
                src.Log.print_with_color(f"{self.model_name} does not exist.", "yellow")
                sys.exit()

            for (client_id, layer_id) in self.list_clients:
                if layer_id == 1:
                    client_split = (optimal_cut,clouds_split["first_cloud"])
                else:
                    client_split = clouds_split[client_id]

                response = {"action": "START",
                            "message": "Server accept the connection",
                            "model": encoded,
                            "splits": client_split,
                            "batch_size": self.batch_size,
                            "num_layers": len(self.total_clients),
                            "model_name": self.model_name,
                            "num_layers_model" : num_layers_model,
                            "data": self.data,
                            "compress": self.compress,
                            # Measurement flags travel WITH the work, so a
                            # worker never reads one locally (invariant 9).
                            "free_time": self._free_time_dispatch,
                            "message_size": self._msg_size_dispatch,
                            # The publisher's share of the memory budget travels
                            # with the work too — a cap that lived in each
                            # worker's config file would be twelve caps.
                            "broker_guard": self._broker_guard.dispatch(),
                            "measure_message_size": bool(
                                self._msg_size_enabled
                                and client_id == self._msg_size_client)}

                self.send_to_response(client_id, pickle.dumps(response))

            if self._msg_size_enabled:
                src.Log.print_with_color(
                    f"[MsgSize] client {self._msg_size_client} selected to measure "
                    f"payload size (first registered at tier 1)", "green")

            # The split decision, overlaid on the timeline as a vertical rule.
            # No `key=value` in the description: the universal parser would pick
            # it up as a kv pair, and this trailing text is meant to be ignored.
            self._log_event(f"queue_{clouds_split['first_cloud']}",
                            f"cut {optimal_cut} "
                            f"({'pdd' if self.pdd else 'static'})")
            self._fps_start_t = time.time()   # shared START: the clock starts when work is dispatched
            # Marks PARTITION the RAM series, they never gate it: sampling does
            # not pause here, so a missing mark coarsens the split rather than
            # leaving a gap (11 §6).
            self._broker_ram.mark("run")
        else:
            response = {"action": "STOP",
                        "message": "Stop inference !!!"}
            for (client_id, layer_id) in self.list_clients:
                if layers is None or layer_id in layers:
                    self.send_to_response(client_id, pickle.dumps(response))

    DEFAULT_CUTS = {"a": 4, "b": 11, "c": 17, "d": 23}

    def _num_layers_model(self):
        """Layer count of the model, read from the layer profile."""
        with open(self.layer_profile_path, "r", encoding="utf-8") as f:
            return len(json.load(f)[0]["cut_data_sizes_mb"]) + 1

    def _static_plan(self):
        """The fixed-cut plan, shaped EXACTLY like the PDD plan.

        Every caller unpacks `(cut, clouds_split, num_layers_model)`, so every
        return path has to produce that shape — returning a bare int or a dict
        on the fallback paths raised a TypeError at dispatch and took the whole
        run with it.

        Policy: cut at the configured layer, then chain every registered cloud
        in registration order with the remaining layers split evenly between
        them. Registration order is the same selector used for the message-size
        worker (12 §1) — it needs no configuration and is stable within a run.
        Splitting evenly rather than parking everything on one cloud keeps every
        registered device doing work; a cloud with no segment would sit in its
        consume loop until STOP and report a run of pure free time."""
        static_cut = self.DEFAULT_CUTS[self.cut_layer]
        try:
            num_layers_model = self._num_layers_model()
        except Exception as e:
            src.Log.print_with_color(
                f"[Split] cannot read {self.layer_profile_path}: {e}", "red")
            num_layers_model = static_cut + 1
        clouds = [client_id for client_id, layer_id in self.list_clients if layer_id > 1]
        if not clouds:
            return static_cut, {"first_cloud": None}, num_layers_model

        n = len(clouds)
        remaining = max(num_layers_model - static_cut, n)
        edges = [static_cut + round(i * remaining / n) for i in range(n)] + [num_layers_model]
        clouds_split = {"first_cloud": clouds[0]}
        for i, client_id in enumerate(clouds):
            clouds_split[client_id] = (edges[i], edges[i + 1],
                                       clouds[i + 1] if i + 1 < n else None)
        return static_cut, clouds_split, num_layers_model

    def _compute_splits_from_profiles(self):
        static_cut = self.DEFAULT_CUTS[self.cut_layer]
        if not self.pdd:
            return self._static_plan()
        try:
            with open(self.devices_path, "r", encoding="utf-8") as f:
                devices_profile = json.load(f)
            with open(self.layer_profile_path, "r", encoding="utf-8") as f:
                layer_profile = json.load(f)
        except Exception as e:
            src.Log.print_with_color(f"[PDD] Đọc profiles thất bại: {e}", "red")
            return self._static_plan()

        # Cloud layer times
        cloud_compute = [cloud["compute_capacity_gflops"] for cloud in devices_profile["clouds"]]

        # Edge clients
        edge_clients = devices_profile["clients"]
        if not edge_clients:
            src.Log.print_with_color(
                "[PDD] Không có edge client trong profiles – dùng static cut", "red"
            )
            return self._static_plan()

        client_compute = [edge["compute_capacity_gflops"] for edge in edge_clients]
        all_cu_flops = np.array(
            client_compute + cloud_compute,
            dtype=float
        )

        bandwidths = edge_clients[0]["bandwidth_mbps"]

        layer_gflops = np.array(layer_profile[0]["layer_gflops"], dtype= float)
        CUT_DATA_SIZES_MB = np.array(layer_profile[0]["cut_data_sizes_mb"], dtype= float)

        num_layers_model = len(CUT_DATA_SIZES_MB)+1
        activation_mb = np.concatenate([[13.0], CUT_DATA_SIZES_MB, [0.0]])
        # Chạy PDD
        try:
            result = evaluate_pdd_for_one_client(
                layer_gflops=layer_gflops,
                all_cu_flops=all_cu_flops,
                activation_mb=activation_mb,
                client_to_cloud_bandwidth_MBps=bandwidths,
                inter_cloud_bandwidth_MBps= 125.0,
            )
            optimal_cut = int(result["local_cut"])
            segments = result["segments"]
            print(segments)
            clouds_split = {}
            clouds_split["first_cloud"] = result["first_cloud"]
            for i in range(len(segments)):
                if i < len(segments)-1 :
                    clouds_split[segments[i][0]] = (segments[i][1],segments[i][2],segments[i+1][0])
                else:
                    clouds_split[segments[i][0]] = (segments[i][1],segments[i][2],None)
            print(clouds_split)
            return optimal_cut, clouds_split , num_layers_model
        except Exception as e:
            src.Log.print_with_color(f"[PDD] Lỗi tính toán: {e}", "red")
            return self._static_plan()
