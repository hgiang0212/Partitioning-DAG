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
        self.result_files = [self.batch_log_path, self.group_rate_ns_path,
                             self.group_rate_path, self.util_log_path,
                             self.util_group_path, self.latency_group_path,
                             self.events_path]
        # Truncate ALL seven UNCONDITIONALLY, once, centrally, before any worker
        # can write (01 §4). Conditional truncation is how a previous run's file
        # leaks into this run's archive (05 §4).
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
                            "compress": self.compress}

                self.send_to_response(client_id, pickle.dumps(response))

            # The split decision, overlaid on the timeline as a vertical rule.
            # No `key=value` in the description: the universal parser would pick
            # it up as a kv pair, and this trailing text is meant to be ignored.
            self._log_event(f"queue_{clouds_split['first_cloud']}",
                            f"cut {optimal_cut} "
                            f"({'pdd' if self.pdd else 'static'})")
            self._fps_start_t = time.time()   # shared START: the clock starts when work is dispatched
        else:
            response = {"action": "STOP",
                        "message": "Stop inference !!!"}
            for (client_id, layer_id) in self.list_clients:
                if layers is None or layer_id in layers:
                    self.send_to_response(client_id, pickle.dumps(response))

    def _compute_splits_from_profiles(self):
        default_splits = {
            "a": 4,
            "b": 11,
            "c": 17,
            "d": 23
        }
        static_cut = default_splits[self.cut_layer]
        if not self.pdd:
            return static_cut
        try:
            with open(self.devices_path, "r", encoding="utf-8") as f:
                devices_profile = json.load(f)
            with open(self.layer_profile_path, "r", encoding="utf-8") as f:
                layer_profile = json.load(f)
        except Exception as e:
            src.Log.print_with_color(f"[PDD] Đọc profiles thất bại: {e}", "red")
            return static_cut

        # Cloud layer times
        cloud_compute = [cloud["compute_capacity_gflops"] for cloud in devices_profile["clouds"]]

        # Edge clients
        edge_clients = devices_profile["clients"]
        if not edge_clients:
            src.Log.print_with_color(
                "[PDD] Không có edge client trong profiles – dùng static cut", "red"
            )
            return static_cut

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
            return {"default": static_cut}
