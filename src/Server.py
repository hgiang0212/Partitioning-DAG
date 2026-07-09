import os
import sys
import json
import time
import base64
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

        # ── FPS measurement (fps_guide.md) ────────────────────────────────
        fps_cfg = config.get("fps") or {}
        self._fps_grace_s = float(fps_cfg.get("grace_s", 10))          # keep collecting after work queues drain
        self._fps_hardcap_s = float(fps_cfg.get("shutdown_timeout_s", 300))  # safety cap after first-tier shutdown

        self.channel.queue_declare(queue="fps_queue", durable=False)
        self.channel.queue_purge(queue="fps_queue")   # start every run from empty
        self.channel.basic_consume(queue="fps_queue", on_message_callback=self.on_fps)

        self._fps_times = []          # arrival time of every DONE (seconds, server clock)
        self._fps_start_t = None      # set when START is dispatched to the workers
        self._fps_window = 16         # DONEs per live smoothed sample
        self._fps_stop_bcast_t = None # time the first tier finished
        self._fps_empty_since = None  # time work queues were first seen empty
        self._fps_work_queues = set() # pipeline queues to watch while draining
        self._fps_printed = False
        self.batch_log_path = f"{log_path}/batch_done_ns.log"
        open(self.batch_log_path, "w").close()   # truncate: a new run never mixes with the previous one

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

            # First tier (edges) finished → broadcast STOP but do NOT exit:
            # keep consuming fps_queue until the work queues drain, so late
            # DONEs from clouds still counting a backlog are not lost (§8).
            if self._fps_stop_bcast_t is None and self.notify_counts[0] >= self.total_clients[0]:
                self.logger.log_info("Stop Inference !!!")
                self.notify_clients(start=False)
                self._fps_stop_bcast_t = time.time()
                self._fps_work_queues = self._fps_discover_work_queues()
                self.connection.call_later(1.0, self._fps_drain_check)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    # ──────────────────────────── FPS (fps_guide.md) ─────────────────────────

    def on_fps(self, ch, method, _props, body):
        # body (b"DONE") is intentionally ignored — the arrival is the event.
        t_ns = time.time_ns()          # ONE clock reading, used for both the FPS math and the log
        self._fps_times.append(t_ns / 1e9)
        n = len(self._fps_times)
        W = self._fps_window
        window_fps = None
        if n >= W:                                   # live smoothed view
            span = self._fps_times[-1] - self._fps_times[-W]
            if span > 0:
                window_fps = (W - 1) * self.batch_size / span
                src.Log.print_with_color(
                    f"[FPS] DONE #{n}  window_fps={window_fps:6.2f} (last {W} batches)", "cyan")
        with open(self.batch_log_path, "a") as f:    # per-batch ns log
            if window_fps is None:
                f.write(f"{t_ns}\n")
            else:
                f.write(f"{t_ns} {window_fps:.2f}\n")
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
        # hard cap: never hang forever
        if self._fps_stop_bcast_t and (now - self._fps_stop_bcast_t) >= self._fps_hardcap_s:
            return self._finish_fps("hard cap reached")

        depth = self._fps_total_work_depth()
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
        try:
            self.channel.stop_consuming()   # now it's safe to end the run
        except Exception:
            pass

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
        self.channel.start_consuming()

    def notify_clients(self, start=True):
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

            self._fps_start_t = time.time()   # clock starts when work is dispatched (§9)
        else:
            response = {"action": "STOP",
                        "message": "Stop inference !!!"}
            for (client_id, layer_id) in self.list_clients:
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
