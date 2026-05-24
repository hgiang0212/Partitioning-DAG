import os
import sys
import json
import base64
import threading
import pickle

import numpy as np
import pika
from ultralytics import YOLO

import src.Log
from src.PDD import run_pdd_single_client, run_pdd_multi_client


class Server:
    def __init__(self, config):
        # ── RabbitMQ ──────────────────────────────────────────────────────
        self.address      = config["rabbit"]["address"]
        self.username     = config["rabbit"]["username"]
        self.password     = config["rabbit"]["password"]
        self.virtual_host = config["rabbit"]["virtual-host"]

        credentials = pika.PlainCredentials(self.username, self.password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.address, 5672, self.virtual_host, credentials)
        )
        self.channel       = self.connection.channel()
        self.reply_channel = self.connection.channel()
        self.channel.queue_declare(queue="rpc_queue")
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue="rpc_queue", on_message_callback=self.on_request)

        # ── Config ────────────────────────────────────────────────────────
        self.model_name    = config["server"]["model"]
        self.total_clients = config["server"]["clients"]
        self.batch_size    = config["server"]["batch-size"]
        self.data          = config["data"]
        self.compress      = config["compress"]

        # PDD config
        pdd_cfg = config.get("pdd", {})
        self.use_pdd         = pdd_cfg.get("enabled", True)
        self.inter_cloud_bw  = pdd_cfg.get("inter_cloud_bandwidth_MBps", 125.0)
        self.profiles_path   = pdd_cfg.get("profiles_path", "devices.json")

        # Fallback static cut khi PDD tắt hoặc profiles không có
        _static_map  = {"a": 4, "b": 11, "c": 17, "d": 23}
        self.static_cut = _static_map.get(config["server"].get("cut-layer", "b"), 11)

        # ── State ─────────────────────────────────────────────────────────
        self.register_clients = [0] * len(self.total_clients)
        self.list_clients     = []   # [(client_id_str, layer_id), …]
        self.count_clients    = 0

        # ── Logger ────────────────────────────────────────────────────────
        self.logger = src.Log.Logger(
            f"{config['log-path']}/app.log", config["debug-mode"]
        )
        src.Log.print_with_color(
            f"Server ready | PDD={'ON' if self.use_pdd else 'OFF'} | "
            f"profiles='{self.profiles_path}' | "
            f"waiting for {self.total_clients} clients.",
            "green",
        )

    # ─────────────────────────────────────────────────────────────────────
    # RabbitMQ message handler
    # ─────────────────────────────────────────────────────────────────────

    def on_request(self, ch, method, _, body):
        msg    = pickle.loads(body)
        action = msg["action"]

        if action == "REGISTER":
            client_id = msg["client_id"]
            layer_id  = msg["layer_id"]

            if (str(client_id), layer_id) not in self.list_clients:
                self.list_clients.append((str(client_id), layer_id))

            src.Log.print_with_color(
                f"[REGISTER] client={str(client_id)[:8]}… layer={layer_id}", "blue"
            )
            self.register_clients[layer_id - 1] += 1

            if self.register_clients == self.total_clients:
                src.Log.print_with_color("All clients connected!", "green")
                # Chạy PDD + START trong thread riêng để không block consume loop
                threading.Thread(target=self._start_inference, daemon=True).start()

        elif action == "NOTIFY":
            self.count_clients += 1
            if self.count_clients == self.total_clients[1]:
                self.logger.log_info("All cloud clients finished.")
                self._broadcast(pickle.dumps({
                    "action": "STOP", "message": "Stop inference!"
                }))
                sys.exit()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    # ─────────────────────────────────────────────────────────────────────
    # Orchestration: load profiles → PDD → send START
    # ─────────────────────────────────────────────────────────────────────

    def _start_inference(self):
        encoded_model = self._load_model_bytes()
        splits        = self._compute_splits_from_profiles()

        src.Log.print_with_color(
            f"[Server] Sending START to all clients | splits={splits}", "green"
        )
        start_body = pickle.dumps({
            "action":     "START",
            "message":    "Server accept the connection",
            "model":      encoded_model,
            "splits":     splits,
            "batch_size": self.batch_size,
            "num_layers": len(self.total_clients),
            "model_name": self.model_name,
            "data":       self.data,
            "compress":   self.compress,
        })
        self._broadcast(start_body)

    def _compute_splits_from_profiles(self) -> int:
        if not self.use_pdd:
            src.Log.print_with_color(
                f"[PDD] Disabled – dùng static cut={self.static_cut}", "yellow"
            )
            return self.static_cut

        if not os.path.exists(self.profiles_path):
            src.Log.print_with_color(
                f"[PDD] '{self.profiles_path}' không tồn tại – dùng static cut={self.static_cut}",
                "red",
            )
            return self.static_cut

        try:
            with open(self.profiles_path, "r", encoding="utf-8") as f:
                profiles = json.load(f)
        except Exception as e:
            src.Log.print_with_color(f"[PDD] Đọc profiles thất bại: {e}", "red")
            return self.static_cut

        # ── Cloud layer times ─────────────────────────────────────────
        cloud_lt = np.asarray(profiles["cloud"]["layer_times"], dtype=float)
        num_layers = len(cloud_lt)
        cloud_layer_time = cloud_lt.reshape(1, -1)   # shape [1, K] – 1 cloud server

        # ── Edge clients ──────────────────────────────────────────────
        edge_clients = [c for c in profiles["clients"] if c.get("layer_id", 1) == 1]
        if not edge_clients:
            src.Log.print_with_color(
                "[PDD] Không có edge client trong profiles – dùng static cut", "red"
            )
            return self.static_cut

        client_layer_times = np.vstack(
            [np.asarray(c["layer_times"], dtype=float) for c in edge_clients]
        )  # shape [N, K]

        # bandwidth mỗi edge → cloud, shape [N, 1]
        bandwidths = np.array(
            [c.get("bandwidth_MBps", 50.0) for c in edge_clients]
        ).reshape(-1, 1)

        # In thông tin thiết bị
        src.Log.print_with_color(
            f"[PDD] {len(edge_clients)} edge client(s) | "
            f"bw={bandwidths.flatten().tolist()} MB/s",
            "cyan",
        )
        for c in edge_clients:
            t = np.asarray(c["layer_times"]).sum()
            src.Log.print_with_color(
                f"      {c.get('device_type', c['client_id'])} | "
                f"total={t*1000:.1f}ms | bw={c.get('bandwidth_MBps',50):.0f}MB/s",
                "cyan",
            )

        # ── Chạy PDD ─────────────────────────────────────────────────
        try:
            if len(edge_clients) == 1:
                result = run_pdd_single_client(
                    client_layer_time=client_layer_times[0],
                    cloud_layer_time=cloud_layer_time,
                    client_to_cloud_bandwidth_MBps=bandwidths[0],
                    inter_cloud_bandwidth_MBps=self.inter_cloud_bw,
                )
                optimal_cut = result["local_cut"]
            else:
                result = run_pdd_multi_client(
                    client_layer_times=client_layer_times,
                    cloud_layer_time=cloud_layer_time,
                    bandwidth_client_cloud_MBps=bandwidths,
                    inter_cloud_bandwidth_MBps=self.inter_cloud_bw,
                )
                optimal_cut = result["global_cut"]

            # Clamp vào [1, K-1]
            return int(np.clip(optimal_cut, 1, num_layers - 1))

        except Exception as e:
            src.Log.print_with_color(f"[PDD] Lỗi tính toán: {e}", "red")
            return self.static_cut

    # ─────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────

    def _load_model_bytes(self) -> str:
        if not os.path.exists(f"{self.model_name}.pt"):
            src.Log.print_with_color(f"Downloading {self.model_name}…", "yellow")
            YOLO(f"{self.model_name}.pt")
        with open(f"{self.model_name}.pt", "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    def _send_to_client(self, client_id: str, body: bytes):
        reply_q = f"reply_{client_id}"
        self.reply_channel.queue_declare(reply_q, durable=False)
        self.reply_channel.basic_publish(exchange="", routing_key=reply_q, body=body)
        src.Log.print_with_color(f"[>>>] → {client_id[:8]}…", "red")

    def _broadcast(self, body: bytes):
        for (client_id, _) in self.list_clients:
            self._send_to_client(client_id, body)

    # Giữ interface gốc
    def send_to_response(self, client_id, message):
        self._send_to_client(str(client_id), message)

    def start(self):
        self.channel.start_consuming()
