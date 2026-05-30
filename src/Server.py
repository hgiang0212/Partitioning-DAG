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

            self.count_clients += 1

            if self.count_clients == self.total_clients[1]:
                self.logger.log_info("Stop Inference !!!")

                self.notify_clients(start=False)

                sys.exit()

        ch.basic_ack(delivery_tag=method.delivery_tag)

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
                    client_split = optimal_cut
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
            for seg in segments:
                clouds_split[seg[0]] = (seg[1],seg[2])
            return optimal_cut, clouds_split , num_layers_model
        except Exception as e:
            src.Log.print_with_color(f"[PDD] Lỗi tính toán: {e}", "red")
            return {"default": static_cut}
