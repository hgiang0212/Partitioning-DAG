"""
RpcClient – không thay đổi giao thức, chỉ xử lý START và STOP.
Không còn pha PROFILE vì devices.json đã có sẵn ở server.
"""

import os
import pickle
import time
import base64

import torch
import src.Log as Log


class RpcClient:
    def __init__(self, client_id, layer_id, channel, logger, inference_func, device):
        self.client_id      = client_id
        self.layer_id       = layer_id
        self.logger         = logger
        self.inference_func = inference_func
        self.device         = device
        self.channel        = channel

    def wait_response(self):
        running = True
        reply_q = f"reply_{self.client_id}"
        self.channel.queue_declare(reply_q, durable=False)
        while running:
            method, _header, body = self.channel.basic_get(queue=reply_q, auto_ack=True)
            if body:
                running = self._dispatch(body)
            time.sleep(0.5)

    def _dispatch(self, body: bytes) -> bool:
        msg    = pickle.loads(body)
        action = msg.get("action", "")
        Log.print_with_color(
            f"[<<<] {action} – {msg.get('message', '')}", "blue"
        )

        if action == "START":
            self._handle_start(msg)
            return False

        # STOP hoặc action không xác định
        Log.print_with_color("[>>>] Stopping.", "red")
        return False

    def _handle_start(self, msg: dict):
        model_name = msg["model_name"]
        num_layers = msg["num_layers"]
        splits     = msg["splits"]
        batch_size = msg["batch_size"]
        model_b64  = msg["model"]
        data       = msg["data"]
        compress   = msg["compress"]

        pt_path = f"{model_name}.pt"
        if not os.path.exists(pt_path):
            with open(pt_path, "wb") as f:
                f.write(base64.b64decode(model_b64))
            Log.print_with_color(f"[Start] Saved {pt_path}", "green")
        else:
            Log.print_with_color(f"[Start] Using {pt_path}", "green")

        ckpt   = torch.load(pt_path, map_location=self.device, weights_only=False)
        model  = ckpt["model"].to(self.device).float()
        layers = model.model

        client_layers = layers[:splits] if self.layer_id == 1 else layers[splits:]

        Log.print_with_color(
            f"[Start] layer_id={self.layer_id} | splits={splits} | "
            f"{len(list(client_layers))} layers on {self.device}",
            "green",
        )
        self.inference_func(
            client_layers, data, num_layers, splits, batch_size, self.logger, compress
        )

    def send_to_server(self, message: dict):
        self.channel.queue_declare("rpc_queue", durable=False)
        self.channel.basic_publish(
            exchange="", routing_key="rpc_queue", body=pickle.dumps(message)
        )
