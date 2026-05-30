import torch
import cv2
import pickle
from tqdm import tqdm
import copy
import time
import csv
import os
import psutil
import numpy as np

from src.Compress import Encoder, Decoder
import src.Log as Log
from src.Model import inference, postprocess_yolo


class Scheduler:
    def __init__(self, client_id, layer_id, channel, device):
        self.client_id = client_id
        self.layer_id = layer_id
        self.channel = channel
        self.device = device

        import glob as _glob
        for f in _glob.glob("metrics_raw_*.csv") + ["metrics_pivoted.csv", "metrics_pivot.lock"]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except PermissionError:
                    Log.print_with_color(f"[!] Cannot delete {f} (file is open). Close it and retry.", "red")

        self.size_message = None
        self.splits = None
        self.middle_queue = "middle_queue"
        self.last_queue = "last_queue"
        self.channel.queue_declare(self.middle_queue, durable=False)
        self.channel.queue_declare(self.last_queue, durable=False)

        self.map_metric = None
        self.gt_dict = {}
        self._det_results = {}
        self._load_gt_dict()

    # ──────────────────────────── Measurement helpers ────────────────────────

    def get_ram_mb(self):
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)

    def write_metrics(self, role, batch_id, batch_size, latency_ms, fps, ram_mb,
                      message_size_bytes=0, e2e_latency_ms=0, edge_start_time=None):
        # best_cut = "N/A" if self.splits is None else self.splits - 1
        best_cut = self.splits
        file_path = f"metrics_raw_{str(self.client_id).replace('-', '')}.csv"
        file_exists = os.path.exists(file_path)
        with open(file_path, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "role", "batch_id", "batch_size", "best_cut",
                    "latency_ms", "fps", "ram_mb",
                    "message_size_bytes", "e2e_latency_ms", "edge_start_time",
                ])
            writer.writerow([
                role, batch_id, batch_size, best_cut,
                round(latency_ms, 3), round(fps, 3), round(ram_mb, 3),
                message_size_bytes, round(e2e_latency_ms, 3),
                edge_start_time if edge_start_time is not None else "",
            ])

    # ──────────────────────────── mAP helpers ────────────────────────────────

    def _load_gt_dict(self, gt_dir="datasets/groundtruth"):
        if not os.path.isdir(gt_dir):
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
            self.map_metric.update(
                [{"boxes":  r["boxes"].cpu().float(),
                  "scores": r["scores"].cpu().float(),
                  "labels": r["classes"].cpu().long()}],
                [self.gt_dict[frame_num]]
            )

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

    def send_next_layer(self, intermediate_queue, data, compress):
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
        
        # Log message size
        Log.print_with_color(f"[>>>] Sending message to {intermediate_queue}: {self.size_message} bytes", "yellow")
        
        self.channel.basic_publish(
            exchange='',
            routing_key=intermediate_queue,
            body=message,
        )

    def send_to_server(self, message):
        self.channel.queue_declare('rpc_queue', durable=False)
        self.channel.basic_publish(exchange='',
                                   routing_key='rpc_queue',
                                   body=pickle.dumps(message))

    def first_layer(self, model, data, batch_size, logger, compress):
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

        pbar = tqdm(desc="Processing video (while loop)", unit="frame")
        batch_id = 0
        prev_batch_end = None
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            frame = cv2.resize(frame, (640, 640))
            orig_images.append(copy.deepcopy(frame))
            frame = frame.astype('float32') / 255.0
            tensor = torch.from_numpy(frame).permute(2, 0, 1)  # shape: (3, 640, 640)
            input_image.append(tensor)

            if len(input_image) == batch_size:
                batch_start = time.perf_counter()
                edge_start_wall = time.time()

                input_image = torch.stack(input_image)
                input_image = input_image.to(self.device)

                y = []
                x, y = inference(model, input_image, y, 0)
                y[-1] = x

                y_msg = {
                    "data": y,
                    "width": width,
                    "height": height,
                    "edge_start_time": edge_start_wall,
                }
                self.send_next_layer(self.middle_queue, y_msg, compress)

                batch_end = time.perf_counter()
                latency_ms = (batch_end - batch_start) * 1000
                fps = batch_size / (batch_end - prev_batch_end) if prev_batch_end is not None else 0.0
                ram_mb = self.get_ram_mb()
                msg_size = self.size_message if self.size_message is not None else 0

                print(f"[Batch {batch_id:4d}] EDGE | latency={latency_ms:.1f}ms | "
                      f"fps={fps:.1f} | ram={ram_mb:.1f}MB | msg={msg_size}B")

                self.write_metrics(
                    role="edge",
                    batch_id=batch_id,
                    batch_size=batch_size,
                    latency_ms=latency_ms,
                    fps=fps,
                    ram_mb=ram_mb,
                    message_size_bytes=msg_size,
                    edge_start_time=edge_start_wall,
                )

                batch_id += 1
                prev_batch_end = batch_end
                input_image = []
                orig_images = []
                pbar.update(batch_size)
            else:
                continue

        print(f'size message: {self.size_message} bytes.')
        cap.release()
        pbar.close()

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

    def last_layer(self, model, batch_size, splits, logger, compress):
        model.eval()
        model.to(self.device)

        self.channel.basic_qos(prefetch_count=10)
        pbar = tqdm(desc="Processing video (while loop)", unit="frame")
        batch_id = 0
        prev_batch_end = None
        while True:
            method_frame, header_frame, body = self.channel.basic_get(queue=self.last_queue, auto_ack=True)
            if method_frame and body:
                batch_start = time.perf_counter()
                received_message_size = len(body)
                received_data = pickle.loads(body)
                y = received_data["data"]
                edge_start_time = y.get("edge_start_time", time.time())

                if compress["enable"]:
                    y["data"] = Decoder(y["data"], y["shape"])
                    y["data"] = [torch.from_numpy(t) if t is not None else None for t in y["data"]]

                y["data"] = [t.to(self.device) if t is not None else None for t in y["data"]]
                list_output = y["data"]
                x = list_output[-1]
                x, _ = inference(model, x, list_output, splits)

                results = postprocess_yolo(x)
                self._update_map(results, batch_id, batch_size)

                batch_end = time.perf_counter()
                cloud_end_wall = time.time()
                latency_ms = (batch_end - batch_start) * 1000
                fps = batch_size / (batch_end - prev_batch_end) if prev_batch_end is not None else 0.0
                e2e_latency_ms = (cloud_end_wall - edge_start_time) * 1000
                ram_mb = self.get_ram_mb()

                print(f"[Batch {batch_id:4d}] CLOUD | latency={latency_ms:.1f}ms | "
                      f"fps={fps:.1f} | e2e={e2e_latency_ms:.1f}ms | "
                      f"ram={ram_mb:.1f}MB | recv={received_message_size}B")

                self.write_metrics(
                    role="cloud",
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

            else:
                broadcast_queue_name = f'reply_{self.client_id}'
                method_frame, header_frame, body = self.channel.basic_get(queue=broadcast_queue_name, auto_ack=True)
                if body:
                    received_data = pickle.loads(body)
                    Log.print_with_color(f"[<<<] Received message from server {received_data}", "blue")
                    if received_data["action"] == "STOP":
                        Log.print_with_color("[>>>] Finish!", "red")
                        break
                else:
                    time.sleep(0.5)

        cv2.destroyAllWindows()
        pbar.close()

    def middle_layer(self, model, batch_size, splits, logger, compress):
        model.eval()
        model.to(self.device)

        self.channel.basic_qos(prefetch_count=10)
        pbar = tqdm(desc="Processing video (while loop)", unit="frame")
        batch_id = 0
        prev_batch_end = None
        while True:
            method_frame, header_frame, body = self.channel.basic_get(queue=self.middle_queue, auto_ack=True)
            if method_frame and body:

                batch_start = time.perf_counter()
                received_message_size = len(body)
                received_data = pickle.loads(body)
                y = received_data["data"]

                if compress["enable"]:
                    y["data"] = Decoder(y["data"], y["shape"])
                    y["data"] = [torch.from_numpy(t) if t is not None else None for t in y["data"]]

                y["data"] = [t.to(self.device) if t is not None else None for t in y["data"]]
                list_output = y["data"]
                x = list_output[-1]
                x, y = inference(model, x, list_output, splits)
                y[-1] = x

                y_msg = {
                    "data": y,
                    # "width": received_data["width"],
                    # "height": received_data["height"],
                    "edge_start_time": received_data["data"]["edge_start_time"],
                }
                print(f"[DEBUG] middle_layer sending to queue: {self.last_queue}")
                self.send_next_layer(self.last_queue, y_msg, compress)
                batch_end = time.perf_counter()
                latency_ms = (batch_end - batch_start) * 1000
                fps = batch_size / (batch_end - prev_batch_end) if prev_batch_end is not None else 0.0
                ram_mb = self.get_ram_mb()

                print(f"[Batch {batch_id:4d}] CLOUD | latency={latency_ms:.1f}ms | "
                      f"fps={fps:.1f} |"
                      f"ram={ram_mb:.1f}MB | recv={received_message_size}B")

                self.write_metrics(
                    role="cloud",
                    batch_id=batch_id,
                    batch_size=batch_size,
                    latency_ms=latency_ms,
                    fps=fps,
                    ram_mb=ram_mb,
                    message_size_bytes=received_message_size,
                )

    def inference_func(self, model, data, num_layers, num_layers_model, splits, batch_size, logger, compress):
        self.splits = splits
        if os.path.exists("detections_stream.jsonl"):
            os.remove("detections_stream.jsonl")
        if self.layer_id == 1:
            self.first_layer(model, data, batch_size, logger, compress)
        elif self.layer_id == num_layers:
            if splits[1] == num_layers_model:
                self.last_layer(model, batch_size, splits[0], logger, compress)
                self._print_summary()
                self._print_map()
                if self._det_results:
                    self._write_detections_json()
            else:
                self.middle_layer(model, batch_size, splits[0], logger, compress)
