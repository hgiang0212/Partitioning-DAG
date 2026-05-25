import json
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Tuple, Any


def build_activation_sizes(raw_input_mb: float, cut_data_sizes_mb: np.ndarray, num_layers: int):
    cut_data_sizes_mb = np.asarray(cut_data_sizes_mb, dtype=float)
    if len(cut_data_sizes_mb) != num_layers - 1:
        raise ValueError(
            f"Expected CUT_DATA_SIZES_MB length = K-1 = {num_layers-1}, "
            f"but got {len(cut_data_sizes_mb)}."
        )
    return np.concatenate([[raw_input_mb], cut_data_sizes_mb, [0.0]])


def estimate_work_and_cloud_speeds(cloud_layer_time: np.ndarray):
    cloud_layer_time = np.asarray(cloud_layer_time, dtype=float)
    num_clouds, num_layers = cloud_layer_time.shape
    work = np.mean(cloud_layer_time, axis=0)
    ref_total = np.sum(work)
    speeds = np.zeros(num_clouds, dtype=float)
    for j in range(num_clouds):
        total_j = np.sum(cloud_layer_time[j])
        speeds[j] = ref_total / max(total_j, 1e-12)
    return work, speeds

def segment_time(work: np.ndarray, speeds: np.ndarray, group: List[int], start: int, end: int):
    if start >= end:
        return 0.0
    total_work = float(np.sum(work[start:end]))
    total_speed = float(np.sum([speeds[u] for u in group]))
    return total_work / max(total_speed, 1e-12)

def split_units_balanced(units: List[int], speeds: np.ndarray):
    if len(units) < 2:
        raise ValueError("Need at least two units to split.")
    units_sorted = sorted(units, key=lambda u: speeds[u])
    best_left = None
    best_right = None
    best_diff = float("inf")
    for cut in range(1, len(units_sorted)):
        left = units_sorted[:cut]
        right = units_sorted[cut:]
        speed_left = sum(speeds[u] for u in left)
        speed_right = sum(speeds[u] for u in right)
        diff = abs(speed_left - speed_right)
        if diff < best_diff:
            best_diff = diff
            best_left = left
            best_right = right
    return best_left, best_right

# ============================================================
# 3. BPA: Bi-Partitioning Algorithm (chỉ dùng khi có ≥2 cloud)
# ============================================================
@dataclass
class BPASolution:
    atd: float
    left_group: List[int]
    right_group: List[int]
    left_range: Tuple[int, int]
    right_range: Tuple[int, int]
    cut: int
    case: str

def bpa(
    work: np.ndarray,
    speeds: np.ndarray,
    activation_mb: np.ndarray,
    group_e: List[int],
    group_c: List[int],
    start: int,
    end: int,
    inter_cloud_bandwidth_MBps: float,
):
    best = None
    for p in range(start, end + 1):
        if p == start or p == end:
            t_comm = 0.0
        else:
            t_comm = activation_mb[p] / max(inter_cloud_bandwidth_MBps, 1e-12)
        # Case 1
        t_e = segment_time(work, speeds, group_e, start, p)
        t_c = segment_time(work, speeds, group_c, p, end)
        atd_1 = max(t_e, t_c, t_comm)
        sol_1 = BPASolution(
            atd=atd_1,
            left_group=group_e,
            right_group=group_c,
            left_range=(start, p),
            right_range=(p, end),
            cut=p,
            case="E_then_C",
        )
        # Case 2
        t_c2 = segment_time(work, speeds, group_c, start, p)
        t_e2 = segment_time(work, speeds, group_e, p, end)
        atd_2 = max(t_c2, t_e2, t_comm)
        sol_2 = BPASolution(
            atd=atd_2,
            left_group=group_c,
            right_group=group_e,
            left_range=(start, p),
            right_range=(p, end),
            cut=p,
            case="C_then_E",
        )
        for sol in [sol_1, sol_2]:
            if best is None or sol.atd < best.atd:
                best = sol
    return best

# ============================================================
# 4. GRB: Grouping Recursion (chỉ dùng khi có ≥2 cloud)
# ============================================================
def pdd_grb(
    work: np.ndarray,
    speeds: np.ndarray,
    activation_mb: np.ndarray,
    units: List[int],
    start: int,
    end: int,
    inter_cloud_bandwidth_MBps: float,
):
    if start >= end:
        return [], 0.0
    if len(units) == 1:
        u = units[0]
        t = segment_time(work, speeds, [u], start, end)
        return [(u, start, end)], t
    group_e, group_c = split_units_balanced(units, speeds)
    sol = bpa(
        work=work,
        speeds=speeds,
        activation_mb=activation_mb,
        group_e=group_e,
        group_c=group_c,
        start=start,
        end=end,
        inter_cloud_bandwidth_MBps=inter_cloud_bandwidth_MBps,
    )
    left_start, left_end = sol.left_range
    right_start, right_end = sol.right_range
    left_segments, left_atd = pdd_grb(
        work, speeds, activation_mb,
        sol.left_group,
        left_start,
        left_end,
        inter_cloud_bandwidth_MBps,
    )
    right_segments, right_atd = pdd_grb(
        work, speeds, activation_mb,
        sol.right_group,
        right_start,
        right_end,
        inter_cloud_bandwidth_MBps,
    )
    if sol.cut == start or sol.cut == end:
        t_comm = 0.0
    else:
        t_comm = activation_mb[sol.cut] / max(inter_cloud_bandwidth_MBps, 1e-12)
    atd = max(left_atd, right_atd, t_comm)
    return left_segments + right_segments, atd

# ============================================================
# 5. PDD cho một client (hỗ trợ 1 hoặc nhiều cloud)
# ============================================================
def first_cloud_of_plan(segments):
    non_empty = [s for s in segments if s[1] < s[2]]
    if not non_empty:
        return None
    non_empty = sorted(non_empty, key=lambda x: x[1])
    return non_empty[0][0]

def evaluate_pdd_for_one_client(
    client_layer_time: np.ndarray,
    cloud_layer_time: np.ndarray,
    activation_mb: np.ndarray,
    client_to_cloud_bandwidth_MBps: np.ndarray,   # shape (num_clouds,)
    inter_cloud_bandwidth_MBps: float,
    allow_full_local: bool = True,
):
    client_layer_time = np.asarray(client_layer_time, dtype=float)
    cloud_layer_time = np.asarray(cloud_layer_time, dtype=float)
    client_to_cloud_bandwidth_MBps = np.asarray(client_to_cloud_bandwidth_MBps, dtype=float)

    num_layers = len(client_layer_time)
    num_clouds = cloud_layer_time.shape[0]
    client_prefix = np.zeros(num_layers + 1, dtype=float)
    client_prefix[1:] = np.cumsum(client_layer_time)

    best = None
    max_z = num_layers if allow_full_local else num_layers - 1

    for z in range(0, max_z + 1):
        local_time = float(client_prefix[z])

        if z == num_layers:
            result = {
                "atd": local_time,
                "local_cut": z,
                "local_time": local_time,
                "upload_time": 0.0,
                "cloud_atd": 0.0,
                "first_cloud": None,
                "segments": [],
            }
        else:
            # Trường hợp chỉ có 1 cloud → không cần phân hoạch
            if num_clouds == 1:
                cloud_time = float(np.sum(cloud_layer_time[0, z:]))
                upload_time = activation_mb[z] / max(client_to_cloud_bandwidth_MBps[0], 1e-12)
                atd = max(local_time, upload_time, cloud_time)
                segments = [(0, z, num_layers)]
                result = {
                    "atd": atd,
                    "local_cut": z,
                    "local_time": local_time,
                    "upload_time": upload_time,
                    "cloud_atd": cloud_time,
                    "first_cloud": 0,
                    "segments": segments,
                }
            else:
                # Nhiều cloud: dùng GRB
                work, speeds = estimate_work_and_cloud_speeds(cloud_layer_time)
                cloud_units = list(range(num_clouds))
                segments, cloud_atd = pdd_grb(
                    work=work,
                    speeds=speeds,
                    activation_mb=activation_mb,
                    units=cloud_units,
                    start=z,
                    end=num_layers,
                    inter_cloud_bandwidth_MBps=inter_cloud_bandwidth_MBps,
                )
                first_cloud = first_cloud_of_plan(segments)
                if first_cloud is None:
                    upload_time = 0.0
                else:
                    upload_time = activation_mb[z] / max(client_to_cloud_bandwidth_MBps[first_cloud], 1e-12)
                atd = max(local_time, upload_time, cloud_atd)
                result = {
                    "atd": atd,
                    "local_cut": z,
                    "local_time": local_time,
                    "upload_time": upload_time,
                    "cloud_atd": cloud_atd,
                    "first_cloud": first_cloud,
                    "segments": segments,
                }

        if best is None or result["atd"] < best["atd"]:
            best = result

    best["fps"] = 1.0 / max(best["atd"], 1e-12)
    return best

# ============================================================
# 6. Multi-client PDD baseline (hỗ trợ 1 hoặc nhiều cloud)
# ============================================================
def evaluate_pdd_multi_client(
    client_layer_time: np.ndarray,
    cloud_layer_time: np.ndarray,
    activation_mb: np.ndarray,
    bandwidth_client_cloud_MBps: np.ndarray,   # shape (N, M)
    inter_cloud_bandwidth_MBps: float,
    shared_cloud_contention: bool = True,
):
    client_layer_time = np.asarray(client_layer_time, dtype=float)
    cloud_layer_time = np.asarray(cloud_layer_time, dtype=float)
    bandwidth_client_cloud_MBps = np.asarray(bandwidth_client_cloud_MBps, dtype=float)

    num_clients, num_layers = client_layer_time.shape
    num_clouds = cloud_layer_time.shape[0]

    per_client_results = []
    for i in range(num_clients):
        res_i = evaluate_pdd_for_one_client(
            client_layer_time=client_layer_time[i],
            cloud_layer_time=cloud_layer_time,
            activation_mb=activation_mb,
            client_to_cloud_bandwidth_MBps=bandwidth_client_cloud_MBps[i],
            inter_cloud_bandwidth_MBps=inter_cloud_bandwidth_MBps,
        )
        per_client_results.append(res_i)

    latencies = np.array([r["atd"] for r in per_client_results], dtype=float)

    if not shared_cloud_contention:
        round_time = float(np.max(latencies))
        return {
            "method": "PDD-GRB per-client",
            "per_client": per_client_results,
            "latencies": latencies,
            "mean_latency": float(np.mean(latencies)),
            "p95_latency": float(np.percentile(latencies, 95)),
            "round_time": round_time,
            "system_fps": num_clients / max(round_time, 1e-12),
            "mean_client_fps": float(np.mean(1.0 / np.maximum(latencies, 1e-12))),
        }

    # Shared cloud contention
    cloud_loads = np.zeros(num_clouds, dtype=float)
    local_loads = np.zeros(num_clients, dtype=float)
    upload_times = np.zeros(num_clients, dtype=float)

    for i, r in enumerate(per_client_results):
        local_loads[i] = r["local_time"]
        upload_times[i] = r["upload_time"]
        for cloud_id, start, end in r["segments"]:
            if start < end:
                cloud_loads[cloud_id] += float(np.sum(cloud_layer_time[cloud_id, start:end]))

    round_time = max(
        float(np.max(local_loads)),
        float(np.max(upload_times)),
        float(np.max(cloud_loads)),
    )

    return {
        "method": "PDD-GRB shared-cloud",
        "per_client": per_client_results,
        "latencies": latencies,
        "cloud_loads": cloud_loads,
        "local_loads": local_loads,
        "upload_times": upload_times,
        "mean_latency": float(np.mean(latencies)),
        "p95_latency": float(np.percentile(latencies, 95)),
        "round_time": round_time,
        "system_fps": num_clients / max(round_time, 1e-12),
        "mean_client_fps": float(np.mean(1.0 / np.maximum(latencies, 1e-12))),
    }

def summarize_pdd_result(result):
    print("\n", result["method"])
    print("-" * len(result["method"]))
    print(f"mean latency   : {result['mean_latency']:.6f} s")
    print(f"p95 latency    : {result['p95_latency']:.6f} s")
    print(f"round time     : {result['round_time']:.6f} s")
    print(f"system FPS     : {result['system_fps']:.2f}")
    print(f"mean client FPS: {result['mean_client_fps']:.2f}")
    if "cloud_loads" in result:
        print("cloud loads    :", result["cloud_loads"])
    rows = []
    for i, r in enumerate(result["per_client"]):
        rows.append({
            "client": i,
            "local_cut": r["local_cut"],
            "atd": r["atd"],
            "fps": r["fps"],
            "local_time": r["local_time"],
            "upload_time": r["upload_time"],
            "cloud_atd": r["cloud_atd"],
            "first_cloud": r["first_cloud"],
            "segments": r["segments"],
        })
    return pd.DataFrame(rows)


def main():
    with open("devices.json", "r") as f:
        config = json.load(f)

    clouds = config["clouds"]
    num_clouds = len(clouds)
    cloud_layer_time = np.array([cloud["layer_times"] for cloud in clouds], dtype=float)
    num_layers = cloud_layer_time.shape[1]

    clients = config["clients"]
    num_clients = len(clients)
    client_layer_time = np.zeros((num_clients, num_layers), dtype=float)
    bandwidth_client_cloud_MBps = np.zeros((num_clients, num_clouds), dtype=float)

    for i, client in enumerate(clients):
        client_layer_time[i] = np.array(client["layer_times"], dtype=float)
        bw = client["bandwidth_MBps"]
        if isinstance(bw, (int, float)):
            # Nếu là số, gán băng thông đó cho tất cả cloud
            bandwidth_client_cloud_MBps[i] = float(bw)
        else:
            # Nếu là mảng, phải có độ dài bằng num_clouds
            bw_arr = np.array(bw, dtype=float)
            if len(bw_arr) != num_clouds:
                raise ValueError(f"Client {client.get('client_id', i)}: bandwidth_MBps length {len(bw_arr)} != num_clouds {num_clouds}")
            bandwidth_client_cloud_MBps[i] = bw_arr

    try:
        raw_input_mb = config["raw_input_mb"]
        cut_data_sizes_mb = np.array(config["cut_data_sizes_mb"], dtype=float)
        inter_cloud_bandwidth_MBps = config["inter_cloud_bandwidth_mbps"]
    except KeyError as e:
        print(f"Thiếu trường bắt buộc trong JSON: {e}")
        print("Vui lòng bổ sung raw_input_mb, cut_data_sizes_mb, inter_cloud_bandwidth_mbps")
        return

    activation_mb = build_activation_sizes(raw_input_mb, cut_data_sizes_mb, num_layers)

    # --- Chạy mô phỏng với 2 chế độ ---
    res_independent = evaluate_pdd_multi_client(
        client_layer_time=client_layer_time,
        cloud_layer_time=cloud_layer_time,
        activation_mb=activation_mb,
        bandwidth_client_cloud_MBps=bandwidth_client_cloud_MBps,
        inter_cloud_bandwidth_MBps=inter_cloud_bandwidth_MBps,
        shared_cloud_contention=False,
    )

    res_shared = evaluate_pdd_multi_client(
        client_layer_time=client_layer_time,
        cloud_layer_time=cloud_layer_time,
        activation_mb=activation_mb,
        bandwidth_client_cloud_MBps=bandwidth_client_cloud_MBps,
        inter_cloud_bandwidth_MBps=inter_cloud_bandwidth_MBps,
        shared_cloud_contention=True,
    )

    print("\n========== KẾT QUẢ MÔ PHỎNG ==========")
    df_ind = summarize_pdd_result(res_independent)
    print("\n" + "="*60)
    df_shared = summarize_pdd_result(res_shared)

    # Lưu kết quả chi tiết ra CSV
    df_ind.to_csv("results_independent.csv", index=False)
    df_shared.to_csv("results_shared.csv", index=False)
    print("\nĐã lưu kết quả vào results_independent.csv và results_shared.csv")
if __name__ == "__main__":
    main()