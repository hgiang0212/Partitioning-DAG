import numpy as np
from dataclasses import dataclass
from typing import List, Tuple, Optional
import src.Log as Log
from collections import Counter


DEFAULT_RAW_INPUT_MB = 13.0

DEFAULT_CUT_DATA_SIZES_MB = np.array([
    13.78, 8.02, 20.5,  5.81, 9.61, 12.54, 12.38, 13.61, 13.71,
    13.83, 13.52, 18.11, 17.89, 13.83, 26.1,  23.93, 9.94,  11.29,
    10.87,  9.57, 10.27, 10.25,  9.42,
], dtype=float)   # length = num_layers - 1 = 23


def build_activation_sizes(
    raw_input_mb: float,
    cut_data_sizes_mb: np.ndarray,
    num_layers: int,
):
    """
    activation_mb[z]:
        z = 0        : raw input size (edge sends full frame → no split)
        z = 1..K-1   : feature-map size when cut is placed after layer z-1
        z = K        : full local (no transmission) → 0
    """
    cut_data_sizes_mb = np.asarray(cut_data_sizes_mb, dtype=float)
    if len(cut_data_sizes_mb) != num_layers - 1:
        raise ValueError(
            f"cut_data_sizes_mb length must be {num_layers - 1}, "
            f"got {len(cut_data_sizes_mb)}"
        )
    return np.concatenate([[raw_input_mb], cut_data_sizes_mb, [0.0]])


def estimate_work_and_cloud_speeds(
    cloud_layer_time: np.ndarray,
):
    """
    Converts measured cloud layer times into PDD's work/speed abstraction.

    Returns:
        work   : reference workload per layer (mean across clouds)
        speeds : relative speed of each cloud (higher = faster)
    """
    cloud_layer_time = np.asarray(cloud_layer_time, dtype=float)
    num_clouds, num_layers = cloud_layer_time.shape

    work = np.mean(cloud_layer_time, axis=0)

    ref_total = float(np.sum(work))
    speeds = np.zeros(num_clouds, dtype=float)
    for j in range(num_clouds):
        total_j = float(np.sum(cloud_layer_time[j]))
        speeds[j] = ref_total / max(total_j, 1e-12)

    return work, speeds


def segment_time(
    work: np.ndarray,
    speeds: np.ndarray,
    group: List[int],
    start: int,
    end: int,
):
    """Compute time for a group of cloud nodes to process layers [start, end)."""
    if start >= end:
        return 0.0
    total_work = float(np.sum(work[start:end]))
    total_speed = float(sum(speeds[u] for u in group))
    return total_work / max(total_speed, 1e-12)


# ─────────────────────────────────────────────────────────────────────────────
# 3. BPA – Bi-Partitioning Algorithm (internal to GRB)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BPASolution:
    atd: float
    left_group: List[int]
    right_group: List[int]
    left_range: Tuple[int, int]
    right_range: Tuple[int, int]
    cut: int
    case: str


def _split_units_balanced(
    units: List[int],
    speeds: np.ndarray,
):
    """Split units into two groups with balanced total speeds."""
    if len(units) < 2:
        raise ValueError("Need ≥ 2 units to split.")
    units_sorted = sorted(units, key=lambda u: speeds[u])
    best_left, best_right, best_diff = None, None, float("inf")
    for cut in range(1, len(units_sorted)):
        left = units_sorted[:cut]
        right = units_sorted[cut:]
        diff = abs(sum(speeds[u] for u in left) - sum(speeds[u] for u in right))
        if diff < best_diff:
            best_diff = diff
            best_left, best_right = left, right
    return best_left, best_right


def _bpa(
    work: np.ndarray,
    speeds: np.ndarray,
    activation_mb: np.ndarray,
    group_e: List[int],
    group_c: List[int],
    start: int,
    end: int,
    inter_cloud_bandwidth_MBps: float,
):
    best: Optional[BPASolution] = None
    for p in range(start, end + 1):
        t_comm = (0.0 if (p == start or p == end)
                  else activation_mb[p] / max(inter_cloud_bandwidth_MBps, 1e-12))

        for (lg, rg, case) in [(group_e, group_c, "E_then_C"),
                               (group_c, group_e, "C_then_E")]:
            atd = max(
                segment_time(work, speeds, lg, start, p),
                segment_time(work, speeds, rg, p, end),
                t_comm,
            )
            sol = BPASolution(
                atd=atd, left_group=lg, right_group=rg,
                left_range=(start, p), right_range=(p, end),
                cut=p, case=case,
            )
            if best is None or sol.atd < best.atd:
                best = sol
    return best


def _pdd_grb(
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
        return [(u, start, end)], segment_time(work, speeds, [u], start, end)

    group_e, group_c = _split_units_balanced(units, speeds)
    sol = _bpa(work, speeds, activation_mb,
               group_e, group_c, start, end, inter_cloud_bandwidth_MBps)

    left_segs, left_atd = _pdd_grb(
        work, speeds, activation_mb,
        sol.left_group, sol.left_range[0], sol.left_range[1],
        inter_cloud_bandwidth_MBps,
    )
    right_segs, right_atd = _pdd_grb(
        work, speeds, activation_mb,
        sol.right_group, sol.right_range[0], sol.right_range[1],
        inter_cloud_bandwidth_MBps,
    )

    t_comm = (0.0 if (sol.cut == start or sol.cut == end)
              else activation_mb[sol.cut] / max(inter_cloud_bandwidth_MBps, 1e-12))

    return left_segs + right_segs, max(left_atd, right_atd, t_comm)


def _first_cloud_of_plan(segments: List[Tuple[int, int, int]]):
    non_empty = [s for s in segments if s[1] < s[2]]
    if not non_empty:
        return None
    return min(non_empty, key=lambda x: x[1])[0]


def _evaluate_for_one_client(
    client_layer_time: np.ndarray,
    cloud_layer_time: np.ndarray,
    activation_mb: np.ndarray,
    client_to_cloud_bandwidth_MBps: np.ndarray,
    inter_cloud_bandwidth_MBps: float,
    allow_full_local: bool = True,
):
    client_layer_time = np.asarray(client_layer_time, dtype=float)
    cloud_layer_time  = np.asarray(cloud_layer_time,  dtype=float)
    bw = np.asarray(client_to_cloud_bandwidth_MBps, dtype=float)

    num_layers = len(client_layer_time)
    num_clouds = cloud_layer_time.shape[0]
    work, speeds = estimate_work_and_cloud_speeds(cloud_layer_time)

    client_prefix = np.zeros(num_layers + 1, dtype=float)
    client_prefix[1:] = np.cumsum(client_layer_time)

    best = None
    max_z = num_layers if allow_full_local else num_layers - 1

    for z in range(0, max_z + 1):
        local_time = float(client_prefix[z])

        if z == num_layers:
            result = dict(atd=local_time, local_cut=z,
                          local_time=local_time, upload_time=0.0,
                          cloud_atd=0.0, first_cloud=None, segments=[])
        else:
            segments, cloud_atd = _pdd_grb(
                work, speeds, activation_mb,
                list(range(num_clouds)), z, num_layers,
                inter_cloud_bandwidth_MBps,
            )
            first_cloud = _first_cloud_of_plan(segments)
            upload_time = (0.0 if first_cloud is None
                           else activation_mb[z] / max(bw[first_cloud], 1e-12))
            atd = max(local_time, upload_time, cloud_atd)
            result = dict(atd=atd, local_cut=z,
                          local_time=local_time, upload_time=upload_time,
                          cloud_atd=cloud_atd, first_cloud=first_cloud,
                          segments=segments)

        if best is None or result["atd"] < best["atd"]:
            best = result

    best["fps"] = 1.0 / max(best["atd"], 1e-12)
    return best


def run_pdd_single_client(
    client_layer_time: np.ndarray,
    cloud_layer_time: np.ndarray,
    client_to_cloud_bandwidth_MBps: np.ndarray,
    inter_cloud_bandwidth_MBps: float = 125.0,
    raw_input_mb: float = DEFAULT_RAW_INPUT_MB,
    cut_data_sizes_mb: Optional[np.ndarray] = None,
):
    """
    Run PDD for a single edge client against M cloud nodes.

    Args:
        client_layer_time            : shape [K] – per-layer time on edge (seconds)
        cloud_layer_time             : shape [M, K] – per-layer time on each cloud
        client_to_cloud_bandwidth_MBps : shape [M] – upload BW from edge to each cloud
        inter_cloud_bandwidth_MBps   : BW between cloud nodes (default 1 Gbps = 125 MB/s)
        raw_input_mb                 : raw input frame size in MB
        cut_data_sizes_mb            : intermediate activation sizes at each cut point
                                       (shape [K-1]); uses YOLOv8 defaults if None

    Returns dict with:
        local_cut   : optimal split index (edge runs layers[:local_cut])
        atd         : minimum approximate task delay (seconds)
        fps         : 1 / atd
        local_time  : edge compute time
        upload_time : transmission time
        cloud_atd   : cloud compute time
        segments    : [(cloud_id, start_layer, end_layer), ...]
    """
    client_layer_time = np.asarray(client_layer_time, dtype=float)
    num_layers = len(client_layer_time)

    if cut_data_sizes_mb is None:
        cut_data_sizes_mb = DEFAULT_CUT_DATA_SIZES_MB[:num_layers - 1]

    activation_mb = build_activation_sizes(raw_input_mb, cut_data_sizes_mb, num_layers)

    result = _evaluate_for_one_client(
        client_layer_time=client_layer_time,
        cloud_layer_time=np.asarray(cloud_layer_time, dtype=float),
        activation_mb=activation_mb,
        client_to_cloud_bandwidth_MBps=np.asarray(client_to_cloud_bandwidth_MBps, dtype=float),
        inter_cloud_bandwidth_MBps=inter_cloud_bandwidth_MBps,
    )

    Log.print_with_color(
        f"[PDD] optimal local_cut={result['local_cut']} | "
        f"ATD={result['atd']*1000:.1f}ms | FPS={result['fps']:.1f} | "
        f"local={result['local_time']*1000:.1f}ms | "
        f"upload={result['upload_time']*1000:.1f}ms | "
        f"cloud={result['cloud_atd']*1000:.1f}ms",
        "green",
    )
    return result


def run_pdd_multi_client(
    client_layer_times: np.ndarray,
    cloud_layer_time: np.ndarray,
    bandwidth_client_cloud_MBps: np.ndarray,
    inter_cloud_bandwidth_MBps: float = 125.0,
    raw_input_mb: float = DEFAULT_RAW_INPUT_MB,
    cut_data_sizes_mb: Optional[np.ndarray] = None,
    shared_cloud_contention: bool = True,
):
    """
    Run PDD for N edge clients against M cloud nodes and derive
    a single global split point that minimises worst-case ATD.

    Args:
        client_layer_times           : shape [N, K]
        cloud_layer_time             : shape [M, K]
        bandwidth_client_cloud_MBps  : shape [N, M]
        ... (other args same as run_pdd_single_client)

    Returns dict with:
        global_cut       : single split index recommended for all clients
        per_client       : list of per-client PDD results
        mean_atd         : average ATD across clients
        p95_atd          : 95th-percentile ATD
        max_atd          : worst-case ATD (bottleneck client)
        system_fps       : 1 / max_atd (system throughput)
        mean_client_fps  : average per-client FPS
    """
    client_layer_times = np.asarray(client_layer_times, dtype=float)
    cloud_layer_time   = np.asarray(cloud_layer_time,   dtype=float)
    bw                 = np.asarray(bandwidth_client_cloud_MBps, dtype=float)

    num_clients, num_layers = client_layer_times.shape

    if cut_data_sizes_mb is None:
        cut_data_sizes_mb = DEFAULT_CUT_DATA_SIZES_MB[:num_layers - 1]

    activation_mb = build_activation_sizes(raw_input_mb, cut_data_sizes_mb, num_layers)

    per_client = []
    for i in range(num_clients):
        r = _evaluate_for_one_client(
            client_layer_time=client_layer_times[i],
            cloud_layer_time=cloud_layer_time,
            activation_mb=activation_mb,
            client_to_cloud_bandwidth_MBps=bw[i],
            inter_cloud_bandwidth_MBps=inter_cloud_bandwidth_MBps,
        )
        per_client.append(r)
        Log.print_with_color(
            f"[PDD] client {i}: cut={r['local_cut']} atd={r['atd']*1000:.1f}ms",
            "cyan",
        )

    atds = np.array([r["atd"] for r in per_client])
    fpss = 1.0 / np.maximum(atds, 1e-12)

    Log.print_with_color(
        f"mean_atd={np.mean(atds)*1000:.1f}ms | "
        f"p95_atd={np.percentile(atds,95)*1000:.1f}ms | "
        f"system_fps={1.0/float(np.max(atds)):.1f}",
        "green",
    )
    return dict(
        per_client=per_client,
        per_client_cuts=[r["local_cut"] for r in per_client],
        mean_atd=float(np.mean(atds)),
        p95_atd=float(np.percentile(atds, 95)),
        max_atd=float(np.max(atds)),
        system_fps=float(1.0 / max(float(np.max(atds)), 1e-12)),
        mean_client_fps=float(np.mean(fpss)),
    )


def evaluate_pdd_multi_client(
        client_layer_time: np.ndarray,
        cloud_layer_time: np.ndarray,
        activation_mb: np.ndarray,
        bandwidth_client_cloud_MBps: np.ndarray,
        inter_cloud_bandwidth_MBps: float,
        shared_cloud_contention: bool = True,
):
    """
    Đánh giá PDD-GRB cho N clients và M cloud nodes.

    Args:
        client_layer_time            : [N, K] thời gian mỗi layer trên từng edge client
        cloud_layer_time             : [M, K] thời gian mỗi layer trên từng cloud
        activation_mb                : [K+1]  kích thước activation tại mỗi cut point
        bandwidth_client_cloud_MBps  : [N, M] bandwidth upload client→cloud
        inter_cloud_bandwidth_MBps   : bandwidth giữa các cloud node
        shared_cloud_contention      : True  → cộng dồn tải lên từng cloud (thực tế)
                                       False → mỗi client dùng cloud độc lập (lý thuyết)

    Returns dict:
        method, per_client, latencies,
        mean_latency, p95_latency, round_time, system_fps, mean_client_fps,
        cloud_loads (chỉ khi shared_cloud_contention=True)
    """
    client_layer_time = np.asarray(client_layer_time, dtype=float)
    cloud_layer_time = np.asarray(cloud_layer_time, dtype=float)
    bandwidth_client_cloud_MBps = np.asarray(bandwidth_client_cloud_MBps, dtype=float)

    num_clients, num_layers = client_layer_time.shape
    num_clouds = cloud_layer_time.shape[0]

    # ── Per-client PDD optimisation ───────────────────────────────────────
    per_client_results = []
    for i in range(num_clients):
        r = _evaluate_for_one_client(
            client_layer_time=client_layer_time[i],
            cloud_layer_time=cloud_layer_time,
            activation_mb=activation_mb,
            client_to_cloud_bandwidth_MBps=bandwidth_client_cloud_MBps[i],
            inter_cloud_bandwidth_MBps=inter_cloud_bandwidth_MBps,
        )
        per_client_results.append(r)

    latencies = np.array([r["atd"] for r in per_client_results], dtype=float)

    # Independent mode
    if not shared_cloud_contention:
        round_time = float(np.max(latencies))
        return {
            "method": "PDD-GRB independent",
            "per_client": per_client_results,
            "latencies": latencies,
            "mean_latency": float(np.mean(latencies)),
            "p95_latency": float(np.percentile(latencies, 95)),
            "round_time": round_time,
            "system_fps": num_clients / max(round_time, 1e-12),
            "mean_client_fps": float(np.mean(1.0 / np.maximum(latencies, 1e-12))),
        }

    # Shared-cloud contention mode
    cloud_loads = np.zeros(num_clouds, dtype=float)
    local_loads = np.zeros(num_clients, dtype=float)
    upload_times = np.zeros(num_clients, dtype=float)

    for i, r in enumerate(per_client_results):
        local_loads[i] = r["local_time"]
        upload_times[i] = r["upload_time"]
        for cloud_id, seg_start, seg_end in r["segments"]:
            if seg_start < seg_end:
                cloud_loads[cloud_id] += float(
                    np.sum(cloud_layer_time[cloud_id, seg_start:seg_end])
                )

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
