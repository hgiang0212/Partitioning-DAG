import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Tuple, Optional


def segment_time(
    layer_flops: np.ndarray,  # [K]  FLOPs từng layer
    cu_flops:    np.ndarray,  # [M]  FLOPS từng CU
    group:       List[int],
    start:       int,
    end:         int,
):
    """
    Fe = Σci(Ge) / Σrj(Me)
    Fc = Σci(Gc) / Σrj(Mc)
    """
    if start >= end:
        return 0.0
    total_flops = float(np.sum(layer_flops[start:end]))
    total_power = float(np.sum(cu_flops[group]))
    return total_flops / max(total_power, 1e-12)


def split_units_balanced(
    units:    List[int],
    cu_flops: np.ndarray,
):
    units_sorted = sorted(units, key=lambda u: cu_flops[u])

    best_left, best_right = None, None
    best_diff = float("inf")

    for cut in range(1, len(units_sorted)):
        left  = units_sorted[:cut]
        right = units_sorted[cut:]
        diff  = abs(
            sum(cu_flops[u] for u in left) -
            sum(cu_flops[u] for u in right)
        )
        if diff < best_diff:
            best_diff  = diff
            best_left  = left
            best_right = right

    return best_left, best_right


@dataclass
class BPASolution:
    atd:         float
    left_group:  List[int]
    right_group: List[int]
    left_range:  Tuple[int, int]
    right_range: Tuple[int, int]
    cut:         int
    case:        str


def bpa(
    layer_flops:                np.ndarray,
    cu_flops:                   np.ndarray,
    activation_mb:              np.ndarray,
    group_e:                    List[int],
    group_c:                    List[int],
    start:                      int,
    end:                        int,
    inter_cloud_bandwidth_MBps: float,
):
    """
    Algorithm 1:
        for i = 1 → n+1:
            Case ONE : [start:p] → E,  [p:end] → C
            Case TWO : [start:p] → C,  [p:end] → E
            ATD = max(Fe, Fc, Ft)
    """
    best: Optional[BPASolution] = None

    for p in range(start, end + 1):

        # Eq. 4: Ft = si / b
        t_comm = (
            0.0 if (p == start or p == end)
            else activation_mb[p] / max(inter_cloud_bandwidth_MBps, 1e-12)
        )

        for (lg, rg, case) in [
            (group_e, group_c, "E_then_C"),
            (group_c, group_e, "C_then_E"),
        ]:
            t_left  = segment_time(layer_flops, cu_flops, lg, start, p)
            t_right = segment_time(layer_flops, cu_flops, rg, p,     end)
            atd     = max(t_left, t_right, t_comm)

            sol = BPASolution(
                atd=atd,
                left_group=lg,  right_group=rg,
                left_range=(start, p), right_range=(p, end),
                cut=p, case=case,
            )
            if best is None or atd < best.atd:
                best = sol

    return best



def pdd_grb(
    layer_gflops:    np.ndarray,
    cu_flops:       np.ndarray,
    activation_mb:  np.ndarray,
    units:          List[int],
    start:          int,
    end:            int,
    inter_cloud_bandwidth_MBps: float,
):
    """
    Algorithm 2 — PARTITION(M, G):
        [Me, Mc] ← partitionM(M)
        [t*, P*] ← BPA(...)
        Đệ quy với (Me, Ge) và (Mc, Gc)
    """
    if start >= end:
        return [], 0.0

    # Base case: 1 CU
    if len(units) == 1:
        u = units[0]
        t = segment_time(layer_gflops, cu_flops, [u], start, end)
        return [(u, start, end)], t

    group_e, group_c = split_units_balanced(units, cu_flops)

    sol = bpa(
        layer_flops=layer_gflops,
        cu_flops=cu_flops,
        activation_mb=activation_mb,
        group_e=group_e,
        group_c=group_c,
        start=start,
        end=end,
        inter_cloud_bandwidth_MBps=inter_cloud_bandwidth_MBps,
    )

    left_segs,  left_atd  = pdd_grb(
        layer_gflops, cu_flops, activation_mb,
        sol.left_group,
        sol.left_range[0], sol.left_range[1],
        inter_cloud_bandwidth_MBps,
    )
    right_segs, right_atd = pdd_grb(
        layer_gflops, cu_flops, activation_mb,
        sol.right_group,
        sol.right_range[0], sol.right_range[1],
        inter_cloud_bandwidth_MBps,
    )

    t_comm = (
        0.0 if (sol.cut == start or sol.cut == end)
        else activation_mb[sol.cut] / max(inter_cloud_bandwidth_MBps, 1e-12)
    )

    atd = max(left_atd, right_atd, t_comm)
    return left_segs + right_segs, atd



def first_cloud_of_plan(segments):
    non_empty = [s for s in segments if s[1] < s[2]]
    if not non_empty:
        return None
    return sorted(non_empty, key=lambda x: x[1])[0][0]


def evaluate_pdd_for_one_client(
    layer_gflops:    np.ndarray,   # [K]     FLOPs từng layer
    all_cu_flops:   np.ndarray,   # [1+M]   index 0 = UE, 1..M = clouds
    activation_mb:  np.ndarray,   # [K+1]
    client_to_cloud_bandwidth_MBps : float,        # băng thông đồng nhất (paper: LAN)
    inter_cloud_bandwidth_MBps : float,
):

    layer_gflops   = np.asarray(layer_gflops,   dtype=float)
    all_cu_flops  = np.asarray(all_cu_flops,  dtype=float)
    activation_mb = np.asarray(activation_mb, dtype=float)

    num_layers   = len(layer_gflops)
    r_ue         = all_cu_flops[0]
    cloud_flops  = all_cu_flops[1:]           # [M]
    cloud_units  = list(range(len(cloud_flops)))

    best = None

    for z in range(0, num_layers + 1):

        # Thời gian UE chạy [0:z] — Eq. 2 với group = {UE}
        local_time = (
            float(np.sum(layer_gflops[:z])) / max(r_ue, 1e-12)
            if z > 0 else 0.0
        )

        if z == num_layers:
            # Full local — không dùng cloud
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
            segments, cloud_atd = pdd_grb(
                layer_gflops=layer_gflops,
                cu_flops=cloud_flops,
                activation_mb=activation_mb,
                units=cloud_units,
                start=z,
                end=num_layers,
                inter_cloud_bandwidth_MBps= inter_cloud_bandwidth_MBps,
            )

            first_cloud = first_cloud_of_plan(segments)
            assert first_cloud is not None, \
                f"GRB trả segments rỗng tại z={z}"

            # Upload activation tại z lên cloud đầu tiên
            upload_time = activation_mb[z] / max(client_to_cloud_bandwidth_MBps, 1e-12)

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

def summarize_pdd_result(result):
    print(f"round time     : {result['atd']:.6f} s")
    print(f"system FPS     : {result['fps']:.2f}")

    rows = []
    rows.append({
            "local_cut": result["local_cut"],
            "atd": result["atd"],
            "fps": result["fps"],
            "local_time": result["local_time"],
            "upload_time": result["upload_time"],
            "cloud_atd": result["cloud_atd"],
            "first_cloud": result["first_cloud"],
            "segments": result["segments"],
        })

    return pd.DataFrame(rows)


# Ví dụ chạy

# FLOPs từng layer (GFLOPs)
LAYER_GFLOPS = np.array([
    29.2552704,   69.8351616,  101.449728,   136.839168,
    97.910784,    136.3673088, 81.1597824,   68.1836544,
    80.216064,    38.1026304,  58.1861376,   0.2359296,
    0.0,         111.3587712, 0.4718592,    0.0,
    128.3457024,  34.209792,   0.0,          88.7095296,
    34.0918272,   0.0,        107.4364416,  359.97696
], dtype=float)

ALL_CU_GFLOPS = np.array([
    440,
    279,
    213.8
], dtype=float)

CUT_DATA_SIZES_MB = np.array([
    13.78, 8.02, 20.5,  5.81,  9.61, 12.54,
    12.38, 13.61, 13.71, 13.83, 13.52, 18.11,
    17.89, 13.83, 26.1,  23.93,  9.94, 11.29,
    10.87,  9.57, 10.27, 10.25,  9.42,
], dtype=float)

activation_mb = np.concatenate([[13.0], CUT_DATA_SIZES_MB, [0.0]])

inter_cloud_bandwidth_MBps = 125.0
client_to_cloud_bandwidth_MBps = 125.0

result = evaluate_pdd_for_one_client(
    layer_gflops    = LAYER_GFLOPS,
    all_cu_flops   = ALL_CU_GFLOPS,
    activation_mb  = activation_mb,
    client_to_cloud_bandwidth_MBps = client_to_cloud_bandwidth_MBps,
    inter_cloud_bandwidth_MBps = inter_cloud_bandwidth_MBps,
)
result = summarize_pdd_result(result)
print(result)
