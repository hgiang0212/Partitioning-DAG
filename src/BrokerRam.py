"""Infrastructure-host RAM sampling (guide/11-broker-ram.md).

The RabbitMQ box runs nothing of ours, so it has no process to instrument — yet
everything handed between tiers is buffered in its memory, and a broker at its
high-water mark does not fail, it **blocks publishers**. On a worker that looks
identical to "the next stage is slow". This module is what separates the two.

Two rules drive the design:

* **The server samples it**, because the server is the only component alive for
  the whole run and it holds the authoritative clock.
* **One long-lived session**, not one connection per sample. Open-read-close-sleep
  makes the machine whose load you are measuring pay a TCP handshake plus an
  authentication every second — 1200 logins added to the thing under test on a
  20-minute run.
"""
import base64
import math
import threading
import time
import urllib.parse

import requests
from requests.auth import HTTPBasicAuth

import src.Log

try:
    import paramiko
except Exception:                       # falls back to the management API, labelled
    paramiko = None


# One line per sample, every field on it, so a partial read can never interleave
# two samples. `^` anchors matter: /Cached:/ also matches SwapCached:.
REMOTE_LOOP = r"""
i=0
while [ $i -lt {max_samples} ]; do
  i=$((i+1))
  r=`ps -eo rss=,comm= | awk '$2 ~ /beam|rabbit/ {{s+=$1}} END{{print s+0}}'`
  awk -v ts="`date +%s%N`" -v r="$r" '
    /^MemTotal:/{{t=$2}} /^MemFree:/{{f=$2}} /^MemAvailable:/{{a=$2}}
    /^Buffers:/{{b=$2}} /^Cached:/{{c=$2}}
    /^SwapTotal:/{{st=$2}} /^SwapFree:/{{sf=$2}}
    END{{print "RAM", ts, t, f, a, b, c, st, sf, r}}' /proc/meminfo
  sleep {interval}
done
"""

KIB_TO_MB = 1024 / 1e6          # /proc/meminfo is in KiB; MB here is 10^6 bytes,
                                # matching guide/12 so a payload size and a host's
                                # memory growth compare without a conversion.


def _pct(sorted_values, q):
    """Nearest-rank percentile — every number printed was actually observed."""
    if not sorted_values:
        return 0.0
    k = max(1, math.ceil(q / 100.0 * len(sorted_values)))
    return sorted_values[k - 1]


class BrokerRamSampler:
    """Samples the queue host's memory for the whole life of the controller.

    The window opens at controller start — before any worker registers or
    anything is published — so the series contains the host **at rest** as well
    as under load. Without that stretch every later number is "this host was
    using N MB", which is unfalsifiable on a box you do not control; with it,
    every number becomes "running the system costs this host N MB".
    """

    def __init__(self, config, address, username, password, virtual_host,
                 series_path, summary_path):
        cfg = config or {}
        self.enabled = bool(cfg.get("enabled", False))
        self.interval_s = float(cfg.get("interval_s", 1.0))
        self.tail_s = float(cfg.get("tail_s", 2.0))
        self.max_run_s = float(cfg.get("max_run_s", 7200))
        ssh = cfg.get("ssh") or {}
        self.ssh_host = ssh.get("host") or address
        self.ssh_port = int(ssh.get("port", 22))
        # Host login credentials, NOT the broker's application credentials.
        # Confusing the two produces an auth failure that looks like a network
        # problem (guide/11 §8.5).
        self.ssh_user = ssh.get("username")
        self.ssh_pass = ssh.get("password")

        self.address = address
        self.mgmt_auth = HTTPBasicAuth(username, password)
        self.virtual_host = virtual_host
        self.series_path = series_path
        self.summary_path = summary_path

        self._samples = []            # dicts, in arrival order
        self._lock = threading.Lock()
        self._phase = "idle"
        self._stop = threading.Event()
        self._thread = None
        self._client = None
        self._stderr_tail = []
        self._source = None
        self._reason = "not started"

    # ─────────────────────────────── lifecycle ───────────────────────────────

    def start(self):
        """Open the window at controller start. Never raises."""
        if not self.enabled:
            self._reason = "disabled in config"
            return
        try:
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        except Exception as e:
            self._reason = f"sampler thread failed: {e}"
            src.Log.print_with_color(f"[BrokerRAM] {self._reason}", "yellow")

    def mark(self, phase):
        """Move the phase forward. Marks PARTITION the series, they never gate
        it: sampling does not pause at a boundary, so a missing mark degrades to
        a coarser split and never to a gap."""
        if not self.enabled:
            return
        with self._lock:
            self._phase = phase

    def stop(self):
        """Sample a short tail past the drain, then stop.

        The drain is precisely when a backed-up host gives memory back, so a
        curve that does NOT fall there is the signal that something is still
        holding units. The tail stays short on purpose — too short to wait out a
        runtime's garbage collection, so a positive tail reads as 'not back yet'
        rather than as proof of a leak."""
        if not self.enabled:
            return
        self.mark("tail")
        time.sleep(max(self.tail_s, 0.0))
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        try:
            if self._client is not None:
                self._client.close()
        except Exception:
            pass

    # ──────────────────────────────── sampling ───────────────────────────────

    def _run(self):
        if paramiko is not None and self.ssh_user:
            if self._run_ssh():
                return
        else:
            self._reason = ("paramiko not installed" if paramiko is None
                            else "no ssh username configured")
        self._run_mgmt_api()

    def _run_ssh(self):
        """One session, one bounded remote loop, one sleep. Returns False so the
        caller can fall back — never raises."""
        try:
            self._client = paramiko.SSHClient()
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._client.connect(self.ssh_host, port=self.ssh_port,
                                 username=self.ssh_user, password=self.ssh_pass,
                                 timeout=10, banner_timeout=10, auth_timeout=10)
            # Bound the loop: a sampler orphaned by a hard kill of the server
            # would otherwise run on the infrastructure host forever.
            max_samples = int(self.max_run_s / max(self.interval_s, 0.05)) + 1
            script = REMOTE_LOOP.format(max_samples=max_samples,
                                        interval=self.interval_s)
            # base64 through a local argv AND a remote login shell: encoding
            # removes both quoting layers at once.
            payload = base64.b64encode(script.encode()).decode()
            stdin, stdout, stderr = self._client.exec_command(
                f"echo {payload} | base64 -d | sh", get_pty=False)

            # stderr on its own thread: when authentication or the shell fails,
            # that tail is the entire diagnosis.
            threading.Thread(target=self._drain_stderr, args=(stderr,),
                             daemon=True).start()

            self._source = "ssh"
            src.Log.print_with_color(
                f"[BrokerRAM] sampling {self.ssh_host} over ssh every "
                f"{self.interval_s:.1f}s", "green")
            for line in stdout:
                if self._stop.is_set():
                    break
                self._on_ssh_line(line)
            if self._samples:
                return True
            self._reason = ("ssh produced no samples: "
                            + (" | ".join(self._stderr_tail[-3:]) or "no output"))
            return False
        except Exception as e:
            self._reason = f"ssh unavailable ({e})"
            src.Log.print_with_color(
                f"[BrokerRAM] {self._reason} — falling back to the management API",
                "yellow")
            return False

    def _drain_stderr(self, stderr):
        try:
            for line in stderr:
                line = line.strip()
                if line:
                    self._stderr_tail.append(line)
                    del self._stderr_tail[:-5]
        except Exception:
            pass

    def _on_ssh_line(self, line):
        parts = line.split()
        if len(parts) != 10 or parts[0] != "RAM":
            return
        try:
            _, ts, total, free, avail, buffers, cached, swap_t, swap_f, rss = parts
            total_mb = int(total) * KIB_TO_MB
            avail_mb = int(avail) * KIB_TO_MB
            # MemTotal - MemAvailable, never - MemFree: MemFree counts reclaimable
            # page cache as used and reads ~90% on any box that touched a disk —
            # always alarming, never actionable.
            used_mb = total_mb - avail_mb
            self._append(dict(
                ts_ns=int(ts), source="ssh", total_mb=total_mb, used_mb=used_mb,
                avail_mb=avail_mb, free_mb=int(free) * KIB_TO_MB,
                cached_mb=(int(cached) + int(buffers)) * KIB_TO_MB,
                swap_used_mb=(int(swap_t) - int(swap_f)) * KIB_TO_MB,
                rss_mb=int(rss) * KIB_TO_MB))
        except Exception:
            return

    def _run_mgmt_api(self):
        """Fallback. `used_mb` here is the BROKER PROCESS, not the host — a
        different question from the ssh source, which is why every line carries
        `source=` and nothing is ever silently substituted."""
        self._source = "rabbitmq_api"
        src.Log.print_with_color(
            f"[BrokerRAM] {self._reason}; sampling the management API instead "
            f"(used_mb = broker process, not host)", "yellow")
        url = f"http://{self.address}:15672/api/nodes"
        while not self._stop.is_set():
            try:
                response = requests.get(url, auth=self.mgmt_auth, timeout=5)
                if response.status_code == 200:
                    nodes = response.json() or []
                    used = sum(float(n.get("mem_used") or 0) for n in nodes) / 1e6
                    limit = sum(float(n.get("mem_limit") or 0) for n in nodes) / 1e6
                    self._append(dict(
                        ts_ns=time.time_ns(), source="rabbitmq_api",
                        total_mb=limit, used_mb=used,
                        avail_mb=max(limit - used, 0.0), free_mb=float("nan"),
                        cached_mb=float("nan"), swap_used_mb=float("nan"),
                        rss_mb=used))
                elif not self._samples:
                    self._reason = f"management API returned {response.status_code}"
            except Exception as e:
                if not self._samples:
                    self._reason = f"management API unreachable ({e})"
            self._stop.wait(self.interval_s)

    def _append(self, sample):
        with self._lock:
            sample["phase"] = self._phase
            self._samples.append(sample)
        used_pct = (100.0 * sample["used_mb"] / sample["total_mb"]
                    if sample["total_mb"] else 0.0)
        # Written LIVE, for the same reason batch_done_ns.log is: a run that dies
        # still leaves the series behind, and the series is the part you cannot
        # reconstruct afterwards.
        try:
            with open(self.series_path, "a") as f:
                f.write(f"{sample['ts_ns']} host={self.ssh_host} "
                        f"source={sample['source']} phase={sample['phase']} "
                        f"total_mb={sample['total_mb']:.1f} "
                        f"used_mb={sample['used_mb']:.1f} used={used_pct:.2f}% "
                        f"avail_mb={sample['avail_mb']:.1f} "
                        f"free_mb={sample['free_mb']:.1f} "
                        f"cached_mb={sample['cached_mb']:.1f} "
                        f"swap_used_mb={sample['swap_used_mb']:.1f} "
                        f"rabbit_rss_mb={sample['rss_mb']:.1f}\n")
        except Exception:
            pass

    # ──────────────────────────────── summary ────────────────────────────────

    def write_summary(self):
        """BROKER / USED / DELTA / RABBIT, then one PHASE line per phase, then
        COMPARE. Never raises."""
        if not self.enabled:
            return
        try:
            self._write_summary()
        except Exception as e:
            src.Log.print_with_color(f"[BrokerRAM] summary failed: {e}", "yellow")

    def _write_summary(self):
        t_ns = time.time_ns()
        with self._lock:
            samples = list(self._samples)

        if not samples:
            # A missing file is indistinguishable from a run where the host was
            # fine. `samples=0 (permission denied)` is not.
            line = (f"{t_ns} BROKER host={self.ssh_host} source={self._source or 'none'} "
                    f"samples=0 interval_s={self.interval_s:.3f} span_s=0.000 "
                    f"total_mb=0.0 ({self._reason})")
            with open(self.summary_path, "w") as f:
                f.write(line + "\n")
            src.Log.print_with_color(f"[BrokerRAM] {line}", "yellow")
            return

        used = [s["used_mb"] for s in samples]
        used_sorted = sorted(used)
        total_mb = samples[-1]["total_mb"]
        span_s = (samples[-1]["ts_ns"] - samples[0]["ts_ns"]) / 1e9
        source = samples[-1]["source"]

        def as_pct(mb):
            return 100.0 * mb / total_mb if total_mb else 0.0

        # A host that is swapping is already past the point where its latency
        # contribution is stable, so a non-zero value here invalidates any
        # latency conclusion drawn from the same run. NaN on the API fallback,
        # which cannot see host swap at all — filtered out rather than counted.
        swaps = [s["swap_used_mb"] for s in samples
                 if s["swap_used_mb"] == s["swap_used_mb"]]
        swap_max = max(swaps) if swaps else 0.0
        mean_rss = sum(s["rss_mb"] for s in samples) / len(samples)
        max_rss = max(s["rss_mb"] for s in samples)

        lines = [
            f"{t_ns} BROKER host={self.ssh_host} source={source} "
            f"samples={len(samples)} interval_s={self.interval_s:.3f} "
            f"span_s={span_s:.1f} total_mb={total_mb:.1f} "
            f"t_start_ns={samples[0]['ts_ns']} t_end_ns={samples[-1]['ts_ns']}",

            f"{t_ns} USED min_mb={used_sorted[0]:.1f} "
            f"mean_mb={sum(used) / len(used):.1f} p50_mb={_pct(used_sorted, 50):.1f} "
            f"p95_mb={_pct(used_sorted, 95):.1f} max_mb={used_sorted[-1]:.1f} "
            f"min={as_pct(used_sorted[0]):.2f}% mean={as_pct(sum(used) / len(used)):.2f}% "
            f"p95={as_pct(_pct(used_sorted, 95)):.2f}% max={as_pct(used_sorted[-1]):.2f}%",

            # DELTA measures the window's own ends. With the window opening at
            # controller start, start_mb IS the at-rest figure.
            f"{t_ns} DELTA start_mb={used[0]:.1f} end_mb={used[-1]:.1f} "
            f"growth_mb={used[-1] - used[0]:.1f} "
            f"peak_over_start_mb={max(used) - used[0]:.1f}",

            f"{t_ns} RABBIT mean_rss_mb={mean_rss:.1f} max_rss_mb={max_rss:.1f} "
            f"swap_max_mb={swap_max:.1f}",
        ]

        phases = {}
        for s in samples:
            phases.setdefault(s["phase"], []).append(s)
        for phase in ("idle", "run", "tail"):
            group = phases.get(phase)
            if not group:
                continue                    # a phase with no samples is OMITTED,
            u = sorted(x["used_mb"] for x in group)   # never written as zeros
            mean = sum(u) / len(u)
            lines.append(
                f"{t_ns} PHASE phase={phase} samples={len(group)} "
                f"span_s={(group[-1]['ts_ns'] - group[0]['ts_ns']) / 1e9:.3f} "
                f"min_mb={u[0]:.1f} mean_mb={mean:.1f} p50_mb={_pct(u, 50):.1f} "
                f"p95_mb={_pct(u, 95):.1f} max_mb={u[-1]:.1f} "
                f"mean={as_pct(mean):.2f}% max={as_pct(u[-1]):.2f}% "
                f"mean_rss_mb={sum(x['rss_mb'] for x in group) / len(group):.1f} "
                f"max_rss_mb={max(x['rss_mb'] for x in group):.1f} "
                f"t_start_ns={group[0]['ts_ns']} t_end_ns={group[-1]['ts_ns']}")

        # The line the whole file was built to produce: what running the system
        # COSTS this host, against the same host at rest. Only written when both
        # phases exist — there is nothing to compare otherwise.
        idle, run, tail = phases.get("idle"), phases.get("run"), phases.get("tail")
        if idle and run:
            idle_mean = sum(s["used_mb"] for s in idle) / len(idle)
            run_mean = sum(s["used_mb"] for s in run) / len(run)
            idle_rss = sum(s["rss_mb"] for s in idle) / len(idle)
            run_rss = sum(s["rss_mb"] for s in run) / len(run)
            compare = (f"{t_ns} COMPARE idle_mean_mb={idle_mean:.1f} "
                       f"run_mean_mb={run_mean:.1f} "
                       f"run_minus_idle_mb={run_mean - idle_mean:.1f} "
                       f"run_peak_over_idle_mb="
                       f"{max(s['used_mb'] for s in run) - idle_mean:.1f} "
                       f"idle_rss_mb={idle_rss:.1f} run_rss_mb={run_rss:.1f} "
                       f"run_rss_over_idle_mb={run_rss - idle_rss:.1f}")
            if tail:
                tail_mean = sum(s["used_mb"] for s in tail) / len(tail)
                compare += (f" tail_mean_mb={tail_mean:.1f} "
                            f"tail_minus_idle_mb={tail_mean - idle_mean:.1f} "
                            f"tail_span_s="
                            f"{(tail[-1]['ts_ns'] - tail[0]['ts_ns']) / 1e9:.3f}")
            lines.append(compare)

        with open(self.summary_path, "w") as f:
            f.write("\n".join(lines) + "\n")
        for line in lines:
            src.Log.print_with_color(f"[BrokerRAM] {line}", "cyan")
