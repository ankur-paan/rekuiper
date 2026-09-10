#!/usr/bin/env python3
"""
Measures cold boot latency and idle memory footprint (RSS) of rekuiper.
Measures both:
  1. Internal daemon bootstrap time (from main() entry to socket ready)
  2. End-to-end OS process spawn latency (process creation + dynamic linking + socket listen)
"""
import subprocess
import time
import socket
import sys
import os
import re

TEST_PORT = 9092

def check_port(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.002)
    try:
        s.connect((host, port))
        s.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

def get_rss_mb(pid):
    if sys.platform == "win32":
        try:
            import psutil
            return psutil.Process(pid).memory_info().rss / (1024 * 1024)
        except Exception:
            return None
    else:
        try:
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        return float(line.split()[1]) / 1024.0
        except Exception:
            return None

def setup_test_etc():
    etc_dir = "/tmp/bench_etc" if sys.platform != "win32" else "target/bench_etc"
    os.makedirs(etc_dir, exist_ok=True)
    with open(os.path.join(etc_dir, "kuiper.yaml"), "w") as f:
        f.write(f"""basic:
  restPort: {TEST_PORT}
  port: 20497
  prometheus: false
""")
    return etc_dir

def measure_boot(iterations=5):
    is_windows = sys.platform == "win32"
    if is_windows:
        exe_path = r"target\release\kuiperd.exe"
        cwd = os.getcwd()
    else:
        exe_path = os.path.expanduser("~/rekuiper_target/release/kuiperd")
        if not os.path.exists(exe_path):
            exe_path = "target/release/kuiperd"
        cwd = os.getcwd()

    etc_dir = setup_test_etc()
    e2e_times = []
    internal_times = []
    rss_list = []

    print(f"=== Cold Boot & Idle Memory Benchmark ({sys.platform}) ===")
    print(f"Binary: {exe_path}")
    print(f"Test Port: {TEST_PORT}")
    print(f"Workdir: {cwd}\n")

    for i in range(iterations):
        t0 = time.perf_counter()
        data_dir = "/tmp/rekuiper_bench_data" if not is_windows else "target/bench_data"
        proc = subprocess.Popen(
            [exe_path, "--etc", etc_dir, "--data", data_dir],
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        ready = False
        t_ready = None
        for _ in range(5000):
            if check_port("127.0.0.1", TEST_PORT):
                t_ready = time.perf_counter()
                ready = True
                break
            time.sleep(0.001)

        rss = get_rss_mb(proc.pid) if ready else None
        proc.terminate()
        try:
            stdout, stderr = proc.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()

        if ready and t_ready:
            e2e_ms = (t_ready - t0) * 1000.0
            e2e_times.append(e2e_ms)
            internal_ms = None
            m = re.search(r"eKuiper server ready in (\d+) ms", stdout + stderr)
            if m:
                internal_ms = float(m.group(1))
                internal_times.append(internal_ms)
            if rss is not None:
                rss_list.append(rss)

            int_str = f"{internal_ms:.2f} ms" if internal_ms else "N/A"
            rss_str = f"{rss:.2f} MB" if rss else "N/A"
            print(f"Run {i+1}: End-to-End = {e2e_ms:.2f} ms | Internal Bootstrap = {int_str} | RSS = {rss_str}")
        else:
            print(f"Run {i+1}: FAILED to bind within timeout")
        time.sleep(0.2)

    if e2e_times:
        print("\n--- Summary ---")
        print(f"End-to-End Process Spawn: min={min(e2e_times):.2f} ms, avg={sum(e2e_times)/len(e2e_times):.2f} ms, max={max(e2e_times):.2f} ms")
        if internal_times:
            print(f"Internal Daemon Init:     min={min(internal_times):.2f} ms, avg={sum(internal_times)/len(internal_times):.2f} ms, max={max(internal_times):.2f} ms")
        if rss_list:
            print(f"Idle Memory RSS:          avg={sum(rss_list)/len(rss_list):.2f} MB")

if __name__ == "__main__":
    measure_boot()
