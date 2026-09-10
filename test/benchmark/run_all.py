#!/usr/bin/env python3
"""
Comprehensive, Automated 500,000 Event Benchmark Suite.
Runs all competitive streaming engines head-to-head on an identical dataset:
  1. rekuiper (Rust)
  2. Upstream Go eKuiper (v2.4.1)
  3. Apache Flink (v2.3.0)
  4. Redpanda Connect / Benthos (Go)
  5. Telegraf (Go)
Prints a markdown table comparing Throughput, Latency, Memory, Boot Time, and Drops.
"""
import sys
import os
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from generate_dataset import generate
from bench_rekuiper import run_rekuiper_bench
from bench_ekuiper import run_ekuiper_bench
from bench_flink import run_flink_bench
from bench_benthos import run_benthos_bench
from bench_telegraf import run_telegraf_bench

def main():
    dataset_path = os.path.join(SCRIPT_DIR, "telemetry_500k.jsonl")
    if not os.path.exists(dataset_path):
        generate(dataset_path, 500_000)

    results = []

    # 1. rekuiper
    try:
        res = run_rekuiper_bench()
        if res:
            results.append(res)
    except Exception as e:
        print("rekuiper bench error:", e)

    # 2. Upstream eKuiper
    try:
        res = run_ekuiper_bench()
        if res:
            results.append(res)
    except Exception as e:
        print("eKuiper bench error:", e)

    # 3. Apache Flink
    try:
        res = run_flink_bench()
        if res:
            results.append(res)
    except Exception as e:
        print("Flink bench error:", e)

    # 4. Redpanda Connect (Benthos)
    try:
        res = run_benthos_bench()
        if res:
            results.append(res)
    except Exception as e:
        print("Benthos bench error:", e)

    # 5. Telegraf
    try:
        res = run_telegraf_bench()
        if res:
            results.append(res)
    except Exception as e:
        print("Telegraf bench error:", e)

    # Tabulate Results in Markdown
    print("\n" + "=" * 90)
    print("FINAL HEAD-TO-HEAD BENCHMARK SUMMARY (500,000 RECORDS)")
    print("=" * 90 + "\n")

    header = "| Engine | Runtime | 500k Elapsed | Throughput | Drops / Loss | Memory RSS | Cold Boot |"
    sep    = "| :--- | :--- | :---: | :---: | :---: | :---: | :---: |"
    print(header)
    print(sep)

    for r in results:
        drops_str = f"{r['drops']:,} ({r['drops']/r['records']*100:.1f}%)" if r['drops'] > 0 else "0 (0.0%)"
        row = f"| **{r['engine']}** | {r['language']} | **{r['elapsed_s']:.3f} s** | **{r['throughput_eps']:,.1f} eps** | {drops_str} | {r['memory_rss']} | {r['cold_boot']} |"
        print(row)

    print("\n" + "=" * 90)

if __name__ == "__main__":
    main()
