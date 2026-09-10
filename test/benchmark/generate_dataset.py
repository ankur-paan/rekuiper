#!/usr/bin/env python3
"""
Generates line-delimited JSON telemetry dataset for streaming benchmarks.
Default: 500,000 records (~17 MB).
"""
import sys
import os

def generate(filename="telemetry_500k.jsonl", count=500_000):
    print(f"Generating {count:,} JSON records in {filename}...")
    dir_path = os.path.dirname(os.path.abspath(filename))
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        for i in range(count):
            temp = 25.0 + float(i % 10)
            f.write(f'{{"id": "dev_{i}", "temp": {temp:.1f}}}\n')
    size_mb = os.path.getsize(filename) / (1024 * 1024)
    print(f"Done! {filename} created ({size_mb:.2f} MB, {count:,} lines).")

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "telemetry_500k.jsonl")
    cnt = int(sys.argv[2]) if len(sys.argv) > 2 else 500_000
    generate(out, cnt)
