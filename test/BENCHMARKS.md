# Fair Over-HTTP Benchmarks & Reproducibility Guide

Same Python harness delivers the identical 500,000 synthetic events through documented
ingestion endpoints into containers with equal `--cpus=1 --memory=1g` constraints,
sequentially (never concurrent), with equivalent SQL/sink work. Source-to-sink
(rule-status sink counters) is measured, not HTTP-ack alone.

- Harness: [`benchmark/bench_http_fair.py`](benchmark/bench_http_fair.py)
- Full audit with per-run evidence: [`../BENCHMARK-AUDIT.md`](../BENCHMARK-AUDIT.md)
- Raw results: preserved under workspace `evidence/bench-fair-full.json` (copied from harness output)

## Methodology (fair)

- Events: 500,000 deterministic `{"id":"dev_N","temp":25.0+(N%10)}` (~30 B, all pass `WHERE temp>20`); private local fixture, no public broker.
- SQL (identical): `SELECT id, temp * 1.8 + 32 AS temp_f FROM bench WHERE temp > 20.0`, sink `[{"nop":{}}]`.
- Batching/HTTP (identical): 500 events/POST × 1000 POSTs, 8 parallel workers via `ThreadPoolExecutor`.
- Containers (equal, sequential):
  `docker run -d --cpus=1 --memory=1g ...` (quota only; NOT claimed as strict physical one-core isolation).
- Ingestion endpoints (documented per engine):
  - rekuiper: `POST /streams/bench/data` (implemented; covered by `test_http_push_data_ingestion`; not in `openapi.json` — recorded gap).
  - eKuiper: `POST :10081/bench/data` with `TYPE="httppush" DATASOURCE="/bench/data"` —
    [HTTP Push source](https://ekuiper.org/docs/en/latest/guide/sources/builtin/http_push.html),
    stream management via [Streams REST API](https://ekuiper.org/docs/en/latest/api/restapi/streams.html).
- eKuiper tested BOTH default and tuned:
  [rule fine-tuning](https://ekuiper.org/docs/en/latest/guide/rules/overview.html#fine-tuning)
  (`bufferLength` default 1024 / `concurrency` default 1 vs tuned 32768/4);
  effective options verified via `GET /rules/<id>`.
  Global defaults reference:
  [global configurations](https://ekuiper.org/docs/en/latest/configuration/global_configurations.html).
- Counts per run: attempted, accepted (HTTP 2xx requests/events), processed (source counters),
  observed sink-output (sink counters), drain timeout 120 s (poll 0.5 s, 5 stable rounds),
  errors, duration boundaries (ingest start/end, drain end).
- 1 warmup (10k, discarded) + 3 measured runs per config. Raw + variance preserved. Not called bulletproof.

## Results (500k, --cpus=1, sequential)

Host: `Linux-6.6.87.2-microsoft-standard-WSL2-x86_64`, CPU `AMD Ryzen 5 PRO 230`, Docker server `29.1.3`, Python `3.12.3`.
Images: `rkfix0424/candidate:0.424-beta` (`sha256:96017eb938b6...`, local build, no RepoDigest);
`lfedge/ekuiper:2.4.1` (`sha256:41205fbf01fc...`, `lfedge/ekuiper@sha256:a346c1e63bd34a...`).

| Config | Run | Attempted | Accepted req/events | Sink-out (observed) | Loss after drain | Ingest ack eps | End-to-end eps | Total s |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| rekuiper 0.424 | 1 | 500,000 | 1000 / 500,000 | 22,098 | 95.6% | 89,804 | 2,570 | 8.60 |
| rekuiper 0.424 | 2 | 500,000 | 1000 / 500,000 | 21,202 | 95.8% | 108,430 | 2,772 | 7.65 |
| rekuiper 0.424 | 3 | 500,000 | 1000 / 500,000 | 21,574 | 95.7% | 119,776 | 2,992 | 7.21 |
| eKuiper default | 1 | 500,000 | 1000 / 500,000 | 365,770 | 26.8% | 77,942 | 6,808 | 53.72 |
| eKuiper default | 2 | 500,000 | 1000 / 500,000 | 248,446 | 50.3% | 87,829 | 5,072 | 48.98 |
| eKuiper default | 3 | 500,000 | 1000 / 500,000 | 219,640 | 56.1% | 85,583 | 4,635 | 47.39 |
| eKuiper tuned 32768/4 | 1 | 500,000 | 1000 / 500,000 | 324,661 | 35.1% | 69,974 | 5,397 | 60.16 |
| eKuiper tuned 32768/4 | 2 | 500,000 | 1000 / 500,000 | 306,739 | 38.7% | 70,559 | 5,238 | 58.56 |
| eKuiper tuned 32768/4 | 3 | 500,000 | 1000 / 500,000 | 336,254 | 32.7% | 48,518 | 4,514 | 74.50 |

Variance (end-to-end eps mean ± pstdev): rekuiper 2,778 ± 172; eKuiper default 5,505 ± 939;
eKuiper tuned 5,049 ± 384. eKuiper default shows high run-to-run variance. No comparative
ranking beyond these three fair configs; no speedup multipliers are claimed.

Throughput/loss tradeoff: larger eKuiper `bufferLength`/`concurrency` reduce loss at the same
burst versus defaults (documented tuning). Undrained output is not called loss — loss above
is after the 120 s drain window stabilizes (5 stable polls). All HTTP POSTs were accepted
(0 failed requests); shortfall is source-to-sink, not ack throughput.

## Internal microbenchmark (separate, NOT comparable)

`crates/rekuiper-server/tests/perf_throughput.rs` (`cargo test --release --test perf_throughput`)
drives the in-process bus directly with chunked catch-up waits — no HTTP, no containers.
It is an internal regression floor (assert > 20,000 eps) and MUST NOT be compared with
end-to-end HTTP numbers.

```bash
cargo test --release --test perf_throughput -- --nocapture
```

## Prior noncomparable numbers (labeled, no ranking)

Old file-source/in-process/JVM-cluster methods for Flink, Benthos, Telegraf and the prior
`bench_*.py` scripts use different ingestion paths and are NOT equivalent to the fair
over-HTTP harness above. They are preserved in `benchmark/bench_flink.py`,
`benchmark/bench_benthos.py`, `benchmark/bench_telegraf.py`, `benchmark/bench_ekuiper.py`
(file-source variant), and `benchmark/bench_rekuiper.py` (in-process variant) for reference
only, without speedup claims.

Cold-boot/RSS figures from [`benchmark/measure_cold_boot.py`](benchmark/measure_cold_boot.py)
are informational process-spawn measurements, not streaming throughput comparisons.

## Reproduce

Prerequisites: Docker, Python 3.8+ with `requests` (`pip install requests`).

```bash
# Fair over-HTTP (rekuiper + eKuiper default + eKuiper tuned), 500k each:
python3 benchmark/bench_http_fair.py --all --events 500000 --batch 500 --concurrency 8 --runs 3

# Single config, e.g. eKuiper tuned only:
python3 benchmark/bench_http_fair.py --engine ekuiper --mode tuned --events 500000

# Internal microbenchmark only (not comparable to above):
cargo test --release --test perf_throughput -- --nocapture

# Legacy noncomparable scripts (reference only):
python3 benchmark/bench_flink.py
python3 benchmark/bench_benthos.py
python3 benchmark/bench_telegraf.py
```
