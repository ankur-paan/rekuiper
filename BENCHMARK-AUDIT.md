# BENCHMARK-AUDIT — fair over-HTTP audit (rekuiper 0.424 vs eKuiper 2.4.1)

HOLD: do not merge/tag/publish on the basis of this file alone — parent review required.
PR #1 is already MERGED on remote (main `f931cc9`); this audit was produced on
`codex/fix-0424` at `c90b059` (one commit ahead of the merged head `e4750da`).
No merge or tag was performed in this run. Engine code unchanged; reuse final
candidate image; docs/harness/provenance scope only.

## 1. Methodology (fair, identical harness)

Same Python harness `test/benchmark/bench_http_fair.py` for rekuiper and eKuiper at minimum:

- 500,000 deterministic synthetic events `{"id":"dev_N","temp":25.0+(N%10)}` (~30 B,
  all pass `WHERE temp>20`); private local fixture, no public broker load.
- Equivalent SQL/output work:
  `SELECT id, temp * 1.8 + 32 AS temp_f FROM bench WHERE temp > 20.0`, sink `[{"nop":{}}]`.
  eKuiper nop sink: https://ekuiper.org/docs/en/latest/guide/sinks/builtin/nop.html ;
  rekuiper lists `nop` as builtin sink.
- Documented ingestion endpoints:
  - rekuiper: `POST /streams/bench/data` (implemented; covered by
    `crates/rekuiper-server/tests/fvt_compat.rs::test_http_push_data_ingestion`;
    NOT present in `openapi.json` — recorded gap).
  - eKuiper: `POST :10081/bench/data` with stream
    `CREATE STREAM bench () WITH (FORMAT="json", TYPE="httppush", DATASOURCE="/bench/data")` —
    [HTTP Push source](https://ekuiper.org/docs/en/latest/guide/sources/builtin/http_push.html),
    stream CRUD via [Streams REST API](https://ekuiper.org/docs/en/latest/api/restapi/streams.html).
- Equal constraints, sequential (never concurrent):
  `docker run -d --name <bench> --cpus=1 --memory=1g -p <host>:<container> <image>`.
  Quota `--cpus=1` only; NOT claimed as strict physical one-core isolation.
  Host ports avoid the manager stack (rekuiper `19082:9081`, eKuiper `19081:9081` + `11081:10081`).
- Batching/HTTP identical: 500 events/POST × 1000 POSTs, 8 workers (`ThreadPoolExecutor`),
  same payload size/SQL/tuning per comparison.
- eKuiper BOTH default and tuned:
  [rule fine-tuning](https://ekuiper.org/docs/en/latest/guide/rules/overview.html#fine-tuning)
  (`bufferLength` 1024 / `concurrency` 1 vs tuned 32768/4);
  effective options verified via `GET /rules/rbench` each setup.
  Global reference: [global configurations](https://ekuiper.org/docs/en/latest/configuration/global_configurations.html).
- Source-to-sink measured (rule-status sink counters), not HTTP-ack alone.
  Per run: attempted, accepted (HTTP 2xx req/events), processed (source counters),
  observed sink-output (sink counters), drain timeout 120 s (poll 0.5 s, 5 stable rounds),
  errors, duration boundaries (ingest start/end wall + perf_counter, drain end).
- 1 warmup (10k, discarded) + 3 measured runs per config. Raw JSON preserved
  (`evidence/bench-fair-full.json` from harness output `/tmp/bench-fair-full.json`).
  Variance reported; nothing called bulletproof.

Internal microbenchmark `cargo test --release --test perf_throughput` drives the in-process
bus directly (chunked catch-up waits, no HTTP/containers) and is labeled separately in
`test/BENCHMARKS.md`. It was never compared with HTTP numbers here.

## 2. Reproducible config / identity

- Host: `Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39`,
  CPU `AMD Ryzen 5 PRO 230 w/ Radeon 760M Graphics`, Docker server `29.1.3`, Python `3.12.3`.
- Images (immutable IDs from `docker inspect`):
  - `rkfix0424/candidate:0.424-beta` → `sha256:96017eb938b6e202451a3ccd0ca74862a443b2946e69ac7f1036a85b9a8cd2e2`, RepoDigests `[]` (local build).
    Source identity: `codex/fix-0424` `c90b059` (engine code unchanged from merged `e4750da` + CI test commit).
  - `lfedge/ekuiper:2.4.1` → `sha256:41205fbf01fcfd3b7f99462d1f2551f8fcd44a25344bbeb620c6b7cb6a5c366a`,
    RepoDigest `lfedge/ekuiper@sha256:a346c1e63bd34a744d6e30a78dc3ecf6c59c52fa2d831d4abf5e65db218a3073`.
- Exact harness invocation:
  `python3 test/benchmark/bench_http_fair.py --all --events 500000 --batch 500 --concurrency 8 --runs 3 --warmup 1 --out /tmp/bench-fair-full.json`
- Rule/stream setup and drain logic are in-harness (see file); no manual tuning per run.

## 3. Per-run evidence (500k attempted each)

### rekuiper 0.424-beta candidate

| Run | Accepted req/events | Source_in / sink_out | Loss | Ingest ack eps | End-to-end eps | Total s (ingest s) |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1000 / 500,000 | 22,098 / 22,098 | 95.6% | 89,804 | 2,570 | 8.60 (5.57) |
| 2 | 1000 / 500,000 | 21,202 / 21,202 | 95.8% | 108,430 | 2,772 | 7.65 (4.61) |
| 3 | 1000 / 500,000 | 21,574 / 21,574 | 95.7% | 119,776 | 2,992 | 7.21 (4.17) |

Mean e2e 2,778 ± 172 (pstdev). 0 failed HTTP requests; shortfall is source-to-sink under burst
(fixed ~10k MPSC + broadcast lag drops; `exceptionsTotal` 0 — drops are silent at the bus,
reported here as loss after drain stabilizes).

### eKuiper 2.4.1 default (bufferLength 1024, concurrency 1 verified)

| Run | Accepted | Sink-out | Loss | Ack eps | E2e eps | Total s |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1000 / 500,000 | 365,770 | 26.8% | 77,942 | 6,808 | 53.72 |
| 2 | 1000 / 500,000 | 248,446 | 50.3% | 87,829 | 5,072 | 48.98 |
| 3 | 1000 / 500,000 | 219,640 | 56.1% | 85,583 | 4,635 | 47.39 |

Mean e2e 5,505 ± 939. High run-to-run variance. `source_bench_0_records_in_total` counts HTTP
requests (1000), not events; sink counters count events. `exceptions_total` 0 — loss occurs
without buffer-full exception in this path; reported as loss after 120 s drain stabilizes,
not as undrained output.

### eKuiper 2.4.1 tuned (bufferLength 32768, concurrency 4 verified via GET /rules/rbench)

| Run | Accepted | Sink-out | Loss | Ack eps | E2e eps | Total s |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1000 / 500,000 | 324,661 | 35.1% | 69,974 | 5,397 | 60.16 |
| 2 | 1000 / 500,000 | 306,739 | 38.7% | 70,559 | 5,238 | 58.56 |
| 3 | 1000 / 500,000 | 336,254 | 32.7% | 48,518 | 4,514 | 74.50 |

Mean e2e 5,049 ± 384. Larger buffers reduce loss vs defaults at the same burst and narrow
variance, at the cost of longer drain (more buffered work). Tradeoff is throughput/loss,
not free throughput — documented tuning behaves as documented.

Raw per-run JSON (walls, counts, errors, effective options) is in
`evidence/bench-fair-full.json`. Smoke 20k probes (`evidence/bench-smoke-*.json`) show the same
pattern at smaller scale (tuned lossless at 20k, lossy at 500k for all).

## 4. Honest findings

- Under identical burst over-HTTP with `--cpus=1`, all three configs lose data after the full
  drain window; HTTP acks alone (48k–120k eps) overstate delivery by 1–2 orders of magnitude.
  Source-to-sink is the only reported throughput here.
- No speedup multipliers are claimed. eKuiper default/tuned deliver ~10–17× more events than
  the rekuiper candidate in this burst regime with higher e2e eps, but with high variance
  (especially default). The candidate's fixed bus/MPSC drops most burst input silently.
- Prior claims removed: 15 µs p99 (no histogram), 320k–540k eps / 425k eps / 0-loss / 9.6× / 7.0× /
  16.4× / 1.8–2.5× rankings, absolute 100% route parity / zero-stubs, and future 0.69 release.
  Flink/Benthos/Telegraf were NOT rerun equivalently — old numbers are labeled noncomparable
  legacy (`test/BENCHMARKS.md`, `visualizer.html`, `render_video.py`) with no ranking.
- Internal `perf_throughput` remains an in-process regression floor only; never compared to HTTP.
- Version kept at 0.424-beta per user direction (no 0.5/0.6/0.7 downgrade applied).

## 5. Claims cleanup performed

- `README.md`: real Rust CI badge (`actions/workflows/ci.yml`), removed throughput/latency badges,
  rewrote performance section to the fair table above, scoped API coverage, removed 0.69 mentions,
  roadmap set to 0.424-beta current, docker tag 0.424-beta.
- `test/BENCHMARKS.md`: rewritten to fair methodology + per-run table + variance; fixed relative
  `benchmark/` links; legacy scripts labeled noncomparable.
- `test/benchmark/bench_http_fair.py`: new fair harness (this audit).
- `test/benchmark/bench_rekuiper.py`: header now marks internal-only, not comparable.
- `test/benchmark/render_video.py`, `visualizer.html`: marked legacy noncomparable, no ranking use.
- `RELEASE_NOTES.md`, `CHANGELOG.md`, `PARITY_TICKETS.md`: absolute parity/multiplier language
  scoped with evidence links; history annotated as noncomparable where applicable.
- No 0.5/0.6/0.7 version change.

## 6. Provenance and naming review (factual, not legal advice)

- `openapi.json` states `x-source-tag: https://github.com/lf-edge/ekuiper/tree/v2.4.1`,
  `x-source-commit: bf8c1258...`, `info.license: Apache 2.0`, `externalDocs` to upstream
  `docs/en_US/api/restapi`. Derivation from LF Edge eKuiper v2.4.1 is verified in-file.
  `LICENSE` / `LICENSE-MIT` already retain `Copyright (c) LF Edge eKuiper Contributors`.
  Added factual `NOTICE` summarizing the above (no legal conclusions).
- `docs/en_US`, `docs/zh_CN`, `deploy/`, `test/` JMeter/txt fixtures entered in initial
  commit `de1952c` and resemble upstream layout; file-level provenance not re-verified here.
  Unresolved: confirm per-file upstream sources/licenses for `docs/`, `deploy/docker/*`,
  `deploy/chart/ekuiper/*`, `test/*.jmx`, `test/*.txt`, `test/benchmark/multiple_rules/*`
  against https://github.com/lf-edge/ekuiper/tree/v2.4.1 before publication; add file-level
  Apache attribution where derivation is confirmed.
- No project/binary renaming performed (user reserved naming decision).
  Open question for user: `rekuiper` vs `kuiperd`/`kuiper` binary naming vs upstream `kuiper`
  remains under user review; no change made here.

## 7. Known residual gaps

- rekuiper `POST /streams/:name/data` ingestion is implemented/tested but absent from
  `openapi.json` (contract gap).
- eKuiper `source_*_records_in_total` counts requests while rekuiper counts events —
  comparison uses sink-out for fairness; counting semantics documented above.
- Parent review hypotheses remain open (not re-proven here): empty intermediate INNER join
  suppressing later RIGHT/FULL rows; window table-join first-ON-match scope; stream RIGHT/FULL
  fanout cap scope. No exhaustive parity claimed.
- Burst regime only; paced/sustainable-rate and file-source comparisons were not rerun.
- Single host (WSL2) only; no multi-host or strict core-pin claims.

## 8. What was NOT done (per HOLD)

- No merge (PR #1 already MERGED remotely before this run; no new merge performed).
- No tag, no release, no Docker publish.
- No revert of concurrent CI test change (`crates/rekuiper-server/tests/fvt_compat.rs` `c90b059` preserved).
- No `cargo clean` beyond existing state (no engine rebuild; docs/harness-only changes).
- No full system cleanup or unrelated changes; healthy `ekuiper-manager-*` stack untouched
  (benchmark used isolated ports/containers `rk-bench-*-fair`, removed after runs).
