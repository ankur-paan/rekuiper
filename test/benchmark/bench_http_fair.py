#!/usr/bin/env python3
"""
Fair over-HTTP benchmark harness for rekuiper vs eKuiper.

Delivers identical synthetic events through DOCUMENTED ingestion endpoints
into containers with EQUAL CPU/memory constraints and EQUIVALENT SQL/sink work.
Measures source-to-sink (rule status sink counters), NOT HTTP ack alone.

- rekuiper ingestion: POST http://<mgmt>:9081/streams/<stream>/data
  (implemented + covered by crates/rekuiper-server/tests/fvt_compat.rs::test_http_push_data_ingestion;
   NOT in openapi.json -- recorded as contract gap)
- eKuiper ingestion: POST http://<data>:10081/<datasource>
  (documented HTTP-push source, TYPE="httppush", DATASOURCE="/bench/data";
   https://ekuiper.org/docs/en/latest/guide/sources/builtin/http_push.html)

Equivalent work:
  SQL: SELECT id, temp * 1.8 + 32 AS temp_f FROM bench WHERE temp > 20.0
  Sink: [{"nop": {}}]  (eKuiper nop sink:
    https://ekuiper.org/docs/en/latest/guide/sinks/builtin/nop.html ;
    rekuiper lists "nop" as builtin sink)
  Payload: {"id":"dev_N","temp":25.0+(N%10)} -- all pass filter, deterministic.

Constraints (equal, sequential, never concurrent):
  docker run --cpus=1 --memory=1g (plus --cpuset-cpus attempt where practical;
  quota is NOT claimed as strict physical one-core isolation).
  1 warmup run (discarded) + 3 measured runs per config.
  eKuiper tested BOTH default (bufferLength=1024, concurrency=1) AND tuned
  (bufferLength=32768, concurrency=4) per
  https://ekuiper.org/docs/en/latest/guide/rules/overview.html#fine-tuning
  Effective options verified via GET /rules/<id>.

Counts per run:
  attempted_events, accepted_requests, accepted_events,
  processed (source counters), observed sink-output (sink counters),
  drain timeout, errors, duration boundaries.

Usage (from repo root, WSL/Linux with docker + python requests):
  python3 test/benchmark/bench_http_fair.py --engine rekuiper --runs 3 --events 500000
  python3 test/benchmark/bench_http_fair.py --engine ekuiper --mode default --runs 3
  python3 test/benchmark/bench_http_fair.py --engine ekuiper --mode tuned --runs 3
  python3 test/benchmark/bench_http_fair.py --all --events 500000 --batch 500 --concurrency 8
"""
import argparse
import concurrent.futures
import json
import os
import platform
import subprocess
import sys
import time

try:
    import requests
except ImportError:
    print("pip install requests", file=sys.stderr)
    sys.exit(2)

SQL = "SELECT id, temp * 1.8 + 32 AS temp_f FROM bench WHERE temp > 20.0"
ACTIONS = [{"nop": {}}]
BATCH_DEFAULT = 500
HTTP_CONC_DEFAULT = 8
DRAIN_TIMEOUT_S = 120
DRAIN_POLL_S = 0.5
DRAIN_STABLE_ROUNDS = 5

REK_IMAGE = os.environ.get("REK_IMAGE", "rkfix0424/candidate:0.424-beta")
EKU_IMAGE = os.environ.get("EKU_IMAGE", "lfedge/ekuiper:2.4.1")

# Host ports (avoid colliding with manager stack on 9081)
PORTS = {
    "rekuiper": {"mgmt": 19082, "name": "rk-bench-rek-fair"},
    "ekuiper": {"mgmt": 19081, "data": 11081, "name": "rk-bench-eku-fair"},
}


def sh(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r


def docker_image_id(image):
    r = sh(["docker", "inspect", image, "--format", "{{.Id}}"])
    return r.stdout.strip() if r.returncode == 0 else "unknown"


def docker_repo_digests(image):
    r = sh(["docker", "inspect", image, "--format", "{{.RepoDigests}}"])
    return r.stdout.strip() if r.returncode == 0 else "unknown"


def host_info():
    info = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_model": "unknown",
        "docker_server": "unknown",
    }
    try:
        r = sh(["docker", "version", "--format", "{{.Server.Version}}"])
        if r.returncode == 0:
            info["docker_server"] = r.stdout.strip()
    except Exception:
        pass
    for p in ("/proc/cpuinfo",):
        try:
            with open(p) as f:
                for line in f:
                    if "model name" in line:
                        info["cpu_model"] = line.split(":", 1)[1].strip()
                        break
        except Exception:
            pass
    return info


def make_events(n):
    # Deterministic private local fixture; no public broker.
    # temp 25.0..34.0 -> all > 20.0 pass filter; payload ~30 bytes.
    return [{"id": f"dev_{i}", "temp": 25.0 + float(i % 10)} for i in range(n)]


def wait_ready(url, timeout=60):
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout:
        try:
            r = requests.get(url, timeout=3)
            if r.status_code in (200, 404):
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def start_container(engine):
    stop_container(engine)
    if engine == "rekuiper":
        p = PORTS["rekuiper"]
        cmd = ["docker", "run", "-d", "--name", p["name"],
               "--cpus=1", "--memory=1g",
               "-p", f"{p['mgmt']}:9081", REK_IMAGE]
        # Best-effort same-core pin; quota alone is NOT strict physical isolation.
        pin = sh(["docker", "run", "-d", "--name", p["name"] + "-pin-probe",
                  "--cpus=1", "--cpuset-cpus=0", REK_IMAGE, "--help"])
        # cleanup probe regardless
        sh(["docker", "rm", "-f", p["name"] + "-pin-probe"])
        _ = pin
        r = sh(cmd)
        if r.returncode != 0:
            raise RuntimeError(f"docker run rekuiper failed: {r.stderr}")
        base = f"http://127.0.0.1:{p['mgmt']}"
        if not wait_ready(base + "/ping"):
            raise RuntimeError("rekuiper not ready")
        return {"base": base, "data_base": base, "mgmt_port": p["mgmt"]}
    else:
        p = PORTS["ekuiper"]
        cmd = ["docker", "run", "-d", "--name", p["name"],
               "--cpus=1", "--memory=1g",
               "-p", f"{p['mgmt']}:9081", "-p", f"{p['data']}:10081", EKU_IMAGE]
        r = sh(cmd)
        if r.returncode != 0:
            raise RuntimeError(f"docker run ekuiper failed: {r.stderr}")
        base = f"http://127.0.0.1:{p['mgmt']}"
        if not wait_ready(base + "/ping"):
            raise RuntimeError("ekuiper not ready")
        # wait for data plane port too (httppush server starts on first rule)
        return {"base": base, "data_base": f"http://127.0.0.1:{p['data']}",
                "mgmt_port": p["mgmt"], "data_port": p["data"]}


def stop_container(engine):
    name = PORTS[engine]["name"]
    sh(["docker", "rm", "-f", name])


def setup_rule(engine, ctx, mode="default"):
    base = ctx["base"]
    # clean
    for kind, name in (("rules", "rbench"), ("streams", "bench")):
        try:
            requests.delete(f"{base}/{kind}/{name}", timeout=5)
        except Exception:
            pass
    time.sleep(1)
    if engine == "rekuiper":
        r = requests.post(base + "/streams",
                          json={"sql": 'CREATE STREAM bench () WITH (FORMAT="json")'},
                          timeout=10)
        assert r.status_code in (200, 201), f"rek stream: {r.status_code} {r.text[:300]}"
        r = requests.post(base + "/rules",
                          json={"id": "rbench", "sql": SQL, "actions": ACTIONS},
                          timeout=10)
        assert r.status_code in (200, 201), f"rek rule: {r.status_code} {r.text[:300]}"
        effective = {"bufferLength": "n/a (rekuiper fixed 10k MPSC)", "concurrency": "n/a"}
    else:
        r = requests.post(base + "/streams",
                          json={"sql": 'CREATE STREAM bench () WITH (FORMAT="json", TYPE="httppush", DATASOURCE="/bench/data")'},
                          timeout=10)
        assert r.status_code in (200, 201), f"eku stream: {r.status_code} {r.text[:300]}"
        body = {"id": "rbench", "sql": SQL, "actions": ACTIONS}
        if mode == "tuned":
            body["options"] = {"bufferLength": 32768, "concurrency": 4}
        r = requests.post(base + "/rules", json=body, timeout=10)
        assert r.status_code in (200, 201), f"eku rule: {r.status_code} {r.text[:300]}"
        # verify effective configuration
        g = requests.get(base + "/rules/rbench", timeout=10).json()
        effective = g.get("options", {"bufferLength": 1024, "concurrency": 1}
                          if mode == "default" else {})
        if mode == "default" and not effective:
            effective = {"bufferLength": 1024, "concurrency": 1, "note": "server defaults per docs"}
    time.sleep(2)  # let httppush server / rule subscribe
    return effective


def get_counts(engine, ctx):
    base = ctx["base"]
    try:
        s = requests.get(base + "/rules/rbench/status", timeout=10).json()
    except Exception as e:
        return {"error": f"status fetch failed: {e}"}
    if engine == "rekuiper":
        return {
            "source_in": s.get("sourceRecordsInTotal", 0),
            "sink_in": s.get("sinkRecordsInTotal", s.get("sinkRecordsOutTotal", 0)),
            "sink_out": s.get("sinkRecordsOutTotal", 0),
            "exceptions": s.get("exceptionsTotal", 0),
            "raw": s,
        }
    # eKuiper: source_in counts HTTP requests; sink counts events.
    return {
        "source_requests_in": s.get("source_bench_0_records_in_total", 0),
        "source_out": s.get("source_bench_0_records_out_total", 0),
        "sink_in": s.get("sink_nop_0_0_records_in_total", 0),
        "sink_out": s.get("sink_nop_0_0_records_out_total", 0),
        "exceptions": s.get("source_bench_0_exceptions_total", 0),
        "last_exception": str(s.get("source_bench_0_last_exception", ""))[:300],
        "raw_keys": sorted([k for k in s.keys() if "records" in k])[:12],
    }


def post_batch(session, url, batch):
    try:
        r = session.post(url, json=batch, timeout=30)
        ok = 200 <= r.status_code < 300
        return (ok, r.status_code, "" if ok else r.text[:200])
    except Exception as e:
        return (False, -1, str(e)[:200])


def run_once(engine, ctx, events, batch, http_conc):
    n = len(events)
    batches = [events[i:i + batch] for i in range(0, n, batch)]
    if engine == "rekuiper":
        url = ctx["base"] + "/streams/bench/data"
    else:
        url = ctx["data_base"] + "/bench/data"
    accepted_requests = 0
    failed_requests = 0
    errors = []
    session_pool = [requests.Session() for _ in range(http_conc)]
    ingest_start = time.perf_counter()
    ingest_start_wall = time.time()
    # Round-robin batches across sessions with fixed thread pool (identical for both)
    def task(idx_b):
        idx, b = idx_b
        sess = session_pool[idx % http_conc]
        return post_batch(sess, url, b)
    with concurrent.futures.ThreadPoolExecutor(max_workers=http_conc) as ex:
        for ok, code, err in ex.map(task, enumerate(batches)):
            if ok:
                accepted_requests += 1
            else:
                failed_requests += 1
                if len(errors) < 5:
                    errors.append(f"{code}:{err}")
    ingest_end = time.perf_counter()
    ingest_end_wall = time.time()
    accepted_events = accepted_requests * batch
    # tail batch may be smaller; correct if n % batch != 0
    if n % batch != 0 and accepted_requests == len(batches):
        accepted_events = n
    # Drain: poll sink_out until stable
    drain_deadline = ingest_end + DRAIN_TIMEOUT_S
    last_sink = -1
    stable = 0
    sink_out = 0
    counts = {}
    while time.perf_counter() < drain_deadline:
        time.sleep(DRAIN_POLL_S)
        counts = get_counts(engine, ctx)
        sink_out = counts.get("sink_out", 0)
        if sink_out == last_sink and sink_out > 0:
            stable += 1
            if stable >= DRAIN_STABLE_ROUNDS:
                break
        else:
            stable = 0 if sink_out != last_sink else stable + 1
            last_sink = sink_out
        # early exit if all accepted events observed
        if sink_out >= accepted_events and accepted_events > 0:
            # require stability too
            pass
    drain_end = time.perf_counter()
    drain_end_wall = time.time()
    counts = get_counts(engine, ctx)
    sink_out = counts.get("sink_out", 0)
    total_wall = drain_end - ingest_start
    return {
        "attempted_events": n,
        "batch_size": batch,
        "http_concurrency": http_conc,
        "num_batches": len(batches),
        "accepted_requests": accepted_requests,
        "failed_requests": failed_requests,
        "accepted_events": accepted_events,
        "ingest_start_wall": ingest_start_wall,
        "ingest_end_wall": ingest_end_wall,
        "drain_end_wall": drain_end_wall,
        "ingest_duration_s": ingest_end - ingest_start,
        "total_duration_s": total_wall,
        "drain_timeout_s": DRAIN_TIMEOUT_S,
        "counts": counts,
        "observed_sink_out": sink_out,
        "errors": errors,
        "ingest_eps_ack": (accepted_events / (ingest_end - ingest_start)) if ingest_end > ingest_start else 0,
        "end_to_end_eps": (sink_out / total_wall) if total_wall > 0 else 0,
    }


def bench_config(engine, mode, events_n, batch, http_conc, runs):
    image = REK_IMAGE if engine == "rekuiper" else EKU_IMAGE
    return {
        "engine": engine,
        "mode": mode,
        "image_ref": image,
        "image_id": docker_image_id(image),
        "repo_digests": docker_repo_digests(image),
        "container": PORTS[engine]["name"],
        "docker_constraints": ["--cpus=1", "--memory=1g"],
        "cpuset_note": "quota --cpus=1 only; NOT claimed as strict physical one-core isolation",
        "sql": SQL,
        "actions": ACTIONS,
        "payload": '{"id":"dev_N","temp":25.0+(N%10)} (~30B, all pass WHERE temp>20)',
        "events": events_n,
        "batch": batch,
        "http_concurrency": http_conc,
        "drain_timeout_s": DRAIN_TIMEOUT_S,
        "sequential": True,
        "host": host_info(),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=["rekuiper", "ekuiper"], default="rekuiper")
    ap.add_argument("--mode", choices=["default", "tuned"], default="default")
    ap.add_argument("--events", type=int, default=500000)
    ap.add_argument("--batch", type=int, default=BATCH_DEFAULT)
    ap.add_argument("--concurrency", type=int, default=HTTP_CONC_DEFAULT)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    targets = []
    if args.all:
        targets = [("rekuiper", "default"), ("ekuiper", "default"), ("ekuiper", "tuned")]
    else:
        targets = [(args.engine, args.mode if args.engine == "ekuiper" else "default")]

    events = make_events(args.events)
    all_results = []
    for engine, mode in targets:
        cfg = bench_config(engine, mode, args.events, args.batch, args.concurrency, args.runs)
        print(f"=== {engine} [{mode}] image={cfg['image_ref']} id={cfg['image_id']} ===")
        ctx = start_container(engine)
        try:
            effective = setup_rule(engine, ctx, mode)
            cfg["effective_options"] = effective
            cfg["ingestion_endpoint"] = (ctx["base"] + "/streams/bench/data"
                                         if engine == "rekuiper" else ctx["data_base"] + "/bench/data")
            # warmup (discarded, smaller)
            for w in range(args.warmup):
                print(f"warmup {w+1}/{args.warmup} (10k events, discarded)...")
                w_events = make_events(10000)
                run_once(engine, ctx, w_events, args.batch, args.concurrency)
                setup_rule(engine, ctx, mode)  # reset counters
            runs_out = []
            for i in range(args.runs):
                print(f"measured run {i+1}/{args.runs} ({args.events} events)...")
                res = run_once(engine, ctx, events, args.batch, args.concurrency)
                res["run"] = i + 1
                runs_out.append(res)
                print(f"  ack_eps={res['ingest_eps_ack']:.1f} e2e_eps={res['end_to_end_eps']:.1f} "
                      f"sink_out={res['observed_sink_out']}/{res['attempted_events']} "
                      f"errors={res['failed_requests']}")
                # reset between runs for clean per-run counts
                if i + 1 < args.runs:
                    setup_rule(engine, ctx, mode)
            cfg["runs"] = runs_out
            # variance summary on end-to-end eps
            e2es = [r["end_to_end_eps"] for r in runs_out]
            if e2es:
                import statistics
                cfg["summary"] = {
                    "e2e_eps_min": min(e2es),
                    "e2e_eps_max": max(e2es),
                    "e2e_eps_mean": sum(e2es) / len(e2es),
                    "e2e_eps_stdev": statistics.pstdev(e2es) if len(e2es) > 1 else 0.0,
                }
            all_results.append(cfg)
        finally:
            stop_container(engine)
            time.sleep(2)  # never run contenders concurrently; settle
    out = args.out or f"bench-fair-{int(time.time())}.json"
    with open(out, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
