//! IIoT / vehicle MQTT benchmark orchestrator (Rust; no Python).
//!
//! Topology: Mosquitto container (cores 8,9, host port 11883, docker network
//! rk-bench-net) <- mqttgen (Rust, cores 10,11) ; engine container pinned to
//! core 2 (1 CPU, 1 GB, no swap) on the same network, reading the broker via
//! CONF_KEY (eKuiper's documented /metadata/sources/mqtt/confKeys API, used
//! identically for both engines) and writing a bind-mounted file sink.
//!
//! Workloads
//!   w1  SELECT id, device, temp, speed * 3.6 AS speed_kmh FROM telem WHERE temp > 21.0
//!       proof: unique ids with the run tag == exact expected filtered count, 0 duplicates
//!   w2  SELECT device, count(*) AS n, avg(temp) AS avg_temp, max(speed) AS max_speed
//!       FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10)
//!       proof: sum(n) over measured devices == messages sent, all devices present
//!   w3  ESPHome: 10k devices each on esphome/<dev>/sensor/temperature/state, plain-text
//!       payloads, FORMAT binary via a wildcard; SELECT meta(topic), self
//!       proof: rows (warm-up topics excluded) == sent, every device topic seen
//!   w4  vehicles: 10k VIN topics via a wildcard, per-vehicle 10 s tumbling aggregates
//!       proof: as w2 over 10k devices
//!   w5  chargers: SESSIONWINDOW(ss, 10, 2) per charger over 2k charger topics
//!       proof: as w2 over 2k devices
//!
//! Before each measured step a warm-up burst (devices "warm_*", separate tag)
//! must show up in the rule's source counter, proving the MQTT subscription is
//! live, so QoS0 messages are never published into a not-yet-subscribed rule.
//!
//! Run under: taskset -c 0,1,4-7 iotrunner   (keep the runner off the engine/broker/generator cores)
//! Env: IOT_HOME (benchmark dir, default cwd), IOT_RATES (default 5000,20000,50000,100000),
//!      IOT_DUR (30), IOT_REPS (2), IOT_WORKLOADS (w1,w2), IOT_ENGINES (rek,eku-def,eku-tun;
//!      also telegraf, rpconnect), IOT_REK_IMAGE (rekuiper-bench:local), IOT_EKU_IMAGE
//!      (lfedge/ekuiper:2.4.1), IOT_OUT (perf-iot-mqtt.json), IOT_LATDIR (sink dir on a local
//!      filesystem, default <tmp>/iot_lat), IOT_ENGINE_CPUSET (2), IOT_BROKER_CPUSET (8,9),
//!      IOT_GEN_CPUSET (10,11)

use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Benchmark home (IOT_HOME, default: current directory). Holds evidence/,
/// scripts/mosquitto/mosquitto.conf and scripts/mqttgen/mqttgen; resolved to an absolute path
/// because docker bind mounts need one.
struct Home;

impl std::fmt::Display for Home {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let base = std::env::var("IOT_HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
        let abs = fs::canonicalize(&base).unwrap_or(base);
        write!(f, "{}", abs.display())
    }
}

const DATA: Home = Home;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// Engine container limits: 1 CPU on a dedicated cpuset (IOT_ENGINE_CPUSET, default 2), 1 GB, no swap.
fn engine_pin() -> [&'static str; 4] {
    let cpuset: &'static str =
        Box::leak(format!("--cpuset-cpus={}", env_or("IOT_ENGINE_CPUSET", "2")).into_boxed_str());
    [cpuset, "--cpus=1", "--memory=1g", "--memory-swap=1g"]
}
const NET: &str = "rk-bench-net";
const BROKER: &str = "rk-bench-mqtt";
const BROKER_HOST_PORT: &str = "11883";
const TOPIC: &str = "bench/telemetry";
const DEVICES: u64 = 1000;
const CONNS: usize = 8;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Rekuiper,
    Ekuiper,
    /// Telegraf 1.40 agent: mqtt_consumer -> processors/aggregators -> file (no rule API).
    Telegraf,
    /// Redpanda Connect (Benthos): mqtt input -> Bloblang/window buffer -> file (no rule API).
    Connect,
}

struct Engine {
    key: &'static str,
    kind: Kind,
    image: String,
    name: &'static str,
    port: u16,
    rule_options: Option<Value>,
}

#[derive(Clone, Copy, PartialEq)]
enum Proof {
    /// Unique ids with the run tag == exact expected filtered count, 0 duplicates.
    ExactFilter,
    /// sum(n) over measured devices == messages sent, every device present.
    GroupSum,
    /// Rows (warm-up topics excluded) == messages sent, every device topic seen.
    RowCount,
}

struct Workload {
    key: &'static str,
    sql: &'static str,
    /// Stream DATASOURCE (may be a wildcard) and FORMAT.
    datasource: &'static str,
    format: &'static str,
    /// mqttgen topic (a `{dev}` placeholder gives one topic per device) and payload mode.
    gen_topic: &'static str,
    gen_mode: &'static str,
    devices: u64,
    proof: Proof,
    stable_s: u64,
    drain_max_s: u64,
}

const WORKLOADS: [Workload; 5] = [
    Workload {
        key: "w1",
        sql: "SELECT id, device, temp, speed * 3.6 AS speed_kmh FROM telem WHERE temp > 21.0",
        datasource: TOPIC,
        format: "json",
        gen_topic: TOPIC,
        gen_mode: "json",
        devices: DEVICES,
        proof: Proof::ExactFilter,
        stable_s: 3,
        drain_max_s: 60,
    },
    Workload {
        key: "w2",
        sql: "SELECT device, count(*) AS n, avg(temp) AS avg_temp, max(speed) AS max_speed FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10)",
        datasource: TOPIC,
        format: "json",
        gen_topic: TOPIC,
        gen_mode: "json",
        devices: DEVICES,
        proof: Proof::GroupSum,
        stable_s: 13,
        drain_max_s: 90,
    },
    Workload {
        key: "w3",
        sql: "SELECT meta(topic) AS topic, self AS state FROM telem",
        datasource: "bench/esphome/+/sensor/temperature/state",
        format: "binary",
        gen_topic: "bench/esphome/{dev}/sensor/temperature/state",
        gen_mode: "esphome",
        devices: 10_000,
        proof: Proof::RowCount,
        stable_s: 3,
        drain_max_s: 60,
    },
    Workload {
        key: "w4",
        sql: "SELECT device, count(*) AS n, avg(speed) AS avg_speed, max(temp) AS max_temp FROM telem GROUP BY device, TUMBLINGWINDOW(ss, 10)",
        datasource: "bench/vehicles/+/telemetry",
        format: "json",
        gen_topic: "bench/vehicles/{dev}/telemetry",
        gen_mode: "json",
        devices: 10_000,
        proof: Proof::GroupSum,
        stable_s: 13,
        drain_max_s: 90,
    },
    Workload {
        key: "w5",
        sql: "SELECT device, count(*) AS n, max(speed) AS max_power FROM telem GROUP BY device, SESSIONWINDOW(ss, 10, 2)",
        datasource: "bench/chargers/+/session",
        format: "json",
        gen_topic: "bench/chargers/{dev}/session",
        gen_mode: "json",
        devices: 2_000,
        proof: Proof::GroupSum,
        stable_s: 13,
        drain_max_s: 90,
    },
];

// ------------------------------------------------------------------ helpers

fn run(cmd: &str, args: &[&str]) -> (i32, String) {
    match Command::new(cmd).args(args).output() {
        Ok(o) => {
            let mut s = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if !o.status.success() {
                s.push_str(String::from_utf8_lossy(&o.stderr).trim());
            }
            (o.status.code().unwrap_or(-1), s)
        }
        Err(e) => (-1, format!("spawn error: {e}")),
    }
}

/// Returns (http status, body).
fn http(method: &str, url: &str, body: Option<&Value>) -> (u16, String) {
    let body_s = body.map(|v| v.to_string()).unwrap_or_default();
    let mut args: Vec<&str> = vec!["-s", "-w", "\n%{http_code}", "--max-time", "15", "-X", method];
    if body.is_some() {
        args.extend(["-H", "Content-Type: application/json", "--data", body_s.as_str()]);
    }
    args.push(url);
    let (_, out) = run("curl", &args);
    match out.rsplit_once('\n') {
        Some((b, code)) => (code.trim().parse().unwrap_or(0), b.to_string()),
        None => (out.trim().parse().unwrap_or(0), String::new()),
    }
}

fn ok(code: u16) -> bool {
    (200..300).contains(&code)
}

fn sleep_s(s: u64) {
    thread::sleep(Duration::from_secs(s));
}

fn round1(x: f64) -> f64 {
    (x * 10.0).round() / 10.0
}

fn clock() -> String {
    run("date", &["+%H:%M:%S"]).1
}

fn median(mut v: Vec<f64>) -> Value {
    if v.is_empty() {
        return Value::Null;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    json!(v[v.len() / 2])
}

// ---------------------------------------------------------------- cgroup CPU

fn cg_dir(name: &str) -> Option<String> {
    let (_, cid) = run("docker", &["inspect", name, "--format", "{{.Id}}"]);
    [
        format!("/sys/fs/cgroup/system.slice/docker-{cid}.scope"),
        format!("/sys/fs/cgroup/docker/{cid}"),
    ]
    .into_iter()
    .find(|d| Path::new(&format!("{d}/cpu.stat")).is_file())
}

fn read_usage(dir: &str) -> Option<u64> {
    fs::read_to_string(format!("{dir}/cpu.stat"))
        .ok()?
        .lines()
        .find_map(|l| l.strip_prefix("usage_usec ").and_then(|v| v.trim().parse().ok()))
}

fn read_rss_mb(dir: &str) -> f64 {
    fs::read_to_string(format!("{dir}/memory.current"))
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .map(|b| b / 1_048_576.0)
        .unwrap_or(0.0)
}

/// Anonymous memory (heap and stacks, excluding page cache) from cgroup memory.stat.
fn read_anon_mb(dir: &str) -> f64 {
    fs::read_to_string(format!("{dir}/memory.stat"))
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("anon ").and_then(|v| v.trim().parse::<f64>().ok()))
        })
        .map(|b| b / 1_048_576.0)
        .unwrap_or(0.0)
}

struct Cg {
    found: bool,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<(Vec<f64>, Vec<f64>, Vec<f64>)>>,
}

impl Cg {
    fn start(name: &str) -> Cg {
        let dir = cg_dir(name);
        let stop = Arc::new(AtomicBool::new(false));
        let handle = dir.clone().map(|d| {
            let stop = stop.clone();
            thread::spawn(move || {
                let (mut cpu, mut rss, mut anon) = (Vec::new(), Vec::new(), Vec::new());
                let mut last = read_usage(&d);
                let mut tick = Instant::now();
                while !stop.load(Ordering::SeqCst) {
                    sleep_s(1);
                    let now = read_usage(&d);
                    let elapsed = tick.elapsed().as_secs_f64();
                    tick = Instant::now();
                    if let (Some(u), Some(l)) = (now, last) {
                        cpu.push(round1(u.saturating_sub(l) as f64 / 10_000.0 / elapsed));
                    }
                    last = now;
                    rss.push(round1(read_rss_mb(&d)));
                    anon.push(round1(read_anon_mb(&d)));
                }
                (cpu, rss, anon)
            })
        });
        Cg { found: dir.is_some(), stop, handle }
    }

    fn finish(self, send_secs: usize) -> Value {
        self.stop.store(true, Ordering::SeqCst);
        let (cpu, rss, anon) = self.handle.map(|h| h.join().unwrap_or_default()).unwrap_or_default();
        let mean = |v: &[f64]| if v.is_empty() { 0.0 } else { round1(v.iter().sum::<f64>() / v.len() as f64) };
        let send_window = &cpu[..cpu.len().min(send_secs)];
        json!({
            "cpu_source": if self.found { "cgroup-v2-host" } else { "cgroup-UNAVAILABLE" },
            "cpu_mean_send_window": mean(send_window),
            "cpu_mean_all": mean(&cpu),
            "cpu_max": cpu.iter().cloned().fold(0.0, f64::max),
            "peak_rss_mb": rss.iter().cloned().fold(0.0, f64::max),
            "peak_anon_mb": anon.iter().cloned().fold(0.0, f64::max),
            "cpu_per_sec": cpu,
        })
    }
}

// ------------------------------------------------------------------ health

fn health() -> Value {
    let (_, ps) = run("bash", &["-c", "ps -eo pcpu,args --sort=-pcpu | head -9 | cut -c1-110"]);
    const OURS: [&str; 14] = [
        "iotrunner", "mqttgen", "mosquitto", "ps -eo", "head", "cut", "dockerd", "containerd", "kuiperd",
        "docker ", "bash -c", "kuiper", "telegraf", "redpanda-connect",
    ];
    let offenders: Vec<String> = ps
        .lines()
        .skip(1)
        .filter_map(|line| {
            let t = line.trim();
            let (pcpu, args) = t.split_once(' ')?;
            let pcpu: f64 = pcpu.parse().ok()?;
            (pcpu > 10.0 && !OURS.iter().any(|o| args.contains(o))).then(|| t.to_string())
        })
        .collect();
    let ps_exe = "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe";
    let (_, host) = run(
        ps_exe,
        &[
            "-NoProfile",
            "-Command",
            "$c=(Get-CimInstance Win32_Processor | Measure-Object -Property LoadPercentage -Average).Average; $m=[math]::Round((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory/1MB,2); \"$c $m\"",
        ],
    );
    let mut parts = host.split_whitespace();
    let host_cpu: Option<f64> = parts.next().and_then(|v| v.parse().ok());
    let host_free_gb: Option<f64> = parts.next().and_then(|v| v.parse().ok());
    json!({
        "t": clock(),
        "loadavg": fs::read_to_string("/proc/loadavg").unwrap_or_default().trim(),
        "ps_top": ps,
        "wsl_offenders": offenders,
        "windows_host_cpu_pct": host_cpu,
        "windows_free_ram_gb": host_free_gb,
        // Windows host CPU is only known under WSL2; on plain Linux only busy processes count.
        "contaminated": !offenders.is_empty() || host_cpu.is_some_and(|c| c > 30.0),
    })
}

// ------------------------------------------------------------------ infra

fn start_broker() -> Result<(), String> {
    let _ = run("docker", &["network", "create", NET]);
    let _ = run("docker", &["container", "remove", "-f", BROKER]);
    let conf = format!("{DATA}/scripts/mosquitto/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro");
    let port = format!("{BROKER_HOST_PORT}:1883");
    let cpus = format!("--cpuset-cpus={}", env_or("IOT_BROKER_CPUSET", "8,9"));
    let (code, out) = run(
        "docker",
        &[
            "run", "-d", "--name", BROKER, "--network", NET, cpus.as_str(), "--memory=512m",
            "-p", &port, "-v", &conf, "eclipse-mosquitto:2",
        ],
    );
    if code != 0 {
        return Err(format!("broker start failed: {out}"));
    }
    sleep_s(2);
    Ok(())
}

fn start_engine(e: &Engine, latdir: &str) -> Result<Value, String> {
    let _ = run("docker", &["container", "remove", "-f", e.name]);
    fs::create_dir_all(latdir).map_err(|x| x.to_string())?;
    if per_step_container(e) {
        return Ok(json!({"image": e.image, "image_id": run("docker", &["inspect", &e.image, "--format", "{{.Id}}"]).1,
                         "container": "started per workload with that workload's pipeline config", "started": clock()}));
    }
    let vol = format!("{latdir}:/lat");
    let port = format!("{}:9081", e.port);
    let mut args = vec!["run", "-d", "--name", e.name, "--network", NET];
    args.extend(engine_pin());
    args.extend(["-v", &vol, "-p", &port]);
    match e.kind {
        Kind::Rekuiper => args.extend(["-e", "TOKIO_WORKER_THREADS=1", "-e", "RUST_LOG=warn"]),
        Kind::Ekuiper => args.extend([
            "-e", "GOMAXPROCS=1", "-e", "GOMEMLIMIT=900MiB", "-e", "KUIPER__BASIC__FILELOG=false",
        ]),
        Kind::Telegraf | Kind::Connect => {}
    }
    args.push(&e.image);
    let (code, out) = run("docker", &args);
    if code != 0 {
        return Err(format!("docker run failed: {out}"));
    }
    let base = format!("http://127.0.0.1:{}", e.port);
    let deadline = Instant::now() + Duration::from_secs(90);
    while http("GET", &format!("{base}/ping"), None).0 != 200 {
        if Instant::now() > deadline {
            return Err("engine not ready in 90 s".into());
        }
        thread::sleep(Duration::from_millis(300));
    }
    let (code, body) = http(
        "PUT",
        &format!("{base}/metadata/sources/mqtt/confKeys/bench"),
        Some(&json!({"server": format!("tcp://{BROKER}:1883")})),
    );
    if !ok(code) {
        return Err(format!("confKey PUT http {code}: {body}"));
    }
    let (_, limits) = run(
        "docker",
        &["inspect", e.name, "--format", "{{.HostConfig.NanoCpus}} {{.HostConfig.CpusetCpus}} {{.HostConfig.Memory}}"],
    );
    Ok(json!({"image": e.image, "image_id": run("docker", &["inspect", &e.image, "--format", "{{.Id}}"]).1,
              "limits": limits, "started": clock()}))
}

fn setup_rule(e: &Engine, w: &Workload, latdir: &str) -> Result<(), String> {
    if per_step_container(e) {
        return start_pipeline(e, w, latdir);
    }
    let base = format!("http://127.0.0.1:{}", e.port);
    http("DELETE", &format!("{base}/rules/rbench"), None);
    http("DELETE", &format!("{base}/streams/telem"), None);
    sleep_s(1);
    let stream = json!({"sql": format!(
        "CREATE STREAM telem () WITH (DATASOURCE=\"{}\", FORMAT=\"{}\", CONF_KEY=\"bench\", TYPE=\"mqtt\")",
        w.datasource, w.format)});
    let (code, body) = http("POST", &format!("{base}/streams"), Some(&stream));
    if !ok(code) {
        return Err(format!("create stream http {code}: {body}"));
    }
    let mut rule = json!({"id": "rbench", "sql": w.sql, "actions": [{"file": {"path": "/lat/rows.jsonl"}}]});
    if let Some(opts) = &e.rule_options {
        rule["options"] = opts.clone();
    }
    let (code, body) = http("POST", &format!("{base}/rules"), Some(&rule));
    if !ok(code) {
        return Err(format!("create rule http {code}: {body}"));
    }
    Ok(())
}

/// Telegraf and Redpanda Connect have no rule API: each workload runs its own pipeline
/// config in a fresh container (same pinning, network and sink mount as the rule engines).
fn per_step_container(e: &Engine) -> bool {
    matches!(e.kind, Kind::Telegraf | Kind::Connect)
}

/// Telegraf pipeline equivalent to the workload SQL (None: not expressible, e.g. session windows).
/// Documented settings, each chosen from Telegraf's own docs/source:
/// - outputs.file use_batch_format: one write per flushed batch (per-metric mode issues one
///   unbuffered write call per metric, which caps throughput on any filesystem);
/// - metric_batch_size 10000 with max_undelivered_messages 10000 (the MQTT input reads more only
///   after written batches are acknowledged); metric_buffer_limit 100000; flush_interval 1s;
/// - basicstats grace 10s: a metric timestamped before the current period is otherwise dropped
///   at every period boundary (running_aggregator.go), so late metrics roll into the next period;
/// - topic_tag "" where the topic is unused (w3 keeps it for meta(topic)).
fn telegraf_conf(w: &Workload) -> Option<String> {
    let parser = match w.key {
        "w1" => "  topic_tag = \"\"\n  data_format = \"json\"\n  tag_keys = [\"device\"]\n  json_string_fields = [\"id\"]\n",
        "w2" | "w4" => "  topic_tag = \"\"\n  data_format = \"json\"\n  tag_keys = [\"device\"]\n  fieldinclude = [\"temp\", \"speed\"]\n",
        "w3" => "  data_format = \"value\"\n  data_type = \"string\"\n",
        _ => return None,
    };
    let stage = match w.key {
        "w1" => "[[processors.starlark]]\n  source = '''\ndef apply(metric):\n    if metric.fields.get(\"temp\", 0.0) > 21.0:\n        metric.fields[\"speed_kmh\"] = metric.fields[\"speed\"] * 3.6\n        return metric\n    return None\n'''\n",
        "w2" | "w4" => "[[aggregators.basicstats]]\n  period = \"10s\"\n  grace = \"10s\"\n  drop_original = true\n  stats = [\"count\", \"mean\", \"max\"]\n",
        _ => "",
    };
    Some(format!(
        "[agent]\n  omit_hostname = true\n  flush_interval = \"1s\"\n  flush_jitter = \"0s\"\n  metric_batch_size = 10000\n  metric_buffer_limit = 100000\n\n\
         [[outputs.file]]\n  files = [\"/lat/rows.jsonl\"]\n  data_format = \"json\"\n  use_batch_format = true\n\n\
         [[inputs.mqtt_consumer]]\n  servers = [\"tcp://{BROKER}:1883\"]\n  topics = [\"{}\"]\n  qos = 0\n  client_id = \"telegraf-bench\"\n  max_undelivered_messages = 10000\n{parser}\n{stage}",
        w.datasource
    ))
}

/// Redpanda Connect pipeline equivalent to the workload SQL (None: not expressible).
fn connect_conf(w: &Workload) -> Option<String> {
    let body = match w.key {
        "w1" => "pipeline:\n  processors:\n    - mapping: |\n        root = if this.temp > 21.0 { {\"id\": this.id, \"device\": this.device, \"temp\": this.temp, \"speed_kmh\": this.speed * 3.6} } else { deleted() }\n".to_string(),
        "w2" | "w4" => {
            let (avg, max) = if w.key == "w2" { ("temp", "speed") } else { ("speed", "temp") };
            format!(
                "buffer:\n  system_window:\n    timestamp_mapping: root = now()\n    size: 10s\npipeline:\n  processors:\n    - group_by_value:\n        value: '${{! json(\"device\") }}'\n    - mapping: |\n        root = if batch_index() == 0 {{\n          {{\n            \"device\": this.device,\n            \"n\": json(\"device\").from_all().length(),\n            \"avg_{avg}\": json(\"{avg}\").from_all().sum() / json(\"{avg}\").from_all().length(),\n            \"max_{max}\": json(\"{max}\").from_all().max()\n          }}\n        }} else {{ deleted() }}\n"
            )
        }
        "w3" => "pipeline:\n  processors:\n    - mapping: |\n        root.topic = @mqtt_topic\n        root.state = content().string()\n".to_string(),
        _ => return None,
    };
    Some(format!(
        "http:\n  address: 0.0.0.0:4195\nlogger:\n  level: WARN\ninput:\n  mqtt:\n    urls: [\"tcp://{BROKER}:1883\"]\n    topics: [\"{}\"]\n    client_id: rpconnect-bench\n    qos: 0\n{body}output:\n  file:\n    path: /lat/rows.jsonl\n    codec: lines\n",
        w.datasource
    ))
}

/// Start a Telegraf / Redpanda Connect container running this workload's pipeline.
fn start_pipeline(e: &Engine, w: &Workload, latdir: &str) -> Result<(), String> {
    let _ = run("docker", &["container", "remove", "-f", e.name]);
    let (conf, file, args): (Option<String>, &str, [&str; 2]) = match e.kind {
        Kind::Telegraf => (telegraf_conf(w), "telegraf.conf", ["--config", "/lat/telegraf.conf"]),
        _ => (connect_conf(w), "connect.yaml", ["-c", "/lat/connect.yaml"]),
    };
    let conf = conf.ok_or_else(|| format!("{} has no pipeline for {}", e.key, w.key))?;
    fs::write(format!("{latdir}/{file}"), conf).map_err(|x| x.to_string())?;
    // The sink file must be gone before the engine opens it: an unlinked file keeps receiving writes.
    let _ = fs::remove_file(format!("{latdir}/rows.jsonl"));
    let vol = format!("{latdir}:/lat");
    let port = format!("{}:4195", e.port);
    let mut docker = vec!["run", "-d", "--name", e.name, "--network", NET];
    docker.extend(engine_pin());
    // Same Go memory limit as the eKuiper containers (both engines are Go).
    docker.extend(["-e", "GOMEMLIMIT=900MiB", "-v", vol.as_str()]);
    if e.kind == Kind::Connect {
        docker.extend(["-p", port.as_str()]);
    }
    docker.push(e.image.as_str());
    docker.extend(args);
    let (code, out) = run("docker", &docker);
    if code != 0 {
        return Err(format!("docker run failed: {out}"));
    }
    let logs = || run("bash", &["-c", &format!("docker logs --tail 30 {} 2>&1", e.name)]).1;
    if e.kind == Kind::Connect {
        let deadline = Instant::now() + Duration::from_secs(30);
        while http("GET", &format!("http://127.0.0.1:{}/ready", e.port), None).0 != 200 {
            if Instant::now() > deadline {
                return Err(format!("redpanda connect not ready: {}", logs()));
            }
            thread::sleep(Duration::from_millis(300));
        }
    } else {
        sleep_s(3);
        if run("docker", &["inspect", e.name, "--format", "{{.State.Running}}"]).1.trim() != "true" {
            return Err(format!("telegraf exited: {}", logs()));
        }
    }
    Ok(())
}

/// End a measured step: DELETE the rule, or SIGTERM the pipeline container (both
/// Telegraf and Redpanda Connect flush their outputs on shutdown).
fn stop_pipeline(e: &Engine) {
    if per_step_container(e) {
        let _ = run("docker", &["stop", "-t", "20", e.name]);
    } else {
        http("DELETE", &format!("http://127.0.0.1:{}/rules/rbench", e.port), None);
    }
}

/// Warm-up proof that the subscription is live before measuring.
fn warm_ok(e: &Engine, rows: &str, before: u64) -> bool {
    match e.kind {
        // No per-pipeline ingest counter is polled for Telegraf: warm-up rows must reach the sink.
        Kind::Telegraf => fs::read_to_string(rows).map(|t| t.contains("warm_")).unwrap_or(false),
        _ => source_in(e) >= before + 1000,
    }
}

/// Telegraf's JSON serializer nests values as {"fields":{..},"tags":{..}}; flatten to the
/// engine-neutral row shape, using basicstats' `temp_count` as the per-device count `n`.
fn flatten_metric(row: Value) -> Value {
    let Some(fields) = row.get("fields").and_then(Value::as_object) else { return row };
    let mut flat = serde_json::Map::new();
    if let Some(tags) = row.get("tags").and_then(Value::as_object) {
        flat.extend(tags.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
    flat.extend(fields.iter().map(|(k, v)| (k.clone(), v.clone())));
    if !flat.contains_key("n") {
        if let Some(count) = flat.get("temp_count").cloned() {
            flat.insert("n".into(), count);
        }
    }
    Value::Object(flat)
}

fn source_in(e: &Engine) -> u64 {
    match e.kind {
        Kind::Telegraf => return 0,
        Kind::Connect => {
            let (_, body) = http("GET", &format!("http://127.0.0.1:{}/metrics", e.port), None);
            return body
                .lines()
                .filter(|l| l.starts_with("input_received"))
                .filter_map(|l| l.rsplit(' ').next()?.trim().parse::<f64>().ok())
                .sum::<f64>() as u64;
        }
        Kind::Rekuiper | Kind::Ekuiper => {}
    }
    let (_, body) = http("GET", &format!("http://127.0.0.1:{}/rules/rbench/status", e.port), None);
    let Ok(Value::Object(map)) = serde_json::from_str::<Value>(&body) else { return 0 };
    map.iter()
        .filter(|(k, _)| {
            k.as_str() == "sourceRecordsInTotal"
                || (k.starts_with("source_") && k.ends_with("_records_in_total"))
        })
        .filter_map(|(_, v)| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)))
        .max()
        .unwrap_or(0)
}

#[allow(clippy::too_many_arguments)]
fn mqttgen(w: &Workload, rate: u64, secs: u64, conns: usize, tag: &str, devprefix: &str, out: &str) -> Option<Value> {
    let gen = format!("{DATA}/scripts/mqttgen/mqttgen");
    let gen_cpus = env_or("IOT_GEN_CPUSET", "10,11");
    let status = Command::new("taskset")
        .args([
            "-c", gen_cpus.as_str(), &gen, "--host", "127.0.0.1", "--port", BROKER_HOST_PORT, "--topic", w.gen_topic,
            "--mode", w.gen_mode, "--rate", &rate.to_string(), "--secs", &secs.to_string(),
            "--conns", &conns.to_string(), "--devices", &w.devices.to_string(), "--tag", tag,
            "--devprefix", devprefix, "--out", out,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()?;
    if !status.success() {
        return None;
    }
    serde_json::from_str(&fs::read_to_string(out).ok()?).ok()
}

/// Exact number of w1 rows expected: mqttgen emits temp = 20.0 + (k % 150) / 10,
/// so `temp > 21.0` keeps k % 150 > 10 (139 of every 150) per connection.
fn expected_w1(rate: u64, secs: u64, conns: usize) -> u64 {
    let per_conn = (rate as f64 / conns as f64 * secs as f64).round() as u64;
    let one = (per_conn / 150) * 139 + (per_conn % 150).saturating_sub(11);
    one * conns as u64
}

fn count_w1(path: &str, tag: &str) -> (i64, i64) {
    let Ok(text) = fs::read_to_string(path) else { return (-1, -1) };
    let pat = format!("\"id\":\"{tag}_");
    let (mut total, mut ids) = (0i64, HashSet::new());
    let mut rest = text.as_str();
    while let Some(i) = rest.find(&pat) {
        let after = &rest[i + pat.len()..];
        let Some(j) = after.find('"') else { break };
        total += 1;
        ids.insert(after[..j].to_string());
        rest = &after[j + 1..];
    }
    (total, ids.len() as i64)
}

/// Returns (sum of n over measured devices, distinct measured devices, unparsable lines).
fn count_w2(path: &str) -> (u64, usize, usize) {
    let Ok(text) = fs::read_to_string(path) else { return (0, 0, 0) };
    let (mut sum, mut devices, mut bad) = (0u64, HashSet::new(), 0usize);
    // Stream JSON values: one per line, arrays, or Telegraf batch documents {"metrics": [...]}.
    for v in serde_json::Deserializer::from_str(&text).into_iter::<Value>() {
        let Ok(v) = v else {
            bad += 1;
            break;
        };
        let rows: Vec<Value> = match v {
            Value::Array(a) => a,
            Value::Object(mut o) if o.get("metrics").is_some_and(Value::is_array) => match o.remove("metrics") {
                Some(Value::Array(a)) => a,
                _ => Vec::new(),
            },
            other => vec![other],
        };
        for row in rows {
            let row = flatten_metric(row);
            let Some(dev) = row.get("device").and_then(Value::as_str) else { continue };
            if dev.starts_with("warm_") {
                continue;
            }
            let n = row.get("n").and_then(|x| x.as_u64().or_else(|| x.as_f64().map(|f| f as u64)));
            if let Some(n) = n {
                sum += n;
                devices.insert(dev.to_string());
            }
        }
    }
    (sum, devices.len(), bad)
}

/// Returns (rows whose topic is not a warm-up topic, distinct measured topics, unparsable lines).
fn count_rows(path: &str) -> (u64, usize, usize) {
    let Ok(text) = fs::read_to_string(path) else { return (0, 0, 0) };
    let (mut rows_n, mut topics, mut bad) = (0u64, HashSet::new(), 0usize);
    // Stream JSON values: one per line, arrays, or Telegraf batch documents {"metrics": [...]}.
    for v in serde_json::Deserializer::from_str(&text).into_iter::<Value>() {
        let Ok(v) = v else {
            bad += 1;
            break;
        };
        let rows: Vec<Value> = match v {
            Value::Array(a) => a,
            Value::Object(mut o) if o.get("metrics").is_some_and(Value::is_array) => match o.remove("metrics") {
                Some(Value::Array(a)) => a,
                _ => Vec::new(),
            },
            other => vec![other],
        };
        for row in rows {
            let row = flatten_metric(row);
            let Some(topic) = row.get("topic").and_then(Value::as_str) else { continue };
            if topic.contains("/warm_") {
                continue;
            }
            rows_n += 1;
            topics.insert(topic.to_string());
        }
    }
    (rows_n, topics.len(), bad)
}

fn step(e: &Engine, w: &Workload, rate: u64, secs: u64, rep: usize, latdir: &str) -> Value {
    let rows = format!("{latdir}/rows.jsonl");
    if !per_step_container(e) {
        // Per-step pipelines remove the sink file themselves before the engine opens it.
        let _ = fs::remove_file(&rows);
    }
    if let Err(err) = setup_rule(e, w, latdir) {
        return json!({"workload": w.key, "rate": rate, "error": err, "complete": false});
    }
    if !per_step_container(e) {
        let _ = fs::remove_file(&rows);
    }
    sleep_s(2);
    // Warm-up proves the subscription is live before measuring (QoS0 is not replayed).
    let warm_tag = format!("WARM{rep}{}{rate}", w.key);
    let warm_out = format!("{DATA}/evidence/iot-warm-{}-{warm_tag}.json", e.key);
    let before = source_in(e);
    let _ = mqttgen(w, 500, 2, 1, &warm_tag, "warm_", &warm_out);
    // Sink-based warm-up proof (Telegraf windows) needs a full aggregation period plus flush.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut subscribed = false;
    while Instant::now() < deadline {
        if warm_ok(e, &rows, before) {
            subscribed = true;
            break;
        }
        thread::sleep(Duration::from_millis(500));
    }
    if !subscribed {
        return json!({"workload": w.key, "rate": rate, "error": "subscription not confirmed (warm-up not counted by rule status)",
                      "source_in_after_warmup": source_in(e), "complete": false});
    }
    sleep_s(1);

    let tag = format!("{}{}{}R{rate}", e.key.to_uppercase().replace('-', ""), rep, w.key.to_uppercase());
    let gen_out = format!("{DATA}/evidence/iot-gen-{tag}.json");
    let in_before = source_in(e);
    let cg = Cg::start(e.name);
    let t_send = Instant::now();
    let gen = mqttgen(w, rate, secs, CONNS, &tag, "dev_", &gen_out);
    let send_s = t_send.elapsed().as_secs_f64();

    let deadline = Instant::now() + Duration::from_secs(w.drain_max_s);
    let (mut last, mut stable, mut last_growth) = (-1i64, 0u64, Instant::now());
    while Instant::now() < deadline {
        sleep_s(1);
        let size = fs::metadata(&rows).map(|m| m.len() as i64).unwrap_or(-1);
        if size == last && size >= 0 {
            stable += 1;
            if stable >= w.stable_s {
                break;
            }
        } else {
            stable = 0;
            last = size;
            last_growth = Instant::now();
        }
    }
    let lag_after_send_s = last_growth.saturating_duration_since(t_send).as_secs_f64() - send_s;
    let cpu = cg.finish(secs as usize);
    let in_after = source_in(e);
    // Graceful stop flushes buffered file sinks (symmetric for every engine).
    stop_pipeline(e);
    sleep_s(5);

    let Some(gen) = gen else {
        return json!({"workload": w.key, "rate": rate, "error": "mqttgen failed", "complete": false, "cpu": cpu});
    };
    let sent = gen.get("sent_messages").and_then(Value::as_u64).unwrap_or(0);
    let gen_ok = gen.get("on_schedule").and_then(Value::as_bool).unwrap_or(false);
    let mut out = json!({
        "workload": w.key, "rate": rate, "secs": secs, "rep": rep, "tag": tag,
        "generator_on_schedule": gen_ok, "sent": sent,
        "source_in_delta": in_after.saturating_sub(in_before),
        "lag_after_send_s": (lag_after_send_s * 10.0).round() / 10.0,
        "cpu": cpu, "health_t": clock(),
    });
    let loss_of = |got: u64| {
        if sent > 0 {
            round1((sent as f64 - got as f64) / sent as f64 * 1000.0) / 10.0
        } else {
            100.0
        }
    };
    let complete = match w.proof {
        Proof::ExactFilter => {
            let expected = expected_w1(rate, secs, CONNS);
            let (rows_n, unique) = count_w1(&rows, &tag);
            out["expected_rows"] = json!(expected);
            out["file_rows"] = json!(rows_n);
            out["unique_ids"] = json!(unique);
            out["duplicates"] = json!(rows_n - unique);
            out["loss_pct"] = json!(if expected > 0 { round1((expected as f64 - unique as f64) / expected as f64 * 1000.0) / 10.0 } else { 100.0 });
            gen_ok && unique == expected as i64 && rows_n == unique
        }
        Proof::GroupSum => {
            let (sum_n, devices, bad) = count_w2(&rows);
            out["sum_n"] = json!(sum_n);
            out["devices_seen"] = json!(devices);
            out["unparsable_lines"] = json!(bad);
            out["loss_pct"] = json!(loss_of(sum_n));
            gen_ok && sum_n == sent && devices as u64 == w.devices
        }
        Proof::RowCount => {
            let (rows_n, topics, bad) = count_rows(&rows);
            out["file_rows"] = json!(rows_n);
            out["topics_seen"] = json!(topics);
            out["unparsable_lines"] = json!(bad);
            out["loss_pct"] = json!(loss_of(rows_n));
            gen_ok && rows_n == sent && topics as u64 == w.devices
        }
    };
    let lag_limit = if w.proof == Proof::GroupSum { 15.0 } else { 5.0 };
    out["complete"] = json!(complete);
    out["sustained"] = json!(complete && lag_after_send_s <= lag_limit);
    out
}

fn save(path: &str, rec: &Value) {
    let _ = fs::write(path, serde_json::to_string_pretty(rec).unwrap_or_default());
}

fn main() {
    if run("pgrep", &["-f", "abrunner"]).0 == 0 {
        eprintln!("ABORT: abrunner (attribution A/B) is running; refusing to contaminate it");
        std::process::exit(3);
    }
    let rates: Vec<u64> = std::env::var("IOT_RATES")
        .unwrap_or_else(|_| "5000,20000,50000,100000".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let secs: u64 = std::env::var("IOT_DUR").ok().and_then(|v| v.parse().ok()).unwrap_or(30);
    let reps: usize = std::env::var("IOT_REPS").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
    let wanted: Vec<String> = std::env::var("IOT_WORKLOADS")
        .unwrap_or_else(|_| "w1,w2".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let rek_image = std::env::var("IOT_REK_IMAGE").unwrap_or_else(|_| "rekuiper-bench:local".into());
    let eku_image = std::env::var("IOT_EKU_IMAGE").unwrap_or_else(|_| "lfedge/ekuiper:2.4.1".into());
    let out_path = format!(
        "{DATA}/evidence/{}",
        std::env::var("IOT_OUT").unwrap_or_else(|_| "perf-iot-mqtt.json".into())
    );
    // Sink on WSL-local ext4. A Windows-drive bind mount makes every write call slow, which
    // penalises engines that write per message (Telegraf, Redpanda Connect) far more than engines
    // that buffer writes. World-writable so each engine's container user can create the sink file.
    let latdir = std::env::var("IOT_LATDIR")
        .unwrap_or_else(|_| std::env::temp_dir().join("iot_lat").to_string_lossy().into_owned());
    let _ = fs::create_dir_all(&latdir);
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&latdir, fs::Permissions::from_mode(0o777));
    }

    let engines = [
        Engine { key: "rek", kind: Kind::Rekuiper, image: rek_image, name: "rk-bench-rek-iot", port: 19082, rule_options: None },
        Engine { key: "eku-def", kind: Kind::Ekuiper, image: eku_image.clone(), name: "rk-bench-eku-iot", port: 19081, rule_options: None },
        Engine {
            key: "eku-tun", kind: Kind::Ekuiper, image: eku_image, name: "rk-bench-eku-iot", port: 19081,
            rule_options: Some(json!({"bufferLength": 131072, "concurrency": 1})),
        },
        Engine {
            key: "telegraf", kind: Kind::Telegraf, image: "telegraf:1.40.0-alpine".into(), name: "rk-bench-telegraf-iot",
            port: 0, rule_options: None,
        },
        Engine {
            key: "rpconnect", kind: Kind::Connect, image: "redpandadata/connect:4.109.0".into(), name: "rk-bench-rpc-iot",
            port: 19083, rule_options: None,
        },
    ];
    // IOT_ENGINES selects engines by key (default: all three).
    let wanted_engines: Vec<String> = std::env::var("IOT_ENGINES")
        .unwrap_or_else(|_| "rek,eku-def,eku-tun".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let mut rec = json!({
        "tool": "scripts/iotrunner (Rust) + scripts/mqttgen (Rust std-only) + eclipse-mosquitto:2",
        "engines": wanted_engines,
        "sink_dir": latdir.clone(),
        "memory_note": "peak_rss_mb = cgroup memory.current (includes page cache from writing the sink file); peak_anon_mb = cgroup anon memory (engine heap)",
        "topology": "broker cores 8,9 | mqttgen cores 10,11 | engine core 2 (cpuset, 1 CPU, 1 GB, no swap) | runner cores 0,1,4-7",
        "rates": rates, "secs": secs, "reps": reps, "workloads": wanted, "devices": DEVICES, "conns": CONNS,
        "started": clock(), "runs": [],
    });
    save(&out_path, &rec);
    if let Err(e) = start_broker() {
        println!("BROKER FAILED: {e}");
        rec["error"] = json!(e);
        save(&out_path, &rec);
        std::process::exit(4);
    }

    for rep in 1..=reps {
        for i in 0..engines.len() {
            let e = &engines[(i + rep - 1) % engines.len()];
            if !wanted_engines.iter().any(|k| k == e.key) {
                continue;
            }
            let slot = rec["runs"].as_array().map_or(0, Vec::len);
            rec["runs"].as_array_mut().expect("runs").push(json!({
                "engine": e.key, "rep": rep, "health_before": health(), "steps": [],
            }));
            save(&out_path, &rec);
            match start_engine(e, &latdir) {
                Ok(life) => {
                    rec["runs"][slot]["life"] = life;
                    for w in WORKLOADS.iter().filter(|w| wanted.iter().any(|k| k == w.key)) {
                        if per_step_container(e) && w.sql.contains("SESSIONWINDOW") {
                            println!("{} rep={} {} unsupported: no session window support", e.key, rep, w.key);
                            rec["runs"][slot]["steps"]
                                .as_array_mut()
                                .expect("steps")
                                .push(json!({"workload": w.key, "unsupported": "no session window support"}));
                            continue;
                        }
                        for &rate in &rates {
                            let s = step(e, w, rate, secs, rep, &latdir);
                            println!(
                                "{} rep={} {} rate={} complete={} sustained={} sent={} loss%={} lag_s={} cpu={} rss={} anon={} err={}",
                                e.key, rep, w.key, rate, s["complete"], s["sustained"], s["sent"], s["loss_pct"],
                                s["lag_after_send_s"], s["cpu"]["cpu_mean_send_window"], s["cpu"]["peak_rss_mb"],
                                s["cpu"]["peak_anon_mb"],
                                s.get("error").cloned().unwrap_or(Value::Null)
                            );
                            rec["runs"][slot]["steps"].as_array_mut().expect("steps").push(s);
                            save(&out_path, &rec);
                        }
                    }
                }
                Err(err) => {
                    println!("{} rep={} START FAILED: {err}", e.key, rep);
                    rec["runs"][slot]["error"] = json!(err);
                }
            }
            let _ = run("docker", &["container", "remove", "-f", e.name]);
            rec["runs"][slot]["health_after"] = health();
            save(&out_path, &rec);
        }
    }
    let _ = run("docker", &["container", "remove", "-f", BROKER]);

    let mut summary = serde_json::Map::new();
    for e in &engines {
        for w in WORKLOADS.iter() {
            for &rate in &rates {
                let (mut n, mut complete, mut sustained) = (0, 0, 0);
                let (mut cpu, mut rss, mut lag) = (Vec::new(), Vec::new(), Vec::new());
                for r in rec["runs"].as_array().into_iter().flatten().filter(|r| r["engine"] == e.key) {
                    for s in r["steps"].as_array().into_iter().flatten() {
                        if s["workload"] != w.key || s["rate"].as_u64() != Some(rate) {
                            continue;
                        }
                        n += 1;
                        complete += s["complete"].as_bool().unwrap_or(false) as u32;
                        sustained += s["sustained"].as_bool().unwrap_or(false) as u32;
                        if let Some(x) = s["cpu"]["cpu_mean_send_window"].as_f64() {
                            cpu.push(x);
                        }
                        if let Some(x) = s["cpu"]["peak_rss_mb"].as_f64() {
                            rss.push(x);
                        }
                        if let Some(x) = s["lag_after_send_s"].as_f64() {
                            lag.push(x);
                        }
                    }
                }
                summary.insert(
                    format!("{}/{}/{}", e.key, w.key, rate),
                    json!({"reps": n, "complete": complete, "sustained": sustained,
                           "cpu_median": median(cpu), "rss_median_mb": median(rss), "lag_median_s": median(lag)}),
                );
            }
        }
    }
    rec["summary"] = Value::Object(summary);
    rec["finished"] = json!(clock());
    save(&out_path, &rec);
    println!("SUMMARY {}", rec["summary"]);
    println!("IOT_DONE");
}
