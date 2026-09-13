//! Open-loop MQTT 3.1.1 QoS0 telemetry publisher (std only).
//!
//! Simulates many devices/vehicles publishing small JSON telemetry through a
//! few gateway connections. Each connection thread follows its own schedule
//! (message k of connection c is due at start + k / rate_per_conn) and writes
//! every due message of a 1 ms tick in one `write_all`, like a gateway
//! flushing a buffer. Sending never waits on the engine.
//!
//! Payload (--mode json): {"id":"<TAG>_<conn>_<seq>","device":"dev_<n>","temp":f,"speed":f,"ts":<scheduled epoch ms>}
//! Every id is unique, so loss and duplicates can be proven at the sink.
//! Payload (--mode esphome): the plain-text state, e.g. `21.4` (ESPHome MQTT
//! component style; count-based proof at the sink).
//! A `{dev}` placeholder in --topic becomes the device name, so each device
//! publishes on its own topic (e.g. esphome/{dev}/sensor/temperature/state).
//!
//! usage: mqttgen --host 127.0.0.1 --port 1883 --topic bench/telemetry --rate 50000
//!                --secs 60 --conns 8 --devices 1000 --tag T1 --out result.json [--mode json|esphome]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Args {
    host: String,
    port: u16,
    topic: String,
    mode: String,
    rate: f64,
    secs: u64,
    conns: usize,
    devices: u64,
    devprefix: String,
    tag: String,
    out: String,
}

fn parse_args() -> Args {
    let mut a = Args {
        host: "127.0.0.1".into(),
        port: 1883,
        topic: "bench/telemetry".into(),
        mode: "json".into(),
        rate: 10_000.0,
        secs: 20,
        conns: 4,
        devices: 1000,
        devprefix: "dev_".into(),
        tag: "MG".into(),
        out: "mqttgen.json".into(),
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i + 1 < argv.len() {
        let v = argv[i + 1].clone();
        match argv[i].as_str() {
            "--host" => a.host = v,
            "--port" => a.port = v.parse().expect("--port"),
            "--topic" => a.topic = v,
            "--mode" => a.mode = v,
            "--rate" => a.rate = v.parse().expect("--rate"),
            "--secs" => a.secs = v.parse().expect("--secs"),
            "--conns" => a.conns = v.parse().expect("--conns"),
            "--devices" => a.devices = v.parse().expect("--devices"),
            "--devprefix" => a.devprefix = v,
            "--tag" => a.tag = v,
            "--out" => a.out = v,
            other => panic!("unknown argument {other}"),
        }
        i += 2;
    }
    assert!(a.conns > 0 && a.rate > 0.0 && a.secs > 0, "rate, secs and conns must be positive");
    a
}

fn remaining_length(mut len: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
}

fn connect(host: &str, port: u16, client_id: &str) -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect((host, port))?;
    stream.set_nodelay(true)?;
    let mut body = Vec::new();
    body.extend_from_slice(&[0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x00]);
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    let mut pkt = vec![0x10];
    remaining_length(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    stream.write_all(&pkt)?;
    let mut ack = [0u8; 4];
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.read_exact(&mut ack)?;
    if ack[0] != 0x20 || ack[3] != 0 {
        return Err(std::io::Error::other(format!("CONNACK rejected: {ack:?}")));
    }
    Ok(stream)
}

fn push_publish(buf: &mut Vec<u8>, topic: &[u8], payload: &[u8]) {
    buf.push(0x30);
    remaining_length(2 + topic.len() + payload.len(), buf);
    buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    buf.extend_from_slice(topic);
    buf.extend_from_slice(payload);
}

struct ConnResult {
    sent: u64,
    per_sec: Vec<u64>,
    error: Option<String>,
    first_ns: Option<u128>,
    last_ns: Option<u128>,
}

fn epoch_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_millis()
}

fn main() {
    let args = Arc::new(parse_args());
    let per_conn_rate = args.rate / args.conns as f64;
    let barrier = Arc::new(Barrier::new(args.conns + 1));
    let start_holder = Arc::new(std::sync::OnceLock::<(Instant, u128)>::new());

    let handles: Vec<_> = (0..args.conns)
        .map(|c| {
            let args = args.clone();
            let barrier = barrier.clone();
            let start_holder = start_holder.clone();
            thread::spawn(move || {
                let mut res = ConnResult {
                    sent: 0,
                    per_sec: vec![0; args.secs as usize + 1],
                    error: None,
                    first_ns: None,
                    last_ns: None,
                };
                let mut stream =
                    match connect(&args.host, args.port, &format!("{}-gen-{c}", args.tag)) {
                        Ok(s) => s,
                        Err(e) => {
                            res.error = Some(format!("connect: {e}"));
                            barrier.wait();
                            return res;
                        }
                    };
                barrier.wait();
                let (t0, t0_ms) = *start_holder.get().expect("start set");
                let template = args.topic.split_once("{dev}");
                let esphome = args.mode == "esphome";
                let mut topic_buf: Vec<u8> = Vec::with_capacity(args.topic.len() + 16);
                let total = (per_conn_rate * args.secs as f64).round() as u64;
                let mut buf = Vec::with_capacity(1 << 16);
                let mut payload = String::with_capacity(160);
                while res.sent < total {
                    let elapsed = t0.elapsed();
                    let due = ((elapsed.as_secs_f64() * per_conn_rate) as u64 + 1).min(total);
                    if due > res.sent {
                        buf.clear();
                        let batch_start = res.sent;
                        for k in batch_start..due {
                            let sched_ms = t0_ms + (k as f64 * 1000.0 / per_conn_rate) as u128;
                            let device = (c as u64 * 1_000_003 + k) % args.devices;
                            let topic: &[u8] = match template {
                                Some((pre, post)) => {
                                    topic_buf.clear();
                                    let _ = write!(topic_buf, "{pre}{}{device}{post}", args.devprefix);
                                    &topic_buf
                                }
                                None => args.topic.as_bytes(),
                            };
                            payload.clear();
                            use std::fmt::Write as _;
                            if esphome {
                                let _ = write!(payload, "{:.1}", 20.0 + (k % 150) as f64 / 10.0);
                            } else {
                                let _ = write!(
                                    payload,
                                    "{{\"id\":\"{}_{}_{}\",\"device\":\"{}{}\",\"temp\":{:.1},\"speed\":{:.1},\"ts\":{}}}",
                                    args.tag,
                                    c,
                                    k,
                                    args.devprefix,
                                    device,
                                    20.0 + (k % 150) as f64 / 10.0,
                                    (k % 1300) as f64 / 10.0,
                                    sched_ms
                                );
                            }
                            push_publish(&mut buf, topic, payload.as_bytes());
                        }
                        if let Err(e) = stream.write_all(&buf) {
                            res.error = Some(format!("write after {} msgs: {e}", res.sent));
                            break;
                        }
                        let now_ns = t0.elapsed().as_nanos();
                        res.first_ns.get_or_insert(now_ns);
                        res.last_ns = Some(now_ns);
                        let bucket = (t0.elapsed().as_secs() as usize).min(res.per_sec.len() - 1);
                        res.per_sec[bucket] += due - batch_start;
                        res.sent = due;
                    } else {
                        thread::sleep(Duration::from_micros(500));
                    }
                }
                let _ = stream.write_all(&[0xE0, 0x00]);
                res
            })
        })
        .collect();

    let _ = start_holder.set((Instant::now() + Duration::from_millis(200), epoch_ms() + 200));
    let wait_start = *start_holder.get().expect("start");
    barrier.wait();
    while Instant::now() < wait_start.0 {
        thread::sleep(Duration::from_millis(1));
    }

    let results: Vec<ConnResult> = handles.into_iter().map(|h| h.join().expect("thread")).collect();
    let wall = wait_start.0.elapsed().as_secs_f64();
    let sent: u64 = results.iter().map(|r| r.sent).sum();
    let mut per_sec = vec![0u64; args.secs as usize + 1];
    for r in &results {
        for (i, v) in r.per_sec.iter().enumerate() {
            per_sec[i] += v;
        }
    }
    let errors: Vec<String> = results.iter().filter_map(|r| r.error.clone()).collect();
    let window = results
        .iter()
        .filter_map(|r| r.last_ns)
        .max()
        .unwrap_or(0) as f64
        / 1e9;
    let nominal = (args.rate * args.secs as f64).round() as u64;
    let core = if per_sec.len() > 2 { &per_sec[1..per_sec.len() - 1] } else { &per_sec[..] };
    let core_min = core.iter().copied().min().unwrap_or(0);
    let core_max = core.iter().copied().max().unwrap_or(0);
    let json = format!(
        "{{\n  \"tool\": \"mqttgen 0.1.0 std-only QoS0\",\n  \"host\": \"{}\", \"port\": {}, \"topic\": \"{}\", \"mode\": \"{}\",\n  \"nominal_rate\": {}, \"secs\": {}, \"conns\": {}, \"devices\": {}, \"tag\": \"{}\",\n  \"clock_start_epoch_ms\": {},\n  \"nominal_messages\": {}, \"sent_messages\": {},\n  \"actual_rate_over_schedule\": {:.1}, \"send_window_s\": {:.3}, \"wall_s\": {:.3},\n  \"per_sec_sent\": {:?},\n  \"per_sec_core_min\": {}, \"per_sec_core_max\": {},\n  \"on_schedule\": {},\n  \"errors\": {:?}\n}}\n",
        args.host,
        args.port,
        args.topic,
        args.mode,
        args.rate,
        args.secs,
        args.conns,
        args.devices,
        args.tag,
        wait_start.1,
        nominal,
        sent,
        sent as f64 / args.secs as f64,
        window,
        wall,
        per_sec,
        core_min,
        core_max,
        errors.is_empty()
            && sent == nominal
            && wall <= args.secs as f64 + 1.5
            && core_min as f64 >= 0.95 * args.rate
            && core_max as f64 <= 1.10 * args.rate,
        errors
    );
    std::fs::write(&args.out, &json).expect("write --out");
    print!("{json}");
}
