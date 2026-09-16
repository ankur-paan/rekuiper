use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

fn remaining_length(mut length: usize, packet: &mut Vec<u8>) {
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length != 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if length == 0 {
            break;
        }
    }
}

fn read_packet(stream: &mut impl Read, body: &mut Vec<u8>) -> std::io::Result<u8> {
    let mut header = [0_u8; 1];
    stream.read_exact(&mut header)?;
    let mut size = 0_usize;
    let mut multiplier = 1_usize;
    for _ in 0..4 {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte)?;
        size += usize::from(byte[0] & 127) * multiplier;
        if byte[0] & 128 == 0 {
            if size > 1024 * 1024 {
                return Err(std::io::Error::other("MQTT packet exceeds 1 MiB"));
            }
            body.resize(size, 0);
            stream.read_exact(body)?;
            return Ok(header[0]);
        }
        multiplier *= 128;
    }
    Err(std::io::Error::other("invalid MQTT remaining length"))
}

fn connect(stream: &mut TcpStream, client_id: &str) -> std::io::Result<()> {
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 0];
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    let mut packet = vec![0x10];
    remaining_length(body.len(), &mut packet);
    packet.extend_from_slice(&body);
    stream.write_all(&packet)?;
    let mut ack = Vec::new();
    let header = read_packet(stream, &mut ack)?;
    if header != 0x20 || ack != [0, 0] {
        return Err(std::io::Error::other("MQTT CONNACK rejected"));
    }
    Ok(())
}

fn subscribe(stream: &mut TcpStream, topic: &str) -> std::io::Result<()> {
    let topic_bytes = topic.as_bytes();
    let mut body = vec![0, 1];
    body.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
    body.extend_from_slice(topic_bytes);
    body.push(0);
    let mut packet = vec![0x82];
    remaining_length(body.len(), &mut packet);
    packet.extend_from_slice(&body);
    stream.write_all(&packet)?;
    let mut ack = Vec::new();
    let header = read_packet(stream, &mut ack)?;
    if header != 0x90 || ack.len() != 3 || ack[0..2] != [0, 1] || ack[2] != 0 {
        return Err(std::io::Error::other("MQTT SUBACK rejected"));
    }
    Ok(())
}

fn value(args: &[String], name: &str, default: &str) -> String {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map_or_else(|| default.to_string(), |pair| pair[1].clone())
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let host = value(&args, "--host", "127.0.0.1");
    let port: u16 = value(&args, "--port", "1883").parse().expect("--port");
    let topic = value(&args, "--topic", "bench/#");
    let seconds: u64 = value(&args, "--secs", "30").parse().expect("--secs");
    let client_id = value(&args, "--client-id", "mqttprobe");
    let output = value(&args, "--out", "mqttprobe.json");
    let expected_tag = value(&args, "--tag", "");
    let mut stream = TcpStream::connect((host.as_str(), port))?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    connect(&mut stream, &client_id)?;
    subscribe(&mut stream, &topic)?;
    println!("mqttprobe subscribed to {topic}");
    std::io::stdout().flush()?;
    let mut stream = BufReader::with_capacity(1024 * 1024, stream);
    let mut body = Vec::with_capacity(256);

    let start = Instant::now();
    let mut per_second = vec![0_u64; seconds as usize];
    let mut total = 0_u64;
    let mut last_seq = [None::<u64>; 8];
    let mut sequence_gaps = 0_u64;
    let mut duplicate_or_reordered = 0_u64;
    let mut unmatched_ids = 0_u64;
    let mut error = None;
    while start.elapsed() < Duration::from_secs(seconds) {
        match read_packet(&mut stream, &mut body) {
            Ok(header) if header >> 4 == 3 => {
                if body.len() < 2 {
                    error = Some("short MQTT PUBLISH".to_string());
                    break;
                }
                let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
                if body.len() < 2 + topic_len {
                    error = Some("short MQTT PUBLISH topic".to_string());
                    break;
                }
                total += 1;
                if !expected_tag.is_empty() {
                    let payload = &body[2 + topic_len..];
                    let parsed = std::str::from_utf8(payload).ok().and_then(|text| {
                        let id = text.split_once("\"id\":\"")?.1.split_once('"')?.0;
                        let (prefix, seq) = id.rsplit_once('_')?;
                        let (tag, conn) = prefix.rsplit_once('_')?;
                        if tag != expected_tag {
                            return None;
                        }
                        Some((conn.parse::<usize>().ok()?, seq.parse::<u64>().ok()?))
                    });
                    if let Some((conn, seq)) = parsed.filter(|(conn, _)| *conn < last_seq.len()) {
                        let expected = last_seq[conn].map_or(0, |previous| previous + 1);
                        if seq > expected {
                            sequence_gaps += seq - expected;
                        }
                        if seq < expected {
                            duplicate_or_reordered += 1;
                        }
                        if seq >= expected {
                            last_seq[conn] = Some(seq);
                        }
                    } else {
                        unmatched_ids += 1;
                    }
                }
                let slot = start.elapsed().as_secs() as usize;
                if let Some(count) = per_second.get_mut(slot) {
                    *count += 1;
                }
            }
            Ok(_) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) => {}
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
    }
    let error_json = error
        .as_ref()
        .map_or_else(|| "null".to_string(), |e| format!("{e:?}"));
    let last_seq_json = format!(
        "[{}]",
        last_seq
            .iter()
            .map(|seq| seq.map_or_else(|| "null".to_string(), |n| n.to_string()))
            .collect::<Vec<_>>()
            .join(",")
    );
    let report = format!(
        "{{\"tool\":\"mqttprobe 0.1.0\",\"topic\":{:?},\"seconds\":{},\"received\":{},\"per_sec_received\":{:?},\"sequence_gaps\":{},\"duplicate_or_reordered\":{},\"unmatched_ids\":{},\"last_seq\":{},\"error\":{}}}\n",
        topic, seconds, total, per_second, sequence_gaps, duplicate_or_reordered, unmatched_ids, last_seq_json, error_json
    );
    std::fs::write(output, &report)?;
    print!("{report}");
    if let Some(error) = error {
        return Err(std::io::Error::other(error));
    }
    Ok(())
}
