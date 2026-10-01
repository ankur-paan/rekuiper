import urllib.request
import json
import time
import paho.mqtt.client as mqtt

BASE_URL = "http://127.0.0.1:9081"
BROKER_HOST = "127.0.0.1"
BROKER_PORT = 1883

def http_req(path, method="GET", body=None):
    url = f"{BASE_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            content = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(content)
            except:
                return resp.status, content
    except urllib.error.HTTPError as e:
        err_content = e.read().decode("utf-8")
        try:
            return e.code, json.loads(err_content)
        except:
            return e.code, err_content

transform_funcs = [
    ("cast", "cast(num_str, 'bigint')"),
    ("convert_tz", "convert_tz('2024-01-01 12:00:00', 'Asia/Shanghai')"),
    ("to_seconds", "to_seconds('2024-01-01T00:00:00Z')"),
    ("encode", "encode(raw_text, 'base64')"),
    ("decode", "decode(b64_text, 'base64')"),
    ("compress", "compress(raw_text, 'zlib')"),
    ("decompress", "decompress(compress(raw_text, 'zlib'), 'zlib')"),
    ("trunc", "trunc(float_val, 2)"),
    ("chr", "chr(ascii_code)"),
    ("hex2dec", "hex2dec(hex_val)"),
    ("dec2hex", "dec2hex(dec_val)")
]

def main():
    print("=================================================================")
    print("EXHAUSTIVE FUNCTION-BY-FUNCTION VERIFICATION: transform_functions.md")
    print("=================================================================")
    
    stream_name = "trans_exhaustive_stream"
    http_req(f"/streams/{stream_name}", "DELETE")
    c, r = http_req("/streams", "POST", {
        "sql": f'create stream {stream_name} () WITH (DATASOURCE="devices/trans_ex", FORMAT="json", TYPE="mqtt", SERVER="tcp://127.0.0.1:1883")'
    })
    assert c in (200, 201), f"Stream create failed: {c}"

    results = {}

    sink_msgs = []
    def on_msg(c, u, m):
        sink_msgs.append(json.loads(m.payload.decode("utf-8")))

    def make_client(cid):
        if hasattr(mqtt, 'CallbackAPIVersion'):
            return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, cid)
        return mqtt.Client(cid)

    sub = make_client("sub_trans_ex")
    sub.on_message = on_msg
    sub.connect(BROKER_HOST, BROKER_PORT, 60)
    sub.subscribe("out/trans_ex/#")
    sub.loop_start()

    pub = make_client("pub_trans_ex")
    pub.connect(BROKER_HOST, BROKER_PORT, 60)

    for fname, expr in transform_funcs:
        rid = f"rule_t_{fname}"
        topic = f"out/trans_ex/{fname}"
        http_req(f"/rules/{rid}", "DELETE")
        rule_body = {
            "id": rid,
            "sql": f"SELECT {expr} AS res FROM {stream_name}",
            "actions": [{
                "mqtt": {
                    "server": "tcp://127.0.0.1:1883",
                    "topic": topic,
                    "sendSingle": True
                }
            }]
        }
        c, r = http_req("/rules", "POST", rule_body)
        if c not in (200, 201):
            results[fname] = ("unsupported", f"Rule creation rejected (HTTP {c}): {str(r)[:70]}")
            continue

        time.sleep(0.5)
        sink_msgs.clear()
        
        # Publish real payload
        pub.publish("devices/trans_ex", json.dumps({
            "num_str": "12345",
            "raw_text": "hello rekuiper",
            "b64_text": "aGVsbG8gcmVrdWlwZXI=",
            "float_val": 3.14159,
            "ascii_code": 65,
            "hex_val": "0x10",
            "dec_val": 16
        }).encode("utf-8"), qos=0)

        time.sleep(0.8)
        matched = [m for m in sink_msgs if "res" in m]
        if len(matched) > 0 and matched[0]["res"] is not None:
            results[fname] = ("verified", f"Live Computed: {matched[0]['res']}")
        else:
            results[fname] = ("broken", f"No Output or Null (Received: {len(sink_msgs)})")

        http_req(f"/rules/{rid}", "DELETE")

    sub.loop_stop()
    pub.disconnect()
    http_req(f"/streams/{stream_name}", "DELETE")

    print("\n--- RESULTS FOR transform_functions.md ---")
    pass_cnt = 0
    fail_cnt = 0
    unsupp_cnt = 0
    for fname, (status, note) in results.items():
        if status == "verified":
            pass_cnt += 1
            status_str = "PASS"
        elif status == "unsupported":
            unsupp_cnt += 1
            status_str = "UNSUPP"
        else:
            fail_cnt += 1
            status_str = "FAIL"
        print(f"[{status_str:6s}] {fname:15s} ({dict(transform_funcs)[fname]:45s}) -> {note}")
    print("-----------------------------------------------------------------")
    print(f"Summary: {pass_cnt} VERIFIED, {unsupp_cnt} UNSUPPORTED, {fail_cnt} BROKEN out of {len(transform_funcs)} functions.")

if __name__ == "__main__":
    main()
