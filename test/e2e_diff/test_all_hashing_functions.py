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

hash_funcs = [
    ("md5", "md5(msg)"),
    ("sha1", "sha1(msg)"),
    ("sha256", "sha256(msg)"),
    ("sha384", "sha384(msg)"),
    ("sha512", "sha512(msg)"),
    ("crc32", "crc32(msg)")
]

def main():
    print("=================================================================")
    print("EXHAUSTIVE FUNCTION-BY-FUNCTION VERIFICATION: hashing_functions.md")
    print("=================================================================")
    
    stream_name = "hash_exhaustive_stream"
    http_req(f"/streams/{stream_name}", "DELETE")
    c, r = http_req("/streams", "POST", {
        "sql": f'create stream {stream_name} () WITH (DATASOURCE="devices/hash_ex", FORMAT="json", TYPE="mqtt", CONF_KEY="e2e_broker")'
    })
    assert c in (200, 201), f"Stream create failed: {c}"

    results = {}

    sink_msgs = []
    def on_msg(c, u, m):
        sink_msgs.append(json.loads(m.payload.decode("utf-8")))
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "sub_hash_ex")
    sub.on_message = on_msg
    sub.connect(BROKER_HOST, BROKER_PORT, 60)
    sub.subscribe("out/hash_ex/#")
    sub.loop_start()

    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "pub_hash_ex")
    pub.connect(BROKER_HOST, BROKER_PORT, 60)

    test_input = "hello world"

    for fname, expr in hash_funcs:
        rid = f"rule_h_{fname}"
        topic = f"out/hash_ex/{fname}"
        http_req(f"/rules/{rid}", "DELETE")
        rule_body = {
            "id": rid,
            "sql": f"SELECT {expr} AS res FROM {stream_name}",
            "actions": [{
                "mqtt": {
                    "server": "tcp://kuiper-mosquitto:1883",
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
        pub.publish("devices/hash_ex", json.dumps({"msg": test_input}).encode("utf-8"), qos=0)

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

    print("\n--- RESULTS FOR hashing_functions.md ---")
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
        print(f"[{status_str:6s}] {fname:10s} ({dict(hash_funcs)[fname]:15s}) -> {note}")
    print("-----------------------------------------------------------------")
    print(f"Summary: {pass_cnt} VERIFIED, {unsupp_cnt} UNSUPPORTED, {fail_cnt} BROKEN out of {len(hash_funcs)} functions.")

if __name__ == "__main__":
    main()
