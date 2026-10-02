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

string_funcs = [
    ("concat", "concat(s1, ':', s2)"),
    ("endswith", "endswith(s1, '123')"),
    ("format_time", "format_time(1727712000000, 'YYYY-MM-dd')"),
    ("indexof", "indexof(s1, 'dev')"),
    ("length", "length(s1)"),
    ("lower", "lower(s2)"),
    ("lpad", "lpad(s1, 10)"),
    ("ltrim", "ltrim(padded)"),
    ("numbytes", "numbytes(s1)"),
    ("regexp_matches", "regexp_matches(s1, '^dev.*')"),
    ("regexp_replace", "regexp_replace(s2, 'ACT.*', 'RUNNING')"),
    ("regexp_substring", "regexp_substring(s1, 'dev')"),
    ("replace", "replace(s2, 'ACTIVE', 'OK')"),
    ("reverse", "reverse(s1)"),
    ("rpad", "rpad(s1, 10)"),
    ("rtrim", "rtrim(padded)"),
    ("split", "split(csv, ',')"),
    ("split_value", "split_value(csv, ',', 1)"),
    ("startswith", "startswith(s1, 'dev')"),
    ("substring", "substring(s1, 1, 3)"),
    ("trim", "trim(padded)"),
    ("upper", "upper(s1)"),
    ("format", "format(123.4567, 2)")
]

def main():
    print("=================================================================")
    print("EXHAUSTIVE FUNCTION-BY-FUNCTION VERIFICATION: string_functions.md")
    print("=================================================================")

    # Ensure broker configuration exists
    http_req("/metadata/sources/mqtt/confKeys/e2e_broker", "PUT", {
        "server": "tcp://kuiper-mosquitto:1883",
        "qos": 0
    })

    stream_name = "str_exhaustive_stream"
    http_req(f"/streams/{stream_name}", "DELETE")
    c, r = http_req("/streams", "POST", {
        "sql": f'create stream {stream_name} () WITH (DATASOURCE="devices/str_ex", FORMAT="json", TYPE="mqtt", CONF_KEY="e2e_broker")'
    })
    assert c in (200, 201), f"Stream create failed: {c}"

    results = {}

    sink_msgs = []
    def on_msg(c, u, m):
        sink_msgs.append(json.loads(m.payload.decode("utf-8")))
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "sub_str_ex")
    sub.on_message = on_msg
    sub.connect(BROKER_HOST, BROKER_PORT, 60)
    sub.subscribe("out/str_ex/#")
    sub.loop_start()

    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "pub_str_ex")
    pub.connect(BROKER_HOST, BROKER_PORT, 60)

    for fname, expr in string_funcs:
        rid = f"rule_s_{fname}"
        topic = f"out/str_ex/{fname}"
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
            results[fname] = (False, f"Creation Error {c}: {str(r)[:60]}")
            continue

        time.sleep(1.0)
        sink_msgs.clear()
        
        # Stream 10 real data points
        for i in range(1, 11):
            pub.publish("devices/str_ex", json.dumps({
                "s1": "dev_123",
                "s2": "ACTIVE",
                "padded": "  Sensor_01  ",
                "num_str": "42.5",
                "csv": "alpha,beta,gamma"
            }).encode("utf-8"), qos=0)
            time.sleep(0.005)

        time.sleep(1.5)
        matched = [m for m in sink_msgs if "res" in m]
        if len(matched) > 0 and matched[0]["res"] is not None:
            results[fname] = (True, f"Live Computed: {matched[0]['res']}")
        else:
            results[fname] = (False, f"No Output (Received: {len(sink_msgs)})")

        http_req(f"/rules/{rid}", "DELETE")

    sub.loop_stop()
    pub.disconnect()
    http_req(f"/streams/{stream_name}", "DELETE")

    print("\n--- RESULTS FOR string_functions.md ---")
    pass_cnt = 0
    fail_cnt = 0
    for fname, (ok, note) in results.items():
        status_str = "PASS" if ok else "FAIL"
        if ok:
            pass_cnt += 1
        else:
            fail_cnt += 1
        print(f"[{status_str:4s}] {fname:15s} ({dict(string_funcs)[fname]:35s}) -> {note}")
    print("-----------------------------------------------------------------")
    print(f"Summary: {pass_cnt} PASSED, {fail_cnt} FAILED / PENDING out of {len(string_funcs)} functions.")

if __name__ == "__main__":
    main()
