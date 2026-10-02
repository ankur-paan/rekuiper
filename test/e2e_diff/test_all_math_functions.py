import urllib.request
import json
import time
import math
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

# Test each math function individually to see if the parser and engine support it
math_funcs = [
    ("abs", "abs(x)"),
    ("acos", "acos(norm)"),
    ("asin", "asin(norm)"),
    ("atan", "atan(x)"),
    ("atan2", "atan2(y, x)"),
    ("bitand", "bitand(i1, i2)"),
    ("bitor", "bitor(i1, i2)"),
    ("bitxor", "bitxor(i1, i2)"),
    ("bitnot", "bitnot(i1)"),
    ("ceil", "ceil(x)"),
    ("ceiling", "ceiling(x)"),
    ("cos", "cos(x)"),
    ("cosh", "cosh(norm)"),
    ("exp", "exp(norm)"),
    ("ln", "ln(abs(x))"),
    ("log", "log(abs(x))"),
    ("mod", "mod(i1, i2)"),
    ("power", "power(x, 2)"),
    ("round", "round(x, 2)"),
    ("sign", "sign(x)"),
    ("sin", "sin(x)"),
    ("sinh", "sinh(norm)"),
    ("sqrt", "sqrt(abs(x))"),
    ("tan", "tan(norm)"),
    ("tanh", "tanh(norm)"),
    ("floor", "floor(x)"),
    ("pi", "pi()"),
    ("pow", "pow(x, 2)"),
    ("rand", "rand()"),
    ("cot", "cot(norm)"),
    ("radians", "radians(deg)"),
    ("degrees", "degrees(rad)"),
    ("conv", "conv(hex_str, 16, 10)")
]

def main():
    print("=================================================================")
    print("EXHAUSTIVE FUNCTION-BY-FUNCTION VERIFICATION: mathematical_functions.md")
    print("=================================================================")
    
    stream_name = "math_exhaustive_stream"
    http_req(f"/streams/{stream_name}", "DELETE")
    c, r = http_req("/streams", "POST", {
        "sql": f'create stream {stream_name} () WITH (DATASOURCE="devices/math_ex", FORMAT="json", TYPE="mqtt", CONF_KEY="e2e_broker")'
    })
    assert c in (200, 201), f"Stream create failed: {c}"

    results = {}

    sink_msgs = []
    def on_msg(c, u, m):
        sink_msgs.append(json.loads(m.payload.decode("utf-8")))
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "sub_math_ex")
    sub.on_message = on_msg
    sub.connect(BROKER_HOST, BROKER_PORT, 60)
    sub.subscribe("out/math_ex/#")
    sub.loop_start()

    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "pub_math_ex")
    pub.connect(BROKER_HOST, BROKER_PORT, 60)

    for fname, expr in math_funcs:
        rid = f"rule_m_{fname}"
        topic = f"out/math_ex/{fname}"
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
            pub.publish("devices/math_ex", json.dumps({
                "x": 3.14 + (i * 0.1),
                "y": 2.0 + (i * 0.1),
                "norm": 0.5,
                "i1": 12,
                "i2": 5,
                "deg": 180.0,
                "rad": 3.141592653589793,
                "hex_str": "1f"
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

    print("\n--- RESULTS FOR mathematical_functions.md ---")
    pass_cnt = 0
    fail_cnt = 0
    for fname, (ok, note) in results.items():
        status_str = "PASS" if ok else "FAIL"
        if ok:
            pass_cnt += 1
        else:
            fail_cnt += 1
        print(f"[{status_str:4s}] {fname:15s} ({dict(math_funcs)[fname]:20s}) -> {note}")
    print("-----------------------------------------------------------------")
    print(f"Summary: {pass_cnt} PASSED, {fail_cnt} FAILED / PENDING out of {len(math_funcs)} functions.")

if __name__ == "__main__":
    main()
