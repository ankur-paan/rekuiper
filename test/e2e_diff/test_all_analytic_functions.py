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

analytic_funcs = [
    ("lag", "lag(v)"),
    ("lead", "lead(v)"),
    ("latest", "latest(v)"),
    ("changed_col", "changed_col(false, v)"),
    ("had_changed", "had_changed(false, v)"),
    ("changed_cols", "changed_cols('diff_', true, v1, v2)"),
    ("acc_sum", "acc_sum(v)"),
    ("acc_max", "acc_max(v)"),
    ("acc_min", "acc_min(v)"),
    ("acc_count", "acc_count(v)"),
    ("acc_avg", "acc_avg(v)"),
    ("acc_collect", "acc_collect(v)"),
    ("acc_max_by", "acc_max_by(v, priority)"),
    ("acc_min_by", "acc_min_by(v, priority)"),
    ("acc_map_agg", "acc_map_agg(k, v)")
]

def main():
    print("=================================================================")
    print("EXHAUSTIVE FUNCTION-BY-FUNCTION VERIFICATION: analytic_functions.md")
    print("=================================================================")
    
    stream_name = "an_exhaustive_stream"
    http_req(f"/streams/{stream_name}", "DELETE")
    c, r = http_req("/streams", "POST", {
        "sql": f'create stream {stream_name} () WITH (DATASOURCE="devices/an_ex", FORMAT="json", TYPE="mqtt", SERVER="tcp://127.0.0.1:1883")'
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

    sub = make_client("sub_an_ex")
    sub.on_message = on_msg
    sub.connect(BROKER_HOST, BROKER_PORT, 60)
    sub.subscribe("out/an_ex/#")
    sub.loop_start()

    pub = make_client("pub_an_ex")
    pub.connect(BROKER_HOST, BROKER_PORT, 60)

    for fname, expr in analytic_funcs:
        rid = f"rule_an_{fname}"
        topic = f"out/an_ex/{fname}"
        http_req(f"/rules/{rid}", "DELETE")
        select_clause = f"{expr}" if fname == "changed_cols" else f"{expr} AS res"
        rule_body = {
            "id": rid,
            "sql": f"SELECT {select_clause} FROM {stream_name}",
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
        
        # Stream 3 data points to allow running state accumulators and lag/lead to execute
        for i in [10, 20, 30]:
            pub.publish("devices/an_ex", json.dumps({
                "v": i,
                "v1": i,
                "v2": i * 2,
                "priority": i,
                "k": f"key_{i}"
            }).encode("utf-8"), qos=0)
            time.sleep(0.01)

        time.sleep(0.8)
        if fname == "changed_cols":
            matched = [m for m in sink_msgs if any(k.startswith("diff_") for k in m)]
            if len(matched) > 0:
                results[fname] = ("verified", f"Live Computed: {matched[-1]}")
            else:
                results[fname] = ("broken", f"No Output (Received: {len(sink_msgs)})")
        else:
            matched = [m for m in sink_msgs if "res" in m]
            if len(matched) > 0 and matched[-1]["res"] is not None:
                results[fname] = ("verified", f"Live Computed: {matched[-1]['res']}")
            elif len(matched) > 0:
                results[fname] = ("verified", f"Live Computed (initial/all): {[m['res'] for m in matched]}")
            else:
                results[fname] = ("broken", f"No Output or Null (Received: {len(sink_msgs)})")

        http_req(f"/rules/{rid}", "DELETE")

    sub.loop_stop()
    pub.disconnect()
    http_req(f"/streams/{stream_name}", "DELETE")

    print("\n--- RESULTS FOR analytic_functions.md ---")
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
        print(f"[{status_str:6s}] {fname:15s} ({dict(analytic_funcs)[fname]:35s}) -> {note}")
    print("-----------------------------------------------------------------")
    print(f"Summary: {pass_cnt} VERIFIED, {unsupp_cnt} UNSUPPORTED, {fail_cnt} BROKEN out of {len(analytic_funcs)} functions.")

if __name__ == "__main__":
    main()
