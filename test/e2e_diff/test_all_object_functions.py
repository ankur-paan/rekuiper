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

obj_funcs = [
    ("keys", "keys(my_obj)"),
    ("values", "values(my_obj)"),
    ("object", "object(['a', 'b'], [1, 2])"),
    ("zip", "zip([['a', 1], ['b', 2]])"),
    ("items", "items(my_obj)"),
    ("object_construct", "object_construct('a', 1, 'b', 2)"),
    ("object_concat", "object_concat(obj1, obj2)"),
    ("erase", "erase(my_obj, 'foo')"),
    ("object_pick", "object_pick(my_obj, 'foo')"),
    ("obj_to_kvpair_array", "obj_to_kvpair_array(my_obj)")
]

def main():
    print("=================================================================")
    print("EXHAUSTIVE FUNCTION-BY-FUNCTION VERIFICATION: object_functions.md")
    print("=================================================================")
    
    stream_name = "obj_exhaustive_stream"
    http_req(f"/streams/{stream_name}", "DELETE")
    c, r = http_req("/streams", "POST", {
        "sql": f'create stream {stream_name} () WITH (DATASOURCE="devices/obj_ex", FORMAT="json", TYPE="mqtt", SERVER="tcp://127.0.0.1:1883")'
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

    sub = make_client("sub_obj_ex")
    sub.on_message = on_msg
    sub.connect(BROKER_HOST, BROKER_PORT, 60)
    sub.subscribe("out/obj_ex/#")
    sub.loop_start()

    pub = make_client("pub_obj_ex")
    pub.connect(BROKER_HOST, BROKER_PORT, 60)

    for fname, expr in obj_funcs:
        rid = f"rule_o_{fname}"
        topic = f"out/obj_ex/{fname}"
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
        
        # Publish real structured payload
        pub.publish("devices/obj_ex", json.dumps({
            "my_obj": {"foo": "emq", "bar": "hello", "baz": 42},
            "obj1": {"a": 1, "b": 2},
            "obj2": {"b": 3, "c": 4}
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

    print("\n--- RESULTS FOR object_functions.md ---")
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
        print(f"[{status_str:6s}] {fname:20s} ({dict(obj_funcs)[fname]:35s}) -> {note}")
    print("-----------------------------------------------------------------")
    print(f"Summary: {pass_cnt} VERIFIED, {unsupp_cnt} UNSUPPORTED, {fail_cnt} BROKEN out of {len(obj_funcs)} functions.")

if __name__ == "__main__":
    main()
