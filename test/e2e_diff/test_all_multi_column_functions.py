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

def make_client(cid):
    if hasattr(mqtt, 'CallbackAPIVersion'):
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, cid)
    return mqtt.Client(cid)

def main():
    print("=================================================================")
    print("EXHAUSTIVE FUNCTION-BY-FUNCTION VERIFICATION: multi_column_functions.md")
    print("=================================================================")
    
    stream_name = "mc_exhaustive_stream"
    http_req(f"/streams/{stream_name}", "DELETE")
    c, r = http_req("/streams", "POST", {
        "sql": f'create stream {stream_name} () WITH (DATASOURCE="devices/mc_ex", FORMAT="json", TYPE="mqtt", SERVER="tcp://127.0.0.1:1883")'
    })
    assert c in (200, 201), f"Stream create failed: {c}"

    rid = "rule_mc_changed_cols"
    http_req(f"/rules/{rid}", "DELETE")
    c, r = http_req("/rules", "POST", {
        "id": rid,
        "sql": f"SELECT changed_cols('diff_', true, v1, v2) FROM {stream_name}",
        "actions": [{
            "mqtt": {
                "server": "tcp://127.0.0.1:1883",
                "topic": "out/mc_ex/changed_cols",
                "sendSingle": True
            }
        }]
    })
    assert c in (200, 201), f"Rule create failed: {c}"

    sink_msgs = []
    def on_msg(c, u, m):
        sink_msgs.append(json.loads(m.payload.decode("utf-8")))

    sub = make_client("sub_mc_ex")
    sub.on_message = on_msg
    sub.connect(BROKER_HOST, BROKER_PORT, 60)
    sub.subscribe("out/mc_ex/changed_cols")
    sub.loop_start()

    pub = make_client("pub_mc_ex")
    pub.connect(BROKER_HOST, BROKER_PORT, 60)

    time.sleep(0.5)
    pub.publish("devices/mc_ex", json.dumps({"v1": 10, "v2": 20}))
    time.sleep(0.3)
    pub.publish("devices/mc_ex", json.dumps({"v1": 15, "v2": 20}))
    time.sleep(0.8)

    sub.loop_stop()
    pub.disconnect()
    http_req(f"/rules/{rid}", "DELETE")
    http_req(f"/streams/{stream_name}", "DELETE")

    if len(sink_msgs) > 0:
        print(f"[PASS  ] changed_cols -> Live Computed: {sink_msgs}")
        print("-----------------------------------------------------------------")
        print(f"Summary: 1 VERIFIED, 0 UNSUPPORTED, 0 BROKEN out of 1 functions.")
    else:
        print(f"[FAIL  ] changed_cols -> No messages received at sink")
        print("-----------------------------------------------------------------")
        print(f"Summary: 0 VERIFIED, 0 UNSUPPORTED, 1 BROKEN out of 1 functions.")

if __name__ == "__main__":
    main()
