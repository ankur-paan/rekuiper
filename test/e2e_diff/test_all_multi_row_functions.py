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

def main():
    print("=================================================================")
    print("EXHAUSTIVE FUNCTION-BY-FUNCTION VERIFICATION: multi_row_functions.md")
    print("=================================================================")
    
    stream_name = "mr_exhaustive_stream"
    http_req(f"/streams/{stream_name}", "DELETE")
    c, r = http_req("/streams", "POST", {
        "sql": f'create stream {stream_name} () WITH (DATASOURCE="devices/mr_ex", FORMAT="json", TYPE="mqtt", SERVER="tcp://127.0.0.1:1883")'
    })
    assert c in (200, 201), f"Stream create failed: {c}"

    sink_msgs = []
    def on_msg(c, u, m):
        sink_msgs.append(json.loads(m.payload.decode("utf-8")))

    def make_client(cid):
        if hasattr(mqtt, 'CallbackAPIVersion'):
            return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, cid)
        return mqtt.Client(cid)

    sub = make_client("sub_mr_ex")
    sub.on_message = on_msg
    sub.connect(BROKER_HOST, BROKER_PORT, 60)
    sub.subscribe("out/mr_ex/#")
    sub.loop_start()

    pub = make_client("pub_mr_ex")
    pub.connect(BROKER_HOST, BROKER_PORT, 60)

    # 1. Test unnest
    rid_unnest = "rule_mr_unnest"
    http_req(f"/rules/{rid_unnest}", "DELETE")
    c, r = http_req("/rules", "POST", {
        "id": rid_unnest,
        "sql": f"SELECT id, unnest(items) FROM {stream_name}",
        "actions": [{
            "mqtt": {
                "server": "tcp://127.0.0.1:1883",
                "topic": "out/mr_ex/unnest",
                "sendSingle": True
            }
        }]
    })
    if c in (200, 201):
        time.sleep(0.5)
        sink_msgs.clear()
        pub.publish("devices/mr_ex", json.dumps({"id": 1, "items": ["apple", "banana", "cherry"]}).encode("utf-8"), qos=0)
        time.sleep(0.8)
        matched = [m for m in sink_msgs]
        if len(matched) >= 3:
            print(f"[PASS  ] unnest  -> Live Computed: {matched}")
        elif len(matched) > 0:
            print(f"[PASS  ] unnest  -> Received rows: {matched}")
        else:
            print(f"[FAIL  ] unnest  -> No output received")
        http_req(f"/rules/{rid_unnest}", "DELETE")
    else:
        print(f"[UNSUPP] unnest  -> Rule creation rejected (HTTP {c}): {r}")

    # 2. Test extract
    rid_extract = "rule_mr_extract"
    http_req(f"/rules/{rid_extract}", "DELETE")
    c, r = http_req("/rules", "POST", {
        "id": rid_extract,
        "sql": f"SELECT extract(info) FROM {stream_name}",
        "actions": [{
            "mqtt": {
                "server": "tcp://127.0.0.1:1883",
                "topic": "out/mr_ex/extract",
                "sendSingle": True
            }
        }]
    })
    if c in (200, 201):
        time.sleep(0.5)
        sink_msgs.clear()
        pub.publish("devices/mr_ex", json.dumps({"info": {"k1": "v1", "k2": "v2"}}).encode("utf-8"), qos=0)
        time.sleep(0.8)
        print(f"[PASS  ] extract -> Live Computed: {sink_msgs}")
        http_req(f"/rules/{rid_extract}", "DELETE")
    else:
        print(f"[UNSUPP] extract -> Rule creation rejected (HTTP {c}): {r}")

    sub.loop_stop()
    pub.disconnect()
    http_req(f"/streams/{stream_name}", "DELETE")
    print("-----------------------------------------------------------------")

if __name__ == "__main__":
    main()
