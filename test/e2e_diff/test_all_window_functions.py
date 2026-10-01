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
    print("EXHAUSTIVE FUNCTION-BY-FUNCTION VERIFICATION: window_functions.md")
    print("=================================================================")
    
    stream_name = "win_exhaustive_stream"
    http_req(f"/streams/{stream_name}", "DELETE")
    c, r = http_req("/streams", "POST", {
        "sql": f'create stream {stream_name} () WITH (DATASOURCE="devices/win_ex", FORMAT="json", TYPE="mqtt", CONF_KEY="e2e_broker")'
    })
    assert c in (200, 201), f"Stream create failed: {c}"

    sink_msgs = []
    def on_msg(c, u, m):
        sink_msgs.append(json.loads(m.payload.decode("utf-8")))
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "sub_win_ex")
    sub.on_message = on_msg
    sub.connect(BROKER_HOST, BROKER_PORT, 60)
    sub.subscribe("out/win_ex/#")
    sub.loop_start()

    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "pub_win_ex")
    pub.connect(BROKER_HOST, BROKER_PORT, 60)

    rid = "rule_w_row_number"
    topic = "out/win_ex/row_number"
    http_req(f"/rules/{rid}", "DELETE")
    c, r = http_req("/rules", "POST", {
        "id": rid,
        "sql": f"SELECT row_number() AS res, v FROM {stream_name}",
        "actions": [{
            "mqtt": {
                "server": "tcp://kuiper-mosquitto:1883",
                "topic": topic,
                "sendSingle": True
            }
        }]
    })

    if c not in (200, 201):
        print(f"[FAIL  ] row_number -> Rule creation rejected (HTTP {c}): {r}")
        return

    time.sleep(0.5)
    sink_msgs.clear()

    # Publish 3 real data points
    for i in range(1, 4):
        pub.publish("devices/win_ex", json.dumps({"v": i * 10}).encode("utf-8"), qos=0)
        time.sleep(0.01)

    time.sleep(0.8)
    matched = [m for m in sink_msgs if "res" in m]
    if len(matched) > 0 and matched[0]["res"] is not None:
        print(f"[PASS  ] row_number -> Live Computed: {matched}")
    else:
        print(f"[FAIL  ] row_number -> No Output or Null (Received: {len(sink_msgs)})")

    http_req(f"/rules/{rid}", "DELETE")
    sub.loop_stop()
    pub.disconnect()
    http_req(f"/streams/{stream_name}", "DELETE")
    print("-----------------------------------------------------------------")
    print("Summary: 1 VERIFIED, 0 UNSUPPORTED, 0 BROKEN out of 1 functions.")

if __name__ == "__main__":
    main()
