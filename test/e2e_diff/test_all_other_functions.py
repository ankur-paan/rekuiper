import json
import time
import requests
import paho.mqtt.client as mqtt

REKUIPER_URL = "http://127.0.0.1:9081"
MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883

OTHER_TESTS = [
    ("isNull", "isNull(val) AS res"),
    ("coalesce", "coalesce(missing_col, val, 999) AS res"),
    ("newuuid", "newuuid() AS res"),
    ("tstamp", "tstamp() AS res"),
    ("event_time", "event_time() AS res"),
    ("rule_id", "rule_id() AS res"),
    ("rule_start", "rule_start() AS res"),
    ("mqtt", "mqtt(topic) AS res"),
    ("meta", "meta(topic) AS res"),
    ("last_hit_count", "last_hit_count() AS res"),
    ("last_hit_time", "last_hit_time() AS res"),
    ("window_start", "window_start() AS res"),
    ("window_end", "window_end() AS res"),
    ("get_keyed_state", "get_keyed_state('dev1', 'float', 0.0) AS res"),
    ("delay", "delay(10, 'delayed_val') AS res"),
]

def make_client(cid):
    if hasattr(mqtt, 'CallbackAPIVersion'):
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, cid)
    return mqtt.Client(cid)

def main():
    print("=== Testing ALL Other Functions in other_functions.md ===")
    results = {}

    requests.delete(f"{REKUIPER_URL}/streams/other_test_stream")
    stream_sql = (
        'create stream other_test_stream () WITH ('
        '  DATASOURCE="devices/other_test", FORMAT="json", TYPE="mqtt", SERVER="tcp://127.0.0.1:1883"'
        ')'
    )
    requests.post(f"{REKUIPER_URL}/streams", json={"sql": stream_sql})

    for name, expr in OTHER_TESTS:
        rule_id = f"rule_other_{name}"
        requests.delete(f"{REKUIPER_URL}/rules/{rule_id}")

        sql = f"SELECT {expr} FROM other_test_stream"
        rule_payload = {
            "id": rule_id,
            "sql": sql,
            "actions": [{
                "mqtt": {
                    "server": "tcp://127.0.0.1:1883",
                    "topic": f"sink/other_{name}",
                    "sendSingle": True
                }
            }]
        }
        r = requests.post(f"{REKUIPER_URL}/rules", json=rule_payload)
        if r.status_code not in (200, 201):
            results[name] = {"supported": False, "status_code": r.status_code, "error": r.text.strip()}
            print(f"[-] {name:20}: UNSUPPORTED -> {r.status_code}: {r.text.strip()}")
            continue

        sink_data = []
        def make_cb(storage):
            def cb(c, u, msg):
                try:
                    storage.append(json.loads(msg.payload.decode("utf-8")))
                except Exception:
                    storage.append(msg.payload.decode("utf-8"))
            return cb

        sub = make_client(f"sub_other_{name}")
        sub.on_message = make_cb(sink_data)
        sub.connect(MQTT_HOST, MQTT_PORT, 60)
        sub.subscribe(f"sink/other_{name}")
        sub.loop_start()

        time.sleep(0.5)

        pub = make_client(f"pub_other_{name}")
        pub.connect(MQTT_HOST, MQTT_PORT, 60)
        sample = {"val": 42.0, "topic": "devices/other_test"}
        pub.publish("devices/other_test", json.dumps(sample))
        pub.disconnect()

        time.sleep(0.8)
        sub.loop_stop()
        sub.disconnect()

        requests.delete(f"{REKUIPER_URL}/rules/{rule_id}")

        if sink_data:
            results[name] = {"supported": True, "data_verified": True, "output": sink_data[0]}
            print(f"[+] {name:20}: VERIFIED -> {sink_data[0]}")
        else:
            results[name] = {"supported": True, "data_verified": False, "note": "Rule accepted, but no sink data received"}
            print(f"[?] {name:20}: ACCEPTED (No sink data)")

    print("\n=== Other Functions Test Scorecard ===")
    print(json.dumps(results, indent=2))
    with open("test/e2e_diff/other_results.json", "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()
