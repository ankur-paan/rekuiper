import json
import time
import requests
import paho.mqtt.client as mqtt

REKUIPER_URL = "http://localhost:9081"
MQTT_HOST = "localhost"
MQTT_PORT = 1883

JSON_TESTS = [
    ("to_json", "to_json(payload) AS res"),
    ("parse_json", "parse_json(raw_str) AS res"),
    ("json_path_exists", "json_path_exists(user_obj, '$.name') AS res"),
    ("json_path_query", "json_path_query(items_obj, '$.items[*].id') AS res"),
    ("json_path_query_first", "json_path_query_first(user_obj, '$.name') AS res"),
]

def main():
    print("=== Testing ALL JSON Functions in json_functions.md ===")
    results = {}

    requests.put(f"{REKUIPER_URL}/metadata/sources/mqtt/confKeys/e2e_broker", json={"server": "tcp://kuiper-mosquitto:1883", "qos": 0})
    requests.delete(f"{REKUIPER_URL}/streams/json_test_stream")
    stream_sql = (
        'create stream json_test_stream () WITH ('
        '  DATASOURCE="devices/json_test", FORMAT="json", TYPE="mqtt", CONF_KEY="e2e_broker"'
        ')'
    )
    requests.post(f"{REKUIPER_URL}/streams", json={"sql": stream_sql})

    for name, expr in JSON_TESTS:
        rule_id = f"rule_json_{name}"
        requests.delete(f"{REKUIPER_URL}/rules/{rule_id}")

        sql = f"SELECT {expr} FROM json_test_stream"
        rule_payload = {
            "id": rule_id,
            "sql": sql,
            "actions": [{
                "mqtt": {
                    "server": "tcp://kuiper-mosquitto:1883",
                    "topic": f"sink/json_{name}",
                    "sendSingle": True
                }
            }]
        }
        r = requests.post(f"{REKUIPER_URL}/rules", json=rule_payload)
        if r.status_code not in (200, 201):
            results[name] = {"supported": False, "status_code": r.status_code, "error": r.text.strip()}
            print(f"[-] {name:25}: UNSUPPORTED -> {r.status_code}: {r.text.strip()}")
            continue

        sink_data = []
        def make_cb(storage):
            def cb(c, u, msg):
                try:
                    storage.append(json.loads(msg.payload.decode("utf-8")))
                except Exception:
                    storage.append(msg.payload.decode("utf-8"))
            return cb

        sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"sub_json_{name}")
        sub.on_message = make_cb(sink_data)
        sub.connect(MQTT_HOST, MQTT_PORT, 60)
        sub.subscribe(f"sink/json_{name}")
        sub.loop_start()

        time.sleep(0.3)

        pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"pub_json_{name}")
        pub.connect(MQTT_HOST, MQTT_PORT, 60)
        sample = {
            "payload": {"a": 1, "b": "hello"},
            "raw_str": json.dumps({"user": {"name": "alice"}, "items": [{"id": 101}, {"id": 102}]}),
            "user_obj": {"name": "alice"},
            "items_obj": {"items": [{"id": 101}, {"id": 102}]}
        }
        pub.publish("devices/json_test", json.dumps(sample))
        pub.disconnect()

        time.sleep(0.8)
        sub.loop_stop()
        sub.disconnect()

        requests.delete(f"{REKUIPER_URL}/rules/{rule_id}")

        if sink_data:
            results[name] = {"supported": True, "data_verified": True, "output": sink_data[0]}
            print(f"[+] {name:25}: VERIFIED -> {sink_data[0]}")
        else:
            results[name] = {"supported": True, "data_verified": False, "note": "Rule accepted, but no sink data received"}
            print(f"[?] {name:25}: ACCEPTED (No sink data)")

    print("\n=== JSON Function Test Scorecard ===")
    print(json.dumps(results, indent=2))
    with open("/mnt/c/Users/paanday/Documents/idacs/rekuiper/ekuiper/test/e2e_diff/json_results.json", "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()
