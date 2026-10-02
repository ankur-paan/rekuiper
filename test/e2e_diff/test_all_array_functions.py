import json
import time
import requests
import paho.mqtt.client as mqtt

REKUIPER_URL = "http://localhost:9081"
MQTT_HOST = "localhost"
MQTT_PORT = 1883

ARRAY_TESTS = [
    ("cardinality", "cardinality(arr) AS res"),
    ("array_position", "array_position(arr, 20) AS res"),
    ("element_at", "element_at(arr, 1) AS res"),
    ("array_contains", "array_contains(arr, 20) AS res"),
    ("array_create", "array_create(1, 2, 3) AS res"),
    ("array_remove", "array_remove(arr, 20) AS res"),
    ("array_last_position", "array_last_position(arr, 20) AS res"),
    ("array_contains_any", "array_contains_any(arr, [20, 99]) AS res"),
    ("array_intersect", "array_intersect(arr, [20, 30, 99]) AS res"),
    ("array_union", "array_union(arr, [99, 100]) AS res"),
    ("array_max", "array_max(arr) AS res"),
    ("array_avg", "array_avg(arr) AS res"),
    ("array_min", "array_min(arr) AS res"),
    ("array_except", "array_except(arr, [20]) AS res"),
    ("repeat", "repeat('abc', 3) AS res"),
    ("sequence", "sequence(1, 5, 1) AS res"),
    ("array_cardinality", "array_cardinality(arr) AS res"),
    ("array_flatten", "array_flatten([[1, 2], [3, 4]]) AS res"),
    ("array_distinct", "array_distinct([1, 2, 2, 3]) AS res"),
    ("array_map", "array_map('abs', [-1, -2, 3]) AS res"),
    ("array_join", "array_join(arr, ',') AS res"),
    ("array_shuffle", "array_shuffle(arr) AS res"),
    ("array_concat", "array_concat(arr, [99]) AS res"),
    ("array_sort", "array_sort([3, 1, 2]) AS res"),
    ("kvpair_array_to_obj", "kvpair_array_to_obj([{'key': 'k1', 'value': 10}]) AS res"),
]

def main():
    print("=== Testing ALL Array Functions in array_functions.md ===")
    results = {}

    # Setup broker confKey and stream
    requests.put(f"{REKUIPER_URL}/metadata/sources/mqtt/confKeys/e2e_broker", json={"server": "tcp://kuiper-mosquitto:1883", "qos": 0})
    requests.delete(f"{REKUIPER_URL}/streams/array_test_stream")
    stream_sql = (
        'create stream array_test_stream () WITH ('
        '  DATASOURCE="devices/arr_test", FORMAT="json", TYPE="mqtt", CONF_KEY="e2e_broker"'
        ')'
    )
    requests.post(f"{REKUIPER_URL}/streams", json={"sql": stream_sql})

    for name, expr in ARRAY_TESTS:
        rule_id = f"rule_arr_{name}"
        requests.delete(f"{REKUIPER_URL}/rules/{rule_id}")

        sql = f"SELECT {expr} FROM array_test_stream"
        rule_payload = {
            "id": rule_id,
            "sql": sql,
            "actions": [{
                "mqtt": {
                    "server": "tcp://kuiper-mosquitto:1883",
                    "topic": f"sink/arr_{name}",
                    "sendSingle": True
                }
            }]
        }
        r = requests.post(f"{REKUIPER_URL}/rules", json=rule_payload)
        if r.status_code not in (200, 201):
            results[name] = {"supported": False, "status_code": r.status_code, "error": r.text.strip()}
            print(f"[-] {name:22}: UNSUPPORTED -> {r.status_code}: {r.text.strip()}")
            continue

        # If rule created, verify under live load
        sink_data = []
        def make_cb(storage):
            def cb(c, u, msg):
                try:
                    storage.append(json.loads(msg.payload.decode("utf-8")))
                except Exception:
                    storage.append(msg.payload.decode("utf-8"))
            return cb

        sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"sub_arr_{name}")
        sub.on_message = make_cb(sink_data)
        sub.connect(MQTT_HOST, MQTT_PORT, 60)
        sub.subscribe(f"sink/arr_{name}")
        sub.loop_start()

        time.sleep(0.3)

        pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"pub_arr_{name}")
        pub.connect(MQTT_HOST, MQTT_PORT, 60)
        test_payload = {"arr": [10, 20, 30, 40]}
        pub.publish("devices/arr_test", json.dumps(test_payload))
        pub.disconnect()

        time.sleep(0.8)
        sub.loop_stop()
        sub.disconnect()

        requests.delete(f"{REKUIPER_URL}/rules/{rule_id}")

        if sink_data:
            results[name] = {"supported": True, "data_verified": True, "output": sink_data[0]}
            print(f"[+] {name:22}: VERIFIED -> {sink_data[0]}")
        else:
            results[name] = {"supported": True, "data_verified": False, "note": "Rule accepted, but no sink data received"}
            print(f"[?] {name:22}: ACCEPTED (No sink data)")

    print("\n=== Array Function Test Scorecard ===")
    print(json.dumps(results, indent=2))
    with open("/mnt/c/Users/paanday/Documents/idacs/rekuiper/ekuiper/test/e2e_diff/array_results.json", "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()
