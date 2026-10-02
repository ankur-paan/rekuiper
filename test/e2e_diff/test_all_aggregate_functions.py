import json
import time
import requests
import paho.mqtt.client as mqtt

REKUIPER_URL = "http://localhost:9081"
MQTT_HOST = "localhost"
MQTT_PORT = 1883

AGG_FUNCS = [
    # (func_name, select_expr, requires_special_setup)
    ("avg", "avg(val) AS res"),
    ("count", "count(val) AS res"),
    ("count_wildcard", "count(*) AS res"),
    ("max", "max(val) AS res"),
    ("min", "min(val) AS res"),
    ("sum", "sum(val) AS res"),
    ("collect", "collect(val) AS res"),
    ("collect_wildcard", "collect(*) AS res"),
    ("last_value", "last_value(val, true) AS res"),
    ("merge_agg", "merge_agg(obj) AS res"),
    ("deduplicate", "deduplicate(val, false) AS res"),
    ("median", "median(val) AS res"),
    ("stddev", "stddev(val) AS res"),
    ("stddevs", "stddevs(val) AS res"),
    ("var", "var(val) AS res"),
    ("vars", "vars(val) AS res"),
    ("percentile", "percentile(val, 0.5) AS res"),
    ("percentile_disc", "percentile_disc(val, 0.5) AS res"),
    ("last_agg_hit_count", "last_agg_hit_count() AS res"),
    ("last_agg_hit_time", "last_agg_hit_time() AS res"),
]

def main():
    print("=== Testing ALL Aggregate Functions in aggregate_functions.md ===")
    results = {}

    # Setup broker confKey and stream
    requests.put(f"{REKUIPER_URL}/metadata/sources/mqtt/confKeys/e2e_broker", json={"server": "tcp://kuiper-mosquitto:1883", "qos": 0})
    requests.delete(f"{REKUIPER_URL}/streams/agg_test_stream")
    stream_sql = (
        'create stream agg_test_stream () WITH ('
        '  DATASOURCE="devices/agg_test", FORMAT="json", TYPE="mqtt", CONF_KEY="e2e_broker"'
        ')'
    )
    requests.post(f"{REKUIPER_URL}/streams", json={"sql": stream_sql})

    for name, expr in AGG_FUNCS:
        rule_id = f"rule_agg_{name}"
        requests.delete(f"{REKUIPER_URL}/rules/{rule_id}")

        sql = f"SELECT {expr} FROM agg_test_stream GROUP BY TumblingWindow(ss, 1)"
        rule_payload = {
            "id": rule_id,
            "sql": sql,
            "actions": [{
                "mqtt": {
                    "server": "tcp://kuiper-mosquitto:1883",
                    "topic": f"sink/agg_{name}",
                    "sendSingle": True
                }
            }]
        }
        r = requests.post(f"{REKUIPER_URL}/rules", json=rule_payload)
        if r.status_code not in (200, 201):
            results[name] = {"supported": False, "status_code": r.status_code, "error": r.text.strip()}
            print(f"[-] {name:20}: UNSUPPORTED -> {r.status_code}: {r.text.strip()}")
            continue

        # If rule was created, test with real data!
        sink_data = []
        def make_cb(storage):
            def cb(c, u, msg):
                try:
                    storage.append(json.loads(msg.payload.decode("utf-8")))
                except Exception as e:
                    storage.append(msg.payload.decode("utf-8"))
            return cb

        sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"sub_agg_{name}")
        sub.on_message = make_cb(sink_data)
        sub.connect(MQTT_HOST, MQTT_PORT, 60)
        sub.subscribe(f"sink/agg_{name}")
        sub.loop_start()

        time.sleep(0.5)

        # Publish 5 data points
        pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"pub_agg_{name}")
        pub.connect(MQTT_HOST, MQTT_PORT, 60)
        for i in [10, 20, 30, 40, 50]:
            payload = {"val": float(i), "obj": {"k": f"v_{i}"}}
            pub.publish("devices/agg_test", json.dumps(payload))
            time.sleep(0.05)
        pub.disconnect()

        # Wait for window trigger (TumblingWindow ss 1)
        time.sleep(2.0)
        sub.loop_stop()
        sub.disconnect()

        requests.delete(f"{REKUIPER_URL}/rules/{rule_id}")

        if sink_data:
            results[name] = {"supported": True, "data_verified": True, "output": sink_data[0]}
            print(f"[+] {name:20}: VERIFIED -> {sink_data[0]}")
        else:
            results[name] = {"supported": True, "data_verified": False, "note": "Rule accepted, but no sink data received in window"}
            print(f"[?] {name:20}: ACCEPTED (No sink data)")

    print("\n=== Aggregation Test Scorecard ===")
    print(json.dumps(results, indent=2))
    with open("/mnt/c/Users/paanday/Documents/idacs/rekuiper/ekuiper/test/e2e_diff/agg_results.json", "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()
