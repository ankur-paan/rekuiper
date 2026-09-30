import json
import time
import requests
import paho.mqtt.client as mqtt

REKUIPER_URL = "http://localhost:9081"
MQTT_HOST = "localhost"
MQTT_PORT = 1883

def run_tests():
    print("=== Testing ALL Methods in streams.md ===")
    results = {}

    # Cleanup any leftovers
    requests.delete(f"{REKUIPER_URL}/rules/rule_streams_test")
    requests.delete(f"{REKUIPER_URL}/streams/stream_lifecycle_test")

    # 1. POST /streams (create stream)
    # Setup broker confKey
    requests.put(f"{REKUIPER_URL}/metadata/sources/mqtt/confKeys/e2e_broker", json={"server": "tcp://kuiper-mosquitto:1883", "qos": 0})

    stream_sql = (
        "create stream stream_lifecycle_test ("
        "  device_id string, "
        "  val float, "
        "  ts bigint"
        ") WITH ("
        "  DATASOURCE=\"devices/lifecycle\", "
        "  FORMAT=\"json\", "
        "  TYPE=\"mqtt\", "
        "  CONF_KEY=\"e2e_broker\""
        ")"
    )
    r1 = requests.post(f"{REKUIPER_URL}/streams", json={"sql": stream_sql})
    print(f"1. POST /streams -> {r1.status_code}: {r1.text}")
    results["POST /streams"] = {"status": r1.status_code, "ok": r1.status_code in [200, 201]}

    # 2. GET /streams (list streams)
    r2 = requests.get(f"{REKUIPER_URL}/streams")
    print(f"2. GET /streams -> {r2.status_code}: {r2.text}")
    streams_list = r2.json() if r2.status_code == 200 else []
    results["GET /streams"] = {"status": r2.status_code, "contains_stream": "stream_lifecycle_test" in streams_list}

    # 3. GET /streamdetails (show streams detail)
    r3 = requests.get(f"{REKUIPER_URL}/streamdetails")
    print(f"3. GET /streamdetails -> {r3.status_code}: {r3.text}")
    results["GET /streamdetails"] = {"status": r3.status_code, "data": r3.json() if r3.status_code == 200 else r3.text}

    # 4. GET /streams/{id} (describe stream)
    r4 = requests.get(f"{REKUIPER_URL}/streams/stream_lifecycle_test")
    print(f"4. GET /streams/{{id}} -> {r4.status_code}: {r4.text}")
    results["GET /streams/{id}"] = {"status": r4.status_code, "data": r4.json() if r4.status_code == 200 else r4.text}

    # 5. GET /streams/{id}/schema (get stream schema)
    r5 = requests.get(f"{REKUIPER_URL}/streams/stream_lifecycle_test/schema")
    print(f"5. GET /streams/{{id}}/schema -> {r5.status_code}: {r5.text}")
    results["GET /streams/{id}/schema"] = {"status": r5.status_code, "data": r5.json() if r5.status_code == 200 else r5.text}

    # 6. Live telemetry verification under load
    print("\n--- Testing Live Telemetry Flow with Created Stream ---")
    sink_messages = []
    def on_message(c, u, msg):
        sink_messages.append(json.loads(msg.payload.decode()))

    sub_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="sub_streams_test")
    sub_client.on_message = on_message
    sub_client.connect(MQTT_HOST, MQTT_PORT, 60)
    sub_client.subscribe("sink/streams_lifecycle")
    sub_client.loop_start()

    rule_sql = "SELECT device_id, val * 2 AS double_val FROM stream_lifecycle_test WHERE val > 5"
    rule_payload = {
        "id": "rule_streams_test",
        "sql": rule_sql,
        "actions": [{
            "mqtt": {
                "server": "tcp://kuiper-mosquitto:1883",
                "topic": "sink/streams_lifecycle",
                "sendSingle": True
            }
        }]
    }
    r_rule = requests.post(f"{REKUIPER_URL}/rules", json=rule_payload)
    print(f"Rule creation status: {r_rule.status_code}: {r_rule.text}")
    time.sleep(1.5)

    # Ingest 50 real telemetry points
    pub_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="pub_streams_test")
    pub_client.connect(MQTT_HOST, MQTT_PORT, 60)
    for i in range(50):
        pub_client.publish("devices/lifecycle", json.dumps({"device_id": f"dev_{i}", "val": float(i), "ts": int(time.time()*1000)}))
    pub_client.disconnect()
    time.sleep(2)

    print(f"Received {len(sink_messages)} messages at sink (expected ~44 where val > 5)")
    results["Live Data Flow on Created Stream"] = {
        "sent": 50,
        "received": len(sink_messages),
        "sample": sink_messages[:2] if sink_messages else None
    }

    # 7. PUT /streams/{id} (update stream definition)
    updated_sql = (
        "create stream stream_lifecycle_test ("
        "  device_id string, "
        "  val float, "
        "  tag string"
        ") WITH ("
        "  DATASOURCE=\"devices/lifecycle_v2\", "
        "  FORMAT=\"json\", "
        "  TYPE=\"mqtt\""
        ")"
    )
    r7 = requests.put(f"{REKUIPER_URL}/streams/stream_lifecycle_test", json={"sql": updated_sql})
    print(f"7. PUT /streams/{{id}} -> {r7.status_code}: {r7.text}")
    results["PUT /streams/{id}"] = {"status": r7.status_code, "data": r7.text}

    # Verify updated stream description
    r7_desc = requests.get(f"{REKUIPER_URL}/streams/stream_lifecycle_test")
    print(f"Verified PUT update: {r7_desc.status_code}: {r7_desc.text}")

    # 8. Clean up rule and test DELETE /streams/{id}
    requests.delete(f"{REKUIPER_URL}/rules/rule_streams_test")
    time.sleep(1)
    r8 = requests.delete(f"{REKUIPER_URL}/streams/stream_lifecycle_test")
    print(f"8. DELETE /streams/{{id}} -> {r8.status_code}: {r8.text}")
    results["DELETE /streams/{id}"] = {"status": r8.status_code, "ok": r8.status_code in [200, 204]}

    # 9. Verify 404 on deleted stream
    r9 = requests.get(f"{REKUIPER_URL}/streams/stream_lifecycle_test")
    print(f"9. GET after DELETE -> {r9.status_code}: {r9.text}")
    results["GET after DELETE (404 expected)"] = {"status": r9.status_code}

    sub_client.loop_stop()
    sub_client.disconnect()

    print("\n=== Final Test Results for streams.md ===")
    print(json.dumps(results, indent=2))
    return results

if __name__ == "__main__":
    run_tests()
