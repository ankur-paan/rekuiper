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
    print("=====================================================")
    print("EXHAUSTIVE METHOD-BY-METHOD VERIFICATION: rules.md")
    print("=====================================================")
    results = {}

    # Setup broker confKey
    http_req("/metadata/sources/mqtt/confKeys/e2e_broker", "PUT", {"server": "tcp://kuiper-mosquitto:1883", "qos": 0})
    
    # Setup stream
    http_req("/streams/rules_test_stream", "DELETE")
    c, r = http_req("/streams", "POST", {
        "sql": 'create stream rules_test_stream () WITH (FORMAT="json", TYPE="mqtt", DATASOURCE="devices/rules_test", CONF_KEY="e2e_broker")'
    })
    assert c in (200, 201), f"Stream create failed: {c}"

    sink_msgs = []
    def on_msg(c, u, m):
        sink_msgs.append(json.loads(m.payload.decode("utf-8")))
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "sub_rules_exhaustive")
    sub.on_message = on_msg
    sub.connect(BROKER_HOST, BROKER_PORT, 60)
    sub.subscribe("out/rules_sink/#")
    sub.loop_start()

    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "pub_rules_exhaustive")
    pub.connect(BROKER_HOST, BROKER_PORT, 60)

    # 1. Method: POST /rules (create a rule)
    rule_id = "test_exhaustive_rule"
    http_req(f"/rules/{rule_id}", "DELETE")
    rule_body = {
        "id": rule_id,
        "sql": "SELECT device_id, (temp * 1.8 + 32.0) AS temp_f FROM rules_test_stream WHERE temp > 20.0",
        "actions": [{
            "mqtt": {
                "server": "tcp://kuiper-mosquitto:1883",
                "topic": "out/rules_sink/out1",
                "sendSingle": True
            }
        }]
    }
    c, r = http_req("/rules", "POST", rule_body)
    results["POST /rules"] = (c in (200, 201), f"Code: {c}")

    time.sleep(1.5)
    # Stream 50 records under load
    sink_msgs.clear()
    for i in range(1, 51):
        pub.publish("devices/rules_test", json.dumps({"device_id": f"D_{i:02d}", "temp": 18.0 + (i * 0.2)}).encode("utf-8"), qos=0)
        time.sleep(0.005)
    time.sleep(2.0)
    data_verified = (len(sink_msgs) == 40 and abs(sink_msgs[0]["temp_f"] - 68.36) < 0.1)
    results["POST /rules (Data Flow Under Load)"] = (data_verified, f"Received {len(sink_msgs)}/40 matching records, sample: {sink_msgs[0] if sink_msgs else None}")

    # 2. Method: GET /rules (show rules)
    c, r = http_req("/rules")
    has_rule = (c == 200 and any(rule_id in str(item) for item in r))
    results["GET /rules"] = (has_rule, f"Code: {c}, Rules: {r}")

    # 3. Method: GET /rules/{id} (describe a rule)
    c, r = http_req(f"/rules/{rule_id}")
    valid_desc = (c == 200 and ("rules_test_stream" in str(r.get("sql", ""))))
    results["GET /rules/{id}"] = (valid_desc, f"Code: {c}, SQL: {r.get('sql') if isinstance(r, dict) else r}")

    # 4. Method: GET /rules/{id}/status (status of a rule)
    c, r = http_req(f"/rules/{rule_id}/status")
    valid_status = (c == 200 and r.get("sourceRecordsInTotal") == 50 and r.get("sinkRecordsOutTotal") == 40)
    results["GET /rules/{id}/status"] = (valid_status, f"In: {r.get('sourceRecordsInTotal')}, Out: {r.get('sinkRecordsOutTotal')}, Filtered: {r.get('sourceRecordsFilteredTotal')}")

    # 5. Method: GET /rules/status/all (status of all rules)
    c, r = http_req("/rules/status/all")
    results["GET /rules/status/all"] = (c in (200, 204), f"Code: {c}, Response: {str(r)[:60]}")

    # 6. Method: GET /rules/{id}/topo (topology structure of a rule)
    c, r = http_req(f"/rules/{rule_id}/topo")
    results["GET /rules/{id}/topo"] = (c in (200, 204), f"Code: {c}, Topo: {str(r)[:60]}")

    # 7. Method: POST /rules/{id}/stop (stop a rule)
    c, r = http_req(f"/rules/{rule_id}/stop", "POST")
    results["POST /rules/{id}/stop"] = (c in (200, 204), f"Code: {c}")
    time.sleep(1.0)
    # Stream while stopped - should NOT emit to sink
    sink_msgs.clear()
    for i in range(1, 11):
        pub.publish("devices/rules_test", json.dumps({"device_id": f"D_{i:02d}", "temp": 30.0}).encode("utf-8"), qos=0)
    time.sleep(1.0)
    results["Rule Stopped (Zero Egress Verified)"] = (len(sink_msgs) == 0, f"Messages emitted while stopped: {len(sink_msgs)}")

    # 8. Method: POST /rules/{id}/start (start a rule)
    c, r = http_req(f"/rules/{rule_id}/start", "POST")
    results["POST /rules/{id}/start"] = (c in (200, 204), f"Code: {c}")
    time.sleep(1.5)
    # Stream while started - should resume emitting
    sink_msgs.clear()
    for i in range(1, 11):
        pub.publish("devices/rules_test", json.dumps({"device_id": f"D_{i:02d}", "temp": 30.0}).encode("utf-8"), qos=0)
    time.sleep(1.5)
    results["Rule Resumed (Data Flow Verified)"] = (len(sink_msgs) == 10, f"Messages emitted after restart: {len(sink_msgs)}")

    # 9. Method: POST /rules/{id}/restart (restart a rule)
    c, r = http_req(f"/rules/{rule_id}/restart", "POST")
    results["POST /rules/{id}/restart"] = (c in (200, 204), f"Code: {c}")
    time.sleep(1.5)

    # 10. Method: PUT /rules/{id} (upsert / update a rule)
    rule_updated = {
        "id": rule_id,
        "sql": "SELECT device_id, (temp * 2.0) AS temp_doubled FROM rules_test_stream WHERE temp > 25.0",
        "actions": [{
            "mqtt": {
                "server": "tcp://kuiper-mosquitto:1883",
                "topic": "out/rules_sink/out1",
                "sendSingle": True
            }
        }]
    }
    c, r = http_req(f"/rules/{rule_id}", "PUT", rule_updated)
    results["PUT /rules/{id}"] = (c in (200, 204), f"Code: {c}")
    time.sleep(1.5)
    sink_msgs.clear()
    for i in range(1, 31):  # temp from 20.2 to 26.0 (only temp > 25.0 will pass = 5 records)
        pub.publish("devices/rules_test", json.dumps({"device_id": f"D_{i:02d}", "temp": 20.0 + (i * 0.2)}).encode("utf-8"), qos=0)
        time.sleep(0.005)
    time.sleep(2.0)
    put_data_ok = (len(sink_msgs) == 5 and "temp_doubled" in sink_msgs[0])
    results["PUT /rules/{id} (Updated Logic Verified)"] = (put_data_ok, f"Received {len(sink_msgs)}/5 records with updated formula: {sink_msgs[0] if sink_msgs else None}")

    # 11. Method: DELETE /rules/{id} (drop a rule)
    c, r = http_req(f"/rules/{rule_id}", "DELETE")
    results["DELETE /rules/{id}"] = (c in (200, 204), f"Code: {c}")
    time.sleep(0.5)
    c, r = http_req(f"/rules/{rule_id}")
    results["Rule Dropped (404 Confirmed)"] = (c == 404, f"GET returned {c}")

    # Cleanup
    http_req("/streams/rules_test_stream", "DELETE")
    sub.loop_stop()
    pub.disconnect()

    print("\n--- RESULTS FOR rules.md ---")
    all_ok = True
    for method, (ok, note) in results.items():
        status_str = "PASS" if ok else "FAIL"
        print(f"[{status_str:4s}] {method:45s} -> {note}")
        if not ok:
            all_ok = False
    print("-----------------------------------------------------")
    print(f"Overall rules.md verification: {'SUCCESSFUL' if all_ok else 'PENDING'}")
    return all_ok

if __name__ == "__main__":
    main()
