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

datetime_funcs = [
    ("now", "now()"),
    ("current_timestamp", "current_timestamp()"),
    ("local_time", "local_time()"),
    ("local_timestamp", "local_timestamp()"),
    ("cur_date", "cur_date()"),
    ("current_date", "current_date()"),
    ("cur_time", "cur_time()"),
    ("current_time", "current_time()"),
    ("format_time", "format_time(ts, 'YYYY-MM-dd')"),
    ("date_calc", "date_calc('2024-01-01', '1h')"),
    ("date_add", "date_add('day', 1, ts)"),
    ("date_diff_3arg", "date_diff('day', ts1, ts2)"),
    ("date_diff_2arg", "date_diff(ts1, ts2)"),
    ("day_name", "day_name(ts)"),
    ("day_of_month", "day_of_month(ts)"),
    ("day", "day(ts)"),
    ("day_of_week", "day_of_week(ts)"),
    ("day_of_year", "day_of_year(ts)"),
    ("from_days", "from_days(737060)"),
    ("from_unix_time", "from_unix_time(1704067200000)"),
    ("hour", "hour(ts)"),
    ("last_day", "last_day(ts)"),
    ("microsecond", "microsecond(ts)"),
    ("minute", "minute(ts)"),
    ("month", "month(ts)"),
    ("month_name", "month_name(ts)"),
    ("second", "second(ts)")
]

def main():
    print("=================================================================")
    print("EXHAUSTIVE FUNCTION-BY-FUNCTION VERIFICATION: datetime_functions.md")
    print("=================================================================")
    
    stream_name = "dt_exhaustive_stream"
    http_req(f"/streams/{stream_name}", "DELETE")
    c, r = http_req("/streams", "POST", {
        "sql": f'create stream {stream_name} () WITH (DATASOURCE="devices/dt_ex", FORMAT="json", TYPE="mqtt", SERVER="tcp://127.0.0.1:1883")'
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

    sub = make_client("sub_dt_ex")
    sub.on_message = on_msg
    sub.connect(BROKER_HOST, BROKER_PORT, 60)
    sub.subscribe("out/dt_ex/#")
    sub.loop_start()

    pub = make_client("pub_dt_ex")
    pub.connect(BROKER_HOST, BROKER_PORT, 60)

    for fname, expr in datetime_funcs:
        rid = f"rule_dt_{fname}"
        topic = f"out/dt_ex/{fname}"
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

        time.sleep(0.6)
        sink_msgs.clear()
        
        # Stream 5 real data points (ts = 1704067200000 ms is 2024-01-01 00:00:00 UTC, a Monday)
        for i in range(1, 6):
            pub.publish("devices/dt_ex", json.dumps({
                "ts": 1704067200000 + (i * 3600000),
                "ts1": 1704067200000,
                "ts2": 1704067200000 + 86400000 * 5
            }).encode("utf-8"), qos=0)
            time.sleep(0.005)

        time.sleep(1.0)
        matched = [m for m in sink_msgs if "res" in m]
        if len(matched) > 0 and matched[0]["res"] is not None:
            results[fname] = ("verified", f"Live Computed: {matched[0]['res']}")
        else:
            results[fname] = ("broken", f"No Output or Null (Received: {len(sink_msgs)}, payload: {sink_msgs[:1]})")

        http_req(f"/rules/{rid}", "DELETE")

    sub.loop_stop()
    pub.disconnect()
    http_req(f"/streams/{stream_name}", "DELETE")

    print("\n--- RESULTS FOR datetime_functions.md ---")
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
        print(f"[{status_str:6s}] {fname:16s} ({dict(datetime_funcs)[fname]:35s}) -> {note}")
    print("-----------------------------------------------------------------")
    print(f"Summary: {pass_cnt} VERIFIED, {unsupp_cnt} UNSUPPORTED, {fail_cnt} BROKEN out of {len(datetime_funcs)} functions.")

if __name__ == "__main__":
    main()
