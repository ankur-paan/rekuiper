import urllib.request
import json

EKUIPER_URL = "http://127.0.0.1:9091"

# Ensure stream exists on eKuiper
try:
    urllib.request.urlopen(urllib.request.Request(f"{EKUIPER_URL}/streams", data=json.dumps({"sql": "create stream telemetry () WITH (FORMAT='json', TYPE='mqtt', DATASOURCE='demo')"}).encode(), headers={"Content-Type": "application/json"}))
except:
    pass

exprs_rule2 = [
    ("upper", "upper(status)"),
    ("lower", "lower(device_id)"),
    ("concat", "concat(device_id, ':', status)"),
    ("length", "length(status)"),
    ("trim", "trim(padded_str)"),
    ("ltrim", "ltrim(padded_str)"),
    ("rtrim", "rtrim(padded_str)"),
    ("regexp_replace", "regexp_replace(status, 'ACTIVE', 'RUNNING')"),
    ("replace", "replace(status, 'ACTIVE', 'OK')"),
    ("reverse", "reverse(device_id)"),
    ("split_value", "split_value(tags_csv, ',', 0)"),
    ("numbytes", "numbytes(device_id)"),
    ("startswith", "startswith(device_id, 'DEV')"),
    ("endswith", "endswith(device_id, '001')"),
    ("substring", "substring(device_id, 1, 3)"),
    ("lpad", "lpad(device_id, 10)"),
    ("rpad", "rpad(device_id, 10)"),
    ("indexof", "indexof(device_id, 'DEV')"),
    ("format", "format(pressure, 2)"),
    ("format_time", "format_time(ts, 'yyyy-MM-dd')"),
]

print("=== CHECKING RULE 2 EXPRS ON EKUIPER ===")
for name, expr in exprs_rule2:
    sql = f"SELECT {expr} FROM telemetry"
    body = {"id": "test_ek_rule", "sql": sql, "actions": [{"log": {}}]}
    try:
        urllib.request.urlopen(urllib.request.Request(f"{EKUIPER_URL}/rules", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}))
        urllib.request.urlopen(urllib.request.Request(f"{EKUIPER_URL}/rules/test_ek_rule", method="DELETE"))
        print(f"[EK OK] {name}")
    except urllib.error.HTTPError as e:
        print(f"[EK ERR] {name}: {e.code} -> {e.read().decode()[:80]}")

exprs_rule5 = [
    ("readings[0]", "readings[0]"),
    ("cardinality", "cardinality(readings)"),
    ("array_contains", "array_contains(readings, 20.0)"),
    ("array_position", "array_position(readings, 20.0)"),
    ("array_last_position", "array_last_position(readings, 20.0)"),
    ("array_min", "array_min(readings)"),
    ("array_max", "array_max(readings)"),
    ("array_avg", "array_avg(readings)"),
    ("array_join", "array_join(readings, ',')"),
    ("array_sort", "array_sort(readings)"),
    ("repeat", "repeat('abc', 2)"),
    ("sequence", "sequence(1, 4, 1)")
]

print("\n=== CHECKING RULE 5 EXPRS ON EKUIPER ===")
for name, expr in exprs_rule5:
    sql = f"SELECT {expr} FROM telemetry"
    body = {"id": "test_ek_rule", "sql": sql, "actions": [{"log": {}}]}
    try:
        urllib.request.urlopen(urllib.request.Request(f"{EKUIPER_URL}/rules", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}))
        urllib.request.urlopen(urllib.request.Request(f"{EKUIPER_URL}/rules/test_ek_rule", method="DELETE"))
        print(f"[EK OK] {name}")
    except urllib.error.HTTPError as e:
        print(f"[EK ERR] {name}: {e.code} -> {e.read().decode()[:80]}")
