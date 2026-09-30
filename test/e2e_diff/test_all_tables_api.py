import json
import requests

REKUIPER_URL = "http://localhost:9081"

def test_tables():
    print("=== Testing ALL Methods in tables.md ===")
    results = {}

    # Cleanup
    requests.delete(f"{REKUIPER_URL}/tables/test_table")

    # 1. POST /tables (create table)
    table_sql = (
        'CREATE TABLE test_table (id BIGINT, name STRING, score FLOAT) '
        'WITH (DATASOURCE="test_table_ds", FORMAT="json", TYPE="mqtt", CONF_KEY="e2e_broker", KEY="id")'
    )
    r1 = requests.post(f"{REKUIPER_URL}/tables", json={"sql": table_sql})
    print(f"1. POST /tables -> {r1.status_code}: {r1.text}")
    results["POST /tables"] = {"status": r1.status_code, "text": r1.text}

    # 2. GET /tables (show tables)
    r2 = requests.get(f"{REKUIPER_URL}/tables")
    print(f"2. GET /tables -> {r2.status_code}: {r2.text}")
    results["GET /tables"] = {"status": r2.status_code, "data": r2.json() if r2.status_code == 200 else r2.text}

    # 2b. GET /tables?kind=lookup & ?kind=scan
    r2_lookup = requests.get(f"{REKUIPER_URL}/tables?kind=lookup")
    print(f"2b. GET /tables?kind=lookup -> {r2_lookup.status_code}: {r2_lookup.text}")
    results["GET /tables?kind=lookup"] = {"status": r2_lookup.status_code, "data": r2_lookup.text}

    # 3. GET /tabledetails
    r3 = requests.get(f"{REKUIPER_URL}/tabledetails")
    print(f"3. GET /tabledetails -> {r3.status_code}: {r3.text}")
    results["GET /tabledetails"] = {"status": r3.status_code, "data": r3.json() if r3.status_code == 200 else r3.text}

    # 4. GET /tables/{id} (describe table)
    r4 = requests.get(f"{REKUIPER_URL}/tables/test_table")
    print(f"4. GET /tables/{{id}} -> {r4.status_code}: {r4.text}")
    results["GET /tables/{id}"] = {"status": r4.status_code, "data": r4.json() if r4.status_code == 200 else r4.text}

    # 5. GET /tables/{id}/schema
    r5 = requests.get(f"{REKUIPER_URL}/tables/test_table/schema")
    print(f"5. GET /tables/{{id}}/schema -> {r5.status_code}: {r5.text}")
    results["GET /tables/{id}/schema"] = {"status": r5.status_code, "data": r5.json() if r5.status_code == 200 else r5.text}

    # 6. PUT /tables/{id} (update table)
    updated_sql = (
        'CREATE TABLE test_table (id BIGINT, name STRING, score FLOAT, active BOOLEAN) '
        'WITH (DATASOURCE="test_table_ds_v2", FORMAT="json", TYPE="mqtt", CONF_KEY="e2e_broker", KEY="id")'
    )
    r6 = requests.put(f"{REKUIPER_URL}/tables/test_table", json={"sql": updated_sql})
    print(f"6. PUT /tables/{{id}} -> {r6.status_code}: {r6.text}")
    results["PUT /tables/{id}"] = {"status": r6.status_code, "text": r6.text}

    # 7. DELETE /tables/{id} (drop table)
    r7 = requests.delete(f"{REKUIPER_URL}/tables/test_table")
    print(f"7. DELETE /tables/{{id}} -> {r7.status_code}: {r7.text}")
    results["DELETE /tables/{id}"] = {"status": r7.status_code, "text": r7.text}

    # 8. GET /tables/{id} after delete (verify 400/404)
    r8 = requests.get(f"{REKUIPER_URL}/tables/test_table")
    print(f"8. GET /tables/{{id}} after DELETE -> {r8.status_code}: {r8.text}")
    results["GET after DELETE"] = {"status": r8.status_code, "text": r8.text}

    # 9. GET /rules/{rule}/scantables
    # Create stream, rule, and check scantables
    requests.post(f"{REKUIPER_URL}/streams", json={"sql": 'create stream scan_st () WITH (DATASOURCE="scan_ds", FORMAT="json", TYPE="mqtt")'})
    requests.post(f"{REKUIPER_URL}/rules", json={"id": "rule_scan_test", "sql": "SELECT * FROM scan_st", "actions": [{"log": {}}]})
    r9 = requests.get(f"{REKUIPER_URL}/rules/rule_scan_test/scantables")
    print(f"9. GET /rules/{{rule}}/scantables -> {r9.status_code}: {r9.text}")
    results["GET /rules/{rule}/scantables"] = {"status": r9.status_code, "data": r9.json() if r9.status_code == 200 else r9.text}
    requests.delete(f"{REKUIPER_URL}/rules/rule_scan_test")
    requests.delete(f"{REKUIPER_URL}/streams/scan_st")

    print("\n=== Final Table Test Results ===")
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    test_tables()
