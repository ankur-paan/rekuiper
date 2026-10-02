# Migration from rekuiper 1.x to 2.x

This guide describes breaking changes and migration procedures when upgrading from rekuiper 1.x to 2.x.

## Breaking Changes

### SQLite Database Storage Format

eKuiper 2.x changes the internal storage schema for streams and tables in the SQLite database (`sqliteKV.db`):

- The 2.x engine cannot deserialize stream and table definitions stored by 1.x.
- Querying 1.x definitions in 2.x triggers deserialization errors: `error unmarshall <name>, the data in db may be corrupted`.

#### Storage Format Comparison

| Resource | eKuiper 1.x Schema | eKuiper 2.x Schema |
| :--- | :--- | :--- |
| Streams | Plain SQL text string | JSON object with `streamType`, `streamKind`, and `statement` |
| Tables | Plain SQL text string | JSON object with `streamType`, `streamKind`, and `statement` |
| Rules | JSON object containing `triggered` field | JSON object without `triggered` field |

## Migration Procedures

### Option 1: Clean Installation (Recommended)

To start with a clean state:

1. Export existing configurations from version 1.x:

```bash
curl http://localhost:9081/data/export > backup.json
```

2. Stop the container or process:

```bash
docker stop ekuiper
```

3. Remove the legacy database file:

```bash
rm -rf /kuiper/data/sqliteKV.db
```

4. Start the 2.x engine:

```bash
docker start ekuiper
```

5. Recreate streams and rules using the REST API, CLI, or ruleset import.

### Option 2: Configure a New Database Filename

To maintain the legacy database file for rollback:

1. Update `etc/kuiper.yaml` before upgrading:

```yaml
store:
  sqlite:
    name: sqliteKV-v2.db
```

2. Start the 2.x instance. The engine initializes `sqliteKV-v2.db` without modifying `sqliteKV.db`.

### Option 3: Delete Incompatible Entries via REST API

If you upgraded an existing database, delete legacy stream and table entries:

```bash
# Delete the legacy stream
curl -X DELETE http://localhost:9081/streams/<stream_name>

# Delete the legacy table
curl -X DELETE http://localhost:9081/tables/<table_name>

# Recreate the stream with 2.x formatting
curl -X POST http://localhost:9081/streams \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE STREAM my_stream () WITH (DATASOURCE=\"topic\", FORMAT=\"JSON\", TYPE=\"mqtt\")"}'
```

### Option 4: Direct SQLite Database Cleanup

For bulk remediation, execute SQL directly against the SQLite database:

```bash
# List stored streams
sqlite3 /kuiper/data/sqliteKV.db "SELECT key FROM stream;"

# Delete a specific legacy stream
sqlite3 /kuiper/data/sqliteKV.db "DELETE FROM stream WHERE key = 'my_stream';"

# Restart the server
docker restart ekuiper
```

## Additional Guidelines

- Clean installations of 2.x are not affected by this schema change.
- Always create a full backup of the `data/` directory before running upgrades.

## Cross References

- [Installation Guide](../installation.md)
- [REST API Reference](../api/restapi/overview.md)
- [Data Export API](../api/restapi/data.md#export-data)
