use rekuiper_core::{
    KvStore, MemKvStore, RuleDefinition, RuleManager, SqliteKvStore, StreamBus, StreamDefinition,
    StreamManager, TableDefinition, TableManager,
};
use std::collections::HashMap;
use std::sync::Arc;

fn temp_db_path(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "rekuiper-kv-test-{}-{}-{}.db",
        tag,
        std::process::id(),
        nanos
    ));
    path
}

fn stream_def(name: &str) -> StreamDefinition {
    StreamDefinition {
        name: name.to_string(),
        sql: format!("CREATE STREAM {} () WITH (FORMAT=\"json\")", name),
        options: HashMap::new(),
    }
}

fn table_def(name: &str) -> TableDefinition {
    TableDefinition {
        name: name.to_string(),
        sql: format!("CREATE TABLE {} () WITH (FORMAT=\"json\")", name),
        options: HashMap::new(),
    }
}

fn rule_def(id: &str) -> RuleDefinition {
    RuleDefinition {
        id: id.to_string(),
        sql: "SELECT * FROM demo".to_string(),
        actions: vec![],
        options: None,
        graph: None,
    }
}

#[tokio::test]
async fn test_kv_basic_operations() {
    let path = temp_db_path("basic");
    let _ = tokio::fs::remove_file(&path).await;
    let kv = SqliteKvStore::new(&path).await.unwrap();

    assert_eq!(kv.get("ns", "k").await.unwrap(), None);

    kv.set("ns", "k", "v1").await.unwrap();
    assert_eq!(kv.get("ns", "k").await.unwrap(), Some("v1".to_string()));

    // Upsert overwrites.
    kv.set("ns", "k", "v2").await.unwrap();
    assert_eq!(kv.get("ns", "k").await.unwrap(), Some("v2".to_string()));

    kv.set("ns", "other", "x").await.unwrap();
    kv.set("elsewhere", "k", "y").await.unwrap();
    let mut listed = kv.list_all("ns").await.unwrap();
    listed.sort();
    assert_eq!(
        listed,
        vec![
            ("k".to_string(), "v2".to_string()),
            ("other".to_string(), "x".to_string())
        ]
    );

    kv.delete("ns", "k").await.unwrap();
    assert_eq!(kv.get("ns", "k").await.unwrap(), None);
    assert_eq!(kv.list_all("elsewhere").await.unwrap().len(), 1);

    // MemKvStore obeys the same contract.
    let mem = MemKvStore::new();
    mem.set("ns", "k", "v").await.unwrap();
    assert_eq!(mem.get("ns", "k").await.unwrap(), Some("v".to_string()));
    mem.delete("ns", "k").await.unwrap();
    assert_eq!(mem.get("ns", "k").await.unwrap(), None);

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn test_manager_persistence_and_restart() {
    let path = temp_db_path("restart");
    let _ = tokio::fs::remove_file(&path).await;

    // First "boot": managers backed by the SQLite file.
    let kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&path).await.unwrap());
    let streams = StreamManager::new_with_kv(kv.clone());
    let tables = TableManager::new_with_kv(kv.clone());
    let bus = StreamBus::new();
    let rules = RuleManager::new_with_kv(bus, kv.clone());

    streams.create_stream(stream_def("demo")).await.unwrap();
    tables.create_table(table_def("alerts")).await.unwrap();
    rules.create_rule(rule_def("rule_running")).await.unwrap();
    rules.create_rule(rule_def("rule_paused")).await.unwrap();
    rules.stop_rule("rule_paused").await.unwrap();

    // Simulate restart: brand-new managers over the same file.
    drop((streams, tables, rules));
    let kv2: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&path).await.unwrap());
    let streams2 = StreamManager::new_with_kv(kv2.clone());
    let tables2 = TableManager::new_with_kv(kv2.clone());
    let bus2 = StreamBus::new();
    let rules2 = RuleManager::new_with_kv(bus2, kv2.clone());

    streams2.load_from_kv(&kv2).await.unwrap();
    tables2.load_from_kv(&kv2).await.unwrap();
    rules2.load_from_kv(&kv2).await.unwrap();

    // Definitions survived.
    assert!(streams2.get_stream("demo").is_some());
    assert!(tables2.get_table("alerts").is_some());
    assert!(rules2.get_rule("rule_running").is_some());
    assert!(rules2.get_rule("rule_paused").is_some());

    // Lifecycle statuses survived: running rules resume, stopped ones stay.
    let running = rules2.get_rule_status("rule_running").unwrap();
    assert_eq!(running.status, "running");
    let paused = rules2.get_rule_status("rule_paused").unwrap();
    assert_eq!(paused.status, "stopped");

    // Deletions propagate too: delete, "restart" again, confirm absence.
    rules2.delete_rule("rule_paused").await.unwrap();
    streams2.delete_stream("demo").await.unwrap();
    drop((streams2, tables2, rules2));
    let kv3: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&path).await.unwrap());
    let streams3 = StreamManager::new_with_kv(kv3.clone());
    let rules3 = RuleManager::new_with_kv(StreamBus::new(), kv3.clone());
    streams3.load_from_kv(&kv3).await.unwrap();
    rules3.load_from_kv(&kv3).await.unwrap();
    assert!(streams3.get_stream("demo").is_none());
    assert!(rules3.get_rule("rule_paused").is_none());
    assert!(rules3.get_rule("rule_running").is_some());

    let _ = tokio::fs::remove_file(&path).await;
}
