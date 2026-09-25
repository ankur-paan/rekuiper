use anyhow::Result;
use async_trait::async_trait;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{KvStore, RuleManager, SqliteKvStore, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{create_router, load_config_maps, restore_running_rules, AppState};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;

struct TestServer {
    base_url: String,
    state: AppState,
    handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start(kv: Arc<dyn KvStore>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream_bus = StreamBus::new();
        let stream_manager = StreamManager::new_with_kv(kv.clone());
        let table_manager = TableManager::new_with_kv(kv.clone());
        let rule_manager = RuleManager::new_with_kv(stream_bus.clone(), kv.clone());

        let _ = stream_manager.load_from_kv(&kv).await;
        let _ = table_manager.load_from_kv(&kv).await;
        let _ = rule_manager.load_from_kv(&kv).await;

        let state = AppState {
            kv: Some(kv),
            ..AppState::new(
                "test".to_string(),
                KuiperConfig::default(),
                stream_manager,
                table_manager,
                rule_manager,
                stream_bus,
            )
        };

        load_config_maps(&state).await.unwrap();
        restore_running_rules(&state).await;

        let app = create_router(state.clone());
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            base_url: format!("http://{}", addr),
            state,
            handle,
        }
    }

    async fn shutdown(self) {
        self.handle.abort();
    }
}

struct TempDb(PathBuf);
impl TempDb {
    fn new(name: &str) -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "rekuiper_cfg_db_{}_{}.db",
            name,
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::remove_file(&p);
        Self(p)
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let mut wal = self.0.clone();
        wal.set_extension("db-wal");
        let _ = std::fs::remove_file(&wal);
        let mut shm = self.0.clone();
        shm.set_extension("db-shm");
        let _ = std::fs::remove_file(&shm);
    }
}

struct FaultyKvStore {
    inner: Arc<dyn KvStore>,
    fail_writes: Arc<AtomicBool>,
}

#[async_trait]
impl KvStore for FaultyKvStore {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<String>> {
        self.inner.get(namespace, key).await
    }
    async fn set(&self, namespace: &str, key: &str, val: &str) -> Result<()> {
        if self.fail_writes.load(Ordering::SeqCst) {
            anyhow::bail!("Injected KV storage write error");
        }
        self.inner.set(namespace, key, val).await
    }
    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        if self.fail_writes.load(Ordering::SeqCst) {
            anyhow::bail!("Injected KV storage delete error");
        }
        self.inner.delete(namespace, key).await
    }
    async fn list_all(&self, namespace: &str) -> Result<Vec<(String, String)>> {
        self.inner.list_all(namespace).await
    }
    async fn apply_transaction(&self, ops: &[rekuiper_core::KvOperation]) -> Result<()> {
        if self.fail_writes.load(Ordering::SeqCst) {
            anyhow::bail!("Injected KV storage transaction write error");
        }
        self.inner.apply_transaction(ops).await
    }
}

struct SelectiveFaultyKvStore {
    inner: Arc<dyn KvStore>,
    fail_keys: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

#[async_trait]
impl KvStore for SelectiveFaultyKvStore {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<String>> {
        self.inner.get(namespace, key).await
    }
    async fn set(&self, namespace: &str, key: &str, val: &str) -> Result<()> {
        if self.fail_keys.lock().unwrap().contains(key) {
            anyhow::bail!("Injected KV storage write error for key: {}", key);
        }
        self.inner.set(namespace, key, val).await
    }
    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        if self.fail_keys.lock().unwrap().contains(key) {
            anyhow::bail!("Injected KV storage delete error for key: {}", key);
        }
        self.inner.delete(namespace, key).await
    }
    async fn list_all(&self, namespace: &str) -> Result<Vec<(String, String)>> {
        self.inner.list_all(namespace).await
    }
    async fn apply_transaction(&self, ops: &[rekuiper_core::KvOperation]) -> Result<()> {
        for op in ops {
            let key = match op {
                rekuiper_core::KvOperation::Set { key, .. } => key,
                rekuiper_core::KvOperation::Delete { key, .. } => key,
            };
            if self.fail_keys.lock().unwrap().contains(key) {
                anyhow::bail!("Injected KV storage transaction error for key: {}", key);
            }
        }
        self.inner.apply_transaction(ops).await
    }
}

struct ReadFailingKvStore {
    inner: Arc<dyn KvStore>,
    fail_reads: Arc<AtomicBool>,
}

#[async_trait]
impl KvStore for ReadFailingKvStore {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<String>> {
        if self.fail_reads.load(Ordering::SeqCst) {
            anyhow::bail!("Injected disk I/O read failure on get");
        }
        self.inner.get(namespace, key).await
    }
    async fn set(&self, namespace: &str, key: &str, val: &str) -> Result<()> {
        self.inner.set(namespace, key, val).await
    }
    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        self.inner.delete(namespace, key).await
    }
    async fn list_all(&self, namespace: &str) -> Result<Vec<(String, String)>> {
        if self.fail_reads.load(Ordering::SeqCst) {
            anyhow::bail!("Injected disk I/O read failure on list_all");
        }
        self.inner.list_all(namespace).await
    }
    async fn apply_transaction(&self, ops: &[rekuiper_core::KvOperation]) -> Result<()> {
        self.inner.apply_transaction(ops).await
    }
}

#[tokio::test]
async fn test_connection_alias_resolution_and_canonical_persistence() {
    let db = TempDb::new("alias_resolution");
    let kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());

    // Pre-populate a legacy dot-key entry directly in KV to simulate upgraded/migrated DB
    kv.set(
        "connections",
        "mqtt.legacy_conn",
        &json!({"server": "tcp://legacy:1883"}).to_string(),
    )
    .await
    .unwrap();

    let server = TestServer::start(kv.clone()).await;
    let client = reqwest::Client::new();

    // 1. Verify legacy dot-key resolves via get_connection_conf_key
    let resp = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/legacy_conn",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let val: Value = resp.json().await.unwrap();
    assert_eq!(val["server"], "tcp://legacy:1883");

    // 2. Also resolves via get_connection with dot alias
    let resp = client
        .get(format!("{}/connections/mqtt.legacy_conn", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 3. Save a connection via /metadata/connections/:name/confKeys/:conf_key
    let payload = json!({
        "server": "tcp://canonical:1883",
        "protocolVersion": "3.1.1"
    });
    let resp = client
        .post(format!(
            "{}/metadata/connections/mqtt/confKeys/new_conn",
            server.base_url
        ))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 4. Verify in KV directly that ONLY the canonical slash key is stored, not a duplicate dot key
    let canonical_in_kv = kv.get("connections", "mqtt/new_conn").await.unwrap();
    assert!(canonical_in_kv.is_some());
    let dot_in_kv = kv.get("connections", "mqtt.new_conn").await.unwrap();
    assert!(
        dot_in_kv.is_none(),
        "Must not create duplicate dot-key in storage"
    );

    // 5. Verify lookup works by both canonical and dot alias
    let resp = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/new_conn",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!(
            "{}/metadata/connections/yaml/mqtt",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .get(format!("{}/connections/mqtt.new_conn", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 6. Delete connection
    let resp = client
        .delete(format!(
            "{}/metadata/connections/mqtt/confKeys/new_conn",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    assert!(kv
        .get("connections", "mqtt/new_conn")
        .await
        .unwrap()
        .is_none());

    server.shutdown().await;
}

#[tokio::test]
async fn test_storage_failure_returns_server_error_without_partial_state() {
    let db = TempDb::new("fail_storage");
    let base_kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());
    let fail_flag = Arc::new(AtomicBool::new(false));
    let faulty_kv: Arc<dyn KvStore> = Arc::new(FaultyKvStore {
        inner: base_kv,
        fail_writes: fail_flag.clone(),
    });

    let server = TestServer::start(faulty_kv.clone()).await;
    let client = reqwest::Client::new();

    // Trigger failure
    fail_flag.store(true, Ordering::SeqCst);

    // Save connection should fail with 500
    let resp = client
        .post(format!(
            "{}/metadata/connections/mqtt/confKeys/fail_conn",
            server.base_url
        ))
        .json(&json!({"server": "tcp://broker:1883"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);

    // Memory must NOT contain the failed connection
    assert!(!server
        .state
        .connections
        .read()
        .contains_key("mqtt/fail_conn"));
    assert!(!server
        .state
        .connections
        .read()
        .contains_key("mqtt.fail_conn"));

    // Save source config should fail with 500
    let resp = client
        .post(format!(
            "{}/metadata/sources/simulator/confKeys/fail_src",
            server.base_url
        ))
        .json(&json!({"interval": 100}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!server
        .state
        .source_configs
        .read()
        .contains_key("simulator/fail_src"));

    // Save sink config should fail with 500
    let resp = client
        .post(format!(
            "{}/metadata/sinks/file/confKeys/fail_snk",
            server.base_url
        ))
        .json(&json!({"path": "/tmp/out"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!server
        .state
        .sink_configs
        .read()
        .contains_key("file/fail_snk"));

    server.shutdown().await;
}

#[tokio::test]
async fn test_restart_restoration_of_configs() {
    let db = TempDb::new("restart_configs");
    let kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());
    let server1 = TestServer::start(kv.clone()).await;
    let client = reqwest::Client::new();

    // Create connection, source config, sink config
    client
        .post(format!(
            "{}/metadata/connections/mqtt/confKeys/persisted_conn",
            server1.base_url
        ))
        .json(&json!({"server": "tcp://remote:1883"}))
        .send()
        .await
        .unwrap();

    client
        .post(format!(
            "{}/metadata/sources/simulator/confKeys/persisted_src",
            server1.base_url
        ))
        .json(&json!({"interval": 50}))
        .send()
        .await
        .unwrap();

    client
        .post(format!(
            "{}/metadata/sinks/file/confKeys/persisted_snk",
            server1.base_url
        ))
        .json(&json!({"path": "/tmp/persisted.json"}))
        .send()
        .await
        .unwrap();

    // Shutdown server 1
    server1.shutdown().await;

    // Start server 2 with same KV
    let server2 = TestServer::start(kv.clone()).await;

    // Verify all 3 configurations reloaded
    let resp: Value = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/persisted_conn",
            server2.base_url
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["server"], "tcp://remote:1883");

    let resp: Value = client
        .get(format!(
            "{}/metadata/sources/simulator/confKeys/persisted_src",
            server2.base_url
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["interval"], 50);

    let resp: Value = client
        .get(format!(
            "{}/metadata/sinks/file/confKeys/persisted_snk",
            server2.base_url
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["path"], "/tmp/persisted.json");

    server2.shutdown().await;
}

#[tokio::test]
async fn test_concurrent_config_mutations() {
    let db = TempDb::new("concurrent_configs");
    let kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());
    let server = TestServer::start(kv.clone()).await;
    let client = reqwest::Client::new();

    let mut tasks = Vec::new();
    for i in 0..20 {
        let base = server.base_url.clone();
        let c = client.clone();
        tasks.push(tokio::spawn(async move {
            let key = format!("k_{}", i % 5);
            if i % 2 == 0 {
                let _ = c
                    .post(format!(
                        "{}/metadata/sources/simulator/confKeys/{}",
                        base, key
                    ))
                    .json(&json!({"val": i}))
                    .send()
                    .await;
            } else {
                let _ = c
                    .delete(format!(
                        "{}/metadata/sources/simulator/confKeys/{}",
                        base, key
                    ))
                    .send()
                    .await;
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    // After all concurrent writes/deletes finish, verify memory and KV storage are perfectly in sync
    let mem_keys: Vec<String> = server.state.source_configs.read().keys().cloned().collect();
    let kv_entries = kv.list_all("source_configs").await.unwrap();
    let kv_keys: Vec<String> = kv_entries.into_iter().map(|(k, _)| k).collect();

    for k in &mem_keys {
        assert!(
            kv_keys.contains(k),
            "Memory key {} must be in KV storage",
            k
        );
    }
    for k in &kv_keys {
        assert!(mem_keys.contains(k), "KV key {} must be in memory", k);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_alias_deletion_failure_does_not_corrupt_memory_or_storage() {
    let db = TempDb::new("fail_alias_delete");
    let base_kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());

    // Pre-populate canonical and dot key
    base_kv
        .set(
            "connections",
            "mqtt/alias_conn",
            &json!({"server": "tcp://alias:1883"}).to_string(),
        )
        .await
        .unwrap();
    base_kv
        .set(
            "connections",
            "mqtt.alias_conn",
            &json!({"server": "tcp://alias:1883"}).to_string(),
        )
        .await
        .unwrap();

    let fail_flag = Arc::new(AtomicBool::new(false));
    let faulty_kv: Arc<dyn KvStore> = Arc::new(FaultyKvStore {
        inner: base_kv.clone(),
        fail_writes: fail_flag.clone(),
    });

    let server1 = TestServer::start(faulty_kv.clone()).await;
    let client = reqwest::Client::new();

    // Verify it is accessible
    let resp = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/alias_conn",
            server1.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Inject failure on delete
    fail_flag.store(true, Ordering::SeqCst);

    let resp = client
        .delete(format!(
            "{}/metadata/connections/mqtt/confKeys/alias_conn",
            server1.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);

    // Memory and storage must retain the connection
    assert!(server1
        .state
        .connections
        .read()
        .contains_key("mqtt/alias_conn"));

    // Clear failure
    fail_flag.store(false, Ordering::SeqCst);

    // Successful delete
    let resp = client
        .delete(format!(
            "{}/metadata/connections/mqtt/confKeys/alias_conn",
            server1.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Both canonical and dot keys must be gone from KV
    assert!(base_kv
        .get("connections", "mqtt/alias_conn")
        .await
        .unwrap()
        .is_none());
    assert!(base_kv
        .get("connections", "mqtt.alias_conn")
        .await
        .unwrap()
        .is_none());

    // Restart daemon to verify connection does NOT reappear
    server1.shutdown().await;

    let server2 = TestServer::start(base_kv.clone()).await;
    let resp = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/alias_conn",
            server2.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    server2.shutdown().await;
}

#[tokio::test]
async fn test_delete_connection_endpoint_persistence_failure() {
    let db = TempDb::new("fail_conn_endpoint_delete");
    let base_kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());

    // Pre-populate canonical and dot key
    base_kv
        .set(
            "connections",
            "mqtt/ep_conn",
            &json!({"server": "tcp://ep:1883"}).to_string(),
        )
        .await
        .unwrap();
    base_kv
        .set(
            "connections",
            "mqtt.ep_conn",
            &json!({"server": "tcp://ep:1883"}).to_string(),
        )
        .await
        .unwrap();

    let fail_flag = Arc::new(AtomicBool::new(false));
    let faulty_kv: Arc<dyn KvStore> = Arc::new(FaultyKvStore {
        inner: base_kv.clone(),
        fail_writes: fail_flag.clone(),
    });

    let server1 = TestServer::start(faulty_kv.clone()).await;
    let client = reqwest::Client::new();

    // Verify it exists via /connections/mqtt.ep_conn
    let resp = client
        .get(format!("{}/connections/mqtt.ep_conn", server1.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Inject failure on delete
    fail_flag.store(true, Ordering::SeqCst);

    // Call DELETE /connections/mqtt.ep_conn -> must fail with 500
    let resp = client
        .delete(format!("{}/connections/mqtt.ep_conn", server1.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);

    // Memory must still contain the connection
    assert!(
        server1
            .state
            .connections
            .read()
            .contains_key("mqtt/ep_conn")
            || server1
                .state
                .connections
                .read()
                .contains_key("mqtt.ep_conn")
    );

    // Clear failure
    fail_flag.store(false, Ordering::SeqCst);

    // Successful delete via /connections/:id
    let resp = client
        .delete(format!("{}/connections/mqtt.ep_conn", server1.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Both canonical and dot keys must be gone from KV
    assert!(base_kv
        .get("connections", "mqtt/ep_conn")
        .await
        .unwrap()
        .is_none());
    assert!(base_kv
        .get("connections", "mqtt.ep_conn")
        .await
        .unwrap()
        .is_none());

    // Restart daemon to verify connection does NOT reappear
    server1.shutdown().await;

    let server2 = TestServer::start(base_kv.clone()).await;
    let resp = client
        .get(format!("{}/connections/mqtt.ep_conn", server2.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    server2.shutdown().await;
}

#[tokio::test]
async fn test_second_storage_operation_failure_rolls_back_atomically() {
    let db = TempDb::new("second_op_fail");
    let base_kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());
    let fail_keys = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let selective_kv: Arc<dyn KvStore> = Arc::new(SelectiveFaultyKvStore {
        inner: base_kv.clone(),
        fail_keys: fail_keys.clone(),
    });

    let server = TestServer::start(selective_kv.clone()).await;
    let client = reqwest::Client::new();

    // --- Scenario 1: delete_connection_conf_key with failure on second op (alias cleanup) ---
    // 1. Establish connection mqtt/del_test
    let setup_resp = client
        .post(format!(
            "{}/metadata/connections/mqtt/confKeys/del_test",
            server.base_url
        ))
        .json(&json!({"server": "tcp://broker:1883"}))
        .send()
        .await
        .unwrap();
    assert_eq!(setup_resp.status(), reqwest::StatusCode::OK);
    assert!(base_kv
        .get("connections", "mqtt/del_test")
        .await
        .unwrap()
        .is_some());
    assert!(server
        .state
        .connections
        .read()
        .contains_key("mqtt/del_test"));

    // 2. Set fail_keys on the alias "mqtt.del_test" so the second op fails while the first (canonical) succeeds
    fail_keys
        .lock()
        .unwrap()
        .insert("mqtt.del_test".to_string());

    let del_resp = client
        .delete(format!(
            "{}/metadata/connections/mqtt/confKeys/del_test",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        del_resp.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );

    // 3. Storage atomicity check: canonical entry must be rolled back and still present in KV
    let kv_val = base_kv.get("connections", "mqtt/del_test").await.unwrap();
    assert!(
        kv_val.is_some(),
        "Canonical key should have been rolled back in KV after alias failure"
    );

    // 4. Memory check: connections map still retains the connection
    assert!(
        server
            .state
            .connections
            .read()
            .contains_key("mqtt/del_test"),
        "In-memory connections must remain intact after failed deletion"
    );

    // 5. Readback verification
    let get_resp = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/del_test",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.status(), reqwest::StatusCode::OK);

    // --- Scenario 2: create_connection_conf_key with failure on second op (alias cleanup) ---
    // Clear previous fail_keys and inject failure on alias for new connection
    fail_keys.lock().unwrap().clear();
    fail_keys
        .lock()
        .unwrap()
        .insert("mqtt.create_test".to_string());

    let create_resp = client
        .post(format!(
            "{}/metadata/connections/mqtt/confKeys/create_test",
            server.base_url
        ))
        .json(&json!({"server": "tcp://broker:1883"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        create_resp.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );

    // Storage atomicity check: newly persisted canonical key must have rolled back (deleted)
    assert!(
        base_kv
            .get("connections", "mqtt/create_test")
            .await
            .unwrap()
            .is_none(),
        "Canonical key must be rolled back (deleted) from KV on alias cleanup failure"
    );

    // Memory check: memory map does not contain partial connection
    assert!(
        !server
            .state
            .connections
            .read()
            .contains_key("mqtt/create_test"),
        "In-memory connections must not contain partially created connection"
    );

    // --- Scenario 3: Restart verification ---
    // Restart daemon from base_kv to ensure persisted state is clean and consistent
    server.shutdown().await;

    let restarted_server = TestServer::start(base_kv.clone()).await;

    // "mqtt/del_test" was rolled back to intact state and must be present on restart
    let get_restarted = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/del_test",
            restarted_server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(get_restarted.status(), reqwest::StatusCode::OK);

    // "mqtt/create_test" was rolled back and must not exist on restart
    let get_nonexistent = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/create_test",
            restarted_server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(get_nonexistent.status(), reqwest::StatusCode::NOT_FOUND);

    restarted_server.shutdown().await;
}

#[tokio::test]
async fn test_storage_transaction_rollback_and_read_failures() {
    let db = TempDb::new("tx_and_read_fail");
    let base_kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());
    let fail_reads = Arc::new(AtomicBool::new(false));
    let read_failing_kv: Arc<dyn KvStore> = Arc::new(ReadFailingKvStore {
        inner: base_kv.clone(),
        fail_reads: fail_reads.clone(),
    });

    // 1. Verify SqliteKvStore::apply_transaction atomicity directly
    let set_op1 = rekuiper_core::KvOperation::Set {
        namespace: "connections".to_string(),
        key: "test/tx_ok".to_string(),
        val: json!({"server": "tcp://ok:1883"}).to_string(),
    };
    let set_op2 = rekuiper_core::KvOperation::Set {
        namespace: "connections".to_string(),
        key: "test/tx_ok2".to_string(),
        val: json!({"server": "tcp://ok2:1883"}).to_string(),
    };
    base_kv
        .apply_transaction(&[set_op1, set_op2])
        .await
        .unwrap();
    assert!(base_kv
        .get("connections", "test/tx_ok")
        .await
        .unwrap()
        .is_some());
    assert!(base_kv
        .get("connections", "test/tx_ok2")
        .await
        .unwrap()
        .is_some());

    // 2. Transaction rollback verification on failure
    struct RollbackTestKvStore {
        inner: Arc<dyn KvStore>,
    }
    #[async_trait]
    impl KvStore for RollbackTestKvStore {
        async fn get(&self, ns: &str, k: &str) -> Result<Option<String>> {
            self.inner.get(ns, k).await
        }
        async fn set(&self, ns: &str, k: &str, v: &str) -> Result<()> {
            self.inner.set(ns, k, v).await
        }
        async fn delete(&self, ns: &str, k: &str) -> Result<()> {
            self.inner.delete(ns, k).await
        }
        async fn list_all(&self, ns: &str) -> Result<Vec<(String, String)>> {
            self.inner.list_all(ns).await
        }
        async fn apply_transaction(&self, _ops: &[rekuiper_core::KvOperation]) -> Result<()> {
            anyhow::bail!("Injected transaction commit failure");
        }
    }

    let rollback_kv: Arc<dyn KvStore> = Arc::new(RollbackTestKvStore {
        inner: base_kv.clone(),
    });
    let server_rb = TestServer::start(rollback_kv).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!(
            "{}/metadata/connections/mqtt/confKeys/fail_tx",
            server_rb.base_url
        ))
        .json(&json!({"server": "tcp://fail:1883"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);

    // KV must have zero partial records
    assert!(base_kv
        .get("connections", "mqtt/fail_tx")
        .await
        .unwrap()
        .is_none());
    assert!(!server_rb
        .state
        .connections
        .read()
        .contains_key("mqtt/fail_tx"));
    server_rb.shutdown().await;

    // 3. Test read failures: failed reads must return 500 Internal Server Error, NOT 404 Not Found
    let server_rf = TestServer::start(read_failing_kv.clone()).await;

    // Verify 404 is returned when key truly does not exist
    let resp_404 = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/non_existent_key",
            server_rf.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp_404.status(), reqwest::StatusCode::NOT_FOUND);

    // Now inject disk I/O read failure
    fail_reads.store(true, Ordering::SeqCst);

    // Reading confKey must return 500 INTERNAL_SERVER_ERROR, rejecting silent conversion to 404
    let resp_err1 = client
        .get(format!(
            "{}/metadata/connections/mqtt/confKeys/some_key",
            server_rf.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp_err1.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "Failed storage read must return 500, not 404"
    );

    // Reading via /connections/:id must also return 500 INTERNAL_SERVER_ERROR
    let resp_err2 = client
        .get(format!("{}/connections/mqtt.some_conn", server_rf.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp_err2.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "Failed storage read must return 500, not 404"
    );

    // load_config_maps must propagate the storage read error rather than silently treating it as empty
    let load_res = load_config_maps(&server_rf.state).await;
    assert!(
        load_res.is_err(),
        "load_config_maps must return Err when underlying storage read fails"
    );

    server_rf.shutdown().await;
}
