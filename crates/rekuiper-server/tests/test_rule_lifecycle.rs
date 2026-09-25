use anyhow::Result;
use async_trait::async_trait;
use rekuiper_conf::KuiperConfig;
use rekuiper_core::{KvStore, RuleManager, SqliteKvStore, StreamBus, StreamManager, TableManager};
use rekuiper_server::routes::{create_router, load_config_maps, restore_running_rules, AppState};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

struct TestServer {
    base_url: String,
    _state: AppState,
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
            _state: state,
            handle,
        }
    }

    async fn shutdown(self) {
        self.handle.abort();
    }
}

struct TempFile(PathBuf);
impl TempFile {
    fn new(name: &str) -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "rekuiper_test_{}_{}.json",
            name,
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::remove_file(&p);
        Self(p)
    }

    fn path_str(&self) -> String {
        self.0.to_string_lossy().to_string().replace('\\', "/")
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

struct TempDb(PathBuf);
impl TempDb {
    fn new(name: &str) -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!("rekuiper_db_{}_{}.db", name, uuid::Uuid::new_v4()));
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
            anyhow::bail!("Injected KV storage failure on write");
        }
        self.inner.set(namespace, key, val).await
    }
    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        if self.fail_writes.load(Ordering::SeqCst) {
            anyhow::bail!("Injected KV storage failure on delete");
        }
        self.inner.delete(namespace, key).await
    }
    async fn list_all(&self, namespace: &str) -> Result<Vec<(String, String)>> {
        self.inner.list_all(namespace).await
    }
}

async fn wait_for_file_lines(path: &Path, count: usize) -> Vec<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(content) = tokio::fs::read_to_string(path).await {
            let lines: Vec<Value> = content
                .lines()
                .filter(|l| !l.trim().is_empty())
                .filter_map(|l| serde_json::from_str(l).ok())
                .collect();
            if lines.len() >= count {
                return lines;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("Timed out waiting for {} lines in {:?}", count, path);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_update_running_rule_resumes_processing_and_persists_status() {
    let db = TempDb::new("update_running");
    let kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());
    let server = TestServer::start(kv.clone()).await;
    let client = reqwest::Client::new();
    let out = TempFile::new("out_update_running");

    // 1. Create stream
    let resp = client
        .post(format!("{}/streams", server.base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_run () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // 2. Create running rule selecting 'a'
    let resp = client
        .post(format!("{}/rules", server.base_url))
        .json(&json!({
            "id": "r_run",
            "sql": "SELECT a FROM s_run",
            "actions": [{ "file": { "path": out.path_str() } }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Check status is running
    let status_resp: Value = client
        .get(format!("{}/rules/r_run/status", server.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status_resp["status"], "running");

    // Send first record
    let resp = client
        .post(format!("{}/streams/s_run/data", server.base_url))
        .json(&json!({ "a": 100, "b": 200 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let lines = wait_for_file_lines(&out.0, 1).await;
    assert_eq!(lines[0].get("a").and_then(|v| v.as_i64()), Some(100));
    assert!(lines[0].get("b").is_none());

    // 3. Update the running rule to select 'b' instead
    let resp = client
        .put(format!("{}/rules/r_run", server.base_url))
        .json(&json!({
            "id": "r_run",
            "sql": "SELECT b FROM s_run",
            "actions": [{ "file": { "path": out.path_str() } }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Verify status remains "running"
    let status_resp: Value = client
        .get(format!("{}/rules/r_run/status", server.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status_resp["status"], "running");

    // Send second record
    let resp = client
        .post(format!("{}/streams/s_run/data", server.base_url))
        .json(&json!({ "a": 300, "b": 400 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let lines = wait_for_file_lines(&out.0, 2).await;
    // Second line must use new projection (b = 400, no a)
    assert_eq!(lines[1].get("b").and_then(|v| v.as_i64()), Some(400));
    assert!(lines[1].get("a").is_none());

    server.shutdown().await;
}

#[tokio::test]
async fn test_restart_restores_updated_running_rule() {
    let db = TempDb::new("restart_updated");
    let kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());
    let server1 = TestServer::start(kv.clone()).await;
    let client = reqwest::Client::new();
    let out = TempFile::new("out_restart");

    // Create stream and rule
    client
        .post(format!("{}/streams", server1.base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_rest () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/rules", server1.base_url))
        .json(&json!({
            "id": "r_rest",
            "sql": "SELECT val FROM s_rest",
            "actions": [{ "file": { "path": out.path_str() } }]
        }))
        .send()
        .await
        .unwrap();

    // Update rule to select val * 2 as doubled
    let resp = client
        .put(format!("{}/rules/r_rest", server1.base_url))
        .json(&json!({
            "id": "r_rest",
            "sql": "SELECT val * 2 AS doubled FROM s_rest",
            "actions": [{ "file": { "path": out.path_str() } }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Stop server 1 (simulate restart)
    server1.shutdown().await;

    // Start server 2 with same KV database
    let server2 = TestServer::start(kv.clone()).await;

    // Check status is restored as "running"
    let status_resp: Value = client
        .get(format!("{}/rules/r_rest/status", server2.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status_resp["status"], "running");

    // Send record to restored server
    let resp = client
        .post(format!("{}/streams/s_rest/data", server2.base_url))
        .json(&json!({ "val": 21 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let lines = wait_for_file_lines(&out.0, 1).await;
    assert_eq!(lines[0].get("doubled").and_then(|v| v.as_i64()), Some(42));

    server2.shutdown().await;
}

#[tokio::test]
async fn test_injected_persistence_failure_preserves_previous_usable_rule() {
    let db = TempDb::new("fail_inject");
    let base_kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());
    let fail_flag = Arc::new(AtomicBool::new(false));
    let faulty_kv: Arc<dyn KvStore> = Arc::new(FaultyKvStore {
        inner: base_kv,
        fail_writes: fail_flag.clone(),
    });

    let server = TestServer::start(faulty_kv.clone()).await;
    let client = reqwest::Client::new();
    let out = TempFile::new("out_fail_inject");

    // 1. Create stream & rule normally
    client
        .post(format!("{}/streams", server.base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_inj () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/rules", server.base_url))
        .json(&json!({
            "id": "r_inj",
            "sql": "SELECT good FROM s_inj",
            "actions": [{ "file": { "path": out.path_str() } }]
        }))
        .send()
        .await
        .unwrap();

    // Verify initial data works
    client
        .post(format!("{}/streams/s_inj/data", server.base_url))
        .json(&json!({ "good": 1, "bad": 99 }))
        .send()
        .await
        .unwrap();
    let lines = wait_for_file_lines(&out.0, 1).await;
    assert_eq!(lines[0].get("good").and_then(|v| v.as_i64()), Some(1));

    // 2. Enable write failures
    fail_flag.store(true, Ordering::SeqCst);

    // 3. Attempt to update rule; must fail with 500
    let resp = client
        .put(format!("{}/rules/r_inj", server.base_url))
        .json(&json!({
            "id": "r_inj",
            "sql": "SELECT bad FROM s_inj",
            "actions": [{ "file": { "path": out.path_str() } }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);

    // 4. Verify previous rule is still usable and running with previous definition
    let rule_def: Value = client
        .get(format!("{}/rules/r_inj", server.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rule_def["sql"], "SELECT good FROM s_inj");

    let status_resp: Value = client
        .get(format!("{}/rules/r_inj/status", server.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status_resp["status"], "running");

    // Post data; rule must still process using original definition
    client
        .post(format!("{}/streams/s_inj/data", server.base_url))
        .json(&json!({ "good": 2, "bad": 999 }))
        .send()
        .await
        .unwrap();

    let lines = wait_for_file_lines(&out.0, 2).await;
    assert_eq!(lines[1].get("good").and_then(|v| v.as_i64()), Some(2));
    assert!(lines[1].get("bad").is_none());

    server.shutdown().await;
}

#[tokio::test]
async fn test_stopped_rule_update_and_concurrent_lifecycle() {
    let db = TempDb::new("stopped_and_concurrent");
    let kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());
    let server = TestServer::start(kv.clone()).await;
    let client = reqwest::Client::new();
    let out = TempFile::new("out_stopped");

    // 1. Create stream & rule
    client
        .post(format!("{}/streams", server.base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_stop () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/rules", server.base_url))
        .json(&json!({
            "id": "r_stop",
            "sql": "SELECT a FROM s_stop",
            "actions": [{ "file": { "path": out.path_str() } }]
        }))
        .send()
        .await
        .unwrap();

    // 2. Stop the rule
    let resp = client
        .post(format!("{}/rules/r_stop/stop", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let status: Value = client
        .get(format!("{}/rules/r_stop/status", server.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["status"], "stopped");

    // 3. Update the stopped rule
    let resp = client
        .put(format!("{}/rules/r_stop", server.base_url))
        .json(&json!({
            "id": "r_stop",
            "sql": "SELECT a * 10 AS a FROM s_stop",
            "actions": [{ "file": { "path": out.path_str() } }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Verify it is STILL stopped
    let status: Value = client
        .get(format!("{}/rules/r_stop/status", server.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["status"], "stopped");

    // Posting data while stopped should NOT emit anything
    client
        .post(format!("{}/streams/s_stop/data", server.base_url))
        .json(&json!({ "a": 5 }))
        .send()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!Path::new(&out.0).exists());

    // 4. Start the rule explicitly
    let resp = client
        .post(format!("{}/rules/r_stop/start", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let status: Value = client
        .get(format!("{}/rules/r_stop/status", server.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["status"], "running");

    // Post data now -> receives updated multiplied value
    client
        .post(format!("{}/streams/s_stop/data", server.base_url))
        .json(&json!({ "a": 7 }))
        .send()
        .await
        .unwrap();

    let lines = wait_for_file_lines(&out.0, 1).await;
    assert_eq!(lines[0].get("a").and_then(|v| v.as_i64()), Some(70));

    // 5. Concurrent lifecycle stress test
    let mut tasks = Vec::new();
    for i in 0..10 {
        let base = server.base_url.clone();
        let c = client.clone();
        let path = out.path_str();
        tasks.push(tokio::spawn(async move {
            match i % 4 {
                0 => {
                    let _ = c.post(format!("{}/rules/r_stop/start", base)).send().await;
                }
                1 => {
                    let _ = c.post(format!("{}/rules/r_stop/stop", base)).send().await;
                }
                2 => {
                    let _ = c
                        .post(format!("{}/rules/r_stop/restart", base))
                        .send()
                        .await;
                }
                _ => {
                    let _ = c
                        .put(format!("{}/rules/r_stop", base))
                        .json(&json!({
                            "id": "r_stop",
                            "sql": "SELECT a * 100 AS a FROM s_stop",
                            "actions": [{ "file": { "path": path } }]
                        }))
                        .send()
                        .await;
                }
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    // Verify engine is still responsive and status is clean (either running or stopped)
    let status: Value = client
        .get(format!("{}/rules/r_stop/status", server.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let s = status["status"].as_str().unwrap();
    assert!(s == "running" || s == "stopped");

    server.shutdown().await;
}

#[tokio::test]
async fn test_restart_rule_persistence_failure_preserves_running_source() {
    let db = TempDb::new("restart_fail_preserves");
    let base_kv: Arc<dyn KvStore> = Arc::new(SqliteKvStore::new(&db.0).await.unwrap());
    let fail_flag = Arc::new(AtomicBool::new(false));
    let faulty_kv: Arc<dyn KvStore> = Arc::new(FaultyKvStore {
        inner: base_kv,
        fail_writes: fail_flag.clone(),
    });

    let server = TestServer::start(faulty_kv.clone()).await;
    let client = reqwest::Client::new();
    let out = TempFile::new("out_restart_preserves");

    // 1. Create stream & rule
    client
        .post(format!("{}/streams", server.base_url))
        .json(&json!({
            "sql": "CREATE STREAM s_rf () WITH (TYPE=\"memory\", FORMAT=\"json\")"
        }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/rules", server.base_url))
        .json(&json!({
            "id": "r_rf",
            "sql": "SELECT val FROM s_rf",
            "actions": [{ "file": { "path": out.path_str() } }]
        }))
        .send()
        .await
        .unwrap();

    // Verify initial message is delivered
    client
        .post(format!("{}/streams/s_rf/data", server.base_url))
        .json(&json!({ "val": 100 }))
        .send()
        .await
        .unwrap();

    let lines = wait_for_file_lines(&out.0, 1).await;
    assert_eq!(lines[0].get("val").and_then(|v| v.as_i64()), Some(100));

    // 2. Inject persistence failure
    fail_flag.store(true, Ordering::SeqCst);

    // 3. Attempt restart; handler MUST return 500
    let resp = client
        .post(format!("{}/rules/r_rf/restart", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);

    // 4. Verify that running rule source was NOT disconnected/cancelled by failed restart!
    client
        .post(format!("{}/streams/s_rf/data", server.base_url))
        .json(&json!({ "val": 200 }))
        .send()
        .await
        .unwrap();

    let lines = wait_for_file_lines(&out.0, 2).await;
    assert_eq!(lines[1].get("val").and_then(|v| v.as_i64()), Some(200));

    // 5. Restore persistence and restart cleanly
    fail_flag.store(false, Ordering::SeqCst);
    let resp = client
        .post(format!("{}/rules/r_rf/restart", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Post third record to confirm restarted rule continues processing
    client
        .post(format!("{}/streams/s_rf/data", server.base_url))
        .json(&json!({ "val": 300 }))
        .send()
        .await
        .unwrap();

    let lines = wait_for_file_lines(&out.0, 3).await;
    assert_eq!(lines[2].get("val").and_then(|v| v.as_i64()), Some(300));

    server.shutdown().await;
}
