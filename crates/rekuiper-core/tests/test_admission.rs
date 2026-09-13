//! Admission and fan-out correctness for the stream bus
//! (reserve-then-commit, accept-once; all-or-nothing feedback publish).

use rekuiper_core::{
    PublishError, StreamBus, StreamReceiver, StreamRecord, ADMISSION_CHUNK_RECORDS,
    MAX_HTTP_BATCH_RECORDS, STREAM_QUEUE_CAPACITY,
};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

fn rec(publisher: u64, seq: u64) -> StreamRecord {
    let mut data = HashMap::new();
    data.insert("p".to_string(), json!(publisher));
    data.insert("seq".to_string(), json!(seq));
    StreamRecord::new(data)
}

fn key(r: &StreamRecord) -> (u64, u64) {
    (
        r.data["p"].as_u64().expect("p"),
        r.data["seq"].as_u64().expect("seq"),
    )
}

fn batch(publisher: u64, n: u64) -> Vec<StreamRecord> {
    (0..n).map(|i| rec(publisher, i)).collect()
}

fn drain_now(rx: &mut StreamReceiver) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    while let Ok(r) = rx.try_recv() {
        out.push(key(&r));
    }
    out
}

async fn recv_exact(rx: &mut StreamReceiver, n: usize) -> Vec<(u64, u64)> {
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let r = tokio::time::timeout(Duration::from_secs(20), rx.recv())
            .await
            .expect("timed out waiting for records")
            .expect("channel closed");
        out.push(key(&r));
    }
    out
}

#[tokio::test]
async fn batch_without_subscribers_is_success() {
    let bus = StreamBus::new();
    assert!(matches!(
        bus.publish_batch("none", batch(0, 10)).await,
        Ok(0)
    ));
    bus.get_or_create("empty");
    assert!(matches!(
        bus.publish_batch("empty", batch(0, 10)).await,
        Ok(0)
    ));
    assert!(matches!(
        bus.try_publish("empty", rec(0, 0)),
        Err(PublishError::NoSubscribers)
    ));
}

/// Every selected subscriber closed before any commit: explicit error, zero
/// admission, and a retry after the purge reaches only new subscribers.
#[tokio::test]
async fn all_closed_before_commit_admits_nothing() {
    let bus = StreamBus::new();
    drop(bus.subscribe("t"));
    drop(bus.subscribe("t"));
    assert!(matches!(
        bus.publish_batch("t", batch(1, 1000)).await,
        Err(PublishError::Closed)
    ));
    let mut fresh = bus.subscribe("t");
    assert!(matches!(bus.publish_batch("t", batch(2, 5)).await, Ok(1)));
    let got = drain_now(&mut fresh);
    assert_eq!(got, (0..5).map(|i| (2, i)).collect::<Vec<_>>());
    assert_eq!(bus.undelivered_after_commit(), 0);
}

/// A subscriber that closed before commit is excluded; live ones get the full
/// request in order, and nothing is counted as undelivered.
#[tokio::test]
async fn closed_before_commit_is_excluded() {
    let bus = StreamBus::new();
    let mut live = bus.subscribe("t");
    drop(bus.subscribe("t"));
    let n = 1000u64;
    assert!(matches!(bus.publish_batch("t", batch(7, n)).await, Ok(1)));
    let got = drain_now(&mut live);
    assert_eq!(got, (0..n).map(|i| (7, i)).collect::<Vec<_>>());
    assert_eq!(bus.undelivered_after_commit(), 0);
}

/// A subscriber closing after the request started committing never turns the
/// request into a retryable failure: the request succeeds, the live
/// subscriber receives every record exactly once in order, and the missed
/// records are counted exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closure_after_commit_is_accepted_and_accounted() {
    let bus = StreamBus::new();
    let mut fast = bus.subscribe("t");
    let slow = bus.subscribe("t");
    let n = MAX_HTTP_BATCH_RECORDS as u64;

    let publisher = tokio::spawn({
        let bus = bus.clone();
        async move { bus.publish_batch("t", batch(3, n)).await }
    });
    let consumer = tokio::spawn(async move { recv_exact(&mut fast, n as usize).await });

    // Wait until the stalled subscriber's queue is full: the publisher has
    // committed several chunks and is blocked reserving the next one.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while slow.len() < STREAM_QUEUE_CAPACITY {
        assert!(
            std::time::Instant::now() < deadline,
            "publisher never filled queue"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let committed_to_slow = slow.len() as u64;
    drop(slow);

    let result = tokio::time::timeout(Duration::from_secs(20), publisher)
        .await
        .expect("publisher hung after closure")
        .expect("publisher panicked");
    assert!(
        matches!(result, Ok(1)),
        "accepted request must succeed: {:?}",
        result
    );

    let got = consumer.await.expect("consumer panicked");
    assert_eq!(got, (0..n).map(|i| (3, i)).collect::<Vec<_>>());
    assert_eq!(committed_to_slow % ADMISSION_CHUNK_RECORDS as u64, 0);
    assert_eq!(bus.undelivered_after_commit(), n - committed_to_slow);
}

/// Concurrent batch publishers on one topic: every subscriber sees the same
/// sequence, and each request's records are contiguous and in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_batches_are_contiguous_and_identical() {
    let bus = StreamBus::new();
    let mut rx_a = bus.subscribe("t");
    let mut rx_b = bus.subscribe("t");
    let publishers = 8u64;
    let per = 2_000u64;
    let total = (publishers * per) as usize;

    let consume_a = tokio::spawn(async move { recv_exact(&mut rx_a, total).await });
    let consume_b = tokio::spawn(async move { recv_exact(&mut rx_b, total).await });
    let mut handles = Vec::new();
    for p in 0..publishers {
        let bus = bus.clone();
        handles.push(tokio::spawn(async move {
            bus.publish_batch("t", batch(p, per)).await
        }));
    }
    for h in handles {
        assert!(matches!(h.await.expect("publisher panicked"), Ok(2)));
    }
    let a = consume_a.await.expect("consumer a");
    let b = consume_b.await.expect("consumer b");
    assert_eq!(a, b, "subscribers must observe identical sequences");

    let mut i = 0usize;
    let mut seen = std::collections::HashSet::new();
    while i < a.len() {
        let p = a[i].0;
        assert!(seen.insert(p), "publisher {} split into several runs", p);
        for seq in 0..per {
            assert_eq!(a[i], (p, seq), "request not contiguous/in order");
            i += 1;
        }
    }
    assert_eq!(seen.len() as u64, publishers);
}

/// Blocker B: a full subscriber rejects the feedback record and no other
/// subscriber receives it (reservations are released).
#[tokio::test]
async fn try_publish_full_admits_nothing_anywhere() {
    let bus = StreamBus::new();
    let mut full = bus.subscribe("t");
    for i in 0..STREAM_QUEUE_CAPACITY as u64 {
        bus.try_publish("t", rec(0, i)).expect("capacity must hold");
    }
    let mut other = bus.subscribe("t");
    assert!(matches!(
        bus.try_publish("t", rec(1, 0)),
        Err(PublishError::Full)
    ));
    assert_eq!(
        other.len(),
        0,
        "rejected record leaked to another subscriber"
    );
    assert_eq!(full.len(), STREAM_QUEUE_CAPACITY);

    full.recv().await.expect("queued record must survive");
    assert!(matches!(bus.try_publish("t", rec(1, 1)), Ok(2)));
    assert_eq!(drain_now(&mut other), vec![(1, 1)]);
}

/// Blocker B under contention: many threads publish feedback records against
/// two slowly drained subscribers. Each record lands on both subscribers or
/// on neither, never a subset, and both observe the same order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_try_publish_is_all_or_nothing() {
    for round in 0..20u64 {
        let bus = StreamBus::new();
        let rx_a = bus.subscribe("t");
        let rx_b = bus.subscribe("t");
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let drain = |mut rx: StreamReceiver,
                     stop: std::sync::Arc<std::sync::atomic::AtomicBool>| {
            tokio::spawn(async move {
                let mut out = Vec::new();
                loop {
                    match rx.try_recv() {
                        Ok(r) => {
                            out.push(key(&r));
                            if out.len() % 64 == 0 {
                                tokio::task::yield_now().await;
                            }
                        }
                        Err(_) if stop.load(std::sync::atomic::Ordering::SeqCst) => {
                            out.extend(drain_now(&mut rx));
                            return out;
                        }
                        Err(_) => tokio::task::yield_now().await,
                    }
                }
            })
        };
        let consume_a = drain(rx_a, stop.clone());
        let consume_b = drain(rx_b, stop.clone());

        let threads: Vec<_> = (0..6u64)
            .map(|p| {
                let bus = bus.clone();
                std::thread::spawn(move || {
                    let mut ok = Vec::new();
                    for seq in 0..3_000u64 {
                        match bus.try_publish("t", rec(p, seq)) {
                            Ok(n) => {
                                assert_eq!(n, 2, "partial fan-out reported");
                                ok.push((p, seq));
                            }
                            Err(PublishError::Full) => {}
                            Err(e) => panic!("unexpected error: {e}"),
                        }
                    }
                    ok
                })
            })
            .collect();
        let mut accepted: Vec<(u64, u64)> = threads
            .into_iter()
            .flat_map(|t| t.join().expect("publisher thread panicked"))
            .collect();
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        let a = consume_a.await.expect("consumer a");
        let b = consume_b.await.expect("consumer b");

        assert_eq!(a, b, "round {}: subscribers diverged", round);
        let mut got = a.clone();
        got.sort_unstable();
        accepted.sort_unstable();
        assert_eq!(
            got, accepted,
            "round {}: delivered set != accepted set",
            round
        );
    }
}

/// A feedback publish racing a blocked batch admission never splits the
/// batch: it is either rejected or lands wholly before/after the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn try_publish_never_interleaves_a_batch() {
    let bus = StreamBus::new();
    let mut rx = bus.subscribe("t");
    let n = (STREAM_QUEUE_CAPACITY * 2) as u64;
    let publisher = tokio::spawn({
        let bus = bus.clone();
        async move { bus.publish_batch("t", batch(1, n)).await }
    });
    let mut feedback_ok = 0u64;
    let mut got = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while got.iter().filter(|k: &&(u64, u64)| k.0 == 1).count() < n as usize {
        assert!(
            std::time::Instant::now() < deadline,
            "batch never completed"
        );
        if bus.try_publish("t", rec(9, feedback_ok)).is_ok() {
            feedback_ok += 1;
        }
        if let Ok(r) = tokio::time::timeout(Duration::from_millis(1), rx.recv()).await {
            got.push(key(&r.expect("closed")));
        }
    }
    assert!(matches!(publisher.await.expect("panicked"), Ok(1)));
    got.extend(drain_now(&mut rx));

    let first = got.iter().position(|k| k.0 == 1).expect("batch start");
    let batch_run: Vec<_> = got[first..first + n as usize].to_vec();
    assert_eq!(batch_run, (0..n).map(|i| (1, i)).collect::<Vec<_>>());
    assert_eq!(
        got.iter().filter(|k| k.0 == 9).count() as u64,
        feedback_ok,
        "accepted feedback records must all be delivered"
    );
}
