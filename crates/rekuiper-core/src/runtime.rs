use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use crate::model::StreamRecord;

pub type StreamSender = broadcast::Sender<StreamRecord>;
pub type StreamReceiver = broadcast::Receiver<StreamRecord>;

#[derive(Clone, Default)]
pub struct StreamBus {
    topics: Arc<RwLock<HashMap<String, StreamSender>>>,
}

impl StreamBus {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_or_create(&self, stream_name: &str) -> StreamSender {
        let mut map = self.topics.write();
        if let Some(sender) = map.get(stream_name) {
            sender.clone()
        } else {
            let (tx, _rx) = broadcast::channel(1024);
            map.insert(stream_name.to_string(), tx.clone());
            tx
        }
    }

    pub fn publish(&self, stream_name: &str, record: StreamRecord) -> Result<usize, broadcast::error::SendError<StreamRecord>> {
        let tx = self.get_or_create(stream_name);
        tx.send(record)
    }

    pub fn subscribe(&self, stream_name: &str) -> StreamReceiver {
        let tx = self.get_or_create(stream_name);
        tx.subscribe()
    }
}
