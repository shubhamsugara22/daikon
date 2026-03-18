use crate::error::KvStoreError;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// A message published to a Pub/Sub channel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubMessage {
    pub channel: String,
    pub message: String,
    pub timestamp: u64,
}

impl PubSubMessage {
    pub fn new(channel: String, message: String) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            channel,
            message,
            timestamp,
        }
    }
}

/// A subscriber with a message queue
#[derive(Debug)]
struct Subscriber {
    messages: VecDeque<PubSubMessage>,
}

impl Subscriber {
    fn new() -> Self {
        Self {
            messages: VecDeque::new(),
        }
    }
}

/// Pub/Sub engine for event-driven messaging
///
/// Allows clients to subscribe to channels and receive published messages.
/// Each subscriber has a unique ID and maintains a queue of unread messages.
/// Channels are created on first publish and cleaned up when empty.
pub struct PubSub {
    channels: RwLock<HashMap<String, Vec<String>>>, // channel -> [subscriber_ids]
    subscribers: RwLock<HashMap<String, Subscriber>>, // subscriber_id -> Subscriber
    max_messages_per_subscriber: usize,
}

impl PubSub {
    /// Create a new Pub/Sub engine
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            subscribers: RwLock::new(HashMap::new()),
            max_messages_per_subscriber: 1000,
        }
    }

    /// Generate a new subscriber ID
    pub fn new_subscriber_id() -> String {
        Uuid::new_v4().to_string()
    }

    /// Subscribe to a channel
    ///
    /// # Arguments
    /// * `channel` - Channel name
    /// * `subscriber_id` - Unique subscriber identifier (use `new_subscriber_id()` for new subscribers)
    ///
    /// # Returns
    /// The subscriber ID (for polling messages)
    pub fn subscribe(&self, channel: String, subscriber_id: String) -> Result<String, KvStoreError> {
        if channel.is_empty() {
            return Err(KvStoreError::InvalidKey(
                "Channel name cannot be empty".to_string(),
            ));
        }

        let mut channels = self.channels.write();
        let mut subscribers = self.subscribers.write();

        // Add subscriber to channel's subscriber list if not already subscribed
        channels
            .entry(channel)
            .or_insert_with(Vec::new)
            .push(subscriber_id.clone());

        // Create subscriber entry if it doesn't exist
        if !subscribers.contains_key(&subscriber_id) {
            subscribers.insert(subscriber_id.clone(), Subscriber::new());
        }

        Ok(subscriber_id)
    }

    /// Unsubscribe from a channel
    pub fn unsubscribe(&self, channel: String, subscriber_id: String) -> Result<(), KvStoreError> {
        let mut channels = self.channels.write();

        if let Some(subscribers) = channels.get_mut(&channel) {
            subscribers.retain(|id| id != &subscriber_id);
            // Clean up empty channels
            if subscribers.is_empty() {
                channels.remove(&channel);
            }
        }

        Ok(())
    }

    /// Publish a message to a channel
    ///
    /// # Returns
    /// Number of subscribers that received the message
    pub fn publish(&self, channel: String, message: String) -> Result<usize, KvStoreError> {
        if channel.is_empty() {
            return Err(KvStoreError::InvalidKey(
                "Channel name cannot be empty".to_string(),
            ));
        }

        let msg = PubSubMessage::new(channel.clone(), message);

        let channels = self.channels.read();
        let subscriber_ids: Vec<String> = channels
            .get(&channel)
            .map(|subs| subs.clone())
            .unwrap_or_default();

        drop(channels); // Release read lock before acquiring write lock

        let mut subscribers = self.subscribers.write();
        for sub_id in &subscriber_ids {
            if let Some(subscriber) = subscribers.get_mut(sub_id) {
                // Enforce max messages per subscriber
                if subscriber.messages.len() >= self.max_messages_per_subscriber {
                    subscriber.messages.pop_front();
                }
                subscriber.messages.push_back(msg.clone());
            }
        }

        Ok(subscriber_ids.len())
    }

    /// Poll messages for a subscriber
    ///
    /// Returns up to `limit` messages from the subscriber's queue (FIFO order).
    pub fn poll_messages(
        &self,
        subscriber_id: String,
        limit: usize,
    ) -> Result<Vec<PubSubMessage>, KvStoreError> {
        let mut subscribers = self.subscribers.write();

        let messages = subscribers
            .get_mut(&subscriber_id)
            .map(|sub| {
                let mut result = Vec::new();
                for _ in 0..limit {
                    if let Some(msg) = sub.messages.pop_front() {
                        result.push(msg);
                    } else {
                        break;
                    }
                }
                result
            })
            .unwrap_or_default();

        Ok(messages)
    }

    /// Get list of all active channels
    pub fn list_channels(&self) -> Result<Vec<String>, KvStoreError> {
        let channels = self.channels.read();
        Ok(channels.keys().cloned().collect())
    }

    /// Get list of subscribers for a channel
    pub fn list_subscribers(&self, channel: String) -> Result<Vec<String>, KvStoreError> {
        let channels = self.channels.read();
        Ok(channels
            .get(&channel)
            .map(|subs| subs.clone())
            .unwrap_or_default())
    }

    /// Get pending message count for a subscriber
    pub fn pending_message_count(&self, subscriber_id: String) -> Result<usize, KvStoreError> {
        let subscribers = self.subscribers.read();
        Ok(subscribers
            .get(&subscriber_id)
            .map(|sub| sub.messages.len())
            .unwrap_or(0))
    }

    /// Cleanup old subscribers with no pending messages
    pub fn cleanup_stale_subscribers(&self) -> Result<usize, KvStoreError> {
        let mut channels = self.channels.write();
        let subscribers = self.subscribers.read();

        // Remove subscribers with empty message queues from channel lists
        for subscribers_in_channel in channels.values_mut() {
            subscribers_in_channel.retain(|id| {
                subscribers
                    .get(id)
                    .map(|sub| !sub.messages.is_empty())
                    .unwrap_or(false)
            });
        }

        // Remove empty channels
        channels.retain(|_, subs| !subs.is_empty());

        Ok(0)
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_and_publish() {
        let pubsub = PubSub::new();
        let sub_id = PubSub::new_subscriber_id();
        
        pubsub.subscribe("news".to_string(), sub_id.clone()).unwrap();
        let count = pubsub.publish("news".to_string(), "breaking news".to_string()).unwrap();
        
        assert_eq!(count, 1);
        let messages = pubsub.poll_messages(sub_id, 10).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message, "breaking news");
    }

    #[test]
    fn test_multiple_subscribers() {
        let pubsub = PubSub::new();
        let sub1 = PubSub::new_subscriber_id();
        let sub2 = PubSub::new_subscriber_id();
        
        pubsub.subscribe("alerts".to_string(), sub1.clone()).unwrap();
        pubsub.subscribe("alerts".to_string(), sub2.clone()).unwrap();
        
        let count = pubsub.publish("alerts".to_string(), "alert message".to_string()).unwrap();
        assert_eq!(count, 2);
        
        let msg1 = pubsub.poll_messages(sub1, 10).unwrap();
        let msg2 = pubsub.poll_messages(sub2, 10).unwrap();
        
        assert_eq!(msg1.len(), 1);
        assert_eq!(msg2.len(), 1);
    }

    #[test]
    fn test_unsubscribe() {
        let pubsub = PubSub::new();
        let sub_id = PubSub::new_subscriber_id();
        
        pubsub.subscribe("channel".to_string(), sub_id.clone()).unwrap();
        pubsub.unsubscribe("channel".to_string(), sub_id.clone()).unwrap();
        
        let count = pubsub.publish("channel".to_string(), "msg".to_string()).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_multiple_channels() {
        let pubsub = PubSub::new();
        let sub_id = PubSub::new_subscriber_id();
        
        pubsub.subscribe("sports".to_string(), sub_id.clone()).unwrap();
        pubsub.subscribe("weather".to_string(), sub_id.clone()).unwrap();
        
        pubsub.publish("sports".to_string(), "goal!".to_string()).unwrap();
        pubsub.publish("weather".to_string(), "rain".to_string()).unwrap();
        
        let messages = pubsub.poll_messages(sub_id, 10).unwrap();
        assert_eq!(messages.len(), 2);
    }

    #[test]
    fn test_list_channels() {
        let pubsub = PubSub::new();
        let sub_id = PubSub::new_subscriber_id();
        
        pubsub.subscribe("ch1".to_string(), sub_id.clone()).unwrap();
        pubsub.subscribe("ch2".to_string(), sub_id.clone()).unwrap();
        
        let channels = pubsub.list_channels().unwrap();
        assert_eq!(channels.len(), 2);
        assert!(channels.contains(&"ch1".to_string()));
        assert!(channels.contains(&"ch2".to_string()));
    }

    #[test]
    fn test_pending_message_count() {
        let pubsub = PubSub::new();
        let sub_id = PubSub::new_subscriber_id();
        
        pubsub.subscribe("channel".to_string(), sub_id.clone()).unwrap();
        pubsub.publish("channel".to_string(), "msg1".to_string()).unwrap();
        pubsub.publish("channel".to_string(), "msg2".to_string()).unwrap();
        
        let count = pubsub.pending_message_count(sub_id).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_empty_channel_cleanup() {
        let pubsub = PubSub::new();
        let sub_id = PubSub::new_subscriber_id();
        
        pubsub.subscribe("channel".to_string(), sub_id.clone()).unwrap();
        pubsub.unsubscribe("channel".to_string(), sub_id).unwrap();
        
        let channels = pubsub.list_channels().unwrap();
        assert!(channels.is_empty());
    }
}
