use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use tokio::sync::oneshot;
use tonic::Status;

use crate::{hash_table::Db, proto};

const IDEMPOTENCY_CACHE_SIZE: usize = 10_000;

#[derive(Deserialize, Serialize, Debug, Clone, Copy, Default, PartialEq)]
pub enum RaftRole {
    Follower,
    #[default]
    Candidate,
    Leader,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Peer {
    pub id: String,
    pub address: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub host: String,
    pub domain: String,
    pub port: u16,
    pub peers: Vec<Peer>,
    pub log_file: String,
    pub state_file: String,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct RaftPersistentState {
    pub id: String,
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub log: Vec<LogEntry>,
    /// Offset of the first log entry (after log compaction).
    /// Real index = log_offset + position in log vector.
    #[serde(default)]
    pub log_offset: u64,
}

/// Snapshot of the state machine for log compaction.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Snapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>, // Serialized Db state
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub client_id: String,
    pub request_id: u64,
    pub command: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ReplicaProgress {
    /// Next log index the leader will try to send to this follower.
    pub next_index: u64,
    /// Highest log index known to be replicated on this follower.
    pub match_index: u64,
}

pub struct RaftVolatileState {
    pub db: Db,
    pub role: RaftRole,
    pub commit_index: u64,
    pub last_applied: u64,
    pub replicas: HashMap<String, ReplicaProgress>,
    pub leader_hint: String,
    pub pending_requests: HashMap<u64, ClientResponder>,
    pub idempotency_cache: LruCache<(String, u64), proto::SubmitCommandResponse>,
}

impl Default for RaftVolatileState {
    fn default() -> Self {
        Self {
            db: Db::default(),
            role: RaftRole::default(),
            commit_index: 0,
            last_applied: 0,
            replicas: HashMap::new(),
            leader_hint: String::new(),
            pending_requests: HashMap::new(),
            idempotency_cache: LruCache::new(
                NonZeroUsize::new(IDEMPOTENCY_CACHE_SIZE).unwrap()
            ),
        }
    }
}

pub struct RaftNode {
    pub persistent: RaftPersistentState,
    pub volatile: RaftVolatileState,
    pub state_path: String,
    pub snapshot_path: String,
}

impl RaftNode {
    pub async fn persist(&self) -> Result<(), std::io::Error> {
        let data = serde_json::to_vec(&self.persistent)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        tokio::fs::write(&self.state_path, data).await
    }

    /// Get a log entry by its absolute index (accounting for log_offset).
    pub fn get_log_entry(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 || index <= self.persistent.log_offset {
            return None;
        }
        let vec_index = (index - self.persistent.log_offset - 1) as usize;
        self.persistent.log.get(vec_index)
    }

    /// Get the absolute index of the last log entry.
    pub fn last_log_index(&self) -> u64 {
        self.persistent.log_offset + self.persistent.log.len() as u64
    }

    /// Get the term of the last log entry (or 0 if log is empty).
    pub fn last_log_term(&self) -> u64 {
        self.persistent.log.last().map_or(0, |e| e.term)
    }

    /// Persist a snapshot to disk.
    pub async fn persist_snapshot(&self, snapshot: &Snapshot) -> Result<(), std::io::Error> {
        let data = serde_json::to_vec(snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // Write to temp file then rename for atomicity
        let temp_path = format!("{}.tmp", self.snapshot_path);
        tokio::fs::write(&temp_path, data).await?;
        tokio::fs::rename(&temp_path, &self.snapshot_path).await
    }

    /// Load snapshot from disk if it exists.
    pub async fn load_snapshot(&self) -> Result<Option<Snapshot>, std::io::Error> {
        match tokio::fs::read(&self.snapshot_path).await {
            Ok(data) => {
                let snapshot: Snapshot = serde_json::from_slice(&data)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                Ok(Some(snapshot))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub type AppendEntriesResponder =
    oneshot::Sender<Result<proto::AppendEntriesResponse, tonic::Status>>;
pub type RequestVoteResponder = oneshot::Sender<Result<proto::RequestVoteResponse, tonic::Status>>;
pub type ClientResponder = oneshot::Sender<Result<proto::SubmitCommandResponse, tonic::Status>>;
pub type InstallSnapshotResponder =
    oneshot::Sender<Result<proto::InstallSnapshotResponse, tonic::Status>>;

#[derive(Debug)]
pub enum RaftEvent {
    ElectionTimeout,
    HeartbeatTick,

    RpcAppendEntries {
        request: proto::AppendEntriesRequest,
        responder: AppendEntriesResponder,
    },

    RpcRequestVote {
        request: proto::RequestVoteRequest,
        responder: RequestVoteResponder,
    },

    AppendEntriesResponse {
        follower_id: String,
        response: Result<proto::AppendEntriesResponse, Status>,
        last_log_index_sent: u64,
    },
    ClientRequest {
        command: proto::SubmitCommandRequest,
        responder: ClientResponder,
    },

    RpcInstallSnapshot {
        request: proto::InstallSnapshotRequest,
        responder: InstallSnapshotResponder,
    },

    InstallSnapshotResponse {
        follower_id: String,
        response: Result<proto::InstallSnapshotResponse, Status>,
        snapshot_last_index: u64,
    },
}
