use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File};
use tokio::sync::oneshot;
use tonic::Status;

use crate::{hash_table::Db, proto};

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

#[derive(Default)]
pub struct RaftVolatileState {
    pub db: Db,
    pub role: RaftRole,
    pub commit_index: u64,
    pub last_applied: u64,
    pub replicas: HashMap<String, ReplicaProgress>,
    pub leader_hint: String,
    pub pending_requests: HashMap<u64, ClientResponder>,
    pub idempotency_cache: HashMap<(String, u64), proto::SubmitCommandResponse>,
}

pub struct RaftNode {
    pub persistent: RaftPersistentState,
    pub volatile: RaftVolatileState,
    pub state_path: String,
}

impl RaftNode {
    pub fn persist(&self) -> Result<(), std::io::Error> {
        let file = File::create(self.state_path.as_str())?;
        serde_json::to_writer_pretty(file, &self.persistent)?;
        Ok(())
    }
}

pub type AppendEntriesResponder =
    oneshot::Sender<Result<proto::AppendEntriesResponse, tonic::Status>>;
pub type RequestVoteResponder = oneshot::Sender<Result<proto::RequestVoteResponse, tonic::Status>>;
pub type ClientResponder = oneshot::Sender<Result<proto::SubmitCommandResponse, tonic::Status>>;

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
}
