use serde::{Deserialize, Serialize};
use tonic::client;
use std::fs::File;

#[derive(Deserialize, Serialize, Debug, Clone, Copy, Default, PartialEq)]
pub enum RaftRole {
    Follower,
    #[default]
    Candidate,
    Leader,
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
    pub command: String,
}

#[derive(Debug, Default,Clone)]
pub struct RaftVolatileState {
    pub role: RaftRole,
    pub commit_index: u64,
    pub last_applied: u64,
}

#[derive(Debug,Clone)]
pub struct RaftNode {
    pub persistent: RaftPersistentState,
    pub volatile: RaftVolatileState,
}

impl RaftNode {
    pub fn persist(&self) -> Result<(), std::io::Error> {
        let file = File::create("state.json")?;
        serde_json::to_writer_pretty(file, &self.persistent)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum RaftEvent {
    ElectionTimeout,
}

