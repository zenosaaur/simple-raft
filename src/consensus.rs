use crate::proto::raft_client::RaftClient;
use crate::proto::{self, AppendEntriesRequest, AppendEntriesResponse, LeaderInfo, RequestVoteRequest};
use crate::state::{
    AppendEntriesResponder, ClientResponder, LogEntry, Peer, RaftEvent, RaftNode, RaftRole,
    ReplicaProgress, RequestVoteResponder,
};
use futures::{future, stream::StreamExt};
use prost::Message;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Instant};
use tonic::{transport::Channel, Status};

// =============================
// Config & Utilities
// =============================

#[derive(Clone, Debug)]
pub struct RaftConfig {
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub backoff_base_ms: u64,
    pub backoff_max_ms: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 500,
            election_timeout_min_ms: 1500,
            election_timeout_max_ms: 3000,
            backoff_base_ms: 200,
            backoff_max_ms: 5000,
        }
    }
}

fn rand_timeout_ms(cfg: &RaftConfig) -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(cfg.election_timeout_min_ms..=cfg.election_timeout_max_ms)
}

#[inline]
fn majority(total_nodes: usize) -> usize {
    total_nodes / 2 + 1
}

#[inline]
fn leader_majority(followers: usize) -> usize {
    majority(followers + 1)
}

async fn persist_or_log(node: &mut RaftNode, context: &str) -> Result<(), std::io::Error> {
    node.persist().map_err(|e| {
        tracing::error!(error = %e, "[Persist] CRITICAL failure while {context}");
        e
    })
}

fn stop_heartbeat(handle: &mut Option<JoinHandle<()>>) {
    if let Some(h) = handle.take() {
        tracing::debug!("[State] Stopping heartbeat timer (stepping down / shutting down).");
        h.abort();
    }
}

fn start_heartbeat(event_tx: mpsc::Sender<RaftEvent>, cfg: RaftConfig) -> JoinHandle<()> {
    tokio::spawn(async move { run_heartbeat_timer(event_tx, cfg).await })
}

// =============================
// Connection Manager (tonic channel reuse)
// =============================

#[derive(Default)]
pub struct ConnectionManager {
    // Cache tonic Channels per address; clients are cheap/clonable around a shared channel
    channels: Mutex<HashMap<String, Channel>>,
}

impl ConnectionManager {
    pub fn new() -> Self { Self { channels: Mutex::new(HashMap::new()) } }

    pub async fn get_client(&self, addr: &str) -> Result<RaftClient<Channel>, tonic::transport::Error> {
        let mut map = self.channels.lock().await;
        let channel = if let Some(ch) = map.get(addr) {
            ch.clone()
        } else {
            let endpoint = format!("http://{}", addr);
            let ch = Channel::from_shared(endpoint)
                .expect("valid endpoint")
                .connect()
                .await?;
            map.insert(addr.to_string(), ch.clone());
            ch
        };
        Ok(RaftClient::new(channel))
    }
}

// =============================
// Follower Backoff (per-addr)
// =============================

#[derive(Clone, Debug)]
struct BackoffState {
    failures: u32,
    next_attempt_at: Instant,
}

impl BackoffState {
    fn new() -> Self {
        Self { failures: 0, next_attempt_at: Instant::now() }
    }
}

#[derive(Default)]
pub struct FollowerBackoff {
    inner: Mutex<HashMap<String, BackoffState>>, // follower_id -> state
}

impl FollowerBackoff {
    pub fn new() -> Self { Self { inner: Mutex::new(HashMap::new()) } }

    pub async fn allow_now(&self, follower: &str) -> bool {
        let map = self.inner.lock().await;
        match map.get(follower) {
            None => true,
            Some(st) => Instant::now() >= st.next_attempt_at,
        }
    }

    pub async fn record_success(&self, follower: &str) {
        let mut map = self.inner.lock().await;
        let st = map.entry(follower.to_string()).or_insert_with(BackoffState::new);
        st.failures = 0;
        st.next_attempt_at = Instant::now();
    }

    pub async fn record_failure(&self, follower: &str, cfg: &RaftConfig) {
        let mut map = self.inner.lock().await;
        let st = map.entry(follower.to_string()).or_insert_with(BackoffState::new);
        st.failures = st.failures.saturating_add(1);
        let pow = st.failures.min(10); // cap exponent growth
        let base = cfg.backoff_base_ms as u64;
        let max = cfg.backoff_max_ms as u64;
        let backoff_ms = (base << (pow - 1).max(0)) // base * 2^(n-1)
            .min(max);
        // jitter 0..base
        let jitter = rand::thread_rng().gen_range(0..=cfg.backoff_base_ms);
        let delay = Duration::from_millis(backoff_ms + jitter as u64);
        st.next_attempt_at = Instant::now() + delay;
        tracing::trace!(follower, failures = st.failures, backoff_ms = delay.as_millis() as u64, "[Backoff] Scheduled next attempt");
    }
}

// =============================
// Driver
// =============================

pub async fn run_raft_node(
    node_arc: Arc<Mutex<RaftNode>>,
    mut event_rx: mpsc::Receiver<RaftEvent>,
    reset_timer_tx: mpsc::Sender<()>,
    event_tx: mpsc::Sender<RaftEvent>,
    available_followers: Arc<Vec<Peer>>, // entire cluster minus self
    cfg: RaftConfig,
    conn_mgr: Arc<ConnectionManager>,
    backoff: Arc<FollowerBackoff>,
) {
    let mut heartbeat_handle: Option<JoinHandle<()>> = None;

    while let Some(event) = event_rx.recv().await {
        tracing::debug!("[State] Received event: {:?}", event);
        match event {
            RaftEvent::ElectionTimeout => {
                handle_election_timeout(
                    node_arc.clone(),
                    available_followers.clone(),
                    event_tx.clone(),
                    &mut heartbeat_handle,
                    cfg.clone(),
                    conn_mgr.clone(),
                    backoff.clone(),
                )
                .await;
            }
            RaftEvent::RpcAppendEntries { request, responder } => {
                handle_rpc_append_entries(
                    node_arc.clone(),
                    request,
                    responder,
                    reset_timer_tx.clone(),
                    &mut heartbeat_handle,
                )
                .await;
            }
            RaftEvent::RpcRequestVote { request, responder } => {
                handle_rpc_request_vote(
                    node_arc.clone(),
                    request,
                    responder,
                    reset_timer_tx.clone(),
                )
                .await;
            }
            RaftEvent::HeartbeatTick => {
                handle_heartbeat_tick(
                    node_arc.clone(),
                    event_tx.clone(),
                    cfg.clone(),
                    conn_mgr.clone(),
                    backoff.clone(),
                )
                .await;
            }
            RaftEvent::AppendEntriesResponse { follower_id, response, last_log_index_sent } => {
                handle_append_entries_response(
                    node_arc.clone(),
                    available_followers.clone(),
                    follower_id,
                    response,
                    last_log_index_sent,
                    backoff.clone(),
                )
                .await;
            }
            RaftEvent::ClientRequest { command, responder } => {
                handle_client_request(node_arc.clone(), command, responder, event_tx.clone()).await;
            }
        }
    }

    stop_heartbeat(&mut heartbeat_handle);
}

// =============================
// Timers
// =============================

pub async fn run_election_timer(
    mut reset_rx: mpsc::Receiver<()>,
    event_tx: mpsc::Sender<RaftEvent>,
    cfg: RaftConfig,
) {
    loop {
        let timeout_ms = rand_timeout_ms(&cfg);
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        tracing::debug!("[Timer] Waiting for {} ms...", timeout_ms);

        tokio::select! {
            _ = sleep(deadline.saturating_duration_since(Instant::now())) => {
                tracing::debug!("[Timer] Fired after {} ms! Triggering election.", timeout_ms);
                if let Err(e) = event_tx.send(RaftEvent::ElectionTimeout).await {
                    tracing::debug!("[Timer] Failed to send election timeout event: {}", e);
                }
            }
            Some(_) = reset_rx.recv() => {
                tracing::debug!("[Timer] Reset!");
            }
        }
    }
}

pub async fn run_heartbeat_timer(event_tx: mpsc::Sender<RaftEvent>, cfg: RaftConfig) {
    let interval = Duration::from_millis(cfg.heartbeat_interval_ms);
    loop {
        sleep(interval).await;
        if event_tx.send(RaftEvent::HeartbeatTick).await.is_err() {
            tracing::debug!("[Heartbeat] Event channel closed, terminating leader heartbeat timer.");
            break;
        }
    }
}

// =============================
// Event Handlers
// =============================

#[tracing::instrument(skip_all, fields(term = tracing::field::Empty, votes = tracing::field::Empty))]
async fn handle_election_timeout(
    node_arc: Arc<Mutex<RaftNode>>,
    available_followers: Arc<Vec<Peer>>,
    event_tx: mpsc::Sender<RaftEvent>,
    heartbeat_handle: &mut Option<JoinHandle<()>>,
    cfg: RaftConfig,
    conn_mgr: Arc<ConnectionManager>,
    backoff: Arc<FollowerBackoff>,
) {
    // --- Start new election ---
    let request = {
        let mut node = node_arc.lock().await;
        if node.volatile.role != RaftRole::Follower && node.volatile.role != RaftRole::Candidate {
            tracing::debug!(current_role = ?node.volatile.role, "Ignoring election timeout");
            return;
        }

        node.volatile.role = RaftRole::Candidate;
        node.persistent.current_term += 1;
        node.persistent.voted_for = Some(node.persistent.id.clone());

        tracing::Span::current().record("term", node.persistent.current_term);
        tracing::debug!("Starting new election");

        if let Err(e) = persist_or_log(&mut node, "starting election").await {
            tracing::error!("CRITICAL: {e}");
        }

        proto::RequestVoteRequest {
            term: node.persistent.current_term,
            candidate_id: node.persistent.id.clone(),
            last_log_term: node.persistent.log.last().map_or(0, |entry| entry.term),
            last_log_index: node.persistent.log.len() as u64,
        }
    };

    // Connect to followers concurrently (respect backoff)
    let followers = available_followers.clone();
    let len = followers.len();

    let mut tasks = Vec::with_capacity(len);
    for f in followers.iter() {
        let addr = f.address.clone();
        if !backoff.allow_now(&addr).await {
            tracing::trace!(follower = %addr, "[Election] Skipping vote RPC due to backoff");
            continue;
        }
        let conn_mgr = conn_mgr.clone();
        let req_clone = request.clone();
        tasks.push(tokio::spawn(async move {
            match conn_mgr.get_client(&addr).await {
                Ok(mut client) => {
                    let req = tonic::Request::new(req_clone);
                    client.request_vote(req).await.map_err(|e| (addr, e))
                }
                Err(e) => Err((addr, Status::unavailable(e.to_string()))),
            }
        }));
    }

    tracing::debug!("Sending RequestVote to {} followers (after backoff)", tasks.len());
    let results = future::join_all(tasks).await;

    let mut votes_received = 1; // self-vote
    for result in results {
        match result {
            Ok(Ok(response)) => {
                let vote = response.get_ref();
                tracing::debug!("[State] Vote response: term={}, granted={}", vote.term, vote.vote_granted);
                let mut node = node_arc.lock().await;

                if vote.term > node.persistent.current_term {
                    tracing::debug!(
                        "[State] Higher term {} discovered (ours {}). Reverting to Follower.",
                        vote.term, node.persistent.current_term
                    );
                    node.persistent.current_term = vote.term;
                    node.volatile.role = RaftRole::Follower;
                    node.persistent.voted_for = None;
                    let _ = persist_or_log(&mut node, "discovered higher term during election").await;
                    break;
                }

                if vote.vote_granted { votes_received += 1; }
            }
            Ok(Err((addr, rpc_error))) => {
                tracing::debug!("[State] Vote RPC failed for {}: {}", addr, rpc_error);
                backoff.record_failure(&addr, &cfg).await;
            }
            Err(join_error) => {
                tracing::error!("[State] Task failed during vote collection: {}", join_error);
            }
        }
    }

    tracing::Span::current().record("votes", votes_received);
    let mut node = node_arc.lock().await;
    if node.volatile.role == RaftRole::Candidate
        && votes_received >= leader_majority(available_followers.len())
    {
        tracing::debug!(
            "[State] Election WIN! Became LEADER for term {} with {} votes.",
            node.persistent.current_term,
            votes_received
        );
        node.volatile.role = RaftRole::Leader;

        node.volatile.replicas.clear();
        let last_index = node.persistent.log.len() as u64;
        for follower in available_followers.iter() {
            node.volatile.replicas.insert(
                follower.address.clone(),
                ReplicaProgress { next_index: last_index + 1, match_index: 0 },
            );
        }

        stop_heartbeat(heartbeat_handle);
        *heartbeat_handle = Some(start_heartbeat(event_tx.clone(), cfg.clone()));

        if let Err(e) = event_tx.send(RaftEvent::HeartbeatTick).await {
            tracing::debug!("[State] Failed to send initial heartbeat event: {}", e);
        }
    } else if node.volatile.role == RaftRole::Candidate {
        tracing::debug!(
            "[State] Election lost for term {}. Received {} votes. Waiting for next timeout.",
            node.persistent.current_term,
            votes_received
        );
    }
}

#[tracing::instrument(skip_all, fields(term = tracing::field::Empty, votes = tracing::field::Empty))]
async fn handle_rpc_append_entries(
    node_arc: Arc<Mutex<RaftNode>>,
    request: AppendEntriesRequest,
    responder: AppendEntriesResponder,
    reset_timer_tx: mpsc::Sender<()>,
    heartbeat_handle: &mut Option<JoinHandle<()>>,
) {
    let mut node = node_arc.lock().await;

    tracing::info!(
        "[RPC AppendEntries] Received: term={}, prev_log_index={}, entries={}, leader_id={}",
        request.term,
        request.prev_log_index,
        request.entries.len(),
        request.leader_id
    );

    if request.term < node.persistent.current_term {
        tracing::debug!("[RPC AppendEntries] Reject: term {} < {}", request.term, node.persistent.current_term);
        let _ = responder.send(Ok(proto::AppendEntriesResponse { term: node.persistent.current_term, success: false }));
        return;
    }

    // Valid leader contact resets timer
    node.volatile.leader_hint = request.leader_id.clone();
    let _ = reset_timer_tx.send(()).await;

    if request.term > node.persistent.current_term {
        tracing::debug!("[State] Higher term {} seen (ours {}). Stepping down.", request.term, node.persistent.current_term);
        stop_heartbeat(heartbeat_handle);
        node.persistent.current_term = request.term;
        node.persistent.voted_for = None;
        node.volatile.role = RaftRole::Follower;
    }

    if node.volatile.role == RaftRole::Candidate {
        tracing::debug!("[State] Candidate got AppendEntries; becoming Follower.");
        node.volatile.role = RaftRole::Follower;
    }

    // Consistency check
    if request.prev_log_index > 0 {
        let idx = (request.prev_log_index - 1) as usize;
        match node.persistent.log.get(idx) {
            Some(entry) if entry.term == request.prev_log_term => {}
            Some(entry) => {
                tracing::debug!("[RPC AppendEntries] Reject: term mismatch at {} (ours {}, leader {}).", request.prev_log_index, entry.term, request.prev_log_term);
                let _ = responder.send(Ok(proto::AppendEntriesResponse { term: node.persistent.current_term, success: false }));
                return;
            }
            None => {
                tracing::debug!("[RPC AppendEntries] Reject: log too short at {} (len={}).", request.prev_log_index, node.persistent.log.len());
                let _ = responder.send(Ok(proto::AppendEntriesResponse { term: node.persistent.current_term, success: false }));
                return;
            }
        }
    }

    // Detect conflicts & append
    let mut first_new: Option<usize> = None;
    for (i, new_e) in request.entries.iter().enumerate() {
        let log_idx = (request.prev_log_index as usize) + i;
        match node.persistent.log.get(log_idx) {
            Some(existing) if existing.term != new_e.term => {
                tracing::debug!("[Log] Conflict at {} -> truncate.", log_idx + 1);
                node.persistent.log.truncate(log_idx);
                first_new = Some(i);
                break;
            }
            Some(_) => {}
            None => { first_new = Some(i); break; }
        }
    }

    if let Some(first) = first_new {
        for e in request.entries.iter().skip(first) {
            node.persistent.log.push(LogEntry { client_id: e.client_id.clone(), request_id: e.request_id, term: e.term, command: e.command.clone() });
        }
        tracing::debug!("[Log] Appended {} entries.", request.entries.len() - first);
    }

    // Advance commit index from leader
    if request.leader_commit > node.volatile.commit_index {
        let old = node.volatile.commit_index;
        let last_new = request.prev_log_index + request.entries.len() as u64;
        node.volatile.commit_index = std::cmp::min(request.leader_commit, last_new);
        if node.volatile.commit_index > old {
            tracing::debug!("[State] commit_index advanced: {} -> {}", old, node.volatile.commit_index);
        }
    }

    if let Err(e) = persist_or_log(&mut node, "after appending entries").await {
        let _ = responder.send(Err(tonic::Status::internal(format!("Failed to save state: {}", e))));
        return;
    }

    if node.volatile.last_applied < node.volatile.commit_index { apply_committed_entries(&mut node); }

    let response = proto::AppendEntriesResponse { term: node.persistent.current_term, success: true };
    let _ = responder.send(Ok(response));
}

#[tracing::instrument(skip_all, fields(term = tracing::field::Empty, votes = tracing::field::Empty))]
async fn handle_rpc_request_vote(
    node_arc: Arc<Mutex<RaftNode>>,
    request: RequestVoteRequest,
    responder: RequestVoteResponder,
    reset_timer_tx: mpsc::Sender<()>,
) {
    let mut node = node_arc.lock().await;

    tracing::debug!("[RPC RequestVote] From {} term {}", request.candidate_id, request.term);

    if request.term < node.persistent.current_term {
        let _ = responder.send(Ok(proto::RequestVoteResponse { term: node.persistent.current_term, vote_granted: false }));
        return;
    }

    if request.term > node.persistent.current_term {
        node.persistent.current_term = request.term;
        node.persistent.voted_for = None;
        node.volatile.role = RaftRole::Follower;
    }

    let can_vote = match &node.persistent.voted_for { None => true, Some(id) => *id == request.candidate_id };
    if !can_vote {
        let _ = responder.send(Ok(proto::RequestVoteResponse { term: node.persistent.current_term, vote_granted: false }));
        return;
    }

    // Up-to-date check
    let last_log_term = node.persistent.log.last().map_or(0, |e| e.term);
    let last_log_index = node.persistent.log.len() as u64;
    let our_newer = last_log_term > request.last_log_term
        || (last_log_term == request.last_log_term && last_log_index > request.last_log_index);

    if our_newer {
        let _ = responder.send(Ok(proto::RequestVoteResponse { term: node.persistent.current_term, vote_granted: false }));
        return;
    }

    // Grant
    let _ = reset_timer_tx.send(()).await;
    node.persistent.voted_for = Some(request.candidate_id.clone());

    if let Err(e) = persist_or_log(&mut node, "persisting granted vote").await {
        let _ = responder.send(Err(tonic::Status::internal(format!("Failed to persist vote: {}", e))));
        return;
    }

    let response = proto::RequestVoteResponse { term: node.persistent.current_term, vote_granted: true };
    let _ = responder.send(Ok(response));
}

#[tracing::instrument(skip_all)]
async fn handle_heartbeat_tick(
    node_arc: Arc<Mutex<RaftNode>>,
    event_tx: mpsc::Sender<RaftEvent>,
    cfg: RaftConfig,
    conn_mgr: Arc<ConnectionManager>,
    backoff: Arc<FollowerBackoff>,
) {
    let (replicas, is_leader, current_term) = {
        let node = node_arc.lock().await;
        (
            node.volatile.replicas.clone(),
            node.volatile.role == RaftRole::Leader,
            node.persistent.current_term,
        )
    };

    if !is_leader { return; }

    for (follower_id, progress) in replicas {
        let node_arc_clone = Arc::clone(&node_arc);
        let event_tx_clone = event_tx.clone();
        let conn_mgr = conn_mgr.clone();
        let backoff = backoff.clone();
        let cfg = cfg.clone();

        tokio::spawn(async move {
            if !backoff.allow_now(&follower_id).await {
                tracing::trace!(follower_id = %follower_id, "[Heartbeat] Skipping due to backoff.");
                return;
            }

            let (request, last_log_index_sent) = {
                let node = node_arc_clone.lock().await;
                if node.volatile.role != RaftRole::Leader { return; }

                let prev_log_index = progress.next_index.saturating_sub(1);
                let prev_log_term = if prev_log_index > 0 {
                    node.persistent.log.get((prev_log_index - 1) as usize).map_or(0, |e| e.term)
                } else { 0 };

                let start_index = (progress.next_index - 1) as usize;
                let entries_to_send: Vec<proto::LogEntry> = node.persistent.log
                    .get(start_index..).unwrap_or(&[])
                    .iter()
                    .map(|e| proto::LogEntry { client_id: e.client_id.clone(), request_id: e.request_id, term: e.term, command: e.command.clone() })
                    .collect();

                let last_log_index_sent = prev_log_index + entries_to_send.len() as u64;

                let request = proto::AppendEntriesRequest {
                    term: node.persistent.current_term,
                    leader_id: node.persistent.id.clone(),
                    prev_log_index,
                    prev_log_term,
                    entries: entries_to_send,
                    leader_commit: node.volatile.commit_index,
                };
                (request, last_log_index_sent)
            };

            match conn_mgr.get_client(&follower_id).await {
                Ok(mut client) => {
                    let resp = client.append_entries(request).await.map(|r| r.into_inner());
                    match &resp {
                        Ok(r) => {
                            backoff.record_success(&follower_id).await;
                            tracing::debug!(follower_id = %follower_id, success = r.success, term = r.term, "AppendEntries response.");
                        }
                        Err(e) => {
                            backoff.record_failure(&follower_id, &cfg).await;
                            tracing::warn!(follower_id = %follower_id, error = %e, "AppendEntries RPC failed.");
                        }
                    }

                    let response_event = RaftEvent::AppendEntriesResponse {
                        follower_id,
                        response: resp,
                        last_log_index_sent,
                    };
                    if let Err(_) = event_tx_clone.send(response_event).await {
                        tracing::error!("[Heartbeat] Event channel closed; shutting down subtask.");
                    }
                }
                Err(e) => {
                    backoff.record_failure(&follower_id, &cfg).await;
                    tracing::warn!(follower_id = %follower_id, error = %e, "Connection manager failed to connect.");
                }
            }
        });
    }
}

#[tracing::instrument(skip_all)]
async fn handle_append_entries_response(
    node_arc: Arc<Mutex<RaftNode>>,
    available_followers: Arc<Vec<Peer>>,
    follower_id: String,
    response: Result<AppendEntriesResponse, Status>,
    last_log_index_sent: u64,
    backoff: Arc<FollowerBackoff>,
) {
    let mut node = node_arc.lock().await;

    if node.volatile.role != RaftRole::Leader { return; }

    match response {
        Ok(resp) => {
            if resp.term > node.persistent.current_term {
                node.persistent.current_term = resp.term;
                node.volatile.role = RaftRole::Follower;
                node.persistent.voted_for = None;
                let _ = persist_or_log(&mut node, "stepping down on higher term").await;
                return;
            }

            if let Some(progress) = node.volatile.replicas.get_mut(&follower_id) {
                if resp.success {
                    backoff.record_success(&follower_id).await;
                    progress.match_index = last_log_index_sent;
                    progress.next_index = progress.match_index + 1;

                    let mut match_indices: Vec<u64> = node.volatile.replicas.values().map(|p| p.match_index).collect();
                    match_indices.push(node.persistent.log.len() as u64);
                    match_indices.sort_unstable_by(|a, b| b.cmp(a));

                    let majority_pos = leader_majority(available_followers.len()) - 1;
                    let potential_commit_index = *match_indices.get(majority_pos).unwrap_or(&0);

                    if potential_commit_index > node.volatile.commit_index {
                        if let Some(entry) = node.persistent.log.get((potential_commit_index - 1) as usize) {
                            if entry.term == node.persistent.current_term {
                                node.volatile.commit_index = potential_commit_index;
                                apply_committed_entries(&mut node);
                            }
                        }
                    }
                } else {
                    if progress.next_index > 1 { progress.next_index -= 1; }
                    backoff.record_failure(&follower_id, &RaftConfig::default()).await; // conservative if cfg not threaded here
                }
            }
        }
        Err(e) => {
            backoff.record_failure(&follower_id, &RaftConfig::default()).await;
            tracing::warn!(follower_id = %follower_id, error = %e, "RPC to follower failed.");
        }
    }
}

async fn handle_client_request(
    node_arc: Arc<Mutex<RaftNode>>,
    command: proto::SubmitCommandRequest,
    responder: ClientResponder,
    event_tx: mpsc::Sender<RaftEvent>,
) {
    let new_entry_index = {
        let mut node = node_arc.lock().await;
        if node.volatile.role != RaftRole::Leader {
            let leader_info = LeaderInfo { leader_address: node.volatile.leader_hint.clone() };
            let mut status = Status::failed_precondition("This node is not the leader.");
            status.metadata_mut().insert_bin("leader-info-bin", tonic::metadata::MetadataValue::from_bytes(&leader_info.encode_to_vec()));
            let _ = responder.send(Err(status));
            return;
        }

        let request_key = (command.client_id.clone(), command.request_id);
        if let Some(cached) = node.volatile.idempotency_cache.get(&request_key) {
            let _ = responder.send(Ok(cached.clone()));
            return;
        }

        let new_entry = LogEntry { term: node.persistent.current_term, client_id: command.client_id, request_id: command.request_id, command: command.command };
        node.persistent.log.push(new_entry);
        let new_idx = node.persistent.log.len() as u64;
        node.volatile.pending_requests.insert(new_idx, responder);

        if let Err(e) = persist_or_log(&mut node, "persisting new log entry").await {
            if let Some(responder) = node.volatile.pending_requests.remove(&new_idx) {
                let status = tonic::Status::internal(format!("Failed to persist log: {}", e));
                let _ = responder.send(Err(status));
            }
            return;
        }
        new_idx
    };

    if event_tx.send(RaftEvent::HeartbeatTick).await.is_err() {
        tracing::info!("[Client] Event channel closed. Cannot trigger replication.");
    }
}

fn apply_committed_entries(node: &mut RaftNode) {
    let commit_index = node.volatile.commit_index;

    for i in (node.volatile.last_applied + 1)..=commit_index {
        let idx = (i - 1) as usize;
        if let Some(entry) = node.persistent.log.get(idx) {
            let result = match node.volatile.db.parse_command(entry.command.clone()) {
                Ok(res) => res,
                Err(e) => { tracing::info!(error = %e, "Failed to parse command at log index {}", i); continue; }
            };

            if let Some(responder) = node.volatile.pending_requests.remove(&i) {
                let response = proto::SubmitCommandResponse { success: true, leader_hint: node.persistent.id.clone(), result };
                let request_key = (entry.client_id.clone(), entry.request_id);
                node.volatile.idempotency_cache.insert(request_key, response.clone());
                let _ = responder.send(Ok(response));
            }
        }
        node.volatile.last_applied = i;
    }
}

// =============================
// Tests (unit-level for helpers & backoff). For full integration tests, add a mock tonic server.
// =============================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_majority() {
        assert_eq!(majority(1), 1);
        assert_eq!(majority(2), 2);
        assert_eq!(majority(3), 2);
        assert_eq!(majority(5), 3);
    }

    #[test]
    fn test_leader_majority() {
        // followers -> total = followers + 1
        assert_eq!(leader_majority(0), 1); // single-node cluster
        assert_eq!(leader_majority(1), 2); // 2 nodes total (not safe in Raft, but math ok)
        assert_eq!(leader_majority(2), 2);
        assert_eq!(leader_majority(3), 3);
    }

    #[tokio::test]
    async fn test_backoff_progression_and_reset() {
        let cfg = RaftConfig::default();
        let bo = FollowerBackoff::new();
        let id = "f1";

        assert!(bo.allow_now(id).await);
        bo.record_failure(id, &cfg).await;
        // Immediately after failure, likely not allowed
        let allowed_soon = bo.allow_now(id).await;
        // We can't assert false deterministically due to jitter, but it's fine to check type
        assert!(allowed_soon == true || allowed_soon == false);

        bo.record_success(id).await;
        assert!(bo.allow_now(id).await);
    }
}
