use crate::proto::raft_client::RaftClient;
use crate::proto::{self, AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest};
use crate::state::{
    AppendEntriesResponder, LogEntry, Peer, RaftEvent, RaftNode, RaftRole, ReplicaProgress,
    RequestVoteResponder,
};
use futures::{future, stream::StreamExt};
use rand::Rng;
use tracing::info;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::Status;

const HEARTBEAT_INTERVAL_MS: u64 = 50;

pub async fn run_election_timer(
    mut reset_rx: mpsc::Receiver<()>,
    event_tx: mpsc::Sender<RaftEvent>,
) {
    loop {
        let timeout_ms = rand::rng().random_range(150..=300);
        let sleep_future = sleep(Duration::from_millis(timeout_ms));
        tokio::pin!(sleep_future);

        println!("[Timer] Waiting for {} ms...", timeout_ms);

        tokio::select! {
            _ = &mut sleep_future => {
                println!("[Timer] Fired after {} ms! Triggering election.", timeout_ms);
                if let Err(e) = event_tx.send(RaftEvent::ElectionTimeout).await {
                    eprintln!("[Timer] Failed to send election timeout event: {}", e);
                }
            }
            Some(_) = reset_rx.recv() => {
                println!("[Timer] Reset!");
            }
        }
    }
}

pub async fn run_heartbeat_timer(event_tx: mpsc::Sender<RaftEvent>) {
    loop {
        sleep(Duration::from_millis(HEARTBEAT_INTERVAL_MS)).await;

        if event_tx.send(RaftEvent::HeartbeatTick).await.is_err() {
            println!("[Heartbeat] Canale chiuso, termino il timer del leader.");
            break;
        }
    }
}

fn stop_heartbeat(handle: &mut Option<JoinHandle<()>>) {
    if let Some(h) = handle.take() {
        println!("[State] Stepping down from Leader, stopping heartbeat timer.");
        h.abort();
    }
}

pub async fn run_raft_node(
    node_arc: Arc<Mutex<RaftNode>>,
    mut event_rx: mpsc::Receiver<RaftEvent>,
    reset_timer_tx: mpsc::Sender<()>,
    event_tx: mpsc::Sender<RaftEvent>,
    available_followers: Vec<Peer>,
) {
    let mut heartbeat_handle: Option<JoinHandle<()>> = None;
    loop {
        if let Some(event) = event_rx.recv().await {
            println!("[State] Received event: {:?}", event);
            match event {
                RaftEvent::ElectionTimeout => {
                    handle_election_timeout(
                        node_arc.clone(),
                        available_followers.clone(),
                        event_tx.clone(),
                        &mut heartbeat_handle,
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
                        &mut heartbeat_handle,
                    )
                    .await;
                }

                RaftEvent::HeartbeatTick => {
                    handle_heartbeat_tick(node_arc.clone(), event_tx.clone()).await;
                }

                RaftEvent::AppendEntriesResponse {
                    follower_id,
                    response,
                    last_log_index_sent,
                } => {
                    handle_append_entries_response(
                        node_arc.clone(),
                        follower_id,
                        available_followers.clone(),
                        response,
                        last_log_index_sent,
                    )
                    .await;
                }
            }
        }
    }
}
#[tracing::instrument(skip_all, fields(term = tracing::field::Empty, votes = tracing::field::Empty))]
async fn handle_election_timeout(
    node_arc: Arc<Mutex<RaftNode>>,
    available_followers: Vec<Peer>,
    event_tx: mpsc::Sender<RaftEvent>,
    heartbeat_handle: &mut Option<JoinHandle<()>>,
) {
    let followers = available_followers.clone();
    let request = {
        let mut node = node_arc.lock().await;
        if node.volatile.role != RaftRole::Follower && node.volatile.role != RaftRole::Candidate {
            tracing::debug!(current_role = ?node.volatile.role, "Ignoring election timeout");
            return;
        }

        // --- Start new election ---
        node.volatile.role = RaftRole::Candidate;
        node.persistent.current_term += 1;
        node.persistent.voted_for = Some(node.persistent.id.clone());

        tracing::Span::current().record("term", node.persistent.current_term);
        tracing::info!("Starting new election");

        if let Err(e) = node.persist() {
            tracing::error!(
                "CRITICAL: Failed to persist state during election start: {}",
                e
            );
        }

        proto::RequestVoteRequest {
            term: node.persistent.current_term,
            candidate_id: node.persistent.id.clone(),
            last_log_term: node.persistent.log.last().map_or(0, |entry| entry.term),
            last_log_index: node.persistent.log.len() as u64,
        }
    };
    let connection_futures = available_followers.into_iter().map(|follower_addr| {
        let addr = format!("http://{}", follower_addr.clone().address);
        async move {
            RaftClient::connect(addr.clone())
                .await
                .map_err(|e| (addr, e))
        }
    });
    let len = followers.len();
    let connection_results: Vec<Result<RaftClient<_>, _>> =
        futures::stream::iter(connection_futures)
            .buffer_unordered(len)
            .collect()
            .await;

    let tasks = connection_results
        .into_iter()
        .filter_map(|result| match result {
            Ok(mut client) => {
                let req = tonic::Request::new(request.clone());
                Some(tokio::spawn(async move { client.request_vote(req).await }))
            }
            Err((addr, err)) => {
                tracing::error!(
                    "[State] Failed to connect to follower {} to request vote: {}",
                    addr,
                    err
                );
                None
            }
        })
        .collect::<Vec<_>>();
    tracing::info!("Sending RequestVote");
    let results = future::join_all(tasks).await;
    let mut votes_received = 1;
    for result in results {
        match result {
            Ok(Ok(response)) => {
                let vote = response.get_ref();
                tracing::debug!(
                    "[State] Vote response received: term={}, granted={}",
                    vote.term,
                    vote.vote_granted
                );
                let mut node = node_arc.lock().await;

                if vote.term > node.persistent.current_term {
                    tracing::debug!(
                        "[State] Discovered higher term {} (our term is {}). Reverting to Follower.",
                        vote.term,
                        node.persistent.current_term
                    );
                    node.persistent.current_term = vote.term;
                    node.volatile.role = RaftRole::Follower;
                    node.persistent.voted_for = None;
                    if let Err(e) = node.persist() {
                        tracing::debug!(
                            "[State] CRITICAL: Failed to persist state after discovering new term: {}",
                            e
                        );
                    }
                    break;
                }

                if vote.vote_granted {
                    votes_received += 1;
                }
            }
            Ok(Err(rpc_error)) => {
                tracing::debug!("[State] RPC failed during vote request: {}", rpc_error);
            }
            Err(join_error) => {
                tracing::error!(
                    "[State] Task failed to execute during vote collection: {}",
                    join_error
                );
            }
        }
    }

    tracing::Span::current().record("votes", votes_received);
    let mut node = node_arc.lock().await;
    if node.volatile.role == RaftRole::Candidate && votes_received > (followers.len() + 1) / 2 {
        tracing::info!(
            "[State] Election WIN! Became LEADER for term {} with {} votes.",
            node.persistent.current_term, votes_received
        );
        node.volatile.role = RaftRole::Leader;

        node.volatile.replicas.clear();
        let last_index = node.persistent.log.len() as u64;
        for follower in followers {
            node.volatile.replicas.insert(
                follower.clone().address,
                ReplicaProgress {
                    next_index: last_index + 1,
                    match_index: 0,
                },
            );
        }

        let leader_event_tx = event_tx.clone();
        let handle = tokio::spawn(async move {
            run_heartbeat_timer(leader_event_tx).await;
        });

        *heartbeat_handle = Some(handle);

        if let Err(e) = event_tx.send(RaftEvent::HeartbeatTick).await {
            tracing::debug!("[State] Failed to send initial heartbeat event: {}", e);
        }
    } else if node.volatile.role == RaftRole::Candidate {
        tracing::info!(
            "[State] Election lost for term {}. Received {} votes. Waiting for next timeout.",
            node.persistent.current_term, votes_received
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
        "[RPC AppendEntries] Received request: term={}, prev_log_index={}, num_entries={}",
        request.term,
        request.prev_log_index,
        request.entries.len()
    );

    if request.term < node.persistent.current_term {
        tracing::debug!(
            "[RPC AppendEntries] Rejecting: request term {} is older than our term {}",
            request.term, node.persistent.current_term
        );
        let _ = responder.send(Ok(proto::AppendEntriesResponse {
            term: node.persistent.current_term,
            success: false,
        }));
        return;
    }

    tracing::trace!("[State] Election timer reset due to valid leader communication.");
    let _ = reset_timer_tx.send(()).await;

    if request.term > node.persistent.current_term {
        tracing::info!(
            "[State] Discovered higher term {}. Current term was {}. Stepping down to Follower.",
            request.term,
            node.persistent.current_term
        );
        stop_heartbeat(heartbeat_handle);
        node.persistent.current_term = request.term;
        node.persistent.voted_for = None;
        node.volatile.role = RaftRole::Follower;
    }

    if node.volatile.role == RaftRole::Candidate {
        tracing::info!(
            "[State] Candidate received AppendEntries from new leader (term {}). Becoming Follower.",
            request.term
        );
        node.volatile.role = RaftRole::Follower;
    }

    // Log consistency check
    if request.prev_log_index > 0 {
        let vec_index = (request.prev_log_index - 1) as usize;
        match node.persistent.log.get(vec_index) {
            Some(entry) => {
                if entry.term != request.prev_log_term {
                    tracing::debug!(
                        "[RPC AppendEntries] Rejecting: Log consistency check failed. Term mismatch at index {}. Our term: {}, Leader's term: {}",
                        request.prev_log_index, entry.term, request.prev_log_term
                    );
                    let _ = responder.send(Ok(proto::AppendEntriesResponse {
                        term: node.persistent.current_term,
                        success: false,
                    }));
                    return;
                }
            }
            None => {
                tracing::debug!(
                    "[RPC AppendEntries] Rejecting: Log consistency check failed. Log is too short. No entry at index {}. Our log length: {}",
                    request.prev_log_index,
                    node.persistent.log.len()
                );
                let _ = responder.send(Ok(proto::AppendEntriesResponse {
                    term: node.persistent.current_term,
                    success: false,
                }));
                return;
            }
        }
    }

    // Find conflicts, truncate if necessary, and find where to start appending.
    let mut first_new_entry_index_opt = None;
    for (i, new_entry) in request.entries.iter().enumerate() {
        let log_index = (request.prev_log_index as usize) + i;
        if let Some(existing_entry) = node.persistent.log.get(log_index) {
            if existing_entry.term != new_entry.term {
                tracing::info!(
                    "[Log] Conflict found at index {}. Our term: {}, Leader's term: {}. Truncating log from this point.",
                    log_index + 1,
                    existing_entry.term,
                    new_entry.term
                );
                node.persistent.log.truncate(log_index);
                first_new_entry_index_opt = Some(i);
                break;
            }
        } else {
            // This is the first entry that doesn't exist in our log.
            first_new_entry_index_opt = Some(i);
            break;
        }
    }

    // Append any new entries that are not already in the log.
    if let Some(first_new_entry_index) = first_new_entry_index_opt {
        let num_to_append = request.entries.len() - first_new_entry_index;
        if num_to_append > 0 {
            tracing::info!(
                "[Log] Appending {} new entries starting at index {}.",
                num_to_append,
                node.persistent.log.len() + 1
            );
            for i in first_new_entry_index..request.entries.len() {
                let entry_to_add = &request.entries[i];
                node.persistent.log.push(LogEntry {
                    term: entry_to_add.term,
                    command: entry_to_add.command.clone(),
                });
            }
        }
    }

    // Update commit_index
    if request.leader_commit > node.volatile.commit_index {
        let old_commit_index = node.volatile.commit_index;
        let last_new_entry_index = request.prev_log_index + request.entries.len() as u64;
        node.volatile.commit_index = std::cmp::min(request.leader_commit, last_new_entry_index);
        
        // Check if the commit_index actually advanced before logging.
        if node.volatile.commit_index > old_commit_index {
            tracing::info!(
                "[State] Advanced commit_index from {} to {}.",
                old_commit_index, node.volatile.commit_index
            );
        }
    }

    // Persist state to stable storage.
    if let Err(e) = node.persist() {
        tracing::error!(
            error = %e,
            "[State] CRITICAL: Failed to persist state after appending entries. This is a fatal error."
        );
        let _ = responder.send(Err(tonic::Status::internal(format!(
            "Failed to save state: {}",
            e
        ))));
        return;
    }

    let response = proto::AppendEntriesResponse {
        term: node.persistent.current_term,
        success: true,
    };

    tracing::debug!("[RPC AppendEntries] Request successful. Responding with term={}, success=true.", response.term);
    let _ = responder.send(Ok(response));
}

#[tracing::instrument(skip_all, fields(term = tracing::field::Empty, votes = tracing::field::Empty))]
async fn handle_rpc_request_vote(
    node_arc: Arc<Mutex<RaftNode>>,
    request: RequestVoteRequest,
    responder: RequestVoteResponder,
    reset_timer_tx: mpsc::Sender<()>,
    heartbeat_handle: &mut Option<JoinHandle<()>>,
) {
    let mut node = node_arc.lock().await;

    tracing::info!(
        "[RPC RequestVote] Received vote request from candidate {} for term {}.",
        request.candidate_id,
        request.term
    );

    if request.term < node.persistent.current_term {
        tracing::debug!(
            "[RPC RequestVote] Rejecting vote: Candidate term {} is less than our term {}.",
            request.term,
            node.persistent.current_term
        );
        // Note: stop_heartbeat is unusual here. A follower/candidate receiving this shouldn't
        // have a heartbeat. If a leader receives this, it should just reject and continue.
        // The logging change is correct regardless of the logic.
        stop_heartbeat(heartbeat_handle);
        let _ = responder.send(Ok(proto::RequestVoteResponse {
            term: node.persistent.current_term,
            vote_granted: false,
        }));
        return;
    }

    // Discovered a higher term
    if request.term > node.persistent.current_term {
        tracing::info!(
            "[State] Discovered higher term {} from RequestVote (our term was {}). Stepping down.",
            request.term,
            node.persistent.current_term
        );
        node.persistent.current_term = request.term;
        node.persistent.voted_for = None;
        node.volatile.role = RaftRole::Follower;
    }

    // Check if we can vote in this term
    let can_vote = match &node.persistent.voted_for {
        None => true,
        Some(voted_id) => *voted_id == request.candidate_id,
    };

    if !can_vote {
        tracing::debug!(
            "[RPC RequestVote] Rejecting vote: Already voted for {:?} in term {}.",
            node.persistent.voted_for,
            node.persistent.current_term
        );
        let _ = responder.send(Ok(proto::RequestVoteResponse {
            term: node.persistent.current_term,
            vote_granted: false,
        }));
        return;
    }

    // Raft safety check: ensure candidate's log is at least as up-to-date as ours
    let last_log_term = node.persistent.log.last().map_or(0, |entry| entry.term);
    let last_log_index = node.persistent.log.len() as u64;

    let our_log_is_more_up_to_date = last_log_term > request.last_log_term
        || (last_log_term == request.last_log_term && last_log_index > request.last_log_index);

    if our_log_is_more_up_to_date {
        // This is the most important part of the voting logic.
        tracing::debug!(
            "[RPC RequestVote] Rejecting vote: Candidate's log is not up-to-date. Our log: [term: {}, index: {}], Candidate's log: [term: {}, index: {}].",
            last_log_term, last_log_index,
            request.last_log_term, request.last_log_index
        );
        let _ = responder.send(Ok(proto::RequestVoteResponse {
            term: node.persistent.current_term,
            vote_granted: false,
        }));
        return;
    }

    // Grant the vote
    tracing::info!(
        "[RPC RequestVote] Granting vote for candidate {} in term {}.",
        request.candidate_id,
        request.term
    );
    tracing::trace!("[State] Election timer reset after granting vote.");
    let _ = reset_timer_tx.send(()).await;
    node.persistent.voted_for = Some(request.candidate_id.clone());

    if let Err(e) = node.persist() {
        tracing::error!(
            error = %e,
            "[State] CRITICAL: Failed to persist vote. This could lead to a safety violation."
        );
        let _ = responder.send(Err(tonic::Status::internal(format!(
            "Failed to persist vote: {}",
            e
        ))));
        return;
    }

    let response = proto::RequestVoteResponse {
        term: node.persistent.current_term,
        vote_granted: true,
    };

    tracing::debug!("[RPC RequestVote] Sending response: vote_granted=true.");
    let _ = responder.send(Ok(response));
}
#[tracing::instrument(skip_all, fields(term = tracing::field::Empty, votes = tracing::field::Empty))]
async fn handle_heartbeat_tick(node_arc: Arc<Mutex<RaftNode>>, event_tx: mpsc::Sender<RaftEvent>) {
    let (replicas_clone, is_leader, current_term) = {
        let node = node_arc.lock().await;
        (
            node.volatile.replicas.clone(),
            node.volatile.role == RaftRole::Leader,
            node.persistent.current_term,
        )
    };

    if !is_leader {
        tracing::trace!("[Heartbeat] Skipping tick: not the leader.");
        return;
    }

    tracing::debug!("[Heartbeat] Tick for term {}: Sending AppendEntries to followers.", current_term);

    for (follower_id, progress) in replicas_clone {
        let node_arc_clone = Arc::clone(&node_arc);
        let event_tx_clone = event_tx.clone();

        tokio::spawn(async move {
            let (request, last_log_index_sent) = {
                let node = node_arc_clone.lock().await;

                // This check is important because the node might have lost leadership
                // while tasks were being spawned.
                if node.volatile.role != RaftRole::Leader {
                    return;
                }

                let prev_log_index = progress.next_index - 1;
                let prev_log_term = if prev_log_index > 0 {
                    node.persistent
                        .log
                        .get((prev_log_index - 1) as usize)
                        .map_or(0, |e| e.term)
                } else {
                    0
                };

                let start_index = (progress.next_index - 1) as usize;

                let entries_to_send: Vec<proto::LogEntry> = node
                    .persistent
                    .log
                    .get(start_index..)
                    .unwrap_or(&[])
                    .iter()
                    .map(|e| proto::LogEntry {
                        term: e.term,
                        command: e.command.clone(),
                    })
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

            tracing::trace!(
                follower_id = %follower_id,
                prev_log_index = request.prev_log_index,
                prev_log_term = request.prev_log_term,
                num_entries = request.entries.len(),
                "Sending AppendEntries RPC to follower."
            );

            // Make the RPC call
            let response =
                match RaftClient::connect(format!("http://{}", follower_id.clone())).await {
                    Ok(mut client) => client
                        .append_entries(request)
                        .await
                        .map(|resp| resp.into_inner()),
                    Err(e) => {
                        tracing::warn!(
                            follower_id = %follower_id,
                            error = %e,
                            "Failed to connect to follower."
                        );
                        Err(tonic::Status::unavailable(format!(
                            "Connection failed: {}",
                            e
                        )))
                    }
                };

            match &response {
                Ok(resp) => tracing::debug!(follower_id = %follower_id, success = resp.success, term = resp.term, "Received AppendEntries response from follower."),
                Err(status) => tracing::warn!(follower_id = %follower_id, ?status, "AppendEntries RPC failed for follower."),
            };

            let response_event = RaftEvent::AppendEntriesResponse {
                follower_id,
                response,
                last_log_index_sent,
            };

            if event_tx_clone.send(response_event).await.is_err() {
                tracing::error!("[Heartbeat] CRITICAL: Raft event channel closed. The core task may have panicked. Shutting down heartbeat task.");
            }
        });
    }
}
#[tracing::instrument(skip_all, fields(term = tracing::field::Empty, votes = tracing::field::Empty))]
async fn handle_append_entries_response(
    node_arc: Arc<Mutex<RaftNode>>,
    follower_id: String,
    available_followers: Vec<Peer>,
    response: Result<AppendEntriesResponse, Status>,
    last_log_index_sent: u64,
) {
    let followers = available_followers.clone();
    let mut node = node_arc.lock().await;
    
    if node.volatile.role != RaftRole::Leader {
        tracing::trace!(
            follower_id = %follower_id,
            "Ignoring AppendEntries response: not in leader state."
        );
        return;
    }

    match response {
        Ok(resp) => {
            // A follower has a higher term. The leader must step down.
            if resp.term > node.persistent.current_term {
                tracing::info!(
                    follower_id = %follower_id,
                    new_term = resp.term,
                    old_term = node.persistent.current_term,
                    "Follower has a higher term. Stepping down."
                );
                node.persistent.current_term = resp.term;
                node.volatile.role = RaftRole::Follower;
                node.persistent.voted_for = None;

                if let Err(e) = node.persist() {
                    tracing::error!(
                        error = %e,
                        "CRITICAL: Failed to persist state after stepping down."
                    );
                }
                return;
            }

            if let Some(progress) = node.volatile.replicas.get_mut(&follower_id) {
                if resp.success {
                    // Follower successfully appended entries.
                    progress.match_index = last_log_index_sent;
                    progress.next_index = progress.match_index + 1;

                    tracing::debug!(
                        follower_id = %follower_id,
                        match_index = progress.match_index,
                        next_index = progress.next_index,
                        "Follower replication successful."
                    );

                    // Leader attempts to advance its commit index.
                    let mut match_indices: Vec<u64> = node
                        .volatile
                        .replicas
                        .values()
                        .map(|p| p.match_index)
                        .collect();

                    match_indices.push(node.persistent.log.len() as u64); // Include leader's own progress
                    match_indices.sort_unstable_by(|a, b| b.cmp(a)); // Sort descending

                    let majority_index = (followers.len() + 1) / 2;
                    let potential_commit_index = match_indices[majority_index];

                    tracing::trace!(?match_indices, majority_quorum_size = majority_index + 1, potential_commit_index, "Calculated potential commit index from follower matches.");

                    if potential_commit_index > node.volatile.commit_index {
                        if let Some(entry) = node
                            .persistent
                            .log
                            .get((potential_commit_index - 1) as usize)
                        {
                            if entry.term == node.persistent.current_term {
                                tracing::info!(
                                    old_commit_index = node.volatile.commit_index,
                                    new_commit_index = potential_commit_index,
                                    "Advancing commit index based on majority consensus."
                                );
                                node.volatile.commit_index = potential_commit_index;
                            } else {
                                tracing::debug!(
                                    potential_commit_index,
                                    entry_term = entry.term,
                                    current_term = node.persistent.current_term,
                                    "Cannot advance commit index yet: majority log entry is from a previous term."
                                );
                            }
                        }
                    }
                } else {
                    // Follower rejected the entries due to a log inconsistency.
                    // Decrement next_index to find the point of agreement.
                    if progress.next_index > 1 {
                        progress.next_index -= 1;
                    }
                    tracing::debug!(
                        follower_id = %follower_id,
                        next_index = progress.next_index,
                        "Follower failed consistency check. Decrementing next_index for retry."
                    );
                }
            }
        }
        Err(rpc_error) => {
            tracing::warn!(
                follower_id = %follower_id,
                error = %rpc_error,
                "RPC to follower failed. Will retry on next heartbeat."
            );
        }
    }
}
