use crate::proto;
use crate::proto::raft_client::RaftClient;
use crate::state::{LogEntry, RaftEvent, RaftNode, RaftRole, ReplicaProgress};
use futures::{future, stream::StreamExt};
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;

const HEARTBEAT_INTERVAL_MS: u64 = 50;

pub async fn run_election_timer(
    mut reset_rx: mpsc::Receiver<()>,
    event_tx: mpsc::Sender<RaftEvent>,
) {
    loop {
        let timeout_ms = rand::thread_rng().gen_range(150..=300);
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
    available_followers: Vec<String>,
) {
    let mut heartbeat_handle: Option<JoinHandle<()>> = None;
    loop {
        if let Some(event) = event_rx.recv().await {
            println!("[State] Received event: {:?}", event);
            match event {
                RaftEvent::ElectionTimeout => {
                    let request = {
                        let mut node = node_arc.lock().await;
                        // Only Followers and Candidates can start an election.
                        if node.volatile.role != RaftRole::Follower
                            && node.volatile.role != RaftRole::Candidate
                        {
                            println!(
                                "[State] Ignoring election timeout as we are a {:?}",
                                node.volatile.role
                            );
                            continue;
                        }

                        // --- Start new election ---
                        node.volatile.role = RaftRole::Candidate;
                        node.persistent.current_term += 1;
                        node.persistent.voted_for = Some(node.persistent.id.clone());
                        println!(
                            "[State] Election timeout: Starting new election for term {}.",
                            node.persistent.current_term
                        );

                        if let Err(e) = node.persist() {
                            eprintln!(
                                "[State] CRITICAL: Failed to persist state during election start: {}",
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
                    let connection_futures =
                        available_followers
                            .clone()
                            .into_iter()
                            .map(|follower_addr| {
                                let addr = follower_addr.clone();
                                async move {
                                    RaftClient::connect(addr.clone())
                                        .await
                                        .map_err(|e| (addr, e))
                                }
                            });

                    let connection_results: Vec<Result<RaftClient<_>, _>> =
                        futures::stream::iter(connection_futures)
                            .buffer_unordered(available_followers.len())
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
                                eprintln!(
                                    "[State] Failed to connect to follower {} to request vote: {}",
                                    addr, err
                                );
                                None
                            }
                        })
                        .collect::<Vec<_>>();

                    let results = future::join_all(tasks).await;
                    let mut votes_received = 1;
                    for result in results {
                        match result {
                            Ok(Ok(response)) => {
                                let vote = response.get_ref();
                                println!(
                                    "[State] Vote response received: term={}, granted={}",
                                    vote.term, vote.vote_granted
                                );
                                let mut node = node_arc.lock().await;

                                if vote.term > node.persistent.current_term {
                                    println!(
                                        "[State] Discovered higher term {} (our term is {}). Reverting to Follower.",
                                        vote.term, node.persistent.current_term
                                    );
                                    node.persistent.current_term = vote.term;
                                    node.volatile.role = RaftRole::Follower;
                                    node.persistent.voted_for = None;
                                    if let Err(e) = node.persist() {
                                        eprintln!(
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
                                eprintln!("[State] RPC failed during vote request: {}", rpc_error);
                            }
                            Err(join_error) => {
                                eprintln!(
                                    "[State] Task failed to execute during vote collection: {}",
                                    join_error
                                );
                            }
                        }
                    }

                    let mut node = node_arc.lock().await;
                    if node.volatile.role == RaftRole::Candidate
                        && votes_received > (available_followers.len() + 1) / 2
                    {
                        println!(
                            "[State] Election WIN! Became LEADER for term {} with {} votes.",
                            node.persistent.current_term, votes_received
                        );
                        node.volatile.role = RaftRole::Leader;

                        // Initialize replica progress tracking
                        node.volatile.replicas.clear();
                        let last_index = node.persistent.log.len() as u64;
                        for follower in &available_followers {
                            node.volatile.replicas.insert(
                                follower.clone(),
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

                        heartbeat_handle = Some(handle);

                        if let Err(e) = event_tx.send(RaftEvent::HeartbeatTick).await {
                            eprintln!("[State] Failed to send initial heartbeat event: {}", e);
                        }
                    } else if node.volatile.role == RaftRole::Candidate {
                        println!(
                            "[State] Election lost for term {}. Received {} votes. Waiting for next timeout.",
                            node.persistent.current_term, votes_received
                        );
                    }
                }

                RaftEvent::RpcAppendEntries { request, responder } => {
                    let mut node = node_arc.lock().await;

                    println!(
                        "[RPC AppendEntries] Received request: term={}, prev_log_index={}, num_entries={}",
                        request.term,
                        request.prev_log_index,
                        request.entries.len()
                    );

                    if request.term < node.persistent.current_term {
                        println!(
                            "[RPC AppendEntries] Rejecting: request term {} is older than our term {}",
                            request.term, node.persistent.current_term
                        );
                        let _ = responder.send(Ok(proto::AppendEntriesResponse {
                            term: node.persistent.current_term,
                            success: false,
                        }));
                        continue;
                    }

                    let _ = reset_timer_tx.send(()).await;

                    if request.term > node.persistent.current_term {
                        println!(
                            "[State] Discovered higher term {} from AppendEntries. Stepping down to Follower.",
                            request.term
                        );
                        stop_heartbeat(&mut heartbeat_handle);
                        node.persistent.current_term = request.term;
                        node.persistent.voted_for = None;
                        node.volatile.role = RaftRole::Follower;
                    }

                    // Also step down if we are a candidate in the same term
                    if node.volatile.role == RaftRole::Candidate {
                        println!(
                            "[State] Received AppendEntries as a Candidate. Acknowledging leader and becoming Follower."
                        );
                        node.volatile.role = RaftRole::Follower;
                    }

                    if request.prev_log_index > 0 {
                        let vec_index = (request.prev_log_index - 1) as usize;
                        match node.persistent.log.get(vec_index) {
                            Some(entry) => {
                                if entry.term != request.prev_log_term {
                                    eprintln!(
                                        "[RPC AppendEntries] Rejecting: Log consistency check failed at index {}. Our term: {}, Leader's term: {}",
                                        request.prev_log_index, entry.term, request.prev_log_term
                                    );
                                    let _ = responder.send(Ok(proto::AppendEntriesResponse {
                                        term: node.persistent.current_term,
                                        success: false,
                                    }));
                                    continue;
                                }
                            }
                            None => {
                                eprintln!(
                                    "[RPC AppendEntries] Rejecting: Log consistency check failed. No entry at index {}",
                                    request.prev_log_index
                                );
                                let _ = responder.send(Ok(proto::AppendEntriesResponse {
                                    term: node.persistent.current_term,
                                    success: false,
                                }));
                                continue;
                            }
                        }
                    }

                    let mut first_new_entry_index_opt = None;
                    for (i, new_entry) in request.entries.iter().enumerate() {
                        let log_index = (request.prev_log_index as usize) + i;
                        if let Some(existing_entry) = node.persistent.log.get(log_index) {
                            if existing_entry.term != new_entry.term {
                                println!(
                                    "[RPC AppendEntries] Conflict found at index {}. Truncating log.",
                                    log_index + 1
                                );
                                node.persistent.log.truncate(log_index);
                                first_new_entry_index_opt = Some(i);
                                break;
                            }
                        } else {
                            first_new_entry_index_opt = Some(i);
                            break;
                        }
                    }

                    if let Some(first_new_entry_index) = first_new_entry_index_opt {
                        println!(
                            "[RPC AppendEntries] Appending {} new entries.",
                            request.entries.len() - first_new_entry_index
                        );
                        for i in first_new_entry_index..request.entries.len() {
                            let entry_to_add = &request.entries[i];
                            node.persistent.log.push(LogEntry {
                                term: entry_to_add.term,
                                command: entry_to_add.command.clone(),
                            });
                        }
                    }

                    if request.leader_commit > node.volatile.commit_index {
                        let old_commit_index = node.volatile.commit_index;
                        let last_new_entry_index =
                            request.prev_log_index + request.entries.len() as u64;
                        node.volatile.commit_index =
                            std::cmp::min(request.leader_commit, last_new_entry_index);
                        println!(
                            "[State] Updated commit_index from {} to {}",
                            old_commit_index, node.volatile.commit_index
                        );
                    }

                    if let Err(e) = node.persist() {
                        eprintln!(
                            "[State] CRITICAL: Failed to persist state after appending entries: {}",
                            e
                        );
                        let _ = responder.send(Err(tonic::Status::internal(format!(
                            "Failed to save state: {}",
                            e
                        ))));
                        continue;
                    }

                    let response = proto::AppendEntriesResponse {
                        term: node.persistent.current_term,
                        success: true,
                    };

                    let _ = responder.send(Ok(response));
                }

                RaftEvent::RpcRequestVote { request, responder } => {
                    let mut node = node_arc.lock().await;
                    println!(
                        "[RPC RequestVote] Received vote request from candidate {} for term {}.",
                        request.candidate_id, request.term
                    );

                    if request.term < node.persistent.current_term {
                        println!(
                            "[RPC RequestVote] Rejecting vote: Candidate term {} is less than our term {}.",
                            request.term, node.persistent.current_term
                        );
                        stop_heartbeat(&mut heartbeat_handle);
                        let _ = responder.send(Ok(proto::RequestVoteResponse {
                            term: node.persistent.current_term,
                            vote_granted: false,
                        }));
                        continue;
                    }

                    if request.term > node.persistent.current_term {
                        println!(
                            "[State] Discovered higher term {} from RequestVote. Stepping down to Follower.",
                            request.term
                        );
                        node.persistent.current_term = request.term;
                        node.persistent.voted_for = None;
                        node.volatile.role = RaftRole::Follower;
                    }

                    let can_vote = match &node.persistent.voted_for {
                        None => true,
                        Some(voted_id) => *voted_id == request.candidate_id,
                    };

                    if !can_vote {
                        println!(
                            "[RPC RequestVote] Rejecting vote: Already voted for {:?} in term {}.",
                            node.persistent.voted_for, node.persistent.current_term
                        );
                        let _ = responder.send(Ok(proto::RequestVoteResponse {
                            term: node.persistent.current_term,
                            vote_granted: false,
                        }));
                        continue;
                    }

                    let last_log_term = node.persistent.log.last().map_or(0, |entry| entry.term);
                    let last_log_index = node.persistent.log.len() as u64;

                    let our_log_is_better = last_log_term > request.last_log_term
                        || (last_log_term == request.last_log_term
                            && last_log_index > request.last_log_index);

                    if our_log_is_better {
                        println!(
                            "[RPC RequestVote] Rejecting vote: Candidate's log is not as up-to-date as ours."
                        );
                        let _ = responder.send(Ok(proto::RequestVoteResponse {
                            term: node.persistent.current_term,
                            vote_granted: false,
                        }));
                        continue;
                    }

                    println!(
                        "[RPC RequestVote] Granting vote for candidate {} in term {}.",
                        request.candidate_id, request.term
                    );
                    let _ = reset_timer_tx.send(()).await;
                    node.persistent.voted_for = Some(request.candidate_id.clone());

                    if let Err(e) = node.persist() {
                        eprintln!("[State] CRITICAL: Failed to persist vote: {}", e);
                        let _ = responder.send(Err(tonic::Status::internal(format!(
                            "Failed to persist vote: {}",
                            e
                        ))));
                        continue;
                    }

                    let _ = responder.send(Ok(proto::RequestVoteResponse {
                        term: node.persistent.current_term,
                        vote_granted: true,
                    }));
                }

                RaftEvent::HeartbeatTick => {
                    // First, get a clone of the replica progress map and check if we are still the leader.
                    // This initial lock is very short.
                    let (replicas_clone, is_leader) = {
                        let node = node_arc.lock().await;
                        (
                            node.volatile.replicas.clone(),
                            node.volatile.role == RaftRole::Leader,
                        )
                    };

                    // If we are no longer the leader, just stop. The timer will be aborted by other events.
                    if !is_leader {
                        continue;
                    }

                    println!("[Heartbeat] Tick! Sending AppendEntries to followers.");

                    // Iterate over the cloned replica progress to avoid holding the lock.
                    for (follower_id, progress) in replicas_clone {
                        // For each follower, spawn a dedicated task to handle the RPC.
                        // Clone the Arcs needed for the task.
                        let node_arc_clone = Arc::clone(&node_arc);
                        let event_tx_clone = event_tx.clone();

                        tokio::spawn(async move {
                            // --- STEP 1: Prepare the request while holding the lock briefly ---
                            // We re-acquire the lock here to get the most up-to-date state.
                            let (request, last_log_index_sent) = {
                                let node = node_arc_clone.lock().await;

                                // It's possible our role changed between the outer check and this lock.
                                // If we're not the leader anymore, abort this specific task.
                                if node.volatile.role != RaftRole::Leader {
                                    return;
                                }

                                // Determine the log entry right before the ones we're about to send.
                                let prev_log_index = progress.next_index - 1;
                                let prev_log_term = if prev_log_index > 0 {
                                    node.persistent
                                        .log
                                        .get((prev_log_index - 1) as usize)
                                        .map_or(0, |e| e.term)
                                } else {
                                    0 // This is the case for an empty log.
                                };

                                // Determine the actual entries to send. For a pure heartbeat, this will be empty.
                                // For log replication, this will contain new entries.
                                let start_index = (progress.next_index - 1) as usize;

                                // Efficiently clone ONLY the necessary slice of the log.
                                let entries_to_send: Vec<proto::LogEntry> = node
                                    .persistent
                                    .log
                                    .get(start_index..)
                                    .unwrap_or(&[])
                                    .iter()
                                    // We need to convert from our internal LogEntry to the gRPC proto version.
                                    .map(|e| proto::LogEntry {
                                        term: e.term,
                                        command: e.command.clone(),
                                    })
                                    .collect();

                                // This is the index of the last entry we are trying to send in this request.
                                let last_log_index_sent =
                                    prev_log_index + entries_to_send.len() as u64;

                                let request = proto::AppendEntriesRequest {
                                    term: node.persistent.current_term,
                                    leader_id: node.persistent.id.clone(),
                                    prev_log_index,
                                    prev_log_term,
                                    entries: entries_to_send,
                                    leader_commit: node.volatile.commit_index,
                                };

                                (request, last_log_index_sent)
                            }; // <-- The lock on `node` is released here!

                            // --- STEP 2: Perform Network I/O *after* releasing the lock ---
                            let response = match RaftClient::connect(follower_id.clone()).await {
                                Ok(mut client) => client
                                    .append_entries(request)
                                    .await
                                    .map(|resp| resp.into_inner()),
                                Err(e) => Err(tonic::Status::unavailable(format!(
                                    "Connection failed: {}",
                                    e
                                ))),
                            };

                            // --- STEP 3: Send the result back to the main event loop ---
                            let response_event = RaftEvent::AppendEntriesResponse {
                                follower_id,
                                response,
                                last_log_index_sent,
                            };

                            if event_tx_clone.send(response_event).await.is_err() {
                                eprintln!(
                                    "[Heartbeat] CRITICAL: Event channel closed. Could not send response."
                                );
                            }
                        });
                    }
                }
                RaftEvent::AppendEntriesResponse {
                    follower_id,
                    response,
                    last_log_index_sent,
                } => {
                    let mut node = node_arc.lock().await;
                    // Ignore responses if we are no longer the leader. This can happen with delayed messages.
                    if node.volatile.role != RaftRole::Leader {
                        continue;
                    }

                    match response {
                        Ok(resp) => {
                            // Rule for all servers: If RPC response contains term T > currentTerm,
                            // convert to follower.
                            if resp.term > node.persistent.current_term {
                                println!(
                                    "[State] Follower {} has higher term {}. Stepping down.",
                                    follower_id, resp.term
                                );
                                // The stop_heartbeat function would need to be defined elsewhere in the loop
                                // to abort the timer task's JoinHandle. For this snippet, we'll assume it exists.
                                // stop_heartbeat(&mut heartbeat_handle);
                                node.persistent.current_term = resp.term;
                                node.volatile.role = RaftRole::Follower;
                                node.persistent.voted_for = None;

                                // Persist the updated term and voted_for status.
                                if let Err(e) = node.persist() {
                                    eprintln!(
                                        "[State] CRITICAL: Failed to persist state after stepping down: {}",
                                        e
                                    );
                                }
                                continue; // Continue the main loop to process the next event.
                            }

                            // Now, handle the response for the current term.
                            if let Some(progress) = node.volatile.replicas.get_mut(&follower_id) {
                                if resp.success {
                                    // --- Success Case ---
                                    // The follower's log is now consistent with the leader's up to last_log_index_sent.
                                    progress.match_index = last_log_index_sent;
                                    progress.next_index = progress.match_index + 1;

                                    println!(
                                        "[State] Follower {} replicated successfully. New match_index: {}",
                                        follower_id, progress.match_index
                                    );

                                    // --- Attempt to advance commit index ---
                                    // A leader can only commit entries from its own term.
                                    // We check if a majority of nodes have replicated an entry.

                                    let mut match_indices: Vec<u64> = node
                                        .volatile
                                        .replicas
                                        .values()
                                        .map(|p| p.match_index)
                                        .collect();

                                    // Include the leader itself in the count.
                                    match_indices.push(node.persistent.log.len() as u64);

                                    // Sort from highest to lowest.
                                    match_indices.sort_unstable_by(|a, b| b.cmp(a));

                                    // The index replicated by the majority is the median.
                                    let majority_index = (available_followers.len() + 1) / 2;
                                    let potential_commit_index = match_indices[majority_index];

                                    // Only advance commit_index if the new index is from the current term.
                                    if potential_commit_index > node.volatile.commit_index {
                                        if let Some(entry) = node
                                            .persistent
                                            .log
                                            .get((potential_commit_index - 1) as usize)
                                        {
                                            if entry.term == node.persistent.current_term {
                                                println!(
                                                    "[State] Advancing commit_index from {} to {}",
                                                    node.volatile.commit_index,
                                                    potential_commit_index
                                                );
                                                node.volatile.commit_index = potential_commit_index;
                                                // In a full implementation, you would now apply committed entries
                                                // to the state machine.
                                            }
                                        }
                                    }
                                } else {
                                    // --- Failure Case ---
                                    // The follower rejected the AppendEntries, likely due to a log mismatch.
                                    // Decrement next_index and retry on the next heartbeat.
                                    if progress.next_index > 1 {
                                        progress.next_index -= 1;
                                    }
                                    println!(
                                        "[State] Follower {} failed consistency check. Retrying with next_index: {}",
                                        follower_id, progress.next_index
                                    );
                                }
                            }
                        }
                        Err(rpc_error) => {
                            // --- RPC Error Case ---
                            // This indicates a network failure or that the follower is down.
                            // We treat it like a failure and retry.
                            if let Some(progress) = node.volatile.replicas.get_mut(&follower_id) {
                                if progress.next_index > 1 {
                                    // It's common to decrement on network failure, but more advanced strategies exist.
                                    // For simplicity, we'll decrement to force a check on the next attempt.
                                    progress.next_index -= 1;
                                }
                            }
                            eprintln!(
                                "[State] RPC to follower {} failed: {}. Will retry.",
                                follower_id, rpc_error
                            );
                        }
                    }
                }
            }
        }
    }
}
