use crate::proto;
use crate::proto::raft_client::RaftClient;
use crate::state::{LogEntry, RaftEvent, RaftNode, RaftRole, ReplicaProgress};
use futures::{future, stream::StreamExt};
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::time::sleep;

pub async fn run_election_timer(
    mut reset_rx: mpsc::Receiver<()>,
    event_tx: mpsc::Sender<RaftEvent>,
) {
    loop {
        let timeout_ms = rand::thread_rng().gen_range(1500..=3000);
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

pub async fn run_raft_node(
    node_arc: Arc<Mutex<RaftNode>>,
    mut event_rx: mpsc::Receiver<RaftEvent>,
    reset_timer_tx: mpsc::Sender<()>,
    available_followers: Vec<String>,
) {
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
                            println!("[State] Ignoring election timeout as we are a {:?}", node.volatile.role);
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
                            eprintln!("[State] CRITICAL: Failed to persist state during election start: {}", e);
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
                    let mut votes_received = 1; // Vote for self
                    for result in results {
                        match result {
                            Ok(Ok(response)) => {
                                let vote = response.get_ref();
                                println!("[State] Vote response received: term={}, granted={}", vote.term, vote.vote_granted);
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
                                        eprintln!("[State] CRITICAL: Failed to persist state after discovering new term: {}", e);
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
                                eprintln!("[State] Task failed to execute during vote collection: {}", join_error);
                            }
                        }
                    }
                    
                    let mut node = node_arc.lock().await;
                    if node.volatile.role == RaftRole::Candidate && votes_received > (available_followers.len() + 1) / 2 {
                        println!(
                            "🎉 [State] Election WIN! Became LEADER for term {} with {} votes.",
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
                    } else if node.volatile.role == RaftRole::Candidate {
                         println!("[State] Election lost for term {}. Received {} votes. Waiting for next timeout.", node.persistent.current_term, votes_received);
                    }
                }

                RaftEvent::RpcAppendEntries { request, responder } => {
                    let mut node = node_arc.lock().await;

                    println!("[RPC AppendEntries] Received request: term={}, prev_log_index={}, num_entries={}",
                        request.term, request.prev_log_index, request.entries.len());

                    if request.term < node.persistent.current_term {
                        println!("[RPC AppendEntries] Rejecting: request term {} is older than our term {}", request.term, node.persistent.current_term);
                        let _ = responder.send(Ok(proto::AppendEntriesResponse {
                            term: node.persistent.current_term,
                            success: false,
                        }));
                        continue;
                    }

                    let _ = reset_timer_tx.send(()).await;

                    if request.term > node.persistent.current_term {
                        println!("[State] Discovered higher term {} from AppendEntries. Stepping down to Follower.", request.term);
                        node.persistent.current_term = request.term;
                        node.persistent.voted_for = None;
                        node.volatile.role = RaftRole::Follower;
                    }
                    
                    // Also step down if we are a candidate in the same term
                    if node.volatile.role == RaftRole::Candidate {
                        println!("[State] Received AppendEntries as a Candidate. Acknowledging leader and becoming Follower.");
                        node.volatile.role = RaftRole::Follower;
                    }

                    if request.prev_log_index > 0 {
                        let vec_index = (request.prev_log_index - 1) as usize;
                        match node.persistent.log.get(vec_index) {
                            Some(entry) => {
                                if entry.term != request.prev_log_term {
                                    eprintln!("[RPC AppendEntries] Rejecting: Log consistency check failed at index {}. Our term: {}, Leader's term: {}", request.prev_log_index, entry.term, request.prev_log_term);
                                    let _ = responder.send(Ok(proto::AppendEntriesResponse {
                                        term: node.persistent.current_term,
                                        success: false,
                                    }));
                                    continue;
                                }
                            }
                            None => {
                                eprintln!("[RPC AppendEntries] Rejecting: Log consistency check failed. No entry at index {}", request.prev_log_index);
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
                                println!("[RPC AppendEntries] Conflict found at index {}. Truncating log.", log_index + 1);
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
                        println!("[RPC AppendEntries] Appending {} new entries.", request.entries.len() - first_new_entry_index);
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
                        println!("[State] Updated commit_index from {} to {}", old_commit_index, node.volatile.commit_index);
                    }

                    if let Err(e) = node.persist() {
                        eprintln!("[State] CRITICAL: Failed to persist state after appending entries: {}", e);
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
                    println!("[RPC RequestVote] Received vote request from candidate {} for term {}.", request.candidate_id, request.term);

                    if request.term < node.persistent.current_term {
                        println!("[RPC RequestVote] Rejecting vote: Candidate term {} is less than our term {}.", request.term, node.persistent.current_term);
                        let _ = responder.send(Ok(proto::RequestVoteResponse {
                            term: node.persistent.current_term,
                            vote_granted: false,
                        }));
                        continue;
                    }

                    if request.term > node.persistent.current_term {
                        println!("[State] Discovered higher term {} from RequestVote. Stepping down to Follower.", request.term);
                        node.persistent.current_term = request.term;
                        node.persistent.voted_for = None;
                        node.volatile.role = RaftRole::Follower;
                    }
                    
                    let can_vote = match &node.persistent.voted_for {
                        None => true,
                        Some(voted_id) => *voted_id == request.candidate_id,
                    };

                    if !can_vote {
                        println!("[RPC RequestVote] Rejecting vote: Already voted for {:?} in term {}.", node.persistent.voted_for, node.persistent.current_term);
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
                        println!("[RPC RequestVote] Rejecting vote: Candidate's log is not as up-to-date as ours.");
                        let _ = responder.send(Ok(proto::RequestVoteResponse {
                            term: node.persistent.current_term,
                            vote_granted: false,
                        }));
                        continue;
                    }

                    println!("[RPC RequestVote] Granting vote for candidate {} in term {}.", request.candidate_id, request.term);
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
            }
        }
    }
}