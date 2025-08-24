use crate::proto;
use crate::proto::raft_client::RaftClient;
use crate::state::{RaftEvent, RaftNode, RaftRole};
use futures::{future, stream::StreamExt};
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::sleep;

pub async fn run_election_timer(mut reset_rx: mpsc::Receiver<()>, event_tx: mpsc::Sender<RaftEvent>) {
    loop {
        let timeout_ms = rand::rng().random_range(150..=300);
        let sleep_future = sleep(Duration::from_millis(timeout_ms));
        tokio::pin!(sleep_future);

        println!("[Timer] Waiting for {} ms...", timeout_ms);

        tokio::select! {
            _ = &mut sleep_future => {
                println!("[Timer] Fired after {} ms!", timeout_ms);
                if let Err(e) = event_tx.send(RaftEvent::ElectionTimeout).await {
                    println!("[Timer] Failed to send event: {:?}", e);
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
    available_followers: Vec<String>,
) {
    loop {
        if let Some(event) = event_rx.recv().await {
            println!("[State] Event recived: {:?}", event);
            let mut node = node_arc.lock().await;

            match event {
                RaftEvent::ElectionTimeout => {
                    if node.volatile.role == RaftRole::Follower
                        || node.volatile.role == RaftRole::Candidate
                    {
                        println!("[State] Timer scaduto. Inizio nuova elezione.");
                        node.volatile.role = RaftRole::Candidate;
                        node.persistent.current_term += 1;
                        node.persistent.voted_for = Some(node.persistent.id.clone());
                        node.persist().unwrap();
                        let request = proto::RequestVoteRequest {
                            term: node.persistent.current_term,
                            candidate_id: node.persistent.id.clone(),
                            last_log_term: node.persistent.log.last().map_or(0, |entry| entry.term),
                            last_log_index: node.persistent.log.len() as u64,
                        };
                        
                        let connection_futures = available_followers.clone().into_iter().map(|follower_addr| {
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
                            .filter_map(|result| {
                                match result {
                                    Ok(mut client) => {
                                        // Connection successful, spawn the RPC task.
                                        let req = tonic::Request::new(request.clone());
                                        Some(tokio::spawn(
                                            async move { client.request_vote(req).await },
                                        ))
                                    }
                                    Err((addr, err)) => {
                                        // Connection failed, log it and move on.
                                        eprintln!(
                                            "[State] Failed to connect to follower {}: {}",
                                            addr, err
                                        );
                                        None // This connection will be skipped.
                                    }
                                }
                            })
                            .collect::<Vec<_>>();

                        let results = future::join_all(tasks).await;
                        let mut total_vote = 1;
                        for result in results {
                            match result {
                                Ok(Ok(response)) => {
                                   
                                    println!(
                                        "[State] Received vote: {:?}",
                                        response.get_ref().vote_granted
                                    );
                                    if response.get_ref().term == node.persistent.current_term && response.get_ref().vote_granted {
                                        total_vote+=1;
                                    }
                                    
                                }
                                Ok(Err(rpc_error)) => {
                                    // Task completed but the RPC failed.
                                    eprintln!("[State] RPC failed: {}", rpc_error);
                                }
                                Err(join_error) => {
                                    // The spawned task panicked.
                                    eprintln!("[State] Task failed to execute: {}", join_error);
                                }
                            }
                        }
                        if total_vote > (available_followers.len()+1)/2 {
                            //node became leader and start hearthbeat to mantain the status
                        }
                    }
                }
            }
        }
    }
}
