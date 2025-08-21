use tokio::time::{sleep};
use std::time::Duration;
use tokio::sync::mpsc;
use crate::state::{RaftEvent, RaftNode, RaftRole};
use std::sync::{Arc, Mutex};
use rand::Rng;

async fn run_election_timer(mut reset_rx: mpsc::Receiver<u64>, event_tx: mpsc::Sender<RaftEvent>) {
    loop {
        let timeout_ms = rand::rng().random_range(150..=300);
        let sleep_future = sleep(Duration::from_millis(timeout_ms));
        tokio::pin!(sleep_future);

        println!(
            "[Timer] Waiting for {} ms...",
            timeout_ms
        );

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

async fn run_raft_node(node_arc: Arc<Mutex<RaftNode>>, mut event_rx: mpsc::Receiver<RaftEvent>) {
    loop {
        if let Some(event) = event_rx.recv().await {
            println!("[State] Event recived: {:?}", event);
            let mut node = node_arc.lock().unwrap();

            match event {
                RaftEvent::ElectionTimeout => {
                    if node.volatile.role == RaftRole::Follower || node.volatile.role == RaftRole::Candidate {
                        println!("[State] Timer scaduto. Inizio nuova elezione.");
                        
                        // 1. Passa allo stato di Candidate
                        node.volatile.role = RaftRole::Candidate;
                        
                        // 2. Incrementa il termine corrente
                        node.persistent.current_term += 1;
                        
                        // 3. Vota per se stesso
                        node.persistent.voted_for = Some(node.persistent.id.clone());
                        
                        // 4. Persisti lo stato prima di inviare RPC
                        node.persist().unwrap();

                        // 5. Invia RequestVote RPC a tutti gli altri nodi (logica da implementare)
                        
                        println!("[State] Termine incrementato a {}. Votato per me stesso. Ora dovrei inviare RequestVote RPCs.", node.persistent.current_term);
                    }
                }
            }
        }
    }
}

