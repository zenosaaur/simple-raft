use tokio::time::{self, sleep, Instant};
use std::time::Duration;
use tokio::sync::mpsc;
use crate::state::{RaftEvent, RaftNode, RaftRole};
use std::sync::{Arc, Mutex};

async fn run_election_timer(mut reset_rx: mpsc::Receiver<u64>, event_tx: mpsc::Sender<RaftEvent>) {
    let mut duration_ms = 1000;
    let start = Instant::now();
    
    loop {
        let sleep_future = sleep(Duration::from_millis(duration_ms));
        tokio::pin!(sleep_future);

        println!(
            "[Timer] Waiting for {} ms...",
            duration_ms
        );

        tokio::select! {
            // Branch 1: The timer completes
            _ = &mut sleep_future => {
                println!("[Timer] Fired after {} ms! Total elapsed: {:?}", duration_ms, start.elapsed());
                // here the state of the node became CANDIDATE
            }
            // Branch 2: A reset message is received
            Some(new_duration_ms) = reset_rx.recv() => {
                println!("[Timer] Reset! New duration is {} ms.", new_duration_ms);
                duration_ms = new_duration_ms;
                // The old sleep_future was automatically cancelled.
                // The loop will now restart with the new duration.
            }
        }
    }
}

async fn run_raft_node(node_arc: Arc<Mutex<RaftNode>>, mut event_rx: mpsc::Receiver<RaftEvent>) {
    loop {
        // Aspettiamo un evento. In questo caso, solo il timeout di elezione.
        if let Some(event) = event_rx.recv().await {
            println!("[State] Ricevuto evento: {:?}", event);
            let mut node = node_arc.lock().unwrap();

            match event {
                RaftEvent::ElectionTimeout => {
                    // Il timer è scaduto. Se siamo Follower o Candidate, iniziamo una nuova elezione.
                    // Se siamo Leader, questo evento dovrebbe essere ignorato perché il leader
                    // resetta il timer degli altri inviando heartbeats.
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
                        // TODO: Implementare la logica per inviare RPC ai peer
                        println!("[State] Termine incrementato a {}. Votato per me stesso. Ora dovrei inviare RequestVote RPCs.", node.persistent.current_term);
                    }
                }
            }
        }
    }
}

