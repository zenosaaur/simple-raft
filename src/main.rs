use proto::raft_server::{Raft, RaftServer};
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tonic::transport::Server;
use uuid::Uuid;

use crate::consensus::{run_election_timer, run_raft_node};
use crate::server::RaftService;
use crate::state::{RaftEvent, RaftNode, RaftPersistentState, RaftVolatileState};

mod consensus;
mod proto;
mod server;
mod state;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- 1. Configuration and Argument Parsing ---
    let args: Vec<_> = env::args().collect();
    if args.len() < 2 { // Indices start at 0, the argument is at index 1
        panic!("The host port is a required argument. Example: `cargo run 50051`");
    }
    let port = &args[1];
    let own_addr_str = format!("http://0.0.0.0:{}", port);
    let addr = format!("0.0.0.0:{}", port).parse()?;

    // --- 2. State Initialization (from file or new) ---
    let node_state: RaftNode;
    if std::path::Path::new("state.json").exists() {
        println!("[Main] State file found! Loading...");
        let file = File::open("state.json")?;
        let reader = BufReader::new(file);
        let persistent_state: RaftPersistentState = serde_json::from_reader(reader)?;
        node_state = RaftNode {
            persistent: persistent_state,
            volatile: RaftVolatileState::default(),
        };
    } else {
        println!("[Main] State file not found. Creating new state...");
        let persistent_state = RaftPersistentState {
            id: Uuid::new_v4().to_string(),
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        };
        node_state = RaftNode {
            persistent: persistent_state,
            volatile: RaftVolatileState::default(),
        };
        node_state.persist()?;
    }

    // --- 3. Setup Shared State, Channels, and Peers ---
    let shared_node_state = Arc::new(Mutex::new(node_state));
    
    // Fix: Define the channels only once
    let (event_tx, event_rx) = mpsc::channel::<RaftEvent>(100);
    let (reset_timer_tx, reset_timer_rx) = mpsc::channel::<()>(10);

    // Improvement: More robust peer handling
    let peers_str = "8080,8081,8082";
    let available_followers: Vec<String> = if peers_str.is_empty() {
        Vec::new()
    } else {
        peers_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|addr| *addr != own_addr_str)
            .collect()
    };
    println!("[Main] Configured peers: {:?}", available_followers);


    // --- 4. Spawning Background Tasks ---
    let timer_event_tx = event_tx.clone();
    tokio::spawn(async move {
        run_election_timer(reset_timer_rx, timer_event_tx).await;
    });
    println!("[Main] Election timer task started.");

    let raft_node_arc = shared_node_state.clone();
    tokio::spawn(async move {
        run_raft_node(raft_node_arc, event_rx, reset_timer_tx, available_followers).await;
    });
    println!("[Main] Raft state machine task started.");

    // --- 5. Starting the gRPC Server ---
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build()?;
        
    let raft_service = RaftService {
        event_tx: event_tx.clone(),
    };

    println!("[Main] Raft Server listening on {}", addr);
    Server::builder()
        .add_service(reflection_service)
        .add_service(RaftServer::new(raft_service))
        .serve(addr)
        .await?;

    Ok(())
}