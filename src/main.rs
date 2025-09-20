use core::panic;
use proto::raft_server::{Raft, RaftServer};
use state::{AppConfig};
use tracing_subscriber::EnvFilter;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tonic::transport::Server;

use crate::consensus::{run_election_timer, run_raft_node};
use crate::server::RaftService;
use crate::state::{RaftEvent, RaftNode, RaftPersistentState, RaftVolatileState};

mod consensus;
mod proto;
mod server;
mod state;
mod hash_table;
mod validator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().collect();
    if args.len() < 2 {
        panic!("Make sure you have passed a correct number of parameter");
    }

    // node config
    let config: AppConfig;
    if std::path::Path::new(&args[1]).exists() {
        println!("[Main] Condiguration file found. Loadding... ");
        {
            let file = File::open(&args[1])?;
            let reader = BufReader::new(file);
            config = serde_yaml::from_reader(reader)?
        }
    } else {
        panic!("[Main] You need to add a correct config file");
    }
    let file_appender = tracing_appender::rolling::daily("logs", config.log_file);

    tracing_subscriber::fmt()
        .with_writer(file_appender)
        .with_ansi(false)
        .init();

    let address = format!("{}:{}", config.host, config.port);
    let node_state: RaftNode;
    if std::path::Path::new(config.state_file.as_str()).exists() {
        println!("[Main] State file found! Loading...");
        let file = File::open(config.state_file.as_str())?;
        let reader = BufReader::new(file);
        let persistent_state: RaftPersistentState = serde_json::from_reader(reader)?;
        node_state = RaftNode {
            persistent: persistent_state,
            volatile: RaftVolatileState::default(),
            state_path: config.state_file        
        };
    } else {
        println!("[Main] State file not found. Creating new state...");
        let persistent_state = RaftPersistentState {
            id: address.clone(),
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        };
        node_state = RaftNode {
            persistent: persistent_state,
            volatile: RaftVolatileState::default(),
            state_path: config.state_file    
        };
        node_state.persist()?;
    }
    let shared_node_state = Arc::new(Mutex::new(node_state));

    let (event_tx, event_rx) = mpsc::channel::<RaftEvent>(100);
    let (reset_timer_tx, reset_timer_rx) = mpsc::channel::<()>(10);

    let available_followers= config.peers;
    println!("[Main] Configured peers: {:?}", available_followers);

    // --- 4. Spawning Background Tasks ---
    let timer_event_tx = event_tx.clone();
    tokio::spawn(async move {
        run_election_timer(reset_timer_rx, timer_event_tx).await;
    });
    println!("[Main] Election timer task started.");

    let raft_node_arc = shared_node_state.clone();
    let event_tx_clone = event_tx.clone();
    tokio::spawn(async move {
        run_raft_node(
            raft_node_arc,
            event_rx,
            reset_timer_tx,
            event_tx_clone,
            available_followers.clone()        
        ).await;
    });
    println!("[Main] Raft state machine task started.");

    // --- 5. Starting the gRPC Server ---
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build()?;

    let raft_service = RaftService {
        event_tx: event_tx.clone(),
    };

    let socket_address= address.parse()?;
    println!("[Main] Raft Server listening on {}", socket_address);
    Server::builder()
        .add_service(reflection_service)
        .add_service(RaftServer::new(raft_service))
        .serve(socket_address)
        .await?;

    Ok(())
}
