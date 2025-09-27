use core::panic;
use proto::raft_server::{Raft, RaftServer};
use state::{AppConfig, Peer, RaftEvent, RaftNode, RaftPersistentState, RaftVolatileState};

use std::env;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use tonic::transport::Server;

use crate::consensus::{
    run_election_timer, run_raft_node, ConnectionManager, FollowerBackoff, RaftConfig,
};
use crate::server::RaftService;

mod consensus;
mod hash_table;
mod proto;
mod server;
mod state;
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
        let file = File::open(&args[1])?;
        let reader = BufReader::new(file);
        config = serde_yaml::from_reader(reader)?;
    } else {
        panic!("[Main] You need to add a correct config file");
    }

    // logging
    let file_appender = tracing_appender::rolling::daily("logs", &config.log_file);
    tracing_subscriber::fmt()
        .with_writer(file_appender)
        .with_ansi(false)
        .init();

    // --- 2) Persistent/volatile state bootstrap ---
    let address = format!("{}:{}", config.host, config.port);
    let node_state: RaftNode = if std::path::Path::new(config.state_file.as_str()).exists() {
        println!("[Main] State file found! Loading...");
        let file = File::open(config.state_file.as_str())?;
        let reader = BufReader::new(file);
        let persistent_state: RaftPersistentState = serde_json::from_reader(reader)?;
        RaftNode {
            persistent: persistent_state,
            volatile: RaftVolatileState::default(),
            state_path: config.state_file.clone(),
        }
    } else {
        println!("[Main] State file not found. Creating new state...");
        let persistent_state = RaftPersistentState {
            id: format!("{}:{}", config.domain, config.port),
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        };
        let node = RaftNode {
            persistent: persistent_state,
            volatile: RaftVolatileState::default(),
            state_path: config.state_file.clone(),
        };
        node.persist()?;
        node
    };
    let shared_node_state = Arc::new(Mutex::new(node_state));

    // --- 3) Cluster peers ---
    // ATTENZIONE: assicurati che config.peers NON contenga te stesso.
    let mut peers: Vec<Peer> = config.peers.clone();
    let self_id = {
        let n = shared_node_state.lock().await;
        n.persistent.id.clone()
    };
    peers.retain(|p| p.address != self_id);
    let available_followers = Arc::new(peers);
    println!("[Main] Configured peers (excluding self): {:?}", available_followers);

    // --- 4) Consensus runtime wiring ---
    let cfg = RaftConfig::default(); // opzionale: puoi popolare da AppConfig se vuoi renderlo configurabile
    let conn_mgr = Arc::new(ConnectionManager::new());
    let backoff = Arc::new(FollowerBackoff::new());

    let (event_tx, event_rx) = mpsc::channel::<RaftEvent>(100);
    let (reset_timer_tx, reset_timer_rx) = mpsc::channel::<()>(10);

    // 4.a) Election timer
    let timer_event_tx = event_tx.clone();
    let cfg_for_timer = cfg.clone();
    tokio::spawn(async move {
        run_election_timer(reset_timer_rx, timer_event_tx, cfg_for_timer).await;
    });
    println!("[Main] Election timer task started.");

    // 4.b) Raft state machine (core)
    let raft_node_arc = shared_node_state.clone();
    let event_tx_clone = event_tx.clone();
    let followers_arc = available_followers.clone();
    let cfg_for_node = cfg.clone();
    let conn_mgr_for_node = conn_mgr.clone();
    let backoff_for_node = backoff.clone();
    tokio::spawn(async move {
        run_raft_node(
            raft_node_arc,
            event_rx,
            reset_timer_tx,
            event_tx_clone,
            followers_arc,
            cfg_for_node,
            conn_mgr_for_node,
            backoff_for_node,
        )
        .await;
    });
    println!("[Main] Raft state machine task started.");

    // --- 5) gRPC server ---
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build()?;

    let raft_service = RaftService { event_tx: event_tx.clone() };

    let socket_address = address.parse()?;
    println!("[Main] Raft Server listening on {}", socket_address);

    Server::builder()
        .add_service(reflection_service)
        .add_service(RaftServer::new(raft_service))
        .serve(socket_address)
        .await?;

    Ok(())
}
