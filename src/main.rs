use proto::raft_server::{Raft, RaftServer};
use tokio::sync::mpsc;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc};
use tokio::sync::{Mutex};
use tonic::transport::Server;
use uuid::Uuid;



mod state;
mod consensus;
mod server;
mod proto;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().collect();
    if args.len() < 1 {
       panic!("Host port is mandotarary");
    }
    let connection_string = format!("0.0.0.0:{}", args[1]);
    let addr = connection_string.parse()?;
    let node_state: state::RaftNode;
    
    // state init
    if std::path::Path::new("state.json").exists() {
        println!("File trovato! Caricamento stato...");
        let file = File::open("state.json")?;
        let reader = BufReader::new(file);
        let persistent_state: state::RaftPersistentState = serde_json::from_reader(reader)?;
        node_state = state::RaftNode {
            persistent: persistent_state,
            volatile: state::RaftVolatileState::default(),
        };
    } else {
        println!("File non trovato. Creazione nuovo stato...");
        let persistent_state = state::RaftPersistentState {
            id: Uuid::new_v4().to_string(),
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        };

        node_state = state::RaftNode {
            persistent: persistent_state,
            volatile: state::RaftVolatileState::default(),
        };
        node_state.persist()?;
    }

    // Crea lo stato condiviso e mutabile
    let shared_node_state = Arc::new(Mutex::new(node_state));
    let (reset_timer_tx, reset_timer_rx) = mpsc::channel::<()>(8);
    let (event_tx, event_rx) = mpsc::channel(8);
    let peers = env::var("RAFT_PEERS").unwrap_or_default();
    
    let available_followers: Vec<String> = peers
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    
    let node_handle = tokio::spawn(consensus::run_raft_node(shared_node_state.clone(), event_rx, available_followers.clone()));
    let timer_handle = tokio::spawn(consensus::run_election_timer(reset_timer_rx, event_tx));




    let raft_service = server::RaftService {
        node: shared_node_state.clone(),
        reset_timer_tx
    };

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build()?;

    println!("Server Raft in ascolto su {}", addr);
    Server::builder()
        .add_service(reflection_service)
        .add_service(RaftServer::new(raft_service))
        .serve(addr)
        .await?;
    let _ = tokio::try_join!(timer_handle, node_handle);

    Ok(())
}
