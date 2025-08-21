use proto::raft_server::{Raft, RaftServer};
use tokio::sync::mpsc;
use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, Mutex};
use tonic::transport::Server;
use uuid::Uuid;


mod state;
mod consensus;
mod server;
mod proto;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:8080".parse()?;
    let node_state: state::RaftNode;
    let file_path = "state.json";
    
    if std::path::Path::new(file_path).exists() {
        println!("File trovato! Caricamento stato...");
        let file = File::open(file_path)?;
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
            log: vec![],
        };

        node_state = state::RaftNode {
            persistent: persistent_state,
            volatile: state::RaftVolatileState::default(),
        };
        node_state.persist()?;
    }

    // Crea lo stato condiviso e mutabile
    let shared_node_state = Arc::new(Mutex::new(node_state));

    let raft_service = server::RaftService {
        node: shared_node_state.clone(),
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

    Ok(())
}
