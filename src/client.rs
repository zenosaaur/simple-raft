use proto::raft_client::RaftClient;
use std::error::Error;
pub mod proto {
    tonic::include_proto!("raft");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = "http://[::1]:50051";
    let mut client = RaftClient::connect(url).await?;

    let req = proto::AppendEntriesRequest {
        term: 5,
        leader_id: "leader-01".to_string(),
        prev_log_index: 12,
        prev_log_term: 4,
        entries: vec![
            proto::LogEntry {
                term: 5,
                command: "SET user:123 name \"Alice\"".to_string(),
            },
            proto::LogEntry {
                term: 5,
                command: "INCR counter:requests".to_string(),
            },
        ],
        leader_commit: 10,
    };
    let request = tonic::Request::new(req);

    let responde = client.append_entries(request).await?;

    println!("Responde {:?}", responde.get_ref().success);
    Ok(())
}
