use prost::Message;
use proto::raft_client::RaftClient;
use std::error::Error;
use std::{env, fs, io};
use tonic::{Code, transport::Channel};

use crate::proto::LeaderInfo;
pub mod proto {
    tonic::include_proto!("raft");
}

mod parser;

// --- Simple durable monotonic counter (per client) ---
#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
struct ClientState {
    last_request_id: u64,
}

fn load_state(path: &str) -> ClientState {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
        Err(_) => ClientState::default(),
    }
}

fn save_state(path: &str, st: &ClientState) {
    if let Ok(json) = serde_json::to_vec_pretty(st) {
        let _ = fs::write(path, json); // best-effort; you can fsync if you want stronger durability
    }
}

fn normalize_url(input: &str) -> String {
    if input.starts_with("http://") || input.starts_with("https://") {
        input.to_string()
    } else {
        format!("http://{}", input)
    }
}

async fn connect(url: &str) -> Result<RaftClient<Channel>, Box<dyn Error>> {
    println!("Connecting to {}...", url);
    let client = RaftClient::connect(url.to_string()).await?;
    Ok(client)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <url>", args[0]);
        std::process::exit(1);
    }
    // Stable identity for this client process
    let client_id = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .map(|h| format!("cli-{}", h))
        .unwrap_or_else(|| "cli-unknown".to_string());

    // Durable request counter path (per client)
    let state_path = format!("{}.state.json", client_id);
    let mut state = load_state(&state_path);

    // Normalize initial URL
    let mut url = normalize_url(&args[1]);
    let mut client = connect(&url).await?;

    let mut input = String::new();
    let mut redirected = false;

    loop {
        if !redirected {
            print!("> ");
            io::Write::flush(&mut io::stdout()).expect("flush failed!");
            input.clear();
            io::stdin()
                .read_line(&mut input)
                .expect("Failed to read line");
        }
        redirected = false;

        let trimmed_input = input.trim();
        if trimmed_input == "exit" {
            break;
        }
        if trimmed_input.is_empty() {
            continue;
        }

        // Monotonic per-client request id
        let request_id = state.last_request_id.wrapping_add(1);
        if let Ok(parsed_command) = parser::parse_commands(trimmed_input) {
            let command_request = proto::SubmitCommandRequest {
                client_id: "desktop-client-01".to_string(),
                request_id,
                command: trimmed_input.to_string(),
            };
            let request = tonic::Request::new(command_request);
            match client.submit_command(request).await {
                Ok(res) => {
                    println!("Success: {:?}", res.get_ref().result);
                    state.last_request_id = request_id;
                    save_state(&state_path, &state);
                }
                Err(status) => {
                    if status.code() == Code::FailedPrecondition {
                        if let Some(details_bin) = status.metadata().get_bin("leader-info-bin") {
                            match details_bin.to_bytes() {
                                Ok(bytes) => match LeaderInfo::decode(bytes) {
                                    Ok(leader_info) => {
                                        let leader = leader_info.leader_address;
                                        let new_url = normalize_url(&leader);
                                        println!("Redirecting to {}...", new_url);
                                        url = new_url;
                                        client = connect(&url).await?;
                                        redirected = true; // retry same input on next loop
                                        continue;
                                    }
                                    Err(e) => eprintln!("Failed to decode leader info: {}", e),
                                },
                                Err(e) => eprintln!("Invalid binary metadata bytes: {e}"),
                            }
                        } else {
                            println!("Leader hint missing; retrying later.");
                        }
                    } else {
                        eprintln!("RPC error: {}", status);
                    }
                }
            }
        }
    }
    Ok(())
}
