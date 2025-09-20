use prost::{Message};
use proto::raft_client::RaftClient;
use std::error::Error;
use std::{env, io};
use tonic::Code;

use crate::proto::LeaderInfo;
pub mod proto {
    tonic::include_proto!("raft");
}

mod validator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <url>", args[0]);
        std::process::exit(1);
    }
    // CHANGE: Made url mutable to allow for redirection.
    let mut url: String = args[1].parse()?;

    // Connect to the initial URL provided.
    println!("Connecting to {}...", url);
    let mut client = RaftClient::connect(url.clone()).await?;

    let mut input = String::new();
    let mut request_id_counter = 0;
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

        if !validator::validate(trimmed_input) {
            let command_request = proto::SubmitCommandRequest {
                client_id: "desktop-client-01".to_string(),
                request_id: request_id_counter,
                // CHANGE: Removed unnecessary .clone()
                command: trimmed_input.to_string(),
            };
            let request = tonic::Request::new(command_request);

            match client.submit_command(request).await {
                Ok(res) => {
                    println!("Success: {:?}", res.get_ref().result);
                    request_id_counter += 1;
                }
                Err(status) => {
                    if status.code() == Code::FailedPrecondition {
                        redirected = true;
                        if let Some(details_bin) = status.metadata().get_bin("leader-info-bin") {
                            match details_bin.to_bytes() {
                                Ok(bytes) => match LeaderInfo::decode(bytes) {
                                    Ok(leader_info) => {
                                        let nuovo_leader_addr = leader_info.leader_address;
                                        url = nuovo_leader_addr.clone();
                                        client = RaftClient::connect(format!("http://{}",url.clone())).await?;
                                    }
                                    Err(e) => {
                                        println!(
                                            "FailedPrecondition error received, but could not decode leader details: {}",
                                            e
                                        );
                                    }
                                },
                                Err(e) => eprintln!("Invalid binary metadata bytes: {e}"),
                            }
                        } else {
                            println!(
                                "FailedPrecondition error received, but no leader details provided."
                            );
                        }
                    } else {
                        eprintln!("Unexpected error during the call: {}", status);
                    }
                }
            };
        }
    }
    Ok(())
}
