use tokio::sync::{mpsc, oneshot};

use crate::state::{RaftEvent};
use crate::{Raft, proto};

#[derive(Debug)]
pub struct RaftService {
    pub event_tx: mpsc::Sender<RaftEvent>,
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn append_entries(
        &self,
        request: tonic::Request<proto::AppendEntriesRequest>,
    ) -> Result<tonic::Response<proto::AppendEntriesResponse>, tonic::Status> {
        let (response_tx, response_rx) = oneshot::channel();
        let event = RaftEvent::RpcAppendEntries {
            request: request.into_inner(),
            responder: response_tx,
        };
        if self.event_tx.send(event).await.is_err() {
            return Err(tonic::Status::internal("Event loop is not running"));
        }
        match response_rx.await {
            Ok(raft_logic_result) => match raft_logic_result {
                Ok(response_payload) => Ok(tonic::Response::new(response_payload)),

                Err(status) => Err(status),
            },
            Err(_) => Err(tonic::Status::internal(
                "Failed to receive response from event loop",
            )),
        }
    }
    async fn request_vote(
        &self,
        request: tonic::Request<proto::RequestVoteRequest>,
    ) -> Result<tonic::Response<proto::RequestVoteResponse>, tonic::Status> {
        let (response_tx, response_rx) = oneshot::channel();
        let event = RaftEvent::RpcRequestVote {
            request: request.into_inner(),
            responder: response_tx,
        };

        if self.event_tx.send(event).await.is_err() {
            return Err(tonic::Status::internal("Event loop is not running"));
        }

        match response_rx.await {
            Ok(raft_logic_result) => match raft_logic_result {
                Ok(response_payload) => Ok(tonic::Response::new(response_payload)),

                Err(status) => Err(status),
            },
            Err(_) => Err(tonic::Status::internal(
                "Failed to receive response from event loop",
            )),
        }
    }
}
