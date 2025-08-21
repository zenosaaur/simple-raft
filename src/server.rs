use std::sync::{Arc, Mutex};
use crate::{proto, Raft};
use crate::state::{RaftNode,RaftRole,LogEntry};

#[derive(Debug)]
pub struct RaftService {
    pub node: Arc<Mutex<RaftNode>>,
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn append_entries(
        &self,
        request: tonic::Request<proto::AppendEntriesRequest>,
    ) -> Result<tonic::Response<proto::AppendEntriesResponse>, tonic::Status> {
        let mut node = self.node.lock().unwrap();
        let req = request.into_inner();

        // Regola Raft #1: Rispondi 'false' se il termine del richiedente è obsoleto.
        if req.term < node.persistent.current_term {
            return Ok(tonic::Response::new(proto::AppendEntriesResponse {
                term: node.persistent.current_term,
                success: false,
            }));
        }

        // Se il termine del leader è più nuovo, aggiorna il nostro e diventa follower.
        if req.term > node.persistent.current_term {
            node.persistent.current_term = req.term;
            node.persistent.voted_for = None;
            node.volatile.role = RaftRole::Follower;
        }

        // Regola Raft #2: Rispondi 'false' se il log non contiene una voce a prev_log_index
        // il cui termine corrisponda a prev_log_term.
        // NOTA: Gli indici nel paper Raft sono 1-based, mentre in Rust i Vec sono 0-based.
        // Qui gestiamo la conversione. Assumiamo che req.prev_log_index = 0 indichi l'inizio.
        if req.prev_log_index > 0 {
            let vec_index = (req.prev_log_index - 1) as usize;
            match node.persistent.log.get(vec_index) {
                Some(entry) => {
                    // L'indice esiste, controlliamo se il termine corrisponde.
                    if entry.term != req.prev_log_term {
                        // Conflitto di termini, log incoerente.
                        return Ok(tonic::Response::new(proto::AppendEntriesResponse {
                            term: node.persistent.current_term,
                            success: false,
                        }));
                    }
                }
                None => {
                    // Il nostro log è troppo corto, non abbiamo l'indice richiesto.
                    return Ok(tonic::Response::new(proto::AppendEntriesResponse {
                        term: node.persistent.current_term,
                        success: false,
                    }));
                }
            }
        }

        // Regola Raft #3: Se una voce esistente è in conflitto con una nuova
        // (stesso indice ma termini diversi), elimina la voce esistente e tutte le successive.
        let mut conflict_found = false;
        let mut first_new_entry_index = 0;

        for (i, new_entry) in req.entries.iter().enumerate() {
            let log_index = (req.prev_log_index as usize) + i;

            if let Some(existing_entry) = node.persistent.log.get(log_index) {
                if existing_entry.term != new_entry.term {
                    // Conflitto trovato! Tronchiamo il log da questo punto.
                    node.persistent.log.truncate(log_index);
                    conflict_found = true;
                    first_new_entry_index = i;
                    break; // Usciamo dal ciclo di controllo dei conflitti
                }
            } else {
                // Il nostro log è più corto, quindi tutte le voci da qui in poi sono nuove.
                conflict_found = true;
                first_new_entry_index = i;
                break;
            }
        }

        // Regola Raft #4: Aggiungi tutte le nuove voci non ancora presenti nel log.
        if conflict_found {
            // Aggiungiamo tutte le voci dalla richiesta a partire dal punto di conflitto/divergenza.
            for i in first_new_entry_index..req.entries.len() {
                let entry_to_add = &req.entries[i];
                node.persistent.log.push(LogEntry {
                    term: entry_to_add.term,
                    command: entry_to_add.command.clone(),
                });
            }
        }

        // --- FINE DELLA LOGICA RICHIESTA ---

        // Regola Raft #5: Se leader_commit > commit_index, imposta commit_index.
        if req.leader_commit > node.volatile.commit_index {
            let last_new_entry_index = (req.prev_log_index + req.entries.len() as u64);
            node.volatile.commit_index = std::cmp::min(req.leader_commit, last_new_entry_index);
        }

        // Se tutto è andato a buon fine, persistiamo lo stato su disco prima di rispondere.
        if let Err(e) = node.persist() {
            return Err(tonic::Status::internal(format!(
                "Fallimento nel salvare lo stato: {}",
                e
            )));
        }

        // Inviamo una risposta di successo.
        let response = proto::AppendEntriesResponse {
            term: node.persistent.current_term,
            success: true,
        };

        Ok(tonic::Response::new(response))
    }
    async fn request_vote(
        &self,
        request: tonic::Request<proto::RequestVoteRequest>,
    ) -> Result<tonic::Response<proto::RequestVoteResponse>, tonic::Status> {
        let mut node = self.node.lock().unwrap();
        let req = request.into_inner();

        if req.term < node.persistent.current_term {
            return Ok(tonic::Response::new(proto::RequestVoteResponse {
                term: node.persistent.current_term,
                vote_granted: false,
            }));
        }

        if req.term > node.persistent.current_term {
            node.persistent.current_term = req.term;
            node.persistent.voted_for = None;
            node.volatile.role = RaftRole::Follower;
        }
        
        let can_vote = match &node.persistent.voted_for {
            Some(voted_id) => *voted_id == req.candidate_id,
            None => true,
        };
        
        if !can_vote {
            return Ok(tonic::Response::new(proto::RequestVoteResponse {
                term: node.persistent.current_term,
                vote_granted: false,
            }));
        }
        
        // Condition B: Check if the candidate's log is at least as up-to-date as ours.
        // This is the crucial safety rule in Raft leader election.
        let last_log_entry = node.persistent.log.last();
        let last_log_term = last_log_entry.map_or(0, |entry| entry.term);
        let last_log_index = node.persistent.log.len() as u64;

        if req.last_log_term < last_log_term
            || (req.last_log_term == last_log_term && req.last_log_index < last_log_index)
        {
            return Ok(tonic::Response::new(proto::RequestVoteResponse {
                term: node.persistent.current_term,
                vote_granted: false,
            }));
        }

        // 3. If both conditions passed, grant the vote.
        node.persistent.voted_for = Some(req.candidate_id);
        node.persist();
        
        Ok(tonic::Response::new(proto::RequestVoteResponse {
            term: node.persistent.current_term,
            vote_granted: true,
        }))
    }
}