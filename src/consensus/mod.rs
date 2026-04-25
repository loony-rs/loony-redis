/// Raft consensus implementation (Phase 5).
///
/// Architecture
/// ────────────
/// • Each node runs one Raft state-machine tokio-task (`run_node`).
/// • A separate RPC-server task listens on `raft_addr` and forwards
///   incoming RESP-encoded RPCs to the state machine via a channel.
/// • When the state machine commits an entry it calls the caller-supplied
///   `apply_fn`, which executes the command against the KV store and
///   returns the RESP response that the waiting client receives.
/// • Clients interact through `Raft::propose(cmd_bytes) -> Frame`.
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info, warn};

use crate::protocol::{parse_frame, serialize_frame, Frame};

// ── Constants ──────────────────────────────────────────────────────────────

const HEARTBEAT_MS: u64 = 50;
const ELECTION_MIN_MS: u64 = 150;
const ELECTION_MAX_MS: u64 = 300;
const RPC_TIMEOUT_MS: u64 = 100;

// ── Data types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    /// Serialised RESP command (Frame::Array).
    pub command: Bytes,
}

#[derive(Debug, Clone, PartialEq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

pub type ApplyFn =
    Arc<dyn Fn(Frame) -> Pin<Box<dyn Future<Output = Frame> + Send>> + Send + Sync>;

struct Proposal {
    command: Bytes,
    reply: oneshot::Sender<Frame>,
}

// ── Public handle ──────────────────────────────────────────────────────────

pub struct Raft {
    pub node_id: String,
    pub is_leader: Arc<AtomicBool>,
    /// Current leader's node_id, if known.
    pub leader_id: Arc<RwLock<Option<String>>>,
    proposal_tx: mpsc::Sender<Proposal>,
}

impl Raft {
    /// Submit a write command to Raft. Blocks until the entry is committed
    /// and applied to the state machine; returns the command's result frame.
    pub async fn propose(&self, command: Bytes) -> Frame {
        let (tx, rx) = oneshot::channel();
        if self.proposal_tx.send(Proposal { command, reply: tx }).await.is_err() {
            return Frame::error("ERR Raft node shut down");
        }
        rx.await.unwrap_or_else(|_| Frame::error("ERR Raft internal error"))
    }

    pub async fn current_leader(&self) -> Option<String> {
        self.leader_id.read().await.clone()
    }
}

// ── RPC message types ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum RaftRpc {
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResp {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResp {
        term: u64,
        success: bool,
        match_index: u64,
    },
}

// ── RESP encoding/decoding for Raft RPCs ───────────────────────────────────

fn rpc_to_frame(rpc: &RaftRpc) -> Frame {
    match rpc {
        RaftRpc::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
            Frame::array(vec![
                Frame::bulk_str("RAFT_REQVOTE"),
                Frame::bulk_str(term.to_string()),
                Frame::bulk_str(candidate_id),
                Frame::bulk_str(last_log_index.to_string()),
                Frame::bulk_str(last_log_term.to_string()),
            ])
        }
        RaftRpc::RequestVoteResp { term, vote_granted } => Frame::array(vec![
            Frame::bulk_str("RAFT_VOTERESP"),
            Frame::bulk_str(term.to_string()),
            Frame::bulk_str(if *vote_granted { "1" } else { "0" }),
        ]),
        RaftRpc::AppendEntries {
            term, leader_id, prev_log_index, prev_log_term, entries, leader_commit,
        } => {
            let mut args = vec![
                Frame::bulk_str("RAFT_APPEND"),
                Frame::bulk_str(term.to_string()),
                Frame::bulk_str(leader_id),
                Frame::bulk_str(prev_log_index.to_string()),
                Frame::bulk_str(prev_log_term.to_string()),
                Frame::bulk_str(leader_commit.to_string()),
            ];
            for e in entries {
                args.push(Frame::bulk_str(e.term.to_string()));
                args.push(Frame::Bulk(Some(e.command.clone())));
            }
            Frame::array(args)
        }
        RaftRpc::AppendEntriesResp { term, success, match_index } => Frame::array(vec![
            Frame::bulk_str("RAFT_APPENDRESP"),
            Frame::bulk_str(term.to_string()),
            Frame::bulk_str(if *success { "1" } else { "0" }),
            Frame::bulk_str(match_index.to_string()),
        ]),
    }
}

fn frame_to_rpc(frame: &Frame) -> Option<RaftRpc> {
    let args = match frame {
        Frame::Array(Some(a)) => a,
        _ => return None,
    };
    let tag = bulk_str(args, 0)?;
    match tag.as_str() {
        "RAFT_REQVOTE" => Some(RaftRpc::RequestVote {
            term: parse_u64(args, 1)?,
            candidate_id: bulk_str(args, 2)?,
            last_log_index: parse_u64(args, 3)?,
            last_log_term: parse_u64(args, 4)?,
        }),
        "RAFT_VOTERESP" => Some(RaftRpc::RequestVoteResp {
            term: parse_u64(args, 1)?,
            vote_granted: bulk_str(args, 2)? == "1",
        }),
        "RAFT_APPEND" => {
            let term = parse_u64(args, 1)?;
            let leader_id = bulk_str(args, 2)?;
            let prev_log_index = parse_u64(args, 3)?;
            let prev_log_term = parse_u64(args, 4)?;
            let leader_commit = parse_u64(args, 5)?;
            let mut entries = Vec::new();
            let mut i = 6;
            while i + 1 < args.len() {
                let entry_term = parse_u64(args, i)?;
                let cmd = match &args[i + 1] {
                    Frame::Bulk(Some(b)) => b.clone(),
                    _ => return None,
                };
                entries.push(LogEntry { term: entry_term, command: cmd });
                i += 2;
            }
            Some(RaftRpc::AppendEntries {
                term, leader_id, prev_log_index, prev_log_term, entries, leader_commit,
            })
        }
        "RAFT_APPENDRESP" => Some(RaftRpc::AppendEntriesResp {
            term: parse_u64(args, 1)?,
            success: bulk_str(args, 2)? == "1",
            match_index: parse_u64(args, 3)?,
        }),
        _ => None,
    }
}

fn bulk_str(args: &[Frame], i: usize) -> Option<String> {
    match args.get(i)? {
        Frame::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

fn parse_u64(args: &[Frame], i: usize) -> Option<u64> {
    bulk_str(args, i)?.trim().parse().ok()
}

// ── Raft state machine ─────────────────────────────────────────────────────

struct RaftNode {
    id: String,
    peers: Vec<String>,

    // Persistent state (in-memory for Phase 5; would be on disk in production)
    current_term: u64,
    voted_for: Option<String>,
    /// log[0] is a sentinel entry (term=0, empty command)
    log: Vec<LogEntry>,

    // Volatile state
    commit_index: u64,
    last_applied: u64,

    // Leader state
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,

    // Role and election
    role: Role,
    votes_received: usize,

    // Pending client proposals: log_index → reply channel
    pending: HashMap<u64, oneshot::Sender<Frame>>,

    // Applied to state machine via this async callback
    apply_fn: ApplyFn,

    // Shared with the public Raft handle
    is_leader_flag: Arc<AtomicBool>,
    leader_id_shared: Arc<RwLock<Option<String>>>,

    // Incoming channels
    proposal_rx: mpsc::Receiver<Proposal>,
    rpc_rx: mpsc::Receiver<(RaftRpc, oneshot::Sender<RaftRpc>)>,
    // Vote/append responses from spawned send-tasks
    vote_resp_rx: mpsc::Receiver<RaftRpc>,
    append_resp_rx: mpsc::Receiver<(String, RaftRpc)>, // (peer_id, resp)

    vote_resp_tx: mpsc::Sender<RaftRpc>,
    append_resp_tx: mpsc::Sender<(String, RaftRpc)>,

    // Timer
    election_deadline: Instant,
    last_heartbeat_sent: Instant,
}

impl RaftNode {
    fn quorum(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    fn last_log_index(&self) -> u64 {
        (self.log.len() as u64).saturating_sub(1)
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    fn log_term_at(&self, index: u64) -> u64 {
        self.log.get(index as usize).map(|e| e.term).unwrap_or(0)
    }

    fn reset_election_timeout(&mut self) {
        let ms = rand::thread_rng().gen_range(ELECTION_MIN_MS..=ELECTION_MAX_MS);
        self.election_deadline = Instant::now() + Duration::from_millis(ms);
    }

    fn step_down(&mut self, term: u64) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        if self.role != Role::Follower {
            info!("[{}] stepping down to follower (term {})", self.id, term);
        }
        self.role = Role::Follower;
        self.is_leader_flag.store(false, Ordering::Release);
        self.reset_election_timeout();
    }

    fn set_leader(&mut self, leader_id: String) {
        let shared = self.leader_id_shared.clone();
        let id = leader_id.clone();
        tokio::spawn(async move {
            *shared.write().await = Some(id);
        });
    }

    // ── Leader election ────────────────────────────────────────────────

    fn start_election(&mut self) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());
        self.votes_received = 1; // self-vote
        self.reset_election_timeout();

        info!("[{}] starting election for term {}", self.id, self.current_term);

        let rpc = RaftRpc::RequestVote {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        for peer in &self.peers {
            let peer = peer.clone();
            let req = rpc.clone();
            let tx = self.vote_resp_tx.clone();
            tokio::spawn(async move {
                if let Some(resp) = send_rpc_to(&peer, &req).await {
                    let _ = tx.send(resp).await;
                }
            });
        }
    }

    fn handle_vote_response(&mut self, resp: RaftRpc) {
        let (term, granted) = match resp {
            RaftRpc::RequestVoteResp { term, vote_granted } => (term, vote_granted),
            _ => return,
        };

        if term > self.current_term {
            self.step_down(term);
            return;
        }
        if self.role != Role::Candidate || term < self.current_term {
            return;
        }
        if granted {
            self.votes_received += 1;
            debug!("[{}] got vote ({}/{})", self.id, self.votes_received, self.quorum());
            if self.votes_received >= self.quorum() {
                self.become_leader();
            }
        }
    }

    fn become_leader(&mut self) {
        info!("[{}] became leader for term {}", self.id, self.current_term);
        self.role = Role::Leader;
        self.is_leader_flag.store(true, Ordering::Release);
        self.set_leader(self.id.clone());

        let next = self.last_log_index() + 1;
        for peer in &self.peers {
            self.next_index.insert(peer.clone(), next);
            self.match_index.insert(peer.clone(), 0);
        }
        // Send immediate heartbeat to assert leadership.
        self.last_heartbeat_sent = Instant::now() - Duration::from_secs(1);
    }

    // ── AppendEntries (leader → follower) ──────────────────────────────

    fn send_heartbeats(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_heartbeat_sent)
            < Duration::from_millis(HEARTBEAT_MS)
        {
            return;
        }
        self.last_heartbeat_sent = now;

        for peer in &self.peers {
            self.send_append_entries_to(peer.clone());
        }
    }

    fn send_append_entries_to(&self, peer: String) {
        let next = *self.next_index.get(&peer).unwrap_or(&1);
        let prev_index = next.saturating_sub(1);
        let prev_term = self.log_term_at(prev_index);

        let entries: Vec<LogEntry> = if next as usize <= self.log.len().saturating_sub(1) {
            self.log[next as usize..].to_vec()
        } else {
            vec![]
        };

        let rpc = RaftRpc::AppendEntries {
            term: self.current_term,
            leader_id: self.id.clone(),
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.commit_index,
        };

        let tx = self.append_resp_tx.clone();
        let peer_clone = peer.clone();
        tokio::spawn(async move {
            if let Some(resp) = send_rpc_to(&peer_clone, &rpc).await {
                let _ = tx.send((peer_clone, resp)).await;
            }
        });
    }

    fn handle_append_response(&mut self, peer: String, resp: RaftRpc) {
        let (term, success, match_idx) = match resp {
            RaftRpc::AppendEntriesResp { term, success, match_index } => {
                (term, success, match_index)
            }
            _ => return,
        };

        if term > self.current_term {
            self.step_down(term);
            return;
        }
        if self.role != Role::Leader {
            return;
        }

        if success {
            let entry = self.next_index.entry(peer.clone()).or_insert(1);
            *entry = match_idx + 1;
            self.match_index.insert(peer, match_idx);
            self.try_advance_commit();
        } else {
            // Log inconsistency: back up next_index and let the next
            // heartbeat retry with more entries.
            let entry = self.next_index.entry(peer.clone()).or_insert(1);
            if *entry > 1 {
                *entry -= 1;
            }
        }
    }

    fn try_advance_commit(&mut self) {
        // Find the highest N such that:
        //   N > commitIndex, log[N].term == currentTerm,
        //   and a majority of matchIndex[i] >= N.
        let last = self.last_log_index();
        for n in (self.commit_index + 1..=last).rev() {
            if self.log_term_at(n) != self.current_term {
                continue;
            }
            let matching = 1 + self
                .match_index
                .values()
                .filter(|&&m| m >= n)
                .count();
            if matching >= self.quorum() {
                self.commit_index = n;
                debug!("[{}] commitIndex advanced to {}", self.id, n);
                break;
            }
        }
    }

    // ── Handle incoming RPCs (from followers/candidates) ───────────────

    fn handle_request_vote(
        &mut self,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    ) -> RaftRpc {
        if term > self.current_term {
            self.step_down(term);
        }

        let grant = term >= self.current_term
            && (self.voted_for.is_none()
                || self.voted_for.as_deref() == Some(&candidate_id))
            && (last_log_term > self.last_log_term()
                || (last_log_term == self.last_log_term()
                    && last_log_index >= self.last_log_index()));

        if grant {
            self.voted_for = Some(candidate_id.clone());
            self.reset_election_timeout();
            info!("[{}] voted for {} in term {}", self.id, candidate_id, term);
        }

        RaftRpc::RequestVoteResp { term: self.current_term, vote_granted: grant }
    }

    fn handle_append_entries(
        &mut self,
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> RaftRpc {
        let reject = |this: &Self| RaftRpc::AppendEntriesResp {
            term: this.current_term,
            success: false,
            match_index: 0,
        };

        if term < self.current_term {
            return reject(self);
        }
        // Valid leader heard — step down if needed, reset timer.
        self.step_down(term);
        self.set_leader(leader_id.clone());

        // Check prev-log consistency.
        if prev_log_index > 0 {
            if prev_log_index as usize >= self.log.len()
                || self.log[prev_log_index as usize].term != prev_log_term
            {
                return reject(self);
            }
        }

        // Append new entries (overwriting any conflicting suffix).
        if !entries.is_empty() {
            let insert_at = prev_log_index as usize + 1;
            // Truncate conflicting entries.
            if insert_at < self.log.len() {
                for (offset, entry) in entries.iter().enumerate() {
                    let idx = insert_at + offset;
                    if idx < self.log.len() {
                        if self.log[idx].term != entry.term {
                            self.log.truncate(idx);
                            break;
                        }
                    }
                }
            }
            for (offset, entry) in entries.into_iter().enumerate() {
                let idx = insert_at + offset;
                if idx >= self.log.len() {
                    self.log.push(entry);
                }
            }
        }

        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(self.last_log_index());
        }

        RaftRpc::AppendEntriesResp {
            term: self.current_term,
            success: true,
            match_index: self.last_log_index(),
        }
    }

    // ── Client proposal ────────────────────────────────────────────────

    fn append_proposal(&mut self, proposal: Proposal) {
        let index = self.log.len() as u64;
        self.log.push(LogEntry { term: self.current_term, command: proposal.command });
        self.pending.insert(index, proposal.reply);
        // Eagerly replicate.
        for peer in &self.peers.clone() {
            self.send_append_entries_to(peer.clone());
        }
    }

    // ── Apply committed entries ────────────────────────────────────────

    async fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = &self.log[self.last_applied as usize];
            let cmd_bytes = entry.command.clone();

            // Parse and apply.
            let response = match parse_frame(&cmd_bytes) {
                Ok(Some((frame, _))) => (self.apply_fn)(frame).await,
                _ => Frame::error("ERR corrupt log entry"),
            };

            // Notify waiting client proposal (only the leader that originally
            // appended the entry will have a pending sender here).
            if let Some(tx) = self.pending.remove(&self.last_applied) {
                let _ = tx.send(response);
            }
        }
    }
}

// ── Main event loop ────────────────────────────────────────────────────────

async fn run_node(mut node: RaftNode) {
    loop {
        // How long until next mandatory wakeup?
        let now = Instant::now();
        let election_wait = if node.role == Role::Leader {
            Duration::from_secs(3600)
        } else {
            node.election_deadline.saturating_duration_since(now)
        };
        let hb_wait = if node.role == Role::Leader {
            let elapsed = now.duration_since(node.last_heartbeat_sent);
            Duration::from_millis(HEARTBEAT_MS).saturating_sub(elapsed)
        } else {
            Duration::from_secs(3600)
        };
        let wait = election_wait.min(hb_wait);

        tokio::select! {
            _ = sleep(wait) => {
                if node.role == Role::Leader {
                    node.send_heartbeats();
                } else if Instant::now() >= node.election_deadline {
                    node.start_election();
                }
            }

            // Incoming RPC from a peer (forwarded by the RPC server).
            Some((rpc, reply_tx)) = node.rpc_rx.recv() => {
                let response = match rpc {
                    RaftRpc::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                        node.handle_request_vote(term, candidate_id, last_log_index, last_log_term)
                    }
                    RaftRpc::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                        node.handle_append_entries(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)
                    }
                    _ => {
                        warn!("[{}] unexpected RPC in node loop: {:?}", node.id, rpc);
                        continue;
                    }
                };
                let _ = reply_tx.send(response);
            }

            // Vote responses from spawned send-tasks.
            Some(resp) = node.vote_resp_rx.recv() => {
                node.handle_vote_response(resp);
            }

            // AppendEntries responses from spawned send-tasks.
            Some((peer, resp)) = node.append_resp_rx.recv() => {
                node.handle_append_response(peer, resp);
            }

            // Client proposal (only accepted when leader).
            Some(proposal) = node.proposal_rx.recv() => {
                if node.role == Role::Leader {
                    node.append_proposal(proposal);
                } else {
                    let leader = node.leader_id_shared.try_read()
                        .ok()
                        .and_then(|g| g.clone())
                        .unwrap_or_default();
                    let _ = proposal.reply.send(Frame::error(format!(
                        "MOVED {leader}"
                    )));
                }
            }
        }

        // Apply any newly committed entries.
        node.apply_committed().await;
    }
}

// ── RPC server (listens on raft_addr) ─────────────────────────────────────

async fn run_rpc_server(
    raft_addr: String,
    rpc_tx: mpsc::Sender<(RaftRpc, oneshot::Sender<RaftRpc>)>,
) {
    let listener = match TcpListener::bind(&raft_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Raft RPC server failed to bind {raft_addr}: {e}");
            return;
        }
    };
    info!("Raft RPC server listening on {raft_addr}");

    loop {
        let Ok((mut socket, _peer)) = listener.accept().await else { continue };
        let rpc_tx = rpc_tx.clone();

        tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(4096);
            loop {
                let n = match socket.read_buf(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                let _ = n;
                match parse_frame(&buf) {
                    Ok(Some((frame, consumed))) => {
                        let _ = buf.split_to(consumed);
                        let Some(rpc) = frame_to_rpc(&frame) else { return };
                        let (reply_tx, reply_rx) = oneshot::channel();
                        if rpc_tx.send((rpc, reply_tx)).await.is_err() {
                            return;
                        }
                        let Ok(resp) = reply_rx.await else { return };
                        let bytes = serialize_frame(&rpc_to_frame(&resp));
                        let _ = socket.write_all(&bytes).await;
                        return; // one RPC per connection
                    }
                    Ok(None) => continue,
                    Err(_) => return,
                }
            }
        });
    }
}

// ── Send a single RPC to a peer (one connection, RPC_TIMEOUT_MS) ───────────

async fn send_rpc_to(peer_addr: &str, rpc: &RaftRpc) -> Option<RaftRpc> {
    let result = tokio::time::timeout(Duration::from_millis(RPC_TIMEOUT_MS), async {
        let mut socket = TcpStream::connect(peer_addr).await?;
        let bytes = serialize_frame(&rpc_to_frame(rpc));
        socket.write_all(&bytes).await?;
        let mut buf = BytesMut::with_capacity(1024);
        loop {
            socket.read_buf(&mut buf).await?;
            if let Ok(Some((frame, _))) = parse_frame(&buf) {
                return Ok::<Frame, anyhow::Error>(frame);
            }
        }
    })
    .await;

    match result {
        Ok(Ok(frame)) => frame_to_rpc(&frame),
        Ok(Err(e)) => {
            debug!("RPC to {peer_addr} failed: {e}");
            None
        }
        Err(_) => {
            debug!("RPC to {peer_addr} timed out");
            None
        }
    }
}

// ── Public constructor ─────────────────────────────────────────────────────

pub async fn start_raft(
    node_id: String,
    peers: Vec<String>,
    raft_bind_addr: String,
    apply_fn: ApplyFn,
) -> Arc<Raft> {
    let (proposal_tx, proposal_rx) = mpsc::channel(256);
    let (rpc_tx, rpc_rx) = mpsc::channel(256);
    let (vote_resp_tx, vote_resp_rx) = mpsc::channel(64);
    let (append_resp_tx, append_resp_rx) = mpsc::channel(256);

    let is_leader = Arc::new(AtomicBool::new(false));
    let leader_id: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));

    let mut node = RaftNode {
        id: node_id.clone(),
        peers,
        current_term: 0,
        voted_for: None,
        log: vec![LogEntry { term: 0, command: Bytes::new() }], // sentinel at index 0
        commit_index: 0,
        last_applied: 0,
        next_index: HashMap::new(),
        match_index: HashMap::new(),
        role: Role::Follower,
        votes_received: 0,
        pending: HashMap::new(),
        apply_fn,
        is_leader_flag: is_leader.clone(),
        leader_id_shared: leader_id.clone(),
        proposal_rx,
        rpc_rx,
        vote_resp_rx,
        append_resp_rx,
        vote_resp_tx,
        append_resp_tx,
        election_deadline: Instant::now(),
        last_heartbeat_sent: Instant::now(),
    };
    node.reset_election_timeout();

    // Start the RPC listener.
    let rpc_tx_server = rpc_tx;
    tokio::spawn(run_rpc_server(raft_bind_addr, rpc_tx_server));

    // Start the state-machine loop.
    tokio::spawn(run_node(node));

    Arc::new(Raft { node_id, is_leader, leader_id, proposal_tx })
}
