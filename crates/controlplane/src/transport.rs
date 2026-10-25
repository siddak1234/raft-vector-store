//! The network between control-plane servers (`docs/PLAN.md` §8, Phase 3).
//!
//! Each server keeps one gRPC stream open to every other server and sends consensus
//! messages on it. A message sent while the stream is down is lost; the algorithms already
//! tolerate lost messages. The transport is also where faults and delay are injected, in
//! shared code (`docs/PLAN.md` §5):
//!
//! - messages to or from chosen servers can be dropped, in one direction or both;
//! - messages that carry log entries can be dropped to chosen servers, as the E2 and E3
//!   procedures require (`docs/RESEARCH.md` amendment A1.5);
//! - every message waits a one-way delay drawn uniformly from `[0.5 d, 1.5 d]`
//!   (`docs/RESEARCH.md` §3). The delay is applied where a message is received, so a
//!   message already sent still arrives if its sender crashes, as on a real network.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use consensus::NodeId;
use rpc::delay::{Jitter, sleep_until};
use rpc::proto::peer_client::PeerClient;
use rpc::proto::peer_server::Peer;
use rpc::proto::{Empty, PeerFrame};
use tokio::sync::mpsc as tokio_mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::driver::Input;

/// The faults and delay a server injects.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Faults {
    /// Messages to these servers are dropped.
    pub drop_to: BTreeSet<u64>,
    /// Messages from these servers are dropped.
    pub drop_from: BTreeSet<u64>,
    /// Messages that carry log entries are dropped to these servers.
    pub drop_entries_to: BTreeSet<u64>,
    /// Mean one-way delay `d` in microseconds for messages this server receives; 0 for
    /// none.
    pub delay_us: u64,
}

/// The sending side: used by the driver thread to send encoded messages.
#[derive(Clone)]
pub struct Outbound {
    peers: Arc<BTreeMap<NodeId, tokio_mpsc::UnboundedSender<Vec<u8>>>>,
    faults: Arc<Mutex<Faults>>,
}

impl Outbound {
    /// The faults this server injects, shared with the API.
    pub fn faults(&self) -> Arc<Mutex<Faults>> {
        Arc::clone(&self.faults)
    }

    /// Sends `bytes`, a message carrying `entries` log entries, to server `to`, unless a
    /// fault drops it.
    pub fn send(&self, to: NodeId, bytes: Vec<u8>, entries: usize) {
        {
            let faults = self.faults.lock().expect("no thread panicked");
            if faults.drop_to.contains(&to.0)
                || (entries > 0 && faults.drop_entries_to.contains(&to.0))
            {
                return;
            }
        }
        if let Some(peer) = self.peers.get(&to) {
            let _ = peer.send(bytes);
        }
    }
}

/// Starts one task per peer on `runtime`. Returns the sending side.
pub fn start(
    runtime: &tokio::runtime::Handle,
    me: NodeId,
    peers: &[(NodeId, String)],
    faults: Arc<Mutex<Faults>>,
) -> Outbound {
    let mut senders = BTreeMap::new();
    for (id, address) in peers {
        if *id == me {
            continue;
        }
        let (tx, rx) = tokio_mpsc::unbounded_channel();
        senders.insert(*id, tx);
        runtime.spawn(peer_task(me, address.clone(), rx));
    }
    Outbound {
        peers: Arc::new(senders),
        faults,
    }
}

/// Keeps a stream open to one peer and sends it every message from `messages`. Messages
/// that arrive while the peer cannot be reached are dropped.
async fn peer_task(
    me: NodeId,
    address: String,
    mut messages: tokio_mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let uri = format!("http://{address}");
    loop {
        let endpoint = match tonic::transport::Endpoint::from_shared(uri.clone()) {
            Ok(endpoint) => endpoint
                .tcp_nodelay(true)
                .connect_timeout(Duration::from_millis(100)),
            Err(_) => return,
        };
        let Ok(channel) = endpoint.connect().await else {
            // Drop whatever was queued for the unreachable peer, then retry.
            while messages.try_recv().is_ok() {}
            tokio::time::sleep(Duration::from_millis(20)).await;
            while messages.try_recv().is_ok() {}
            continue;
        };
        let mut client = PeerClient::new(channel);
        let (frames, stream) = tokio_mpsc::channel::<PeerFrame>(4096);
        let mut call =
            tokio::spawn(async move { client.stream(ReceiverStream::new(stream)).await });
        loop {
            tokio::select! {
                message = messages.recv() => {
                    let Some(message) = message else {
                        call.abort();
                        return;
                    };
                    let frame = PeerFrame { from: me.0, message };
                    if frames.try_send(frame).is_err() && frames.is_closed() {
                        break;
                    }
                }
                _ = &mut call => break,
            }
        }
    }
}

/// The receiving side: the `Peer` gRPC service, which hands messages to the driver after
/// the injected delay.
pub struct PeerService {
    inputs: mpsc::Sender<Input>,
    delayed: mpsc::Sender<(Instant, NodeId, Vec<u8>)>,
    faults: Arc<Mutex<Faults>>,
    jitter: Mutex<Jitter>,
}

impl PeerService {
    /// A service that sends incoming messages to `inputs`. Starts the thread that releases
    /// delayed messages.
    pub fn new(me: NodeId, inputs: mpsc::Sender<Input>, faults: Arc<Mutex<Faults>>) -> Self {
        let (delayed, queue) = mpsc::channel();
        let release = inputs.clone();
        std::thread::Builder::new()
            .name("delay".into())
            .spawn(move || release_delayed(&queue, &release))
            .expect("cannot start the delay thread");
        PeerService {
            inputs,
            delayed,
            faults,
            jitter: Mutex::new(Jitter::new(rpc::now_micros() ^ me.0)),
        }
    }
}

#[tonic::async_trait]
impl Peer for PeerService {
    async fn stream(
        &self,
        request: Request<Streaming<PeerFrame>>,
    ) -> Result<Response<Empty>, Status> {
        let mut frames = request.into_inner();
        while let Some(frame) = frames.message().await? {
            let (dropped, delay_us) = {
                let faults = self.faults.lock().expect("no thread panicked");
                (faults.drop_from.contains(&frame.from), faults.delay_us)
            };
            if dropped {
                continue;
            }
            let from = NodeId(frame.from);
            let sent = if delay_us == 0 {
                self.inputs
                    .send(Input::Message {
                        from,
                        bytes: frame.message,
                    })
                    .is_ok()
            } else {
                let delay = self
                    .jitter
                    .lock()
                    .expect("no thread panicked")
                    .delay(delay_us);
                self.delayed
                    .send((Instant::now() + delay, from, frame.message))
                    .is_ok()
            };
            if !sent {
                break;
            }
        }
        Ok(Response::new(Empty {}))
    }
}

/// Holds delayed messages until their time, then hands them to the driver.
fn release_delayed(queue: &mpsc::Receiver<(Instant, NodeId, Vec<u8>)>, out: &mpsc::Sender<Input>) {
    const SPIN: Duration = Duration::from_micros(300);
    let mut held = Held::default();
    loop {
        while let Ok(item) = queue.try_recv() {
            held.hold(item);
        }
        let Some(&Reverse((at, id, from))) = held.heap.peek() else {
            match queue.recv() {
                Ok(item) => held.hold(item),
                Err(_) => return,
            }
            continue;
        };
        let wait = at.saturating_duration_since(Instant::now());
        if wait > SPIN {
            match queue.recv_timeout(wait - SPIN) {
                Ok(item) => held.hold(item),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
            continue;
        }
        held.heap.pop();
        sleep_until(at);
        if let Some(bytes) = held.messages.remove(&id)
            && out.send(Input::Message { from, bytes }).is_err()
        {
            return;
        }
    }
}

/// Delayed messages, ordered by release time.
#[derive(Default)]
struct Held {
    heap: BinaryHeap<Reverse<(Instant, u64, NodeId)>>,
    messages: BTreeMap<u64, Vec<u8>>,
    seq: u64,
}

impl Held {
    fn hold(&mut self, (at, from, bytes): (Instant, NodeId, Vec<u8>)) {
        self.seq += 1;
        self.heap.push(Reverse((at, self.seq, from)));
        self.messages.insert(self.seq, bytes);
    }
}
