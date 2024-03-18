use log::{debug, error, info, warn};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use std::{str, time::Duration};
use tentacle::{
    async_trait,
    builder::{MetaBuilder, ServiceBuilder},
    context::{ProtocolContext, ProtocolContextMutRef, ServiceContext},
    service::{
        ProtocolHandle, ProtocolMeta, ServiceAsyncControl, ServiceError, ServiceEvent,
        TargetProtocol, TargetSession,
    },
    traits::{ServiceHandle, ServiceProtocol},
    ProtocolId, SessionId,
};
use tentacle::{bytes::Bytes, secio::PeerId};
use tokio::select;
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use super::Command;
use crate::CkbConfig;

const PCN_PROTOCOL_ID: ProtocolId = ProtocolId::new(42);
const PCN_TARGET_PROTOCOL: TargetProtocol = TargetProtocol::Single(PCN_PROTOCOL_ID);

#[derive(Default)]
struct PHandle {
    peer_state: PeerState,
}

impl PHandle {
    fn new(state: PeerState) -> Self {
        Self { peer_state: state }
    }

    fn create_meta(self, id: ProtocolId) -> ProtocolMeta {
        MetaBuilder::new()
            .id(id)
            .service_handle(move || {
                let handle = Box::new(self);
                ProtocolHandle::Callback(handle)
            })
            .build()
    }
}

#[async_trait]
impl ServiceProtocol for PHandle {
    async fn init(&mut self, _context: &mut ProtocolContext) {}

    async fn connected(&mut self, context: ProtocolContextMutRef<'_>, version: &str) {
        let session = context.session;
        info!(
            "proto id [{}] open on session [{}], address: [{}], type: [{:?}], version: {}",
            context.proto_id, session.id, session.address, session.ty, version
        );

        let peer_id = context.session.remote_pubkey.clone().map(Into::into);
        match peer_id {
            Some(peer_id) => {
                let mut peer_state = self.peer_state.lock().unwrap();
                debug!("Trying to save session of peer {:?}", peer_id);
                let peer = peer_state.entry(peer_id).or_default();
                peer.sessions.insert(context.session.id);
            }
            _ => {
                warn!("Connected to a peer without public key");
                return;
            }
        }
    }

    async fn disconnected(&mut self, context: ProtocolContextMutRef<'_>) {
        info!(
            "proto id [{}] close on session [{}]",
            context.proto_id, context.session.id
        );
        let peer_id = context.session.remote_pubkey.clone().map(Into::into);
        match peer_id.as_ref() {
            Some(peer_id) => {
                let mut peer_state = self.peer_state.lock().unwrap();
                debug!("Trying to save session of peer {:?}", peer_id);
                let peer = peer_state.get_mut(peer_id);
                match peer {
                    Some(peer) => {
                        peer.sessions.remove(&context.session.id);
                        if peer.sessions.is_empty() {
                            debug!(
                                "Peer {:?} disconnected, the last session was {}",
                                peer_id, context.session.id
                            );
                            peer_state.remove(peer_id);
                        }
                    }
                    None => {
                        warn!(
                            "Trying to reomve a peer not recorded in peer state {:?}",
                            peer_id
                        )
                    }
                }
            }
            _ => {
                warn!("Disconnected from a peer without public key");
                return;
            }
        }
    }

    async fn received(&mut self, context: ProtocolContextMutRef<'_>, data: Bytes) {
        info!(
            "received from [{}]: proto [{}] data {:?}",
            context.session.id,
            context.proto_id,
            str::from_utf8(data.as_ref()).unwrap(),
        );
    }

    async fn notify(&mut self, _context: &mut ProtocolContext, _token: u64) {}
}

pub struct SHandle;

#[async_trait]
impl ServiceHandle for SHandle {
    // A lot of internal error events will be output here, but not all errors need to close the service,
    // some just tell users that they need to pay attention
    async fn handle_error(&mut self, _context: &mut ServiceContext, error: ServiceError) {
        info!("service error: {:?}", error);
    }
    async fn handle_event(&mut self, context: &mut ServiceContext, event: ServiceEvent) {
        info!("service event: {:?}", event);
        if let ServiceEvent::SessionOpen { .. } = event {
            let delay_sender = context.control().clone();

            let _ = context
                .future_task(async move {
                    tokio::time::sleep_until(tokio::time::Instant::now() + Duration::from_secs(3))
                        .await;
                    let _ = delay_sender
                        .filter_broadcast(
                            TargetSession::All,
                            0.into(),
                            Bytes::from("I am a delayed message"),
                        )
                        .await;
                })
                .await;
        }
    }
}

type PeerState = Arc<Mutex<HashMap<PeerId, PeerInfo>>>;

fn new_peer_state() -> PeerState {
    Arc::new(Mutex::new(HashMap::new()))
}

struct NetworkState {
    control: ServiceAsyncControl,
    peer_state: PeerState,
    token: CancellationToken,
    command_receiver: mpsc::Receiver<Command>,
}

impl NetworkState {
    fn new(
        control: ServiceAsyncControl,
        peer_state: PeerState,
        token: CancellationToken,
        command_receiver: mpsc::Receiver<Command>,
    ) -> Self {
        Self {
            control,
            peer_state,
            token,
            command_receiver,
        }
    }

    async fn process_command(&self, command: Command) {
        debug!("Processing command {:?}", command);
        match command {
            Command::Connect(addr) => {
                // TODO: It is more than just dialing a peer. We need to exchange capabilities of the peer,
                // e.g. whether the peer support some specific feature.
                // TODO: If we are already connected to the peer, skip connecting.
                debug!("Dialing {}", &addr);
                let result = self.control.dial(addr.clone(), PCN_TARGET_PROTOCOL).await;
                if let Err(err) = result {
                    error!("Dialing {} failed: {}", &addr, err);
                }
            }
        }
    }

    async fn run(mut self) {
        loop {
            select! {
                _ = self.token.cancelled() => {
                    debug!("Cancellation received, shutting down tentacle service");
                    let _ = self.control.shutdown().await;
                    break;
                }
                command = self.command_receiver.recv() => {
                    match command {
                        None => {
                            debug!("Command receiver completed, shutting down tentacle service");
                            let _ = self.control.shutdown().await;
                            break;
                        }
                        Some(command) => {
                            self.process_command(command).await;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct PeerInfo {
    sessions: HashSet<SessionId>,
}

pub async fn start_ckb(
    config: CkbConfig,
    command_receiver: mpsc::Receiver<Command>,
    token: CancellationToken,
    tracker: TaskTracker,
) {
    let kp = config
        .read_or_generate_secret_key()
        .expect("read or generate secret key");
    let pk = kp.public_key();
    let peer_state = new_peer_state();
    let mut service = ServiceBuilder::default()
        .insert_protocol(PHandle::new(peer_state.clone()).create_meta(PCN_PROTOCOL_ID))
        .key_pair(kp)
        .build(SHandle);
    let listen_addr = service
        .listen(
            format!("/ip4/127.0.0.1/tcp/{}", config.listening_port)
                .parse()
                .expect("valid tentacle address"),
        )
        .await
        .expect("listen tentacle");

    info!(
        "Started listening tentacle on {}/p2p/{}",
        listen_addr,
        PeerId::from(pk).to_base58()
    );

    let control = service.control().to_owned();

    tracker.spawn(async move {
        service.run().await;
        debug!("Tentacle service shutdown");
    });

    tracker.spawn(async move {
        NetworkState::new(control, peer_state, token, command_receiver)
            .run()
            .await;
    });
}
