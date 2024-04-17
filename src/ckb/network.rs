use lightning::ln::channel_keys;
use log::{debug, error, info, warn};
use ractor::{async_trait as rasync_trait, Actor, ActorCell, ActorProcessingErr, ActorRef};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr, FromInto};
use std::{collections::HashMap, str};
use tentacle::context::SessionContext;
use tentacle::{multiaddr::Multiaddr, secio::PeerId, SessionId};

use tentacle::{
    async_trait,
    builder::{MetaBuilder, ServiceBuilder},
    bytes::Bytes,
    context::{ProtocolContext, ProtocolContextMutRef, ServiceContext},
    service::{
        ProtocolHandle, ProtocolMeta, ServiceAsyncControl, ServiceError, ServiceEvent,
        TargetProtocol,
    },
    traits::{ServiceHandle, ServiceProtocol},
    ProtocolId,
};

use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;

use super::channel::{ChannelState, NegotiatingFundingFlags, ProcessingChannelError};
use super::{
    channel::ChannelCommand,
    channel::{ChannelActor, ChannelInitializationParameter},
    peer::get_peer_actor_name,
    peer::PeerActor,
    peer::PeerActorMessage,
    types::PCNMessage,
    CkbConfig,
};

use crate::ckb::channel;
use crate::{unwrap_or_return, Error};

pub const PCN_PROTOCOL_ID: ProtocolId = ProtocolId::new(42);

#[serde_as]
#[derive(Debug, Deserialize)]
pub enum NetworkActorCommand {
    /// Network commands
    ConnectPeer(Multiaddr),
    // For internal use and debugging only. Most of the messages requires some
    // changes to local state. Even if we can send a message to a peer, some
    // part of the local state is not changed.
    SendPcnMessage(PCNMessageWithPeerId),
    // Directly send a message to session
    SendPcnMessageToSession(PCNMessageWithSessionId),
    ControlPcnChannel(ChannelCommand),
}

impl NetworkActorMessage {
    pub fn new_event(event: NetworkActorEvent) -> Self {
        Self::Event(event)
    }

    pub fn new_command(command: NetworkActorCommand) -> Self {
        Self::Command(command, None)
    }
}

#[derive(Debug)]
pub enum NetworkServiceEvent {
    ServiceError(ServiceError),
    ServiceEvent(ServiceEvent),
    PeerConnected(Multiaddr),
    PeerDisConnected(Multiaddr),
}

#[derive(Debug)]
pub enum NetworkActorEvent {
    /// Network eventss to be processed by this actor.
    PeerConnected(PeerId, SessionContext),
    PeerDisconnected(PeerId, SessionContext),
    PeerMessage(PeerId, SessionContext, PCNMessage),

    /// Network service events to be sent to outside observers.
    NetworkServiceEvent(NetworkServiceEvent),
}

#[derive(Debug)]
pub enum NetworkActorMessage {
    Command(
        NetworkActorCommand,
        // TODO: we may need to refine the following type according to each commands.
        Option<mpsc::Sender<crate::Result<()>>>,
    ),
    Event(NetworkActorEvent),
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct PCNMessageWithPeerId {
    #[serde_as(as = "DisplayFromStr")]
    pub peer_id: PeerId,
    pub message: PCNMessage,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct PCNMessageWithSessionId {
    #[serde_as(as = "FromInto<usize>")]
    pub session_id: SessionId,
    pub message: PCNMessage,
}

pub struct NetworkActor {
    // An event emitter to notify ourside observers.
    event_sender: mpsc::Sender<NetworkServiceEvent>,
}

impl NetworkActor {
    pub async fn emit_event(&self, event: NetworkServiceEvent) {
        let _ = self.event_sender.send(event).await;
    }

    pub async fn handle_command(
        &self,
        myself: ActorRef<NetworkActorMessage>,
        state: &mut NetworkActorState,
        command: NetworkActorCommand,
    ) -> crate::Result<()> {
        match command {
            NetworkActorCommand::SendPcnMessageToSession(PCNMessageWithSessionId {
                session_id,
                message,
            }) => {
                debug!(
                    "SendPcnMessageToSession command received: Sending message to session {:?}",
                    session_id
                );
                state
                    .control
                    .send_message_to(session_id, PCN_PROTOCOL_ID, message.to_molecule_bytes())
                    .await?;
            }

            NetworkActorCommand::SendPcnMessage(PCNMessageWithPeerId { peer_id, message }) => {
                debug!(
                    "SendPcnMessage command received: sending message to peer {:?}",
                    &peer_id
                );
                match state.peers.get(&peer_id) {
                    Some(actor) => {
                        actor
                            .send_message(PeerActorMessage::SendMessage(message))
                            .expect("peer actor alive");
                    }
                    None => {
                        error!("Trying to send message to a not found peer {:?}", &peer_id);
                        return Err(Error::PeerNotFound(peer_id));
                    }
                }
            }

            NetworkActorCommand::ConnectPeer(addr) => {
                // TODO: It is more than just dialing a peer. We need to exchange capabilities of the peer,
                // e.g. whether the peer support some specific feature.
                // TODO: If we are already connected to the peer, skip connecting.
                debug!("ConnectPeer command received, dialing {}", &addr);
                state
                    .control
                    .dial(addr.clone(), TargetProtocol::All)
                    .await?
                // TODO: note that the dial function does not return error immediately even if dial fails.
                // Tentacle sends an event by calling handle_error function instead, which
                // may receive errors like DialerError.
            }

            NetworkActorCommand::ControlPcnChannel(c) => match c {
                ChannelCommand::OpenChannel(open_channel) => {
                    debug!("OpenChannel command received: {:?}", &open_channel);
                    let peer_actor = state.peers.get(&open_channel.peer_id).cloned();
                    match peer_actor {
                        None => {
                            warn!(
                                "Trying to control a not found peer {:?}",
                                &open_channel.peer_id
                            );
                            return Err(Error::PeerNotFound(open_channel.peer_id));
                        }
                        Some(peer_actor) => {
                            Actor::spawn_linked(
                                Some("channel".to_string()),
                                ChannelActor::new(myself.clone(), peer_actor),
                                ChannelInitializationParameter::OpenChannelCommand(open_channel),
                                myself.clone().get_cell(),
                            )
                            .await?;
                        }
                    }
                }
                ChannelCommand::TxAdd(tx) => {
                     match self.state {
                        ChannelState::NegotiatingFunding(NegotiatingFundingFlags::INIT_SENT) => {
                            // Only the initator should start sending tx_add messages.
                            let was_initially_inbound = false;
                            if was_initially_inbound {
                                return Err(ProcessingChannelError::InvalidState(format!(
                                    "Try to send a TxAdd message, but we are the acceptor of this channel"),
                                ).into());
                            } else {
                                let message = PCNMessage::TxAdd(TxAdd {
                                    channel_id: self.id(),
                                    tx: tx.clone(),
                                });
                                // TODO: send message to peer.
                            }
                        }
                    }
                },
                ChannelCommand::TxRemove(_) => todo!(),
                ChannelCommand::TxComplete(_) => todo!(),
            },
        };
        Ok(())
    }
}

pub struct NetworkActorState {
    peer_id: PeerId,
    // This immutable attribute is placed here because we need to create it in
    // the pre_start function.
    control: ServiceAsyncControl,
    peers: HashMap<PeerId, ActorRef<PeerActorMessage>>,
}

impl NetworkActorState {
    /// Get or create a peer actor.
    pub async fn get_or_create_peer(
        &mut self,
        id: PeerId,
        control: &ActorRef<NetworkActorMessage>,
    ) -> ActorRef<PeerActorMessage> {
        match self.peers.get(&id) {
            Some(actor) => actor.clone(),
            None => {
                let peer_name = get_peer_actor_name(&id);
                let actor = Actor::spawn_linked(
                    Some(peer_name),
                    PeerActor::new(Some(id.clone()), control.clone()),
                    (),
                    control.get_cell(),
                )
                .await
                .expect("spawn peer actor")
                .0;
                self.peers.insert(id, actor.clone());
                actor
            }
        }
    }
}

#[rasync_trait]
impl Actor for NetworkActor {
    type Msg = NetworkActorMessage;
    type State = NetworkActorState;
    type Arguments = (CkbConfig, TaskTracker);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        (config, tracker): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let kp = config
            .read_or_generate_secret_key()
            .expect("read or generate secret key");
        let pk = kp.public_key();
        let handle = Handle::new(myself.clone());
        let mut service = ServiceBuilder::default()
            .insert_protocol(handle.clone().create_meta(PCN_PROTOCOL_ID))
            .key_pair(kp)
            .build(handle);
        let listen_addr = service
            .listen(
                format!("/ip4/127.0.0.1/tcp/{}", config.listening_port)
                    .parse()
                    .expect("valid tentacle address"),
            )
            .await
            .expect("listen tentacle");

        let my_peer_id: PeerId = PeerId::from(pk);
        info!(
            "Started listening tentacle on {}/p2p/{}",
            listen_addr,
            my_peer_id.to_base58()
        );

        let control = service.control().to_owned();

        tracker.spawn(async move {
            service.run().await;
            debug!("Tentacle service shutdown");
        });

        Ok(NetworkActorState {
            peer_id: my_peer_id,
            peers: HashMap::new(),
            control,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        debug!("Network actor processing message {:?}", message);

        match message {
            NetworkActorMessage::Event(event) => match event {
                NetworkActorEvent::NetworkServiceEvent(e) => {
                    self.emit_event(e).await;
                }

                NetworkActorEvent::PeerConnected(id, session) => match state.peers.get(&id) {
                    Some(_actor) => {
                        warn!("Duplicated peer connected event. Are we connecting here? The connection reestablishment processing is not implemented");
                    }
                    None => {
                        let peer_name = get_peer_actor_name(&id);
                        let actor = Actor::spawn_linked(
                            Some(peer_name),
                            PeerActor::new(Some(id.clone()), myself.clone()),
                            (),
                            myself.get_cell(),
                        )
                        .await
                        .expect("spawn peer actor")
                        .0;

                        self.emit_event(NetworkServiceEvent::PeerConnected(
                            session.address.clone(),
                        ))
                        .await;

                        actor
                            .send_message(PeerActorMessage::Connected(session))
                            .expect("peer actor alive");
                        state.peers.insert(id, actor.clone());
                    }
                },
                NetworkActorEvent::PeerDisconnected(id, session) => match state.peers.remove(&id) {
                    Some(actor) => {
                        debug!("Removed actor for peer {:?} from network actor", id);
                        self.emit_event(NetworkServiceEvent::PeerDisConnected(
                            session.address.clone(),
                        ))
                        .await;
                        actor
                            .send_message(PeerActorMessage::Disconnected(session))
                            .expect("peer actor alive");
                    }
                    None => {
                        warn!("Trying to remove a not found peer {:?}", &id);
                    }
                },
                NetworkActorEvent::PeerMessage(id, session, message) => {
                    match state.peers.get(&id) {
                        Some(actor) => {
                            actor
                                .send_message(PeerActorMessage::ReceivedMessage(session, message))
                                .expect("peer actor alive");
                        }
                        None => {
                            warn!("Received message for a not found peer {:?}", &id);
                        }
                    }
                }
            },
            NetworkActorMessage::Command(command, sender) => {
                let result = self.handle_command(myself, state, command).await;
                if let Some(sender) = sender {
                    sender.send(result).await.expect("receiver not closed");
                }
            }
        }
        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        if let Err(err) = state.control.close().await {
            error!("Failed to close tentacle service: {}", err);
        }
        debug!("Network service shutdown");
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Handle {
    actor: ActorRef<NetworkActorMessage>,
}

impl Handle {
    pub fn new(actor: ActorRef<NetworkActorMessage>) -> Self {
        Self { actor }
    }

    fn send_actor_message(&self, message: NetworkActorMessage) {
        // If we are closing the whole network service, we may have already stopped the network actor.
        // In that case the send_message will fail.
        // Ideally, we should close tentacle network service first, then stop the network actor.
        // But ractor provides only api for `post_stop` instead of `pre_stop`.
        let _ = self.actor.send_message(message);
    }

    fn emit_event(&self, event: NetworkServiceEvent) {
        self.send_actor_message(NetworkActorMessage::Event(
            NetworkActorEvent::NetworkServiceEvent(event),
        ));
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
impl ServiceProtocol for Handle {
    async fn init(&mut self, _context: &mut ProtocolContext) {}

    async fn connected(&mut self, context: ProtocolContextMutRef<'_>, version: &str) {
        let session = context.session;
        info!(
            "proto id [{}] open on session [{}], address: [{}], type: [{:?}], version: {}",
            context.proto_id, session.id, session.address, session.ty, version
        );

        if let Some(peer_id) = context.session.remote_pubkey.clone().map(PeerId::from) {
            self.send_actor_message(NetworkActorMessage::new_event(
                NetworkActorEvent::PeerConnected(peer_id, context.session.clone()),
            ));
        } else {
            warn!("Peer connected without remote pubkey {:?}", context.session);
        }
    }

    async fn disconnected(&mut self, context: ProtocolContextMutRef<'_>) {
        info!(
            "proto id [{}] close on session [{}]",
            context.proto_id, context.session.id
        );

        if let Some(peer_id) = context.session.remote_pubkey.clone().map(PeerId::from) {
            self.send_actor_message(NetworkActorMessage::new_event(
                NetworkActorEvent::PeerDisconnected(peer_id, context.session.clone()),
            ));
        } else {
            warn!(
                "Peer disconnected without remote pubkey {:?}",
                context.session
            );
        }
    }

    async fn received(&mut self, context: ProtocolContextMutRef<'_>, data: Bytes) {
        info!(
            "received from [{}]: proto [{}] data {:?}",
            context.session.id,
            context.proto_id,
            hex::encode(data.as_ref()),
        );

        let msg = unwrap_or_return!(PCNMessage::from_molecule_slice(&data), "parse message");
        if let Some(peer_id) = context.session.remote_pubkey.clone().map(PeerId::from) {
            self.send_actor_message(NetworkActorMessage::new_event(
                NetworkActorEvent::PeerMessage(peer_id, context.session.clone(), msg),
            ));
        } else {
            warn!(
                "Received message from a peer without remote pubkey {:?}",
                context.session
            );
        }
    }

    async fn notify(&mut self, _context: &mut ProtocolContext, _token: u64) {}
}

#[async_trait]
impl ServiceHandle for Handle {
    async fn handle_error(&mut self, _context: &mut ServiceContext, error: ServiceError) {
        self.emit_event(NetworkServiceEvent::ServiceError(error));
    }
    async fn handle_event(&mut self, _context: &mut ServiceContext, event: ServiceEvent) {
        self.emit_event(NetworkServiceEvent::ServiceEvent(event));
    }
}

pub async fn start_ckb(
    config: CkbConfig,
    event_sender: mpsc::Sender<NetworkServiceEvent>,
    tracker: TaskTracker,
    root_actor: ActorCell,
) -> ActorRef<NetworkActorMessage> {
    let (actor, _handle) = Actor::spawn_linked(
        Some("network actor".to_string()),
        NetworkActor { event_sender },
        (config, tracker),
        root_actor,
    )
    .await
    .expect("Failed to start network actor");

    actor
}
