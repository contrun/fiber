use bitflags::bitflags;
use ckb_hash::{blake2b_256, new_blake2b};
use ckb_sdk::Since;
use ckb_types::{
    core::{DepType, TransactionBuilder, TransactionView},
    packed::{
        Byte32, Bytes, CellDep, CellDepVec, CellInput, CellOutput, OutPoint, Script, Transaction,
    },
    prelude::{AsTransactionBuilder, IntoTransactionView, Pack, PackVec},
};

use log::{debug, error, info, warn};
use molecule::{
    bytes,
    prelude::{Builder, Entity},
};
use musig2::{
    aggregate_partial_signatures,
    errors::{SigningError, VerifyError},
    sign_partial, verify_partial, AggNonce, BinaryEncoding, CompactSignature, KeyAggContext,
    PartialSignature, PubNonce, SecNonce,
};
use ractor::{async_trait as rasync_trait, Actor, ActorProcessingErr, ActorRef, SpawnErr};

use serde::Deserialize;
use serde_with::serde_as;
use tentacle::secio::PeerId;
use thiserror::Error;
use tokio::sync::mpsc::error::TrySendError;

use std::{
    borrow::Borrow,
    collections::{hash_map, HashMap},
    fmt::Debug,
};

use crate::ckb::{
    temp::{get_commitment_lock_context, CommitmentLockContext},
    types::Shutdown,
};

use super::{
    key::blake2b_hash_with_salt,
    network::{OpenChannelCommand, PCNMessageWithPeerId},
    serde_utils::EntityWrapperHex,
    temp::get_always_success_script,
    types::{
        AcceptChannel, AddTlc, ChannelReady, ClosingSigned, CommitmentSigned, Hash256, LockTime,
        OpenChannel, PCNMessage, Privkey, Pubkey, RemoveTlc, RemoveTlcReason, RevokeAndAck,
        TxCollaborationMsg, TxComplete, TxSignatures, TxUpdate,
    },
    NetworkActorCommand, NetworkActorEvent, NetworkActorMessage,
};

pub enum ChannelActorMessage {
    /// Command are the messages that are sent to the channel actor to perform some action.
    /// It is normally generated from a user request.
    Command(ChannelCommand),
    /// Some system events associated to a channel, such as the funding transaction confirmed.
    Event(ChannelEvent),
    /// PeerMessage are the messages sent from the peer.
    PeerMessage(PCNMessage),
}

#[derive(Clone, Debug, Deserialize)]
pub enum ChannelCommand {
    TxCollaborationCommand(TxCollaborationCommand),
    // TODO: maybe we should automatically send commitment_signed message after receiving
    // tx_complete event.
    CommitmentSigned(),
    AddTlc(AddTlcCommand),
    RemoveTlc(RemoveTlcCommand),
    Shutdown(ShutdownCommand),
}

#[derive(Clone, Debug, Deserialize)]
pub enum TxCollaborationCommand {
    TxUpdate(TxUpdateCommand),
    TxComplete(TxCompleteCommand),
}

#[derive(Copy, Clone, Debug, Deserialize)]
pub struct AddTlcCommand {
    amount: u128,
    preimage: Option<Hash256>,
    expiry: LockTime,
}

#[derive(Copy, Clone, Debug, Deserialize)]
pub struct RemoveTlcCommand {
    id: u64,
    reason: RemoveTlcReason,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct ShutdownCommand {
    #[serde_as(as = "EntityWrapperHex<Script>")]
    close_script: Script,
}

fn get_random_preimage() -> Hash256 {
    let mut preimage = [0u8; 32];
    preimage.copy_from_slice(&rand::random::<[u8; 32]>());
    preimage.into()
}

#[derive(Clone, Debug, Deserialize)]
pub struct ChannelCommandWithId {
    pub channel_id: Hash256,
    pub command: ChannelCommand,
}

pub const DEFAULT_FEE_RATE: u64 = 0;
pub const DEFAULT_COMMITMENT_FEE_RATE: u64 = 0;
pub const DEFAULT_MAX_TLC_VALUE_IN_FLIGHT: u128 = u128::MAX;
pub const DEFAULT_MAX_ACCEPT_TLCS: u64 = u64::MAX;
pub const DEFAULT_MIN_TLC_VALUE: u128 = 0;
pub const DEFAULT_TO_SELF_DELAY_BLOCKS: u64 = 10;

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct TxUpdateCommand {
    #[serde_as(as = "EntityWrapperHex<Transaction>")]
    pub transaction: Transaction,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TxCompleteCommand {}

impl OpenChannelCommand {
    pub fn create_channel(&self) -> Result<ChannelActorState, ProcessingChannelError> {
        // Use a deterministic RNG for now to facilitate development.
        let seed = 42u64.to_le_bytes();

        Ok(ChannelActorState::new_outbound_channel(
            &seed,
            self.peer_id.clone(),
            self.funding_amount,
            LockTime::new(DEFAULT_TO_SELF_DELAY_BLOCKS),
        ))
    }
}

pub enum ChannelInitializationParameter {
    /// To open a new channel to another peer, the funding amount and
    /// a unique channel seed to generate unique channel id,
    /// must be given.
    OpenChannel(u128, [u8; 32]),
    /// To accept a new channel from another peer, the funding amount,
    /// a unique channel seed to generate unique channel id,
    /// and original OpenChannel message must be given.
    AcceptChannel(u128, [u8; 32], OpenChannel),
}

#[derive(Debug)]
pub struct ChannelActor {
    peer_id: PeerId,
    network: ActorRef<NetworkActorMessage>,
}

impl ChannelActor {
    pub fn new(peer_id: PeerId, network: ActorRef<NetworkActorMessage>) -> Self {
        Self { peer_id, network }
    }

    pub fn send_tx_collaboration_command(
        &self,
        state: &mut ChannelActorState,
        command: TxCollaborationCommand,
    ) -> Result<(), ProcessingChannelError> {
        let pcn_msg = match command {
            TxCollaborationCommand::TxUpdate(tx_update) => PCNMessage::TxUpdate(TxUpdate {
                channel_id: state.get_id(),
                tx: tx_update.transaction.clone(),
            }),
            TxCollaborationCommand::TxComplete(_) => PCNMessage::TxComplete(TxComplete {
                channel_id: state.get_id(),
            }),
        };
        self.network
            .send_message(NetworkActorMessage::new_command(
                NetworkActorCommand::SendPcnMessage(PCNMessageWithPeerId::new(
                    self.peer_id.clone(),
                    pcn_msg,
                )),
            ))
            .expect("network actor alive");
        Ok(())
    }

    pub fn handle_commitment_signed_command(
        &self,
        state: &mut ChannelActorState,
    ) -> ProcessingChannelResult {
        let flags = match state.state {
            ChannelState::CollaboratingFundingTx(flags)
                if !flags.contains(CollaboratingFundingTxFlags::COLLABRATION_COMPLETED) =>
            {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Unable to process commitment_signed command in state {:?}, as collaboration is not completed yet.",
                    &state.state
                )));
            }
            ChannelState::CollaboratingFundingTx(_) => {
                debug!(
                    "Processing commitment_signed command in from CollaboratingFundingTx state {:?}",
                    &state.state
                );
                CommitmentSignedFlags::SigningCommitment(SigningCommitmentFlags::empty())
            }
            ChannelState::SigningCommitment(flags)
                if flags.contains(SigningCommitmentFlags::OUR_COMMITMENT_SIGNED_SENT) =>
            {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Unable to process commitment_signed command in state {:?}, as we have already sent our commitment_signed message.",
                    &state.state
                )));
            }
            ChannelState::SigningCommitment(flags) => {
                debug!(
                    "Processing commitment_signed command in from SigningCommitment state {:?}",
                    &state.state
                );
                CommitmentSignedFlags::SigningCommitment(flags)
            }
            ChannelState::ChannelReady(flags)
                if flags.contains(ChannelReadyFlags::AWAITING_REMOTE_REVOKE) =>
            {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Unable to process commitment_signed command in state {:?}, which requires the remote to send a revoke message first.",
                    &state.state
                )));
            }
            ChannelState::ChannelReady(flags) => CommitmentSignedFlags::ChannelReady(flags),
            _ => {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Unable to send commitment signed message in state {:?}",
                    &state.state
                )));
            }
        };

        let PartiallySignedCommitmentTransaction { tx, signature } =
            state.build_and_sign_commitment_tx()?;
        debug!(
            "Build a funding tx ({:?}) with partial signature {:?}",
            &tx, &signature
        );

        let commitment_signed = CommitmentSigned {
            channel_id: state.get_id(),
            partial_signature: signature,
            next_local_nonce: state.get_next_holder_nonce(),
        };
        debug!(
            "Sending built commitment_signed message: {:?}",
            &commitment_signed
        );
        self.network
            .send_message(NetworkActorMessage::new_command(
                NetworkActorCommand::SendPcnMessage(PCNMessageWithPeerId {
                    peer_id: state.peer_id.clone(),
                    message: PCNMessage::CommitmentSigned(commitment_signed),
                }),
            ))
            .expect("network actor alive");

        state.holder_commitment_number = state.get_next_commitment_number(true);
        match flags {
            CommitmentSignedFlags::SigningCommitment(flags) => {
                let flags = flags | SigningCommitmentFlags::OUR_COMMITMENT_SIGNED_SENT;
                state.state = ChannelState::SigningCommitment(flags);
                state.maybe_transition_to_tx_signatures(flags, self.network.clone())?;
            }
            CommitmentSignedFlags::ChannelReady(flags) => {
                let flags = flags | ChannelReadyFlags::AWAITING_REMOTE_REVOKE;
                state.state = ChannelState::ChannelReady(flags);
            }
        }
        Ok(())
    }

    pub fn handle_add_tlc_command(
        &self,
        state: &mut ChannelActorState,
        command: AddTlcCommand,
    ) -> ProcessingChannelResult {
        state.check_state_for_tlc_update()?;

        let tlc = state.create_outbounding_tlc(command);

        // TODO: Note that since message sending is async,
        // we can't guarantee anything about the order of message sending
        // and state updating. And any of these may fail while the other succeeds.
        // We may need to handle all these possibilities.
        // To make things worse, we currently don't have a way to ACK all the messages.

        // Send tlc update message to peer.
        let msg = PCNMessageWithPeerId {
            peer_id: self.peer_id.clone(),
            message: PCNMessage::AddTlc(AddTlc {
                channel_id: state.get_id(),
                tlc_id: tlc.id,
                amount: tlc.amount,
                payment_hash: tlc.payment_hash,
                expiry: tlc.lock_time,
            }),
        };
        self.network
            .send_message(NetworkActorMessage::new_command(
                NetworkActorCommand::SendPcnMessage(msg),
            ))
            .expect("network actor alive");

        // Update the state for this tlc.
        state.pending_offered_tlcs.insert(tlc.id, tlc);
        state.to_self_amount -= tlc.amount;
        state.next_offering_tlc_id += 1;
        debug!(
            "Added tlc with id {:?} to pending offered tlcs: {:?}",
            tlc.id, &tlc
        );
        debug!(
            "Current pending offered tlcs: {:?}",
            &state.pending_offered_tlcs
        );
        debug!(
            "Current pending received tlcs: {:?}",
            &state.pending_received_tlcs
        );
        debug!(
            "Balance after addtlccommand: to_self_amount: {} to_remote_amount: {}",
            state.to_self_amount, state.to_remote_amount
        );
        Ok(())
    }

    pub fn handle_remove_tlc_command(
        &self,
        state: &mut ChannelActorState,
        command: RemoveTlcCommand,
    ) -> ProcessingChannelResult {
        state.check_state_for_tlc_update()?;
        // Notes: state updating and message sending are not atomic.
        match state.pending_received_tlcs.remove(&command.id) {
            Some(tlc) => {
                let msg = PCNMessageWithPeerId {
                    peer_id: self.peer_id.clone(),
                    message: PCNMessage::RemoveTlc(RemoveTlc {
                        channel_id: state.get_id(),
                        tlc_id: tlc.id,
                        reason: command.reason,
                    }),
                };
                if let RemoveTlcReason::RemoveTlcFulfill(fulfill) = command.reason {
                    if tlc.payment_hash != blake2b_256(fulfill.payment_preimage).into() {
                        state.pending_received_tlcs.insert(tlc.id, tlc);
                        return Err(ProcessingChannelError::InvalidParameter(format!(
                            "Preimage {:?} is hashed to {:?}, which does not match payment hash {:?}",
                            fulfill.payment_preimage, blake2b_256(fulfill.payment_preimage), tlc.payment_hash,
                        )));
                    }
                }
                self.network
                    .send_message(NetworkActorMessage::new_command(
                        NetworkActorCommand::SendPcnMessage(msg),
                    ))
                    .expect("network actor alive");
                match command.reason {
                    RemoveTlcReason::RemoveTlcFail(_) => {
                        state.to_remote_amount += tlc.amount;
                    }
                    RemoveTlcReason::RemoveTlcFulfill(_) => {
                        state.to_self_amount += tlc.amount;
                    }
                }
            }
            None => {
                return Err(ProcessingChannelError::InvalidParameter(format!(
                    "Trying to remove tlc with id {:?} that is not in pending received tlcs",
                    command.id
                )));
            }
        }
        debug!(
            "Balance after removetlccommand: to_self_amount: {} to_remote_amount: {}",
            state.to_self_amount, state.to_remote_amount
        );
        state.maybe_transition_to_closing_signed(self.network.clone())?;

        Ok(())
    }

    pub fn handle_shutdown_command(
        &self,
        state: &mut ChannelActorState,
        command: ShutdownCommand,
    ) -> ProcessingChannelResult {
        debug!("Handling shutdown command: {:?}", &command);
        let flags = match state.state {
            ChannelState::ChannelReady(_) => {
                debug!("Handling shutdown command in ChannelReady state");
                ShuttingDownFlags::empty()
            }
            ChannelState::ShuttingDown(flags) => flags,
            _ => {
                debug!("Handling shutdown command in state {:?}", &state.state);
                return Err(ProcessingChannelError::InvalidState(
                    "Trying to send shutdown message while in invalid state".to_string(),
                ));
            }
        };
        self.network
            .send_message(NetworkActorMessage::new_command(
                NetworkActorCommand::SendPcnMessage(PCNMessageWithPeerId {
                    peer_id: self.peer_id.clone(),
                    message: PCNMessage::Shutdown(Shutdown {
                        channel_id: state.get_id(),
                        close_script: command.close_script.clone(),
                    }),
                }),
            ))
            .expect("network actor alive");
        state.holder_shutdown_script = Some(command.close_script.clone());
        let flags = flags | ShuttingDownFlags::OUR_SHUTDOWN_SENT;
        state.state = ChannelState::ShuttingDown(flags);
        debug!(
            "Channel state updated to {:?} after processing shutdown command",
            &state.state
        );

        state.maybe_transition_to_closing_signed(self.network.clone())?;
        Ok(())
    }

    // This is the dual of `handle_tx_collaboration_msg`. Any logic error here is likely
    // to present in the other function as well.
    pub fn handle_tx_collaboration_command(
        &self,
        state: &mut ChannelActorState,
        command: TxCollaborationCommand,
    ) -> Result<(), ProcessingChannelError> {
        debug!("Handling tx collaboration command: {:?}", &command);
        let is_complete_command = match command {
            TxCollaborationCommand::TxComplete(_) => true,
            _ => false,
        };
        let is_waiting_for_remote = match state.state {
            ChannelState::CollaboratingFundingTx(flags) => {
                flags.contains(CollaboratingFundingTxFlags::AWAITING_REMOTE_TX_COLLABORATION_MSG)
            }
            _ => false,
        };

        // We first exclude below cases that are invalid for tx collaboration,
        // and then process the commands.
        let flags = match state.state {
            ChannelState::NegotiatingFunding(NegotiatingFundingFlags::INIT_SENT)
                if state.is_acceptor =>
            {
                return Err(ProcessingChannelError::InvalidState(
                    "Acceptor tries to start sending tx collaboration message".to_string(),
                ));
            }
            ChannelState::NegotiatingFunding(_) => {
                debug!("Beginning processing tx collaboration command");
                CollaboratingFundingTxFlags::empty()
            }
            ChannelState::CollaboratingFundingTx(_)
                if !is_complete_command && is_waiting_for_remote =>
            {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Trying to process command {:?} while in {:?} (should only send non-complete message after received response from peer)",
                    &command, state.state
                )));
            }
            ChannelState::CollaboratingFundingTx(flags) => {
                if flags.contains(CollaboratingFundingTxFlags::OUR_TX_COMPLETE_SENT) {
                    return Err(ProcessingChannelError::InvalidState(format!(
                        "Trying to process a tx collaboration command {:?} while in collaboration already completed on our side",
                        &command
                    )));
                }
                debug!(
                    "Processing tx collaboration command {:?} for state {:?}",
                    &command, &state.state
                );
                flags
            }
            _ => {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Invalid tx collaboration command {:?} for state {:?}",
                    &command, state.state
                )));
            }
        };

        self.send_tx_collaboration_command(state, command.clone())?;

        // TODO: Note that we may deadlock here if send_tx_collaboration_command does successfully send the message,
        // as in that case both us and the remote are waiting for each other to send the message.
        match command {
            TxCollaborationCommand::TxUpdate(tx_update) => {
                state.update_funding_tx(tx_update.transaction)?;
                state.state = ChannelState::CollaboratingFundingTx(
                    CollaboratingFundingTxFlags::AWAITING_REMOTE_TX_COLLABORATION_MSG,
                );
            }
            TxCollaborationCommand::TxComplete(_) => {
                state.check_tx_complete_preconditions()?;
                state.state = ChannelState::CollaboratingFundingTx(
                    flags | CollaboratingFundingTxFlags::OUR_TX_COMPLETE_SENT,
                );
            }
        }

        Ok(())
    }

    pub fn handle_command(
        &self,
        state: &mut ChannelActorState,
        command: ChannelCommand,
    ) -> Result<(), ProcessingChannelError> {
        match command {
            ChannelCommand::TxCollaborationCommand(tx_collaboration_command) => {
                self.handle_tx_collaboration_command(state, tx_collaboration_command)
            }
            ChannelCommand::CommitmentSigned() => self.handle_commitment_signed_command(state),
            ChannelCommand::AddTlc(command) => self.handle_add_tlc_command(state, command),
            ChannelCommand::RemoveTlc(command) => self.handle_remove_tlc_command(state, command),
            ChannelCommand::Shutdown(command) => self.handle_shutdown_command(state, command),
        }
    }
}

#[rasync_trait]
impl Actor for ChannelActor {
    type Msg = ChannelActorMessage;
    type State = ChannelActorState;
    type Arguments = ChannelInitializationParameter;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        // startup the event processing
        match args {
            ChannelInitializationParameter::AcceptChannel(
                my_funding_amount,
                seed,
                open_channel,
            ) => {
                let peer_id = self.peer_id.clone();
                debug!(
                    "Accepting channel {:?} to peer {:?}",
                    &open_channel, &peer_id
                );

                let counterpart_pubkeys = (&open_channel).into();
                let OpenChannel {
                    channel_id,
                    chain_hash,
                    funding_type_script,
                    funding_amount,
                    to_self_delay,
                    first_per_commitment_point,
                    second_per_commitment_point,
                    next_local_nonce,
                    ..
                } = &open_channel;

                if *chain_hash != [0u8; 32].into() {
                    return Err(Box::new(ProcessingChannelError::InvalidParameter(format!(
                        "Invalid chain hash {:?}",
                        chain_hash
                    ))));
                }

                if funding_type_script.is_some() {
                    // We have not implemented funding type script yet.
                    // But don't panic, otherwise adversary can easily take us down.
                    return Err(Box::new(ProcessingChannelError::InvalidParameter(
                        "Funding type script is not none, but we are currently unable to process this".to_string(),
                    )));
                }

                let mut state = ChannelActorState::new_inbound_channel(
                    *channel_id,
                    my_funding_amount,
                    &seed,
                    peer_id.clone(),
                    *funding_amount,
                    *to_self_delay,
                    counterpart_pubkeys,
                    next_local_nonce.clone(),
                    *first_per_commitment_point,
                    *second_per_commitment_point,
                );

                let commitment_number = 0;

                let accept_channel = AcceptChannel {
                    channel_id: *channel_id,
                    funding_amount: my_funding_amount,
                    max_tlc_value_in_flight: DEFAULT_MAX_TLC_VALUE_IN_FLIGHT,
                    max_accept_tlcs: DEFAULT_MAX_ACCEPT_TLCS,
                    to_self_delay: *to_self_delay,
                    funding_pubkey: state.signer.funding_key.pubkey(),
                    revocation_basepoint: state.signer.revocation_base_key.pubkey(),
                    payment_basepoint: state.signer.payment_key.pubkey(),
                    min_tlc_value: DEFAULT_MIN_TLC_VALUE,
                    delayed_payment_basepoint: state.signer.delayed_payment_base_key.pubkey(),
                    tlc_basepoint: state.signer.tlc_base_key.pubkey(),
                    first_per_commitment_point: state
                        .signer
                        .get_commitment_point(commitment_number),
                    second_per_commitment_point: state
                        .signer
                        .get_commitment_point(commitment_number + 1),
                    next_local_nonce: state.get_holder_musig2_pubnonce(),
                };

                let command = PCNMessageWithPeerId {
                    peer_id,
                    message: PCNMessage::AcceptChannel(accept_channel),
                };
                // TODO: maybe we should not use try_send here.
                self.network
                    .send_message(NetworkActorMessage::new_command(
                        NetworkActorCommand::SendPcnMessage(command),
                    ))
                    .expect("network actor alive");

                self.network
                    .send_message(NetworkActorMessage::new_event(
                        NetworkActorEvent::ChannelCreated(
                            state.get_id(),
                            self.peer_id.clone(),
                            myself,
                        ),
                    ))
                    .expect("peer actor alive");
                state.state = ChannelState::NegotiatingFunding(NegotiatingFundingFlags::INIT_SENT);
                Ok(state)
            }
            ChannelInitializationParameter::OpenChannel(funding_amount, seed) => {
                let peer_id = self.peer_id.clone();
                info!("Trying to open a channel to {:?}", &peer_id);

                let mut channel = ChannelActorState::new_outbound_channel(
                    &seed,
                    self.peer_id.clone(),
                    funding_amount,
                    LockTime::new(DEFAULT_TO_SELF_DELAY_BLOCKS),
                );

                let commitment_number = 0;
                let message = PCNMessage::OpenChannel(OpenChannel {
                    chain_hash: Hash256::default(),
                    channel_id: channel.get_id(),
                    funding_type_script: None,
                    funding_amount: channel.to_self_amount,
                    funding_fee_rate: DEFAULT_FEE_RATE,
                    commitment_fee_rate: DEFAULT_COMMITMENT_FEE_RATE,
                    max_tlc_value_in_flight: DEFAULT_MAX_TLC_VALUE_IN_FLIGHT,
                    max_accept_tlcs: DEFAULT_MAX_ACCEPT_TLCS,
                    min_tlc_value: DEFAULT_MIN_TLC_VALUE,
                    to_self_delay: LockTime::new(DEFAULT_TO_SELF_DELAY_BLOCKS),
                    channel_flags: 0,
                    first_per_commitment_point: channel
                        .signer
                        .get_commitment_point(commitment_number),
                    second_per_commitment_point: channel
                        .signer
                        .get_commitment_point(commitment_number + 1),
                    funding_pubkey: channel
                        .get_holder_channel_parameters()
                        .pubkeys
                        .funding_pubkey,
                    revocation_basepoint: channel
                        .get_holder_channel_parameters()
                        .pubkeys
                        .revocation_base_key,
                    payment_basepoint: channel
                        .get_holder_channel_parameters()
                        .pubkeys
                        .payment_base_key,
                    delayed_payment_basepoint: channel
                        .get_holder_channel_parameters()
                        .pubkeys
                        .delayed_payment_base_key,
                    tlc_basepoint: channel.get_holder_channel_parameters().pubkeys.tlc_base_key,
                    next_local_nonce: channel.get_holder_musig2_pubnonce(),
                });

                debug!(
                    "Created OpenChannel message to {:?}: {:?}",
                    &peer_id, &message
                );
                self.network
                    .send_message(NetworkActorMessage::new_command(
                        NetworkActorCommand::SendPcnMessage(PCNMessageWithPeerId {
                            peer_id,
                            message,
                        }),
                    ))
                    .expect("network actor alive");
                // TODO: note that we can't actually guarantee that this OpenChannel message is sent here.
                // It is even possible that the peer_id is bogus, and we can't send a message to it.
                // We need some book-keeping service to remove all the OUR_INIT_SENT channels.
                channel.state =
                    ChannelState::NegotiatingFunding(NegotiatingFundingFlags::OUR_INIT_SENT);
                debug!(
                    "Channel to peer {:?} with id {:?} created: {:?}",
                    &self.peer_id,
                    &channel.get_id(),
                    &channel
                );

                // There is a slim chance that this message is not immediately processed by
                // the network actor, while the peer already receive the message AcceptChannel and
                // starts sending following messages. This is a problem of transactionally updating
                // states across multiple actors (NetworkActor and ChannelActor).
                // See also the notes [state updates across multiple actors](docs/notes/state-update-across-multiple-actors.md).
                self.network
                    .send_message(NetworkActorMessage::new_event(
                        NetworkActorEvent::ChannelCreated(
                            channel.get_id(),
                            self.peer_id.clone(),
                            myself,
                        ),
                    ))
                    .expect("network actor alive");
                Ok(channel)
            }
        }
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ChannelActorMessage::PeerMessage(message) => {
                if let Err(error) = state.handle_peer_message(message, self.network.clone()) {
                    error!("Error while processing channel message: {:?}", error);
                }
            }
            ChannelActorMessage::Command(command) => {
                if let Err(err) = self.handle_command(state, command) {
                    error!("Error while processing channel command: {:?}", err);
                }
            }
            ChannelActorMessage::Event(e) => match e {
                ChannelEvent::FundingTransactionConfirmed() => {
                    let flags = match state.state {
                        ChannelState::AwaitingChannelReady(flags) => flags,
                        ChannelState::AwaitingTxSignatures(f)
                            if f.contains(AwaitingTxSignaturesFlags::TX_SIGNATURES_SENT) =>
                        {
                            AwaitingChannelReadyFlags::empty()
                        }
                        _ => {
                            panic!("Expecting funding transaction confirmed event in state AwaitingChannelReady or after TX_SIGNATURES_SENT, but got state {:?}", &state.state);
                        }
                    };
                    self.network
                        .send_message(NetworkActorMessage::new_command(
                            NetworkActorCommand::SendPcnMessage(PCNMessageWithPeerId {
                                peer_id: state.peer_id.clone(),
                                message: PCNMessage::ChannelReady(ChannelReady {
                                    channel_id: state.get_id(),
                                }),
                            }),
                        ))
                        .expect("network actor alive");
                    let flags = flags | AwaitingChannelReadyFlags::OUR_CHANNEL_READY;
                    state.state = ChannelState::AwaitingChannelReady(flags);
                    if flags.contains(AwaitingChannelReadyFlags::CHANNEL_READY) {
                        state.state = ChannelState::ChannelReady(ChannelReadyFlags::empty());
                        self.network
                            .send_message(NetworkActorMessage::new_event(
                                NetworkActorEvent::ChannelReady(
                                    state.get_id(),
                                    self.peer_id.clone(),
                                ),
                            ))
                            .expect("network actor alive");
                    }
                }
            },
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct FundingTxInput(Transaction);

impl Eq for FundingTxInput {}

impl PartialEq for FundingTxInput {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_bytes() == other.0.as_bytes()
    }
}

impl From<Transaction> for FundingTxInput {
    fn from(tx: Transaction) -> Self {
        Self(tx)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelActorState {
    pub state: ChannelState,
    pub peer_id: PeerId,
    pub temp_id: Hash256,

    pub funding_tx: Option<TransactionView>,

    // Is this channel initially inbound?
    // An inbound channel is one where the counterparty is the funder of the channel.
    pub is_acceptor: bool,

    pub to_self_amount: u128,
    pub to_remote_amount: u128,

    // Signer is used to sign the commitment transactions.
    pub signer: InMemorySigner,

    // Cached channel parameter for easier of access.
    pub holder_channel_parameters: ChannelParametersOneParty,
    // The holder has set a shutdown script.
    pub holder_shutdown_script: Option<Script>,

    // Commitment numbers that are used to derive keys.
    // This value is guaranteed to be 1 when channel is just created.
    pub holder_commitment_number: u64,
    pub counterparty_commitment_number: u64,

    // Below are fields that are only usable after the channel is funded,
    // (or at some point of the state).
    pub id: Option<Hash256>,

    // The id of our next offering tlc, must increment by 1 for each new offered tlc.
    pub next_offering_tlc_id: u64,
    // The id of our next receiving tlc, must increment by 1 for each new offered tlc.
    pub next_receiving_tlc_id: u64,
    // HashMap of tlc ids to pending offered tlcs. Resovled tlcs (both failed and succeeded)
    // will be removed from this map.
    pub pending_offered_tlcs: HashMap<u64, TLC>,
    // HashMap of tlc ids to pending offered tlcs. Resovled tlcs (both failed and succeeded)
    // will be removed from this map.
    pub pending_received_tlcs: HashMap<u64, TLC>,

    // The counterparty has already sent a shutdown message with this script.
    pub counterparty_shutdown_script: Option<Script>,

    pub counterparty_nonce: Option<PubNonce>,

    // All the commitment point that are sent from the counterparty.
    // We need to save all these points to derive the keys for the commitment transactions.
    pub counterparty_commitment_points: Vec<Pubkey>,
    pub counterparty_channel_parameters: Option<ChannelParametersOneParty>,
    pub holder_shutdown_signature: Option<PartialSignature>,
    pub counterparty_shutdown_signature: Option<PartialSignature>,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ClosedChannel {}

#[derive(Debug)]
pub enum ChannelEvent {
    FundingTransactionConfirmed(),
}

pub type ProcessingChannelResult = Result<(), ProcessingChannelError>;

#[derive(Error, Debug)]
pub enum ProcessingChannelError {
    #[error("Invalid chain hash: {0}")]
    InvalidChainHash(Hash256),
    #[error("Unsupported operation: {0}")]
    Unsupported(String),
    #[error("Invalid state: ")]
    InvalidState(String),
    #[error("Repeated processing message: {0}")]
    RepeatedProcessing(String),
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
    #[error("Unimplemented operation: {0}")]
    Unimplemented(String),
    #[error("Failed to send command: {0}")]
    CommanderSendingError(#[from] TrySendError<NetworkActorCommand>),
    #[error("Failed to spawn actor: {0}")]
    SpawnErr(#[from] SpawnErr),
    #[error("Musig2 VerifyError: {0}")]
    Musig2VerifyError(#[from] VerifyError),
    #[error("Musig2 SigningError: {0}")]
    Musig2SigningError(#[from] SigningError),
}

bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct NegotiatingFundingFlags: u32 {
        const OUR_INIT_SENT = 1;
        const THEIR_INIT_SENT = 1 << 1;
        const INIT_SENT = NegotiatingFundingFlags::OUR_INIT_SENT.bits() | NegotiatingFundingFlags::THEIR_INIT_SENT.bits();
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct CollaboratingFundingTxFlags: u32 {
        const AWAITING_REMOTE_TX_COLLABORATION_MSG = 1;
        const PREPARING_LOCAL_TX_COLLABORATION_MSG = 1 << 1;
        const OUR_TX_COMPLETE_SENT = 1 << 2;
        const THEIR_TX_COMPLETE_SENT = 1 << 3;
        const COLLABRATION_COMPLETED = CollaboratingFundingTxFlags::OUR_TX_COMPLETE_SENT.bits() | CollaboratingFundingTxFlags::THEIR_TX_COMPLETE_SENT.bits();
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct SigningCommitmentFlags: u32 {
        const OUR_COMMITMENT_SIGNED_SENT = 1;
        const THEIR_COMMITMENT_SIGNED_SENT = 1 << 1;
        const COMMITMENT_SIGNED_SENT = SigningCommitmentFlags::OUR_COMMITMENT_SIGNED_SENT.bits() | SigningCommitmentFlags::THEIR_COMMITMENT_SIGNED_SENT.bits();
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct AwaitingTxSignaturesFlags: u32 {
        const OUR_TX_SIGNATURES_SENT = 1;
        const THEIR_TX_SIGNATURES_SENT = 1 << 1;
        const TX_SIGNATURES_SENT = AwaitingTxSignaturesFlags::OUR_TX_SIGNATURES_SENT.bits() | AwaitingTxSignaturesFlags::THEIR_TX_SIGNATURES_SENT.bits();
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct AwaitingChannelReadyFlags: u32 {
        const OUR_CHANNEL_READY = 1;
        const THEIR_CHANNEL_READY = 1 << 1;
        const CHANNEL_READY = AwaitingChannelReadyFlags::OUR_CHANNEL_READY.bits() | AwaitingChannelReadyFlags::THEIR_CHANNEL_READY.bits();
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct ChannelReadyFlags: u32 {
        /// Indicates that we have sent a `commitment_signed` but are awaiting the responding
        ///	`revoke_and_ack` message. During this period, we can't generate new messages as
        /// we'd be unable to determine which TLCs they included in their `revoke_and_ack`
        ///	implicit ACK, so instead we have to hold them away temporarily to be sent later.
        const AWAITING_REMOTE_REVOKE = 1;
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct ShuttingDownFlags: u32 {
        /// Indicates that we have sent a `shutdown` message.
        const OUR_SHUTDOWN_SENT = 1;
        /// Indicates that they have sent a `shutdown` message.
        const THEIR_SHUTDOWN_SENT = 1 << 1;
        /// Indicates that both we and they have sent `shutdown` messages,
        /// but some HTLCs are still pending resolution.
        const AWAITING_PENDING_TLCS = ShuttingDownFlags::OUR_SHUTDOWN_SENT.bits() | ShuttingDownFlags::THEIR_SHUTDOWN_SENT.bits();
        /// Indicates that we have sent a `closing_signed` message, implying both
        /// we and they have sent `shutdown` messages and all HTLCs are resolved.
        const OUR_CLOSING_SIGNED = 1 << 2;
        /// Indicates that they have sent a `closing_signed` message, implying both
        /// we and they have sent `shutdown` messages and all HTLCs are resolved.
        const THEIR_CLOSING_SIGNED = 1 << 3;
        /// Indicates all pending HTLCs are resolved, and this channel will be dropped.
        const DROPPING_PENDING = ShuttingDownFlags::AWAITING_PENDING_TLCS.bits() | ShuttingDownFlags::OUR_CLOSING_SIGNED.bits() | ShuttingDownFlags::THEIR_CLOSING_SIGNED.bits();
    }
}

// Depending on the state of the channel, we may process the commitment_signed command differently.
// Below are all the channel state flags variants that we may encounter
// in normal commitment_signed processing flow.
enum CommitmentSignedFlags {
    SigningCommitment(SigningCommitmentFlags),
    ChannelReady(ChannelReadyFlags),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ChannelState {
    /// We are negotiating the parameters required for the channel prior to funding it.
    NegotiatingFunding(NegotiatingFundingFlags),
    /// We're collaborating with the other party on the funding transaction.
    CollaboratingFundingTx(CollaboratingFundingTxFlags),
    /// We have collaborated over the funding and are now waiting for CommitmentSigned messages.
    SigningCommitment(SigningCommitmentFlags),
    /// We've received and sent `commitment_signed` and are now waiting for both
    /// party to collaborate on creating a valid funding transaction.
    AwaitingTxSignatures(AwaitingTxSignaturesFlags),
    /// We've received/sent `funding_created` and `funding_signed` and are thus now waiting on the
    /// funding transaction to confirm.
    AwaitingChannelReady(AwaitingChannelReadyFlags),
    /// Both we and our counterparty consider the funding transaction confirmed and the channel is
    /// now operational.
    ChannelReady(ChannelReadyFlags),
    /// We've successfully negotiated a `closing_signed` dance. At this point, the `ChannelManager`
    /// is about to drop us, but we store this anyway.
    ShuttingDown(ShuttingDownFlags),
    /// This channel is closed.
    Closed,
}

fn new_channel_id_from_seed(seed: &[u8]) -> Hash256 {
    blake2b_256(seed).into()
}

fn derive_channel_id_from_revocation_keys(
    revocation_basepoint1: &Pubkey,
    revocation_basepoint2: &Pubkey,
) -> Hash256 {
    let holder_revocation = revocation_basepoint1.0.serialize();
    let counterparty_revocation = revocation_basepoint2.0.serialize();

    let preimage = if holder_revocation >= counterparty_revocation {
        counterparty_revocation
            .into_iter()
            .chain(holder_revocation)
            .collect::<Vec<_>>()
    } else {
        holder_revocation
            .into_iter()
            .chain(counterparty_revocation)
            .collect()
    };
    new_channel_id_from_seed(&preimage)
}

pub fn get_commitment_secret(commitment_seed: &[u8; 32], commitment_number: u64) -> [u8; 32] {
    // Note that here, we hold the same assumption to bolts for commitment number,
    // i.e. this number should be in the range [0, 2^48).
    let mut res: [u8; 32] = *commitment_seed;
    for i in 0..48 {
        let bitpos = 47 - i;
        if commitment_number & (1 << bitpos) == (1 << bitpos) {
            res[bitpos / 8] ^= 1 << (bitpos & 7);
            res = blake2b_256(res);
        }
    }
    res
}

pub fn get_commitment_point(commitment_seed: &[u8; 32], commitment_number: u64) -> Pubkey {
    Privkey::from(&get_commitment_secret(commitment_seed, commitment_number)).pubkey()
}

// Constructors for the channel actor state.
impl ChannelActorState {
    pub fn new_inbound_channel<'a>(
        temp_channel_id: Hash256,
        holder_value: u128,
        seed: &[u8],
        peer_id: PeerId,
        counterparty_value: u128,
        counterparty_delay: LockTime,
        counterparty_pubkeys: ChannelBasePublicKeys,
        counterparty_nonce: PubNonce,
        counterparty_commitment_point: Pubkey,
        counterparty_prev_commitment_point: Pubkey,
    ) -> Self {
        let commitment_number = 1;
        let signer = InMemorySigner::generate_from_seed(seed);
        let holder_pubkeys = signer.to_channel_public_keys(commitment_number);

        let channel_id = derive_channel_id_from_revocation_keys(
            &holder_pubkeys.revocation_base_key,
            &counterparty_pubkeys.revocation_base_key,
        );

        debug!(
            "Generated channel id ({:?}) for temporary channel {:?}",
            &channel_id, &temp_channel_id
        );

        Self {
            state: ChannelState::NegotiatingFunding(NegotiatingFundingFlags::THEIR_INIT_SENT),
            peer_id,
            funding_tx: None,
            is_acceptor: true,
            to_self_amount: holder_value,
            temp_id: temp_channel_id,
            id: Some(channel_id),
            next_offering_tlc_id: 0,
            next_receiving_tlc_id: 0,
            pending_offered_tlcs: Default::default(),
            pending_received_tlcs: Default::default(),
            to_remote_amount: counterparty_value,
            holder_shutdown_script: None,
            holder_channel_parameters: ChannelParametersOneParty {
                pubkeys: holder_pubkeys,
                selected_contest_delay: counterparty_delay,
            },
            signer,
            counterparty_channel_parameters: Some(ChannelParametersOneParty {
                pubkeys: counterparty_pubkeys,
                selected_contest_delay: counterparty_delay,
            }),
            holder_commitment_number: commitment_number,
            counterparty_commitment_number: 1,
            counterparty_shutdown_script: None,
            counterparty_nonce: Some(counterparty_nonce),
            counterparty_commitment_points: vec![
                counterparty_prev_commitment_point,
                counterparty_commitment_point,
            ],
            holder_shutdown_signature: None,
            counterparty_shutdown_signature: None,
        }
    }

    pub fn new_outbound_channel(
        seed: &[u8],
        peer_id: PeerId,
        value: u128,
        to_self_delay: LockTime,
    ) -> Self {
        let new_channel_id = new_channel_id_from_seed(seed);
        let signer = InMemorySigner::generate_from_seed(seed);
        let commitment_number = 0;
        let holder_pubkeys = signer.to_channel_public_keys(commitment_number);
        Self {
            state: ChannelState::NegotiatingFunding(NegotiatingFundingFlags::empty()),
            peer_id,
            funding_tx: None,
            is_acceptor: false,
            to_self_amount: value,
            temp_id: new_channel_id,
            id: None,
            next_offering_tlc_id: 0,
            next_receiving_tlc_id: 0,
            pending_offered_tlcs: Default::default(),
            pending_received_tlcs: Default::default(),
            to_remote_amount: 0,
            signer,
            holder_channel_parameters: ChannelParametersOneParty {
                pubkeys: holder_pubkeys,
                selected_contest_delay: to_self_delay,
            },
            counterparty_channel_parameters: None,
            holder_commitment_number: commitment_number,
            counterparty_nonce: None,
            counterparty_commitment_number: 1,
            counterparty_commitment_points: vec![],
            holder_shutdown_script: None,
            counterparty_shutdown_script: None,
            holder_shutdown_signature: None,
            counterparty_shutdown_signature: None,
        }
    }
}

// Properties for the channel actor state.
impl ChannelActorState {
    pub fn get_id(&self) -> Hash256 {
        self.id.unwrap_or(self.temp_id)
    }

    pub fn get_holder_nonce(&self) -> impl Borrow<PubNonce> {
        self.get_next_next_secnonce().public_nonce()
    }

    pub fn get_next_next_secnonce(&self) -> SecNonce {
        self.signer
            .derive_musig2_nonce(self.get_next_commitment_number(true))
    }

    pub fn get_next_holder_nonce(&self) -> PubNonce {
        self.signer
            .derive_musig2_nonce(self.get_next_commitment_number(true))
            .public_nonce()
    }

    pub fn get_counterparty_nonce(&self) -> &PubNonce {
        self.counterparty_nonce.as_ref().unwrap()
    }

    pub fn get_channel_parameters(&self, local: bool) -> &ChannelParametersOneParty {
        if local {
            self.get_holder_channel_parameters()
        } else {
            self.get_counterparty_channel_parameters()
        }
    }

    pub fn get_holder_channel_parameters(&self) -> &ChannelParametersOneParty {
        &self.holder_channel_parameters
    }

    pub fn get_counterparty_channel_parameters(&self) -> &ChannelParametersOneParty {
        self.counterparty_channel_parameters.as_ref().unwrap()
    }

    pub fn get_next_commitment_number(&self, local: bool) -> u64 {
        if local {
            self.holder_commitment_number + 1
        } else {
            self.counterparty_commitment_number + 1
        }
    }

    pub fn get_funding_transaction_outpoint(&self) -> OutPoint {
        let tx = self
            .funding_tx
            .as_ref()
            .expect("Funding transaction is present");
        // By convention, the funding tx output for the channel is the first output.
        tx.output_pts_iter()
            .next()
            .expect("Funding transaction output is present")
    }

    pub fn get_commitment_transaction_cell_deps(&self) -> CellDepVec {
        if is_testing() {
            get_commitment_lock_context()
                .read()
                .unwrap()
                .cell_deps
                .clone()
        } else {
            // TODO: Fill in the actual cell deps here.
            [CellDep::new_builder()
                .out_point(get_commitment_lock_outpoint())
                .dep_type(DepType::Code.into())
                .build()]
            .pack()
        }
    }

    pub fn get_holder_shutdown_script(&self) -> &Script {
        // TODO: what is the best strategy for shutdown script here?
        self.holder_shutdown_script
            .as_ref()
            .expect("Holder shutdown script is present")
    }

    pub fn get_counterparty_shutdown_script(&self) -> &Script {
        self.counterparty_shutdown_script
            .as_ref()
            .expect("Counterparty shutdown script is present")
    }

    pub fn get_commitment_point(&self, local: bool, commitment_number: u64) -> Pubkey {
        if local {
            self.get_holder_commitment_point(commitment_number)
        } else {
            self.get_counterparty_commitment_point(commitment_number)
        }
    }

    pub fn get_current_commitment_point(&self, local: bool) -> Pubkey {
        if local {
            self.get_current_holder_commitment_point()
        } else {
            self.get_current_counterparty_commitment_point()
        }
    }

    pub fn get_holder_commitment_point(&self, commitment_number: u64) -> Pubkey {
        self.signer.get_commitment_point(commitment_number)
    }

    pub fn get_current_holder_commitment_point(&self) -> Pubkey {
        self.get_holder_commitment_point(self.holder_commitment_number)
    }

    pub fn get_current_counterparty_commitment_point(&self) -> Pubkey {
        self.get_counterparty_commitment_point(self.counterparty_commitment_number)
    }

    /// Get the counterparty commitment point for the given commitment number.
    pub fn get_counterparty_commitment_point(&self, commitment_number: u64) -> Pubkey {
        let index = commitment_number as usize;
        self.counterparty_commitment_points[index]
    }

    pub fn get_musig2_agg_context(&self) -> KeyAggContext {
        let holder_pubkey = self.get_holder_channel_parameters().pubkeys.funding_pubkey;
        let counterparty_pubkey = self
            .get_counterparty_channel_parameters()
            .pubkeys
            .funding_pubkey;
        let keys = self.order_things_for_musig2(holder_pubkey, counterparty_pubkey);
        KeyAggContext::new(keys).expect("Valid pubkeys")
    }

    pub fn get_holder_musig2_secnonce(&self) -> SecNonce {
        self.signer
            .derive_musig2_nonce(self.holder_commitment_number)
    }

    pub fn get_holder_musig2_pubnonce(&self) -> PubNonce {
        self.get_holder_musig2_secnonce().public_nonce()
    }

    pub fn get_musig2_agg_pubnonce(&self) -> AggNonce {
        let holder_nonce = self.get_holder_nonce();
        let holder_nonce = holder_nonce.borrow();
        let counterparty_nonce = self.get_counterparty_nonce();
        let nonces = self.order_things_for_musig2(holder_nonce, counterparty_nonce);
        AggNonce::sum(nonces)
    }

    pub fn get_broadcaster_and_countersignatory_amounts(&self, local: bool) -> (u128, u128) {
        if local {
            (self.to_self_amount, self.to_remote_amount)
        } else {
            (self.to_remote_amount, self.to_self_amount)
        }
    }

    pub fn get_all_tlcs(&self) -> Vec<TLC> {
        debug!("Getting tlcs for commitment tx");
        debug!(
            "Current pending offered tlcs: {:?}",
            self.pending_offered_tlcs
        );
        debug!(
            "Current pending received tlcs: {:?}",
            self.pending_received_tlcs
        );
        self.pending_offered_tlcs
            .values()
            .chain(self.pending_received_tlcs.values())
            .cloned()
            .collect()
    }

    fn any_tlc_pending(&self) -> bool {
        !self.pending_offered_tlcs.is_empty() || !self.pending_received_tlcs.is_empty()
    }

    pub fn get_counterparty_funding_pubkey(&self) -> &Pubkey {
        &self
            .get_counterparty_channel_parameters()
            .pubkeys
            .funding_pubkey
    }

    pub fn check_state_for_tlc_update(&self) -> ProcessingChannelResult {
        match self.state {
            ChannelState::ChannelReady(_) => Ok(()),
            ChannelState::ShuttingDown(_) => Ok(()),
            _ => {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Invalid state {:?} for adding tlc",
                    self.state
                )))
            }
        }
    }

    pub fn create_outbounding_tlc(&mut self, command: AddTlcCommand) -> TLC {
        // TODO: we are filling the user command with a new id here.
        // The advantage of this is that we don't need to burden the users to
        // provide a next id for each tlc. The disadvantage is that users may
        // inadvertently click the same button twice, and we will process the same
        // twice, the frontend needs to prevent this kind of behaviour.
        // Is this what we want?
        let id = self.next_offering_tlc_id;
        assert!(
            self.pending_offered_tlcs.get(&id).is_none(),
            "Must not have the same id in pending offered tlcs"
        );

        let preimage = command.preimage.unwrap_or(get_random_preimage());
        let hash = blake2b_256(&preimage);
        TLC {
            id,
            amount: command.amount,
            payment_hash: hash.into(),
            lock_time: command.expiry,
            is_offered: true,
            payment_preimage: Some(preimage),
            local_commitment_number: self.holder_commitment_number,
            remote_commitment_number: self.counterparty_commitment_number,
            local_payment_key_hash: None,
            remote_payment_key_hash: None,
        }
    }

    pub fn create_inbounding_tlc(&self, message: AddTlc) -> TLC {
        let local_commitment_number = self.holder_commitment_number;
        let remote_commitment_number = self.counterparty_commitment_number;
        TLC {
            id: message.tlc_id,
            amount: message.amount,
            payment_hash: message.payment_hash,
            lock_time: message.expiry,
            is_offered: false,
            payment_preimage: None,
            local_commitment_number,
            remote_commitment_number,
            local_payment_key_hash: None,
            remote_payment_key_hash: None,
        }
    }
}

impl From<&ChannelActorState> for Musig2Context {
    fn from(value: &ChannelActorState) -> Self {
        Musig2Context {
            key_agg_ctx: value.get_musig2_agg_context(),
            agg_nonce: value.get_musig2_agg_pubnonce(),
            holder_seckey: value.signer.funding_key,
            holder_secnonce: value.get_holder_musig2_secnonce(),
            counterparty_pubkey: *value.get_counterparty_funding_pubkey(),
            counterparty_pubnonce: value.get_counterparty_nonce().clone(),
        }
    }
}

impl From<&ChannelActorState> for Musig2SignContext {
    fn from(value: &ChannelActorState) -> Self {
        Musig2SignContext {
            key_agg_ctx: value.get_musig2_agg_context(),
            agg_nonce: value.get_musig2_agg_pubnonce(),
            seckey: value.signer.funding_key,
            secnonce: value.get_holder_musig2_secnonce(),
        }
    }
}

impl From<&ChannelActorState> for Musig2VerifyContext {
    fn from(value: &ChannelActorState) -> Self {
        Musig2VerifyContext {
            key_agg_ctx: value.get_musig2_agg_context(),
            agg_nonce: value.get_musig2_agg_pubnonce(),
            pubkey: *value.get_counterparty_funding_pubkey(),
            pubnonce: value.get_counterparty_nonce().clone(),
        }
    }
}

// State transition handlers for the channel actor state.
impl ChannelActorState {
    pub fn handle_peer_message(
        &mut self,
        message: PCNMessage,
        network: ActorRef<NetworkActorMessage>,
    ) -> Result<(), ProcessingChannelError> {
        match message {
            PCNMessage::OpenChannel(_) => {
                panic!("OpenChannel message should be processed while prestarting")
            }
            PCNMessage::AcceptChannel(accept_channel) => {
                self.handle_accept_channel_message(accept_channel)?;
                self.fill_in_channel_id();
                network
                    .send_message(NetworkActorMessage::new_event(
                        NetworkActorEvent::ChannelAccepted(self.get_id(), self.temp_id),
                    ))
                    .expect("network actor alive");
                Ok(())
            }
            PCNMessage::TxUpdate(tx) => {
                self.handle_tx_collaboration_msg(TxCollaborationMsg::TxUpdate(tx))
            }
            PCNMessage::TxComplete(tx) => {
                self.handle_tx_collaboration_msg(TxCollaborationMsg::TxComplete(tx))
            }
            PCNMessage::CommitmentSigned(commitment_signed) => {
                self.handle_commitment_signed_message(commitment_signed, network.clone())?;
                if let ChannelState::SigningCommitment(flags) = self.state {
                    if !flags.contains(SigningCommitmentFlags::OUR_COMMITMENT_SIGNED_SENT) {
                        // TODO: maybe we should send our commitment_signed message here.
                        debug!("CommitmentSigned message received, but we haven't sent our commitment_signed message yet");
                    }
                }
                Ok(())
            }
            PCNMessage::TxSignatures(tx_signatures) => {
                // We're the one who send tx_signature first, and we received a tx_signature message.
                // This means that the tx_signature procedure is now completed. Just change state,
                // and exit.
                if self.should_holder_send_tx_signatures_first() {
                    network
                        .send_message(NetworkActorMessage::new_event(
                            NetworkActorEvent::FundingTransactionPending(
                                Transaction::default(),
                                self.get_funding_transaction_outpoint(),
                                self.get_id(),
                            ),
                        ))
                        .expect("network actor alive");

                    self.state =
                        ChannelState::AwaitingChannelReady(AwaitingChannelReadyFlags::empty());
                    return Ok(());
                };
                self.handle_tx_signatures(network, Some(tx_signatures.witnesses))?;
                Ok(())
            }
            PCNMessage::RevokeAndAck(revoke_and_ack) => {
                self.handle_revoke_and_ack_message(revoke_and_ack, network.clone())?;
                self.state = ChannelState::ChannelReady(ChannelReadyFlags::empty());
                Ok(())
            }
            PCNMessage::ChannelReady(channel_ready) => {
                let flags = match self.state {
                    ChannelState::AwaitingTxSignatures(flags) => {
                        if flags.contains(AwaitingTxSignaturesFlags::TX_SIGNATURES_SENT) {
                            AwaitingChannelReadyFlags::empty()
                        } else {
                            return Err(ProcessingChannelError::InvalidState(format!(
                                "received ChannelReady message, but we're not ready for ChannelReady, state is currently {:?}",
                                self.state
                            )));
                        }
                    }
                    ChannelState::AwaitingChannelReady(flags) => flags,
                    _ => {
                        return Err(ProcessingChannelError::InvalidState(format!(
                            "received ChannelReady message, but we're not ready for ChannelReady, state is currently {:?}", self.state
                        )));
                    }
                };
                let flags = flags | AwaitingChannelReadyFlags::THEIR_CHANNEL_READY;
                self.state = ChannelState::AwaitingChannelReady(flags);
                debug!(
                    "ChannelReady: {:?}, current state: {:?}",
                    &channel_ready, &self.state
                );

                if flags.contains(AwaitingChannelReadyFlags::CHANNEL_READY) {
                    self.state = ChannelState::ChannelReady(ChannelReadyFlags::empty());
                    network
                        .send_message(NetworkActorMessage::new_event(
                            NetworkActorEvent::ChannelReady(self.get_id(), self.peer_id.clone()),
                        ))
                        .expect("network actor alive");
                }

                Ok(())
            }
            PCNMessage::AddTlc(add_tlc) => {
                self.check_state_for_tlc_update()?;

                let tlc = self.create_inbounding_tlc(add_tlc);
                let id = tlc.id;

                match self.pending_received_tlcs.get(&id) {
                    Some(current) if current == &tlc => {
                        debug!(
                            "Repeated processing of AddTlcCommand with id {:?}: current tlc {:?}",
                            id, current,
                        )
                    }
                    Some(current) => {
                        return Err(ProcessingChannelError::RepeatedProcessing(format!(
                                    "Repeated processing of AddTlcCommand with id {:?}: current tlc {:?}, tlc to be inserted {:?}",
                                    id,
                                    current,
                                    &tlc
                                )));
                    }
                    None => {
                        debug!("Adding tlc {:?} to channel {:?}", &tlc, &self.get_id());
                    }
                }

                self.pending_received_tlcs.insert(tlc.id, tlc);
                self.to_remote_amount -= tlc.amount;

                debug!("Saved tlc {:?} to pending_received_tlcs", &tlc);
                debug!(
                    "Current pending_received_tlcs: {:?}",
                    &self.pending_received_tlcs
                );
                debug!(
                    "Current pending_offered_tlcs: {:?}",
                    &self.pending_offered_tlcs
                );
                debug!(
                    "Balance after tlc added: to_self_amount: {}, to_remote_amount: {}",
                    self.to_self_amount, self.to_remote_amount
                );
                // TODO: here we didn't send any ack message to the peer.
                // The peer may falsely believe that we have already processed this message,
                // while we have crashed. We need a way to make sure that the peer will resend
                // this message, and our processing of this message is idempotent.
                Ok(())
            }
            PCNMessage::RemoveTlc(remove_tlc) => {
                self.check_state_for_tlc_update()?;

                let channel_id = self.get_id();
                match self.pending_offered_tlcs.entry(remove_tlc.tlc_id) {
                    hash_map::Entry::Occupied(entry) => {
                        let current = entry.get();
                        debug!("Removing tlc {:?} from channel {:?}", &current, &channel_id);
                        match remove_tlc.reason {
                            RemoveTlcReason::RemoveTlcFail(fail) => {
                                debug!("TLC {:?} failed with code {}", &current, &fail.error_code);
                                self.to_self_amount += current.amount;
                                debug!("Balance after tlc removed: to_self_amount: {}, to_remote_amount: {}", self.to_self_amount, self.to_remote_amount);
                            }
                            RemoveTlcReason::RemoveTlcFulfill(fulfill) => {
                                let preimage = fulfill.payment_preimage;
                                debug!(
                                    "TLC {:?} succeeded with preimage {:?}",
                                    &current, &preimage
                                );
                                if current.payment_hash != blake2b_256(preimage).into() {
                                    return Err(ProcessingChannelError::InvalidParameter(
                                        "Payment preimage does not match the hash".to_string(),
                                    ));
                                }
                                self.to_remote_amount += current.amount;
                                debug!("Balance after tlc removed: to_self_amount: {}, to_remote_amount: {}", self.to_self_amount, self.to_remote_amount);
                            }
                        }
                        entry.remove();
                        if self.pending_offered_tlcs.is_empty() {
                            self.maybe_transition_to_closing_signed(network)?;
                        }
                        Ok(())
                    }
                    hash_map::Entry::Vacant(_) => {
                        Err(ProcessingChannelError::InvalidParameter(format!(
                            "TLC with id {:?} not found in pending_received_tlcs",
                            remove_tlc.tlc_id
                        )))
                    }
                }
            }
            PCNMessage::Shutdown(shutdown) => {
                let flags = match self.state {
                    ChannelState::ChannelReady(_) => ShuttingDownFlags::empty(),
                    ChannelState::ShuttingDown(flags)
                        if flags.contains(ShuttingDownFlags::THEIR_SHUTDOWN_SENT) =>
                    {
                        return Err(ProcessingChannelError::InvalidParameter(
                            "Received Shutdown message, but we're already in ShuttingDown state"
                                .to_string(),
                        ));
                    }
                    ChannelState::ShuttingDown(flags) => flags,
                    _ => {
                        return Err(ProcessingChannelError::InvalidState(format!(
                            "received Shutdown message, but we're not ready for Shutdown, state is currently {:?}",
                            self.state
                        )));
                    }
                };
                self.counterparty_shutdown_script = Some(shutdown.close_script);

                let flags = flags | ShuttingDownFlags::THEIR_SHUTDOWN_SENT;
                self.state = ChannelState::ShuttingDown(flags);
                debug!(
                    "Channel state updated to {:?} after processing shutdown command",
                    &self.state
                );
                self.maybe_transition_to_closing_signed(network)?;

                Ok(())
            }
            PCNMessage::ClosingSigned(closing) => {
                let flags = match self.state {
                    ChannelState::ShuttingDown(flags)
                        if flags.contains(ShuttingDownFlags::AWAITING_PENDING_TLCS) =>
                    {
                        if self.any_tlc_pending() {
                            return Err(ProcessingChannelError::InvalidState(format!(
                                "received ClosingSigned message, but we're not ready for ClosingSigned because there are still some pending tlcs, state: {:?}, pending tlcs: {:?}",
                                self.state,
                                self.get_all_tlcs()
                            )));
                        }
                        flags
                    }
                    _ => {
                        return Err(ProcessingChannelError::InvalidState(format!(
                            "received ClosingSigned message, but we're not ready for ClosingSigned, state is currently {:?}",
                            self.state
                        )));
                    }
                };
                let ClosingSigned {
                    partial_signature,
                    channel_id,
                    fee: _,
                } = closing;

                if channel_id != self.get_id() {
                    return Err(ProcessingChannelError::InvalidParameter(
                        "Channel id mismatch".to_string(),
                    ));
                }

                let verify_ctx = Musig2VerifyContext::from(&*self);
                let tx = self.build_shutdown_tx(
                    self.get_holder_shutdown_script(),
                    self.get_counterparty_shutdown_script(),
                );
                let message = get_tx_message_to_sign(&tx);
                verify_ctx.verify(partial_signature, message.as_slice())?;
                debug!("Successfully verified ClosingSigned message");
                self.counterparty_shutdown_signature = Some(partial_signature);

                let flags = flags | ShuttingDownFlags::THEIR_CLOSING_SIGNED;
                self.state = ChannelState::ShuttingDown(flags);
                if flags.contains(ShuttingDownFlags::DROPPING_PENDING) {
                    self.state = ChannelState::Closed;
                    let partial_signatures = self.order_things_for_musig2(
                        self.holder_shutdown_signature.unwrap(),
                        self.counterparty_shutdown_signature.unwrap(),
                    );
                    let tx =
                        aggregate_partial_signatures_for_tx(tx, verify_ctx, partial_signatures)
                            .expect("The validity of the signatures verified");

                    network
                        .send_message(NetworkActorMessage::new_event(
                            NetworkActorEvent::ChannelClosed(
                                self.get_id(),
                                self.peer_id.clone(),
                                tx,
                            ),
                        ))
                        .expect("network actor alive");
                }
                Ok(())
            }

            _ => {
                warn!("Received unsupported message: {:?}", &message);
                Ok(())
            }
        }
    }

    pub fn maybe_transition_to_closing_signed(
        &mut self,
        network: ActorRef<NetworkActorMessage>,
    ) -> ProcessingChannelResult {
        let flags = match self.state {
            ChannelState::ShuttingDown(flags) => flags,
            _ => {
                return Ok(());
            }
        };
        // TODO: should automatically check this periodically.
        if flags.contains(ShuttingDownFlags::AWAITING_PENDING_TLCS) && !self.any_tlc_pending() {
            debug!("All pending tlcs are resolved, transitioning to ClosingSigned");
            self.state = ChannelState::ShuttingDown(flags | ShuttingDownFlags::DROPPING_PENDING);

            let signature = self.build_and_sign_shutdown_tx(
                self.get_holder_shutdown_script(),
                self.get_counterparty_shutdown_script(),
            )?;
            network
                .send_message(NetworkActorMessage::new_command(
                    NetworkActorCommand::SendPcnMessage(PCNMessageWithPeerId {
                        peer_id: self.peer_id.clone(),
                        message: PCNMessage::ClosingSigned(ClosingSigned {
                            partial_signature: signature,
                            channel_id: self.get_id(),
                            fee: 0,
                        }),
                    }),
                ))
                .expect("network actor alive");

            self.holder_shutdown_signature = Some(signature);
            network
                .send_message(NetworkActorMessage::new_event(
                    NetworkActorEvent::ChannelShutdown(self.get_id(), self.peer_id.clone()),
                ))
                .expect("network actor alive");
        }
        debug!(
            "Not transitioning to ClosingSigned, current state: {:?}, pending tlcs: {:?}",
            &self.state,
            &self.get_all_tlcs()
        );
        Ok(())
    }

    pub fn handle_accept_channel_message(
        &mut self,
        accept_channel: AcceptChannel,
    ) -> ProcessingChannelResult {
        if self.state != ChannelState::NegotiatingFunding(NegotiatingFundingFlags::OUR_INIT_SENT) {
            return Err(ProcessingChannelError::InvalidState(format!(
                "accepting a channel while in state {:?}, expecting NegotiatingFundingFlags::OUR_INIT_SENT",
                self.state
            )));
        }

        self.state = ChannelState::NegotiatingFunding(NegotiatingFundingFlags::INIT_SENT);
        self.to_remote_amount = accept_channel.funding_amount;
        self.counterparty_nonce = Some(accept_channel.next_local_nonce.clone());

        let counterparty_pubkeys = (&accept_channel).into();
        self.counterparty_channel_parameters = Some(ChannelParametersOneParty {
            pubkeys: counterparty_pubkeys,
            selected_contest_delay: accept_channel.to_self_delay,
        });
        self.counterparty_commitment_points
            .push(accept_channel.first_per_commitment_point);
        self.counterparty_commitment_points
            .push(accept_channel.second_per_commitment_point);

        debug!(
            "Successfully processed AcceptChannel message {:?}",
            &accept_channel
        );
        Ok(())
    }

    // This is the dual of `handle_tx_collaboration_command`. Any logic error here is likely
    // to present in the other function as well.
    pub fn handle_tx_collaboration_msg(
        &mut self,
        msg: TxCollaborationMsg,
    ) -> ProcessingChannelResult {
        debug!("Processing tx collaboration message: {:?}", &msg);
        let is_complete_message = match msg {
            TxCollaborationMsg::TxComplete(_) => true,
            _ => false,
        };
        let is_waiting_for_remote = match self.state {
            ChannelState::CollaboratingFundingTx(flags) => {
                flags.contains(CollaboratingFundingTxFlags::AWAITING_REMOTE_TX_COLLABORATION_MSG)
            }
            _ => false,
        };
        let flags = match self.state {
            // Starting transaction collaboration
            ChannelState::NegotiatingFunding(NegotiatingFundingFlags::INIT_SENT)
                if !self.is_acceptor =>
            {
                return Err(ProcessingChannelError::InvalidState(
                    "Initiator received a tx collaboration message".to_string(),
                ));
            }
            ChannelState::NegotiatingFunding(_) => {
                debug!("Beginning processing tx collaboration message");
                CollaboratingFundingTxFlags::empty()
            }
            ChannelState::CollaboratingFundingTx(_)
                if !is_complete_message && !is_waiting_for_remote =>
            {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Trying to process message {:?} while in {:?} (should only receive non-complete message after sent response from peer)",
                    &msg, self.state
                )));
            }
            ChannelState::CollaboratingFundingTx(flags) => {
                if flags.contains(CollaboratingFundingTxFlags::THEIR_TX_COMPLETE_SENT) {
                    return Err(ProcessingChannelError::InvalidState(format!(
                        "Trying to process a tx collaboration message {:?} while in collaboration already completed on our side",
                        &msg
                    )));
                }
                debug!(
                    "Processing tx collaboration message {:?} for state {:?}",
                    &msg, &self.state
                );
                flags
            }
            _ => {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Invalid tx collaboration message {:?} for state {:?}",
                    &msg, &self.state
                )));
            }
        };
        match msg {
            TxCollaborationMsg::TxUpdate(msg) => {
                self.update_funding_tx(msg.tx)?;
                self.state = ChannelState::CollaboratingFundingTx(
                    CollaboratingFundingTxFlags::PREPARING_LOCAL_TX_COLLABORATION_MSG,
                );
            }
            TxCollaborationMsg::TxComplete(_msg) => {
                self.check_tx_complete_preconditions()?;
                self.state = ChannelState::CollaboratingFundingTx(
                    flags | CollaboratingFundingTxFlags::THEIR_TX_COMPLETE_SENT,
                );
            }
        }
        Ok(())
    }

    pub fn handle_commitment_signed_message(
        &mut self,
        commitment_signed: CommitmentSigned,
        network: ActorRef<NetworkActorMessage>,
    ) -> ProcessingChannelResult {
        let flags = match self.state {
            ChannelState::CollaboratingFundingTx(flags)
                if !flags.contains(CollaboratingFundingTxFlags::COLLABRATION_COMPLETED) =>
            {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Unable to process commitment_signed command in state {:?}, as collaboration is not completed yet.",
                    &self.state
                )));
            }
            ChannelState::CollaboratingFundingTx(_) => {
                debug!(
                    "Processing commitment_signed command in from CollaboratingFundingTx state {:?}",
                    &self.state
                );
                CommitmentSignedFlags::SigningCommitment(SigningCommitmentFlags::empty())
            }
            ChannelState::SigningCommitment(flags)
                if flags.contains(SigningCommitmentFlags::THEIR_COMMITMENT_SIGNED_SENT) =>
            {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Unable to process commitment_signed message in state {:?}, as we have already received our commitment_signed message.",
                    &self.state
                )));
            }
            ChannelState::SigningCommitment(flags) => {
                debug!(
                    "Processing commitment_signed command in from SigningCommitment state {:?}",
                    &self.state
                );
                CommitmentSignedFlags::SigningCommitment(flags)
            }
            // TODO: There is a chance that both parties unknowingly send commitment_signed messages,
            // and both of they thought they are the first one to send the message and fail the other
            // party's message. What to do in this case?
            ChannelState::ChannelReady(flags)
                if flags.contains(ChannelReadyFlags::AWAITING_REMOTE_REVOKE) =>
            {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Unable to process commitment_signed message in state {:?}, as we are have already sent a commitment_signed, the remote should have sent a revoke message first.",
                    &self.state
                )));
            }
            ChannelState::ChannelReady(flags) => {
                debug!(
                    "Processing commitment_signed command in from ChannelReady state {:?}",
                    &self.state
                );
                CommitmentSignedFlags::ChannelReady(flags)
            }
            _ => {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Unable to send commitment signed message in state {:?}",
                    &self.state
                )));
            }
        };

        let tx = self.build_and_verify_commitment_tx(commitment_signed.partial_signature)?;

        // Try to create an transaction which spends the commitment transaction, to
        // verify that our code actually works.
        if is_testing() {
            let output_lock_script = get_always_success_script(b"whatever");

            let commitment_tx = self.verify_and_complete_tx(commitment_signed.partial_signature)?;

            println!("The complete commitment transaction is {:?}", commitment_tx);
            dbg!(
                commitment_tx.hash(),
                commitment_tx.cell_deps(),
                commitment_tx.inputs(),
                commitment_tx.outputs(),
                commitment_tx.witnesses()
            );

            let context = get_commitment_lock_context().write().unwrap();
            let context = &mut context.context.clone();

            let revocation_keys = [
                "8172cbf168dcb988d2849ea229603f843614a038e1baa83783aee2f9aeac32ea",
                "e16a04c9d5d3d80bc0bb03c19dceddfc34a5017a885a57b9316bd9944022a088",
            ]
            .iter()
            .map(|x| hex::decode(x).unwrap())
            .collect::<Vec<_>>();
            dbg!(&revocation_keys);

            for local in [true, false] {
                // Use the second output as an input to the new transaction.
                // The first output is an immediate spendable output,
                // while the second output is a delayed output locked by commitment lock.
                let commitment_lock_index = 1;
                let commitment_out_point = &commitment_tx.output_pts()[commitment_lock_index];
                dbg!("commitment_out_point: {:?}", commitment_out_point);
                let commitment_out_point = commitment_tx
                    .output_pts()
                    .get(commitment_lock_index)
                    .unwrap()
                    .clone();
                let (commitment_lock_cell, commitment_lock_cell_data) = commitment_tx
                    .output_with_data(commitment_lock_index)
                    .unwrap();
                dbg!("inputs to new_tx saved: {:?}", &commitment_tx);
                dbg!("outpoint: {:?}", &commitment_out_point);
                dbg!("Cell: {:?}", &commitment_lock_cell);
                context.create_cell_with_out_point(
                    commitment_out_point.clone(),
                    commitment_lock_cell,
                    commitment_lock_cell_data,
                );
                // We can verify revocation tx works by
                // verify the funding tx, commitment tx and revocation tx verify successfully.
                dbg!(
                    "add the funding tx to test_verify_fixed_tx to verify our construction works",
                    &self.funding_tx
                );
                dbg!("add the commitment tx to test_verify_fixed_tx to verify our construction works", &commitment_tx);

                let input = CellInput::new_builder()
                    .previous_output(commitment_out_point.clone())
                    .build();

                dbg!("input: {:?}", &input);
                let commitment_output_lock_script = commitment_tx
                    .output_with_data(commitment_lock_index)
                    .unwrap()
                    .0
                    .lock();
                let revocation_tx = TransactionBuilder::default()
                    .cell_deps(commitment_tx.cell_deps().clone())
                    .inputs(vec![input])
                    .outputs(vec![CellOutput::new_builder()
                        .capacity(20.pack())
                        .lock(output_lock_script.clone())
                        .build()])
                    .outputs_data(vec![Default::default()])
                    .build();
                dbg!(
                    "Built spending transaction with cell deps and inputs: {:?}",
                    &revocation_tx
                );
                let message: [u8; 32] = revocation_tx.hash().as_slice().try_into().unwrap();
                dbg!("message: {:?}", &message);
                let (key, witness_script) = revocation_keys
                    .iter()
                    .filter_map(|key| {
                        let witness_script =
                            self.build_previous_commitment_transaction_witnesses(local);
                        dbg!(
                            "witness of previous commitment transaction",
                            hex::encode(&witness_script)
                        );
                        let witness_hash = blake2b_256(&witness_script);
                        // We have two asymmetrical transactions, we only use the one that has the matching hash.
                        dbg!("witness_hash: {:?}", &witness_hash[..20]);
                        dbg!(
                            "commitment_output_lock_script.args(): {:?}",
                            commitment_output_lock_script.args().as_slice()
                        );
                        if commitment_output_lock_script.args().as_slice() == &witness_hash[..20] {
                            Some((key, witness_script))
                        } else {
                            None
                        }
                    })
                    .next()
                    .expect("Must have one matching key");
                let signature = ckb_crypto::secp::Privkey::from_slice(key)
                    .sign_recoverable(&message.into())
                    .unwrap()
                    .serialize();
                let witness = [witness_script.clone(), vec![0xFF], signature].concat();

                let revocation_tx = revocation_tx
                    .as_advanced_builder()
                    .witnesses(vec![witness.pack()])
                    .build();

                dbg!("add the revocation tx to test_verify_fixed_tx to verify our construction works", &revocation_tx);

                dbg!(
                    "Verifying spending transaction of commitment tx: {:?}",
                    &revocation_tx
                );

                let result = context.verify_tx(&revocation_tx, 10_000_000);
                dbg!(&result);
                assert!(result.is_ok());
            }
        }

        debug!(
            "Successfully handled commitment signed message: {:?}, tx: {:?}",
            &commitment_signed, &tx
        );

        debug!("Updating peer next local nonce");
        self.counterparty_nonce = Some(commitment_signed.next_local_nonce);
        match flags {
            CommitmentSignedFlags::SigningCommitment(flags) => {
                let flags = flags | SigningCommitmentFlags::THEIR_COMMITMENT_SIGNED_SENT;
                self.state = ChannelState::SigningCommitment(flags);
                self.maybe_transition_to_tx_signatures(flags, network)?;
            }
            CommitmentSignedFlags::ChannelReady(_) => {
                // Now we should revoke previous transation by revealing preimage.
                let revocation_preimage = self
                    .signer
                    .get_commitment_secret(self.holder_commitment_number - 1);
                debug!(
                    "Revealing preimage for revocation: {:?}",
                    &revocation_preimage
                );
                self.holder_commitment_number = self.get_next_commitment_number(true);
                let next_commitment_point = self.get_current_holder_commitment_point();
                network
                    .send_message(NetworkActorMessage::new_command(
                        NetworkActorCommand::SendPcnMessage(PCNMessageWithPeerId {
                            peer_id: self.peer_id.clone(),
                            message: PCNMessage::RevokeAndAck(RevokeAndAck {
                                channel_id: self.get_id(),
                                per_commitment_secret: revocation_preimage.into(),
                                next_per_commitment_point: next_commitment_point,
                            }),
                        }),
                    ))
                    .expect("network actor alive");
                self.state = ChannelState::ChannelReady(ChannelReadyFlags::empty());
            }
        }
        Ok(())
    }

    pub fn maybe_transition_to_tx_signatures(
        &mut self,
        flags: SigningCommitmentFlags,
        network: ActorRef<NetworkActorMessage>,
    ) -> ProcessingChannelResult {
        if flags.contains(SigningCommitmentFlags::COMMITMENT_SIGNED_SENT) {
            debug!("Commitment signed message sent by both sides, tranitioning to AwaitingTxSignatures state");
            self.state = ChannelState::AwaitingTxSignatures(AwaitingTxSignaturesFlags::empty());
            if self.should_holder_send_tx_signatures_first() {
                debug!("It is our turn to send tx_signatures, so we will do it now.");
                self.handle_tx_signatures(network, None)?;
            }
        }
        Ok(())
    }

    // TODO: currently witnesses in the tx_signatures molecule message are a list of bytes.
    // It is unclear how can we compose two partial sets witnesses into a complete
    // set of witnesses.
    pub fn handle_tx_signatures(
        &mut self,
        network: ActorRef<NetworkActorMessage>,
        // If partial_witnesses is given, then it is the counterparty that send a message
        // to us, and we must combine them to make a full list of witnesses.
        // Otherwise, we are the one who is to start send the tx_signatures.
        // We can just create a partial set of witnesses, and sent them to the peer.
        partial_witnesses: Option<Vec<Vec<u8>>>,
    ) -> ProcessingChannelResult {
        let flags = match self.state {
            ChannelState::AwaitingTxSignatures(flags)
                if flags.contains(AwaitingTxSignaturesFlags::THEIR_TX_SIGNATURES_SENT)
                    && partial_witnesses.is_some() =>
            {
                return Err(ProcessingChannelError::RepeatedProcessing(format!(
                    "tx_signatures partial witnesses {:?}",
                    partial_witnesses.unwrap()
                )));
            }
            ChannelState::AwaitingTxSignatures(flags)
                if flags.contains(AwaitingTxSignaturesFlags::OUR_TX_SIGNATURES_SENT)
                    && partial_witnesses.is_none() =>
            {
                return Err(ProcessingChannelError::RepeatedProcessing(
                    "We have already sent our tx_signatures".to_string(),
                ));
            }
            ChannelState::SigningCommitment(flags)
                if flags.contains(SigningCommitmentFlags::COMMITMENT_SIGNED_SENT) =>
            {
                AwaitingTxSignaturesFlags::empty()
            }
            ChannelState::AwaitingTxSignatures(flags) => flags,
            _ => {
                return Err(ProcessingChannelError::InvalidState(format!(
                    "Unable to build and sign funding tx in state {:?}",
                    &self.state
                )));
            }
        };

        let flags = if partial_witnesses.is_some() {
            flags | AwaitingTxSignaturesFlags::THEIR_TX_SIGNATURES_SENT
        } else {
            flags
        };
        self.state = ChannelState::AwaitingTxSignatures(flags);

        let funding_tx = self
            .funding_tx
            .as_ref()
            .ok_or(ProcessingChannelError::InvalidState(
                "Funding transaction is not present".to_string(),
            ))?;

        let msg = match partial_witnesses {
            Some(ref _partial_witnesses) => {
                // TODO: filling the whole witnesses here.
                let full_witnesses: Vec<ckb_types::packed::Bytes> = vec![];
                let full_witnesses_u8 = full_witnesses
                    .iter()
                    .map(|w| w.as_slice().to_vec())
                    .collect();

                let funding_tx = funding_tx
                    .as_advanced_builder()
                    .set_witnesses(full_witnesses)
                    .build();
                self.funding_tx = Some(funding_tx.clone());
                self.state = ChannelState::AwaitingTxSignatures(
                    flags | AwaitingTxSignaturesFlags::OUR_TX_SIGNATURES_SENT,
                );

                // Since we have received a valid tx_signatures message, we're now sure that
                // we can broadcast a valid transaction to the network, i.e. we can wait for
                // the funding transaction to be confirmed.
                network
                    .send_message(NetworkActorMessage::new_event(
                        NetworkActorEvent::FundingTransactionPending(
                            Transaction::default(),
                            self.get_funding_transaction_outpoint(),
                            self.get_id(),
                        ),
                    ))
                    .expect("network actor alive");

                PCNMessageWithPeerId {
                    peer_id: self.peer_id.clone(),
                    message: PCNMessage::TxSignatures(TxSignatures {
                        channel_id: self.get_id(),
                        witnesses: full_witnesses_u8,
                        tx_hash: funding_tx.hash().into(),
                    }),
                }
            }
            None => {
                // TODO: creating partial witnesses here.
                let partial_witnesses = vec![];
                self.state = ChannelState::AwaitingTxSignatures(
                    flags | AwaitingTxSignaturesFlags::OUR_TX_SIGNATURES_SENT,
                );

                PCNMessageWithPeerId {
                    peer_id: self.peer_id.clone(),
                    message: PCNMessage::TxSignatures(TxSignatures {
                        channel_id: self.get_id(),
                        witnesses: partial_witnesses,
                        tx_hash: funding_tx.hash().into(),
                    }),
                }
            }
        };
        debug!(
            "Handled tx_signatures, peer: {:?}, previous witnesses: {:?}, messge to send: {:?}",
            &self.peer_id, &partial_witnesses, &msg
        );
        network
            .send_message(NetworkActorMessage::new_command(
                NetworkActorCommand::SendPcnMessage(msg),
            ))
            .expect("network actor alive");
        Ok(())
    }

    pub fn handle_revoke_and_ack_message(
        &mut self,
        revoke_and_ack: RevokeAndAck,
        _network: ActorRef<NetworkActorMessage>,
    ) -> ProcessingChannelResult {
        let RevokeAndAck {
            channel_id: _,
            per_commitment_secret,
            next_per_commitment_point,
        } = revoke_and_ack;
        let per_commitment_point = self.get_current_counterparty_commitment_point();
        if per_commitment_point != Privkey::from(per_commitment_secret).pubkey() {
            return Err(ProcessingChannelError::InvalidParameter(
                "Invalid per_commitment_secret".to_string(),
            ));
        }
        self.counterparty_commitment_points
            .push(next_per_commitment_point);
        self.counterparty_commitment_number = self.get_next_commitment_number(false);
        Ok(())
    }

    pub fn update_funding_tx(&mut self, tx: Transaction) -> ProcessingChannelResult {
        // TODO: check if the tx is valid
        let tx = tx.into_view();
        let _ = tx
            .outputs()
            .get(0)
            .ok_or(ProcessingChannelError::InvalidParameter(
                "Funding transaction should have at least one output".to_string(),
            ))?;
        debug!("Updating funding transaction to {:?}", &tx);
        self.funding_tx = Some(tx);
        Ok(())
    }

    // TODO: More checks to the funding tx.
    fn check_tx_complete_preconditions(&mut self) -> ProcessingChannelResult {
        match self.funding_tx.as_ref() {
            None => {
                return Err(ProcessingChannelError::InvalidState(
                    "Received TxComplete message without a funding transaction".to_string(),
                ));
            }
            Some(tx) => {
                debug!(
                    "Received TxComplete message, funding tx is present {:?}",
                    tx
                );
                let first_output =
                    tx.outputs()
                        .get(0)
                        .ok_or(ProcessingChannelError::InvalidParameter(
                            "Funding transaction should have at least one output".to_string(),
                        ))?;

                let first_output_capacity =
                    u64::from_le_bytes(first_output.capacity().as_slice().try_into().unwrap())
                        as u128;

                if self.to_self_amount + self.to_remote_amount != first_output_capacity {
                    return Err(ProcessingChannelError::InvalidParameter(
                        format!("Funding transaction output amount mismatch ({} given, {} to self , {} to remote)", first_output_capacity, self.to_self_amount, self.to_remote_amount) 
                    ));
                }

                // Just create a cell with the same capacity as the first output of the funding tx.
                // This is to test the validity of the commitment tx that spends the funding tx.
                if is_testing() {
                    let always_success = get_always_success_script(b"funding transaction test");
                    let mut context = get_commitment_lock_context().write().unwrap();
                    let context = &mut context.context;
                    let funding_tx = Transaction::default()
                        .as_advanced_builder()
                        .outputs([CellOutput::new_builder()
                            .capacity(first_output.capacity())
                            .lock(always_success)
                            .build()])
                        .outputs_data(vec![Default::default()])
                        .build();
                    dbg!("funding_tx before completion: {:?}", &funding_tx);
                    let funding_tx = context.complete_tx(funding_tx);

                    dbg!("funding_tx after completion: {:?}", &funding_tx);

                    let result = context.verify_tx(&funding_tx, 10_000_000);
                    dbg!(&result);
                    assert!(result.is_ok());

                    // Save this transaction so that we can find it later.
                    let outpoint = funding_tx.output_pts().get(0).unwrap().clone();
                    let (cell, cell_data) = funding_tx.output_with_data(0).unwrap();
                    dbg!("funding_tx saved: {:?}", &funding_tx);
                    dbg!("outpoint: {:?}", &outpoint);
                    dbg!("Cell: {:?}", &cell);
                    context.create_cell_with_out_point(outpoint, cell, cell_data);
                    self.funding_tx = Some(funding_tx);
                }
            }
        }
        Ok(())
    }

    pub fn fill_in_channel_id(&mut self) {
        assert!(self.id.is_none(), "Channel id is already filled in");
        assert!(
            self.counterparty_channel_parameters.is_some(),
            "Counterparty pubkeys is required to derive actual channel id"
        );
        let counterparty_revocation = &self
            .get_counterparty_channel_parameters()
            .pubkeys
            .revocation_base_key;
        let holder_revocation = &self
            .get_holder_channel_parameters()
            .pubkeys
            .revocation_base_key;
        let channel_id =
            derive_channel_id_from_revocation_keys(holder_revocation, counterparty_revocation);

        self.id = Some(channel_id);
        debug!(
            "Channel Id changed from {:?} to {:?}",
            &self.temp_id,
            &self.id.unwrap()
        );
    }

    // Whose pubkey should go first in musig2?
    // We define a definitive order for the pubkeys in musig2 to makes it easier
    // to aggregate musig2 signatures.
    fn should_holder_go_first_in_musig2(&self) -> bool {
        let holder_pubkey = self.get_holder_channel_parameters().pubkeys.funding_pubkey;
        let counterparty_pubkey = self
            .get_counterparty_channel_parameters()
            .pubkeys
            .funding_pubkey;
        holder_pubkey <= counterparty_pubkey
    }

    // Order some items (like pubkey and nonce) from holders and counterparty in musig2.
    fn order_things_for_musig2<T>(&self, holder: T, counterparty: T) -> [T; 2] {
        if self.should_holder_go_first_in_musig2() {
            [holder, counterparty]
        } else {
            [counterparty, holder]
        }
    }

    // Should we (also called holder) send tx_signatures first?
    // In order to avoid deadlock, we need to define an order for sending tx_signatures.
    // Currently the order of sending tx_signatures is defined as follows:
    // If the amount to self is less than the amount to remote, then we should send,
    // else if the amount to self is equal to the amount to remote and we have
    // smaller funding_pubkey, then we should send first. Otherwise, we should wait
    // the counterparty to send tx_signatures first.
    fn should_holder_send_tx_signatures_first(&self) -> bool {
        self.to_self_amount < self.to_remote_amount
            || self.to_self_amount == self.to_remote_amount
                && self.should_holder_go_first_in_musig2()
    }

    // TODO: the parameter with type Script is not enough to build a valid
    // ckb transaction (we also need cell deps, witnesses, etc.).
    pub fn build_shutdown_tx(
        &self,
        _holder_shutdown_script: &Script,
        _counterparty_shutdown_script: &Script,
    ) -> TransactionView {
        let tx_builder = TransactionBuilder::default()
            .cell_deps(self.get_commitment_transaction_cell_deps())
            .input(
                CellInput::new_builder()
                    .previous_output(self.get_funding_transaction_outpoint())
                    .build(),
            );

        // TODO: Check UDT type, if it is ckb then convert it into u64.
        let holder_value = self.to_self_amount as u64;
        let counterparty_value = self.to_remote_amount as u64;
        let holder_output = CellOutput::new_builder()
            .capacity(holder_value.pack())
            .lock(self.get_holder_shutdown_script().clone())
            .build();
        let counterparty_output = CellOutput::new_builder()
            .capacity(counterparty_value.pack())
            .lock(self.get_counterparty_shutdown_script().clone())
            .build();
        let outputs = self.order_things_for_musig2(holder_output, counterparty_output);
        let tx_builder = tx_builder.set_outputs(outputs.to_vec());
        tx_builder.build()
    }

    pub fn build_and_sign_shutdown_tx(
        &self,
        holder_shutdown_script: &Script,
        counterparty_shutdown_script: &Script,
    ) -> Result<PartialSignature, ProcessingChannelError> {
        let tx = self.build_shutdown_tx(holder_shutdown_script, counterparty_shutdown_script);
        let message = get_tx_message_to_sign(&tx);

        let sign_ctx = Musig2SignContext::from(self);
        let signature = sign_ctx.sign(message.as_slice())?;
        debug!(
            "Signed shutdown tx ({:?}) message {:?} with signature {:?}",
            &tx, &message, &signature,
        );
        Ok(signature)
    }

    pub fn build_and_verify_shutdown_tx(
        &self,
        holder_shutdown_script: &Script,
        counterparty_shutdown_script: &Script,
        signature: PartialSignature,
    ) -> bool {
        let tx = self.build_shutdown_tx(holder_shutdown_script, counterparty_shutdown_script);
        let message = get_tx_message_to_sign(&tx);
        let verify_ctx = Musig2VerifyContext::from(self);
        let result = verify_ctx.verify(signature, message.as_slice());
        debug!(
            "Verified commitment tx ({:?}) message {:?} with signature {:?}",
            &tx, &message, &result,
        );
        result.is_ok()
    }

    pub fn build_tx_creation_keys(&self, local: bool, commitment_number: u64) -> TxCreationKeys {
        debug!(
            "Building commitment transaction #{} for {}",
            commitment_number,
            if local { "us" } else { "them" },
        );
        let (
            broadcaster,
            countersignatory,
            broadcaster_commitment_point,
            countersignatory_commitment_point,
        ) = if local {
            (
                self.get_holder_channel_parameters(),
                self.get_counterparty_channel_parameters(),
                self.get_holder_commitment_point(commitment_number),
                self.get_counterparty_commitment_point(commitment_number),
            )
        } else {
            (
                self.get_counterparty_channel_parameters(),
                self.get_holder_channel_parameters(),
                self.get_counterparty_commitment_point(commitment_number),
                self.get_holder_commitment_point(commitment_number),
            )
        };
        let tx_creation_keys = TxCreationKeys {
            broadcaster_delayed_payment_key: derive_delayed_payment_pubkey(
                broadcaster.delayed_payment_base_key(),
                &broadcaster_commitment_point,
            ),
            countersignatory_payment_key: derive_payment_pubkey(
                countersignatory.payment_base_key(),
                &countersignatory_commitment_point,
            ),
            countersignatory_revocation_key: derive_revocation_pubkey(
                countersignatory.revocation_base_key(),
                &countersignatory_commitment_point,
            ),
            broadcaster_tlc_key: derive_tlc_pubkey(
                broadcaster.tlc_base_key(),
                &broadcaster_commitment_point,
            ),
            countersignatory_tlc_key: derive_tlc_pubkey(
                countersignatory.tlc_base_key(),
                &countersignatory_commitment_point,
            ),
        };
        tx_creation_keys
    }

    pub fn build_commitment_tx(&self, local: bool) -> TransactionView {
        let input = self.get_funding_transaction_outpoint();
        let tx_builder = TransactionBuilder::default()
            .cell_deps(self.get_commitment_transaction_cell_deps())
            .input(CellInput::new_builder().previous_output(input).build());

        let (outputs, outputs_data) = self.build_commitment_transaction_outputs(local);
        debug!("Built outputs for commitment transaction: {:?}", &outputs);
        let tx_builder = tx_builder.set_outputs(outputs);
        let tx_builder = tx_builder.set_outputs_data(outputs_data);
        tx_builder.build()
    }

    fn build_current_commitment_transaction_witnesses(&self, local: bool) -> Vec<u8> {
        let commitment_number = if local {
            self.holder_commitment_number
        } else {
            self.counterparty_commitment_number
        };
        self.build_commitment_transaction_witnesses(local, commitment_number)
    }

    fn build_previous_commitment_transaction_witnesses(&self, local: bool) -> Vec<u8> {
        let commitment_number = if local {
            self.holder_commitment_number - 1
        } else {
            self.counterparty_commitment_number - 1
        };
        self.build_commitment_transaction_witnesses(local, commitment_number)
    }

    // We need this function both for building new commitment transaction and revoking old commitment transaction.
    fn build_commitment_transaction_witnesses(
        &self,
        local: bool,
        commitment_number: u64,
    ) -> Vec<u8> {
        let (delayed_epoch, delayed_payment_key, revocation_key) = {
            dbg!(
                self.get_current_holder_commitment_point(),
                self.get_current_counterparty_commitment_point()
            );
            let (delay, commitment_point, base_delayed_payment_key, base_revocation_key) = if local
            {
                (
                    self.get_holder_channel_parameters().selected_contest_delay,
                    self.get_holder_commitment_point(commitment_number),
                    self.get_holder_channel_parameters()
                        .delayed_payment_base_key(),
                    self.get_holder_channel_parameters().revocation_base_key(),
                )
            } else {
                (
                    self.get_counterparty_channel_parameters()
                        .selected_contest_delay,
                    self.get_counterparty_commitment_point(commitment_number),
                    self.get_counterparty_channel_parameters()
                        .delayed_payment_base_key(),
                    self.get_counterparty_channel_parameters()
                        .revocation_base_key(),
                )
            };
            dbg!(base_delayed_payment_key, base_revocation_key);
            (
                delay,
                derive_delayed_payment_pubkey(base_delayed_payment_key, &commitment_point),
                derive_revocation_pubkey(base_revocation_key, &commitment_point),
            )
        };

        // Build a sorted array of TLC so that both party can generate the same commitment transaction.
        // TODO: we actually need a historical snapshot of tlcs for the commitment number.
        // This currently only works for the current commitment number.
        let tlcs = {
            let (mut received_tlcs, mut offered_tlcs) = (
                self.pending_received_tlcs
                    .values()
                    .cloned()
                    .collect::<Vec<_>>(),
                self.pending_offered_tlcs
                    .values()
                    .cloned()
                    .collect::<Vec<_>>(),
            );
            for tlc in received_tlcs.iter_mut().chain(offered_tlcs.iter_mut()) {
                tlc.fill_in_pubkeys(local, &self);
            }
            let (mut a, mut b) = if local {
                (received_tlcs, offered_tlcs)
            } else {
                for tlc in received_tlcs.iter_mut().chain(offered_tlcs.iter_mut()) {
                    // Need to flip these fields for the counterparty.
                    tlc.is_offered = !tlc.is_offered;
                    (tlc.local_payment_key_hash, tlc.remote_payment_key_hash) =
                        (tlc.remote_payment_key_hash, tlc.local_payment_key_hash);
                }
                (offered_tlcs, received_tlcs)
            };
            a.sort_by(|x, y| x.id.cmp(&y.id));
            b.sort_by(|x, y| x.id.cmp(&y.id));
            [a, b].concat()
        };

        let delayed_payment_key_hash = blake2b_256(delayed_payment_key.serialize());
        let revocation_key_hash = blake2b_256(revocation_key.serialize());

        dbg!(
            &tlcs,
            local,
            delayed_payment_key,
            hex::encode(&delayed_payment_key_hash[..20]),
            revocation_key,
            hex::encode(&revocation_key_hash[..20])
        );
        let witnesses: Vec<u8> = [
            (Since::from(delayed_epoch).value()).to_le_bytes().to_vec(),
            blake2b_256(delayed_payment_key.serialize())[0..20].to_vec(),
            blake2b_256(revocation_key.serialize())[0..20].to_vec(),
            tlcs.iter()
                .map(|tlc| tlc.serialize_to_lock_args())
                .flatten()
                .collect(),
        ]
        .concat();
        witnesses
    }

    fn build_commitment_transaction_outputs(&self, local: bool) -> (Vec<CellOutput>, Vec<Bytes>) {
        let (to_broadcaster_value, to_countersignatory_value) =
            self.get_broadcaster_and_countersignatory_amounts(local);

        // The to_broadcaster_value is amount of immediately spendable assets of the broadcaster.
        // In the commitment transaction, we need also to include the amount of the TLCs.
        let to_broadcaster_value = to_broadcaster_value as u64
            + self
                .get_all_tlcs()
                .iter()
                .map(|tlc| tlc.amount)
                .sum::<u128>() as u64;

        let immediate_payment_key = {
            let (commitment_point, base_payment_key) = if local {
                (
                    self.get_current_counterparty_commitment_point(),
                    self.get_counterparty_channel_parameters()
                        .payment_base_key(),
                )
            } else {
                (
                    self.get_current_holder_commitment_point(),
                    self.get_holder_channel_parameters().payment_base_key(),
                )
            };
            derive_payment_pubkey(base_payment_key, &commitment_point)
        };

        let witnesses: Vec<u8> = self.build_current_commitment_transaction_witnesses(local);

        let hash = blake2b_256(&witnesses);
        dbg!(
            "witness in host",
            hex::encode(&witnesses),
            "witness hash",
            hex::encode(&hash[..20])
        );
        let secp256k1_lock_script =
            get_secp256k1_lock_script(&blake2b_256(immediate_payment_key.serialize())[0..20]);
        let commitment_lock_script = get_commitment_lock_script(&blake2b_256(witnesses)[0..20]);

        let outputs = vec![
            CellOutput::new_builder()
                .capacity((to_countersignatory_value as u64).pack())
                .lock(secp256k1_lock_script)
                .build(),
            CellOutput::new_builder()
                .capacity((to_broadcaster_value as u64).pack())
                .lock(commitment_lock_script)
                .build(),
        ];
        let outputs_data = vec![Bytes::default(); outputs.len()];

        (outputs, outputs_data)
    }

    pub fn build_and_verify_commitment_tx(
        &self,
        signature: PartialSignature,
    ) -> Result<PartiallySignedCommitmentTransaction, ProcessingChannelError> {
        let verify_ctx = Musig2VerifyContext::from(self);

        let tx = self.build_commitment_tx(false);
        let message = get_tx_message_to_sign(&tx);
        debug!(
            "Verifying partial signature ({:?}) of commitment tx ({:?}) message {:?}",
            &signature, &tx, &message
        );
        verify_ctx.verify(signature, message.as_slice())?;
        Ok(PartiallySignedCommitmentTransaction { tx, signature })
    }

    pub fn build_and_sign_commitment_tx(
        &self,
    ) -> Result<PartiallySignedCommitmentTransaction, ProcessingChannelError> {
        let sign_ctx = Musig2SignContext::from(self);

        let tx = self.build_commitment_tx(true);
        let message = get_tx_message_to_sign(&tx);

        let signature = sign_ctx.sign(message.as_slice())?;
        debug!(
            "Signed commitment tx ({:?}) message {:?} with signature {:?}",
            &tx, &message, &signature,
        );

        Ok(PartiallySignedCommitmentTransaction { tx, signature })
    }

    /// Verify the partial signature from the peer and create a complete transaction
    /// with valid witnesses.
    pub fn verify_and_complete_tx(
        &self,
        signature: PartialSignature,
    ) -> Result<TransactionView, ProcessingChannelError> {
        let tx = self.build_and_verify_commitment_tx(signature)?;
        assert_eq!(tx.tx, self.build_commitment_tx(false));
        dbg!(
            "verify_and_complete_tx build_and_verify_commitment_tx tx: {:?}",
            &tx.tx,
            tx.tx.hash(),
            tx.tx.cell_deps(),
            tx.tx.inputs(),
            tx.tx.outputs(),
            tx.tx.witnesses()
        );
        let sign_ctx = Musig2SignContext::from(self);

        let message = get_tx_message_to_sign(&tx.tx);

        let signature2 = sign_ctx.sign(message.as_slice())?;
        debug!(
            "Signed commitment tx ({:?}) message {:?} with signature {:?}",
            &tx, &message, &signature2,
        );

        let signatures = self.order_things_for_musig2(signature, signature2);
        let tx = aggregate_partial_signatures_for_tx(
            tx.tx,
            Musig2VerifyContext::from(&*self),
            signatures,
        )
        .expect("The validity of the signatures verified");
        dbg!("verify_and_complete_tx tx: {:?}", &tx);
        Ok(tx)
    }
}

/// The commitment transactions are the transaction that each parties holds to
/// spend the funding transaction and create a new partition of the channel
/// balance between each parties. This struct contains all the information
/// that we need to build the actual CKB transaction.
/// Note that these commitment transactions are asymmetrical,
/// meaning that counterparties have different resulting CKB transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitmentTransaction {
    // The input to the commitment transactions,
    // always the first output of the  funding transaction.
    pub input: OutPoint,
    // Will change after each commitment to derive new commitment secrets.
    // Currently always 0.
    pub commitment_number: u64,
    // The broadcaster's balance, may be spent after timelock by the broadcaster or
    // by the countersignatory with revocation key.
    pub to_broadcaster_value: u128,
    // The countersignatory's balance, may be spent immediately by the countersignatory.
    pub to_countersignatory_value: u128,
    // The list of TLC commitmentments. These outputs are already multisiged to another
    // set of transactions, whose output may be spent by countersignatory with revocation key,
    // the original sender after delay, or the receiver if they has correct preimage,
    pub tlcs: Vec<TLCOutputInCommitment>,
    // A cache of the parties' pubkeys required to construct the transaction.
    pub keys: TxCreationKeys,
}

/// A wrapper on CommitmentTransaction that has a partial signature along with
/// the ckb transaction.
#[derive(Clone, Debug)]
pub struct PartiallySignedCommitmentTransaction {
    tx: TransactionView,
    signature: PartialSignature,
}

pub struct Musig2Context {
    pub key_agg_ctx: KeyAggContext,
    pub agg_nonce: AggNonce,
    pub holder_seckey: Privkey,
    pub holder_secnonce: SecNonce,
    pub counterparty_pubkey: Pubkey,
    pub counterparty_pubnonce: PubNonce,
}

impl Musig2Context {
    pub fn split(self) -> (Musig2SignContext, Musig2VerifyContext) {
        let Musig2Context {
            key_agg_ctx,
            agg_nonce,
            holder_seckey,
            holder_secnonce,
            counterparty_pubkey,
            counterparty_pubnonce,
        } = self;
        (
            Musig2SignContext {
                key_agg_ctx: key_agg_ctx.clone(),
                agg_nonce: agg_nonce.clone(),
                seckey: holder_seckey,
                secnonce: holder_secnonce,
            },
            Musig2VerifyContext {
                key_agg_ctx,
                agg_nonce,
                pubkey: counterparty_pubkey,
                pubnonce: counterparty_pubnonce,
            },
        )
    }
}

pub struct Musig2VerifyContext {
    pub key_agg_ctx: KeyAggContext,
    pub agg_nonce: AggNonce,
    pub pubkey: Pubkey,
    pub pubnonce: PubNonce,
}

impl Musig2VerifyContext {
    pub fn verify(&self, signature: PartialSignature, message: &[u8]) -> ProcessingChannelResult {
        Ok(verify_partial(
            &self.key_agg_ctx,
            signature,
            &self.agg_nonce,
            self.pubkey,
            &self.pubnonce,
            message,
        )?)
    }
}
pub struct Musig2SignContext {
    key_agg_ctx: KeyAggContext,
    agg_nonce: AggNonce,
    seckey: Privkey,
    secnonce: SecNonce,
}

impl Musig2SignContext {
    pub fn sign(self, message: &[u8]) -> Result<PartialSignature, ProcessingChannelError> {
        Ok(sign_partial(
            &self.key_agg_ctx,
            self.seckey,
            self.secnonce,
            &self.agg_nonce,
            message,
        )?)
    }
}

fn is_testing() -> bool {
    true
}

// TODO: fill in the actual lock script.
fn get_commitment_lock_script(args: &[u8]) -> Script {
    if is_testing() {
        return super::temp::get_commitment_lock_script(args);
    } else {
        Script::new_builder().args(args.pack()).build()
    }
}

// TODO: fill in the actual lock script.
fn get_secp256k1_lock_script(args: &[u8]) -> Script {
    // Just need a script that can be found by ckb to pass the test.
    // So we use commitment lock script instead of a non-existing secp256k1 lock script.
    get_always_success_script(args)
}

fn get_commitment_lock_outpoint() -> OutPoint {
    if is_testing() {
        let out_point = super::temp::get_commitment_lock_outpoint();
        debug!("Using commitment lock outpoint {:?}", &out_point);
        return out_point;
    }

    // TODO: Use real commitment lock outpoint here.
    let commitment_lock_outpoint = OutPoint::new_builder()
        .tx_hash(Byte32::zero())
        .index(0u32.pack())
        .build();

    commitment_lock_outpoint
}

fn get_tx_message_to_sign(tx: &TransactionView) -> Byte32 {
    let hash = tx.hash();
    hash
}

pub fn aggregate_partial_signatures_for_tx(
    tx: TransactionView,
    verify_ctx: Musig2VerifyContext,
    partial_signatures: [PartialSignature; 2],
) -> Result<TransactionView, ProcessingChannelError> {
    debug!("Aggregating partial signatures for tx {:?}", &tx);

    let message = get_tx_message_to_sign(&tx);
    let signature: CompactSignature = aggregate_partial_signatures(
        &verify_ctx.key_agg_ctx,
        &verify_ctx.agg_nonce,
        partial_signatures,
        message.as_bytes(),
    )?;

    // TODO: check the witness format of commitment transaction.
    Ok(tx
        .as_advanced_builder()
        .set_witnesses(vec![signature.to_bytes().pack()])
        .build())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelParametersOneParty {
    pub pubkeys: ChannelBasePublicKeys,
    pub selected_contest_delay: LockTime,
}

impl ChannelParametersOneParty {
    pub fn funding_pubkey(&self) -> &Pubkey {
        &self.pubkeys.funding_pubkey
    }

    pub fn payment_base_key(&self) -> &Pubkey {
        &self.pubkeys.payment_base_key
    }

    pub fn delayed_payment_base_key(&self) -> &Pubkey {
        &self.pubkeys.delayed_payment_base_key
    }

    pub fn revocation_base_key(&self) -> &Pubkey {
        &self.pubkeys.revocation_base_key
    }

    pub fn tlc_base_key(&self) -> &Pubkey {
        &self.pubkeys.tlc_base_key
    }
}

/// One counterparty's public keys which do not change over the life of a channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelBasePublicKeys {
    /// The public key which is used to sign all commitment transactions, as it appears in the
    /// on-chain channel lock-in 2-of-2 multisig output.
    pub funding_pubkey: Pubkey,
    /// The base point which is used (with derive_public_revocation_key) to derive per-commitment
    /// revocation keys. This is combined with the per-commitment-secret generated by the
    /// counterparty to create a secret which the counterparty can reveal to revoke previous
    /// states.
    pub revocation_base_key: Pubkey,
    /// The public key on which the non-broadcaster (ie the countersignatory) receives an immediately
    /// spendable primary channel balance on the broadcaster's commitment transaction. This key is
    /// static across every commitment transaction.
    pub payment_base_key: Pubkey,
    /// The base point which is used (with derive_public_key) to derive a per-commitment payment
    /// public key which receives non-HTLC-encumbered funds which are only available for spending
    /// after some delay (or can be claimed via the revocation path).
    pub delayed_payment_base_key: Pubkey,
    /// The base point which is used (with derive_public_key) to derive a per-commitment public key
    /// which is used to encumber HTLC-in-flight outputs.
    pub tlc_base_key: Pubkey,
}

impl From<&OpenChannel> for ChannelBasePublicKeys {
    fn from(value: &OpenChannel) -> Self {
        ChannelBasePublicKeys {
            funding_pubkey: value.funding_pubkey,
            revocation_base_key: value.revocation_basepoint,
            payment_base_key: value.payment_basepoint,
            delayed_payment_base_key: value.delayed_payment_basepoint,
            tlc_base_key: value.tlc_basepoint,
        }
    }
}

impl From<&AcceptChannel> for ChannelBasePublicKeys {
    fn from(value: &AcceptChannel) -> Self {
        ChannelBasePublicKeys {
            funding_pubkey: value.funding_pubkey,
            revocation_base_key: value.revocation_basepoint,
            payment_base_key: value.payment_basepoint,
            delayed_payment_base_key: value.delayed_payment_basepoint,
            tlc_base_key: value.tlc_basepoint,
        }
    }
}

pub struct CounterpartyChannelTransactionParameters {
    /// Counter-party public keys
    pub pubkeys: ChannelBasePublicKeys,
    /// The contest delay selected by the counterparty, which applies to holder-broadcast transactions
    pub selected_contest_delay: u16,
}

type ShortHash = [u8; 20];

/// A tlc output.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct TLC {
    /// The id of a received TLC. Must be empty if this is an offered HTLC.
    /// We will fill in the id when we send this tlc to the counterparty.
    /// Otherwise must be the next sequence number of the counterparty.
    pub id: u64,
    /// Is this HTLC being received by us or offered by us?
    pub is_offered: bool,
    /// The value as it appears in the commitment transaction
    pub amount: u128,
    /// The CLTV lock-time at which this HTLC expires.
    pub lock_time: LockTime,
    /// The hash of the preimage which unlocks this HTLC.
    pub payment_hash: Hash256,
    /// The preimage of the hash to be sent to the counterparty.
    pub payment_preimage: Option<Hash256>,
    /// The commitment number is kept here because we need to derive
    /// the local and remote payment pubkey. Note that commitment number
    /// is different for initiator and acceptor. So we keep both
    /// a local_commitment_number and a remote_commitment_number.
    pub local_commitment_number: u64,
    pub remote_commitment_number: u64,
    // Below are the actual public keys used in the commitment transaction.
    // These are derived from the channel base keys and per-commitment data.
    pub local_payment_key_hash: Option<ShortHash>,
    pub remote_payment_key_hash: Option<ShortHash>,
}

impl TLC {
    fn get_hash(&self) -> ShortHash {
        self.payment_hash.as_ref()[..20].try_into().unwrap()
    }

    fn fill_in_pubkeys(&mut self, local: bool, channel: &ChannelActorState) {
        self.local_payment_key_hash = Some(self.get_local_pubkey_hash(local, channel));
        self.remote_payment_key_hash = Some(self.get_remote_pubkey_hash(local, channel));
    }

    fn get_local_pubkey_hash(&self, local: bool, channel: &ChannelActorState) -> ShortHash {
        let pubkey = self.get_pubkey(true, local, channel);
        blake2b_256(pubkey.serialize())[..20].try_into().unwrap()
    }

    fn get_remote_pubkey_hash(&self, local: bool, channel: &ChannelActorState) -> ShortHash {
        let pubkey = self.get_pubkey(false, local, channel);
        blake2b_256(pubkey.serialize())[..20].try_into().unwrap()
    }

    fn get_pubkey(
        &self,
        local_pubkey: bool,
        local_commitment: bool,
        channel: &ChannelActorState,
    ) -> Pubkey {
        let commitment_number = if local_commitment {
            self.local_commitment_number
        } else {
            self.remote_commitment_number
        };
        let (base_key, commitment_point) = if local_pubkey {
            (
                channel.get_holder_channel_parameters().pubkeys.tlc_base_key,
                channel.get_holder_commitment_point(commitment_number),
            )
        } else {
            (
                channel
                    .get_counterparty_channel_parameters()
                    .pubkeys
                    .tlc_base_key,
                channel.get_counterparty_commitment_point(commitment_number),
            )
        };
        derive_tlc_pubkey(&base_key, &commitment_point)
    }

    // Serialize the tlc output to lock args.
    // Must be called after fill_in_pubkeys.
    pub fn serialize_to_lock_args(&self) -> Vec<u8> {
        [
            (if self.is_offered { [0] } else { [1] }).to_vec(),
            self.amount.to_le_bytes().to_vec(),
            self.get_hash().to_vec(),
            self.remote_payment_key_hash.unwrap().to_vec(),
            self.local_payment_key_hash.unwrap().to_vec(),
            Since::from(self.lock_time).value().to_le_bytes().to_vec(),
        ]
        .concat()
    }
}

/// A tlc output in a commitment transaction, including both the tlc output
/// and the commitment_number that it first appeared in the commitment transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TLCOutputInCommitment {
    pub output: TLC,
}

/// The set of public keys which are used in the creation of one commitment transaction.
/// These are derived from the channel base keys and per-commitment data.
///
/// A broadcaster key is provided from potential broadcaster of the computed transaction.
/// A countersignatory key is coming from a protocol participant unable to broadcast the
/// transaction.
///
/// These keys are assumed to be good, either because the code derived them from
/// channel basepoints via the new function, or they were obtained via
/// CommitmentTransaction.trust().keys() because we trusted the source of the
/// pre-calculated keys.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct TxCreationKeys {
    /// Broadcaster's Payment Key (which isn't allowed to be spent from for some delay)
    pub broadcaster_delayed_payment_key: Pubkey,
    /// Countersignatory's payment key, used to receiving balance that should go to
    /// the countersignatory immediately.
    pub countersignatory_payment_key: Pubkey,
    /// The revocation key which is used to allow the broadcaster of the commitment
    /// transaction to provide their counterparty the ability to punish them if they broadcast
    /// an old state.
    pub countersignatory_revocation_key: Pubkey,
    /// Broadcaster's HTLC Key
    pub broadcaster_tlc_key: Pubkey,
    /// Countersignatory's HTLC Key
    pub countersignatory_tlc_key: Pubkey,
}

pub fn derive_private_key(secret: &Privkey, _per_commitment_point: &Pubkey) -> Privkey {
    // TODO: Currently we only copy the input secret. We need to actually derive new private keys
    // from the per_commitment_point.
    *secret
}

/// A simple implementation of [`WriteableEcdsaChannelSigner`] that just keeps the private keys in memory.
///
/// This implementation performs no policy checks and is insufficient by itself as
/// a secure external signer.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct InMemorySigner {
    /// Holder secret key in the 2-of-2 multisig script of a channel. This key also backs the
    /// holder's anchor output in a commitment transaction, if one is present.
    pub funding_key: Privkey,
    /// Holder secret key for blinded revocation pubkey.
    pub revocation_base_key: Privkey,
    /// Holder secret key used for our balance in counterparty-broadcasted commitment transactions.
    pub payment_key: Privkey,
    /// Holder secret key used in an HTLC transaction.
    pub delayed_payment_base_key: Privkey,
    /// Holder HTLC secret key used in commitment transaction HTLC outputs.
    pub tlc_base_key: Privkey,
    /// SecNonce used to generate valid signature in musig.
    // TODO: use rust's ownership to make sure musig_nonce is used once.
    pub musig2_base_nonce: SecNonce,
    /// Seed to derive above keys (per commitment).
    pub commitment_seed: [u8; 32],
}

pub fn derive_revocation_pubkey(base_key: &Pubkey, _commitment_point: &Pubkey) -> Pubkey {
    *base_key
}

pub fn derive_payment_pubkey(base_key: &Pubkey, _commitment_point: &Pubkey) -> Pubkey {
    *base_key
}

pub fn derive_delayed_payment_pubkey(base_key: &Pubkey, _commitment_point: &Pubkey) -> Pubkey {
    *base_key
}

pub fn derive_tlc_pubkey(base_key: &Pubkey, _commitment_point: &Pubkey) -> Pubkey {
    *base_key
}

impl InMemorySigner {
    pub fn generate_from_seed(params: &[u8]) -> Self {
        let seed = ckb_hash::blake2b_256(params);

        let commitment_seed = {
            let mut hasher = new_blake2b();
            hasher.update(&seed);
            hasher.update(&b"commitment seed"[..]);
            let mut result = [0u8; 32];
            hasher.finalize(&mut result);
            result
        };

        let key_derive = |seed: &[u8], info: &[u8]| {
            let result = blake2b_hash_with_salt(seed, info);
            Privkey::from_slice(&result)
        };

        let funding_key = key_derive(&seed, b"funding key");
        let revocation_base_key = key_derive(funding_key.as_ref(), b"revocation base key");
        let payment_key = key_derive(revocation_base_key.as_ref(), b"payment key");
        let delayed_payment_base_key =
            key_derive(payment_key.as_ref(), b"delayed payment base key");
        let tlc_base_key = key_derive(delayed_payment_base_key.as_ref(), b"HTLC base key");
        let misig_nonce = key_derive(tlc_base_key.as_ref(), b"musig nocne");
        let musig_nonce = SecNonce::build(misig_nonce.as_ref()).build();

        Self {
            funding_key,
            revocation_base_key,
            payment_key,
            delayed_payment_base_key,
            tlc_base_key,
            musig2_base_nonce: musig_nonce,
            commitment_seed,
        }
    }

    fn to_channel_public_keys(&self, commitment_number: u64) -> ChannelBasePublicKeys {
        ChannelBasePublicKeys {
            funding_pubkey: self.funding_key.pubkey(),
            revocation_base_key: self.derive_revocation_key(commitment_number).pubkey(),
            payment_base_key: self.derive_payment_key(commitment_number).pubkey(),
            delayed_payment_base_key: self.derive_delayed_payment_key(commitment_number).pubkey(),
            tlc_base_key: self.derive_tlc_key(commitment_number).pubkey(),
        }
    }

    pub fn get_commitment_point(&self, commitment_number: u64) -> Pubkey {
        get_commitment_point(&self.commitment_seed, commitment_number)
    }

    pub fn get_commitment_secret(&self, commitment_number: u64) -> [u8; 32] {
        get_commitment_secret(&self.commitment_seed, commitment_number)
    }

    pub fn derive_revocation_key(&self, commitment_number: u64) -> Privkey {
        let per_commitment_point = self.get_commitment_point(commitment_number);
        debug!(
            "Revocation key: {}",
            hex::encode(
                derive_private_key(&self.revocation_base_key, &per_commitment_point).as_ref()
            )
        );
        derive_private_key(&self.revocation_base_key, &per_commitment_point)
    }

    pub fn derive_payment_key(&self, new_commitment_number: u64) -> Privkey {
        let per_commitment_point = self.get_commitment_point(new_commitment_number);
        derive_private_key(&self.payment_key, &per_commitment_point)
    }

    pub fn derive_delayed_payment_key(&self, new_commitment_number: u64) -> Privkey {
        let per_commitment_point = self.get_commitment_point(new_commitment_number);
        derive_private_key(&self.delayed_payment_base_key, &per_commitment_point)
    }

    pub fn derive_tlc_key(&self, new_commitment_number: u64) -> Privkey {
        let per_commitment_point = self.get_commitment_point(new_commitment_number);
        derive_private_key(&self.tlc_base_key, &per_commitment_point)
    }

    pub fn derive_musig2_nonce(&self, _new_commitment_number: u64) -> SecNonce {
        // TODO: generate new musig nonce here
        self.musig2_base_nonce.clone()
    }
}

#[cfg(test)]
mod tests {

    use ckb_sdk::core::TransactionBuilder;
    use ckb_types::{
        core::{DepType, ScriptHashType},
        packed::{CellDep, CellInput, CellOutput, OutPoint, Transaction},
        prelude::{AsTransactionBuilder, Pack, Unpack},
    };
    use molecule::prelude::{Builder, Entity};
    use rand::rngs::mock;

    use crate::{
        ckb::{
            network::{AcceptChannelCommand, OpenChannelCommand},
            temp::get_commitment_lock_context,
            test_utils::NetworkNode,
            NetworkActorCommand, NetworkActorMessage,
        },
        NetworkServiceEvent,
    };

    use super::{super::types::Privkey, derive_private_key, derive_tlc_pubkey, InMemorySigner};

    #[test]
    fn test_per_commitment_point_and_secret_consistency() {
        let signer = InMemorySigner::generate_from_seed(&[1; 32]);
        assert_eq!(
            signer.get_commitment_point(0),
            Privkey::from(&signer.get_commitment_secret(0)).pubkey()
        );
    }

    #[test]
    fn test_derive_private_and_public_keys() {
        let privkey = Privkey::from(&[1; 32]);
        let per_commitment_point = Privkey::from(&[2; 32]).pubkey();
        let derived_privkey = derive_private_key(&privkey, &per_commitment_point);
        let derived_pubkey = derive_tlc_pubkey(&privkey.pubkey(), &per_commitment_point);
        assert_eq!(derived_privkey.pubkey(), derived_pubkey);
    }

    // This test is used to dump transactions that are used in the tx collaboration of e2e tests.
    // We generate the following transactions (tx1, tx2, tx3) by:
    // 1. A add input1 to inputs, and outputs output1, call this tx tx1.
    // 2. B add input2 to tx1, and outputs output1 and output2, call this tx tx2.
    // 3. A remove input1 from tx2 and add input3 to tx2, keep outputs
    // output1 and output2, call this tx tx3.
    // TODO: change this into a unit test.
    #[test]
    fn test_dump_fake_tx_inputs_to_funding_tx() {
        let always_success_out_point1 = ckb_types::packed::OutPoint::new_builder()
            .tx_hash([0u8; 32].pack())
            .index(0u32.pack())
            .build();

        let always_success_out_point2 = ckb_types::packed::OutPoint::new_builder()
            .tx_hash([2u8; 32].pack())
            .index(0u32.pack())
            .build();

        let funding_tx_lock_script_out_point = ckb_types::packed::OutPoint::new_builder()
            .tx_hash([2u8; 32].pack())
            .index(0u32.pack())
            .build();

        let secp256k1_code_hash = [42u8; 32];
        let secp256k1_hash_type = ScriptHashType::Data.into();

        let funding_tx_lock_script = ckb_types::packed::Script::new_builder()
            .code_hash([2u8; 32].pack())
            .hash_type(ScriptHashType::Data2.into())
            .args([3u8; 20].pack())
            .build();

        let change1_lock_script = ckb_types::packed::Script::new_builder()
            .code_hash(secp256k1_code_hash.pack())
            .hash_type(secp256k1_hash_type)
            .args([1u8; 20].pack())
            .build();

        let change2_lock_script = ckb_types::packed::Script::new_builder()
            .code_hash(secp256k1_code_hash.pack())
            .hash_type(secp256k1_hash_type)
            .args([2u8; 20].pack())
            .build();

        let input1_capacity = 1000u64;
        let input2_capacity = 2000u64;
        let input3_capacity = 3000u64;
        let output1_capacity = 500u64;
        let output2_capacity = 1000u64;

        let build_input_cell = |capacity: u64, lock_script_outpoint: OutPoint| -> CellInput {
            let mut tx_builder = TransactionBuilder::default();
            tx_builder
                .cell_dep(
                    CellDep::new_builder()
                        .out_point(lock_script_outpoint)
                        .dep_type(DepType::Code.into())
                        .build(),
                )
                .output(CellOutput::new_builder().capacity(capacity.pack()).build());
            let tx = tx_builder.build();
            let outpoint = tx.output_pts_iter().next().unwrap();
            CellInput::new_builder().previous_output(outpoint).build()
        };
        let tx1 = Transaction::default()
            .as_advanced_builder()
            .set_cell_deps(vec![
                CellDep::new_builder()
                    .out_point(funding_tx_lock_script_out_point.clone())
                    .dep_type(DepType::Code.into())
                    .build(),
                CellDep::new_builder()
                    .out_point(always_success_out_point1.clone())
                    .dep_type(DepType::Code.into())
                    .build(),
            ])
            .set_inputs(vec![build_input_cell(
                input1_capacity,
                always_success_out_point1.clone(),
            )])
            .set_outputs(vec![
                CellOutput::new_builder()
                    .capacity(output1_capacity.pack())
                    .lock(funding_tx_lock_script.clone())
                    .build(),
                CellOutput::new_builder()
                    .capacity((input1_capacity - output1_capacity).pack())
                    .lock(change1_lock_script.clone())
                    .build(),
            ])
            .build();
        dbg!(&tx1);
        let tx2 = {
            let inputs = tx1.inputs().into_iter().chain(
                vec![build_input_cell(
                    input2_capacity,
                    always_success_out_point2.clone(),
                )]
                .into_iter(),
            );
            let cell_deps = tx1.cell_deps().into_iter().chain(
                vec![CellDep::new_builder()
                    .out_point(always_success_out_point2.clone())
                    .dep_type(DepType::Code.into())
                    .build()]
                .into_iter(),
            );
            Transaction::default()
                .as_advanced_builder()
                .set_cell_deps(cell_deps.collect())
                .set_inputs(inputs.collect())
                .set_outputs(vec![
                    CellOutput::new_builder()
                        .capacity((output1_capacity + output2_capacity).pack())
                        .lock(funding_tx_lock_script.clone())
                        .build(),
                    CellOutput::new_builder()
                        .capacity((input1_capacity - output1_capacity).pack())
                        .lock(change1_lock_script.clone())
                        .build(),
                    CellOutput::new_builder()
                        .capacity((input2_capacity - output2_capacity).pack())
                        .lock(change2_lock_script.clone())
                        .build(),
                ])
                .build()
        };
        dbg!(&tx2);

        let tx3 = {
            let inputs = tx2
                .inputs()
                .into_iter()
                .skip(1)
                .chain(vec![build_input_cell(
                    input3_capacity,
                    always_success_out_point2,
                )]);
            let cell_deps = tx2.cell_deps().into_iter().skip(1);
            let outputs = tx2.outputs().into_iter();
            Transaction::default()
                .as_advanced_builder()
                .set_cell_deps(cell_deps.collect())
                .set_inputs(inputs.collect())
                .set_outputs(outputs.collect())
                .build()
        };
        dbg!(&tx3);
    }

    #[tokio::test]
    async fn test_open_channel_to_peer() {
        let [node_a, mut node_b] = NetworkNode::new_n_interconnected_nodes(2)
            .await
            .try_into()
            .unwrap();

        let open_channel_command = OpenChannelCommand {
            peer_id: node_b.peer_id.clone(),
            funding_amount: 1000,
        };
        node_a
            .network_actor
            .send_message(NetworkActorMessage::new_command(
                NetworkActorCommand::OpenChannel(open_channel_command),
            ))
            .expect("node_a alive");
        node_b
            .expect_event(|event| match event {
                NetworkServiceEvent::ChannelPendingToBeAccepted(peer_id, channel_id) => {
                    println!("A channel ({:?}) to {:?} create", channel_id, peer_id);
                    assert_eq!(peer_id, &node_a.peer_id);
                    true
                }
                _ => false,
            })
            .await;
    }

    #[tokio::test]
    async fn test_open_and_accept_channel() {
        let [mut node_a, mut node_b] = NetworkNode::new_n_interconnected_nodes(2)
            .await
            .try_into()
            .unwrap();

        node_a
            .network_actor
            .send_message(NetworkActorMessage::new_command(
                NetworkActorCommand::OpenChannel(OpenChannelCommand {
                    peer_id: node_b.peer_id.clone(),
                    funding_amount: 1000,
                }),
            ))
            .expect("node_a alive");
        let channel_id = node_b
            .expect_to_process_event(|event| match event {
                NetworkServiceEvent::ChannelPendingToBeAccepted(peer_id, channel_id) => {
                    println!("A channel ({:?}) to {:?} create", &channel_id, peer_id);
                    assert_eq!(peer_id, &node_a.peer_id);
                    Some(channel_id.clone())
                }
                _ => None,
            })
            .await;

        node_b
            .network_actor
            .send_message(NetworkActorMessage::new_command(
                NetworkActorCommand::AcceptChannel(AcceptChannelCommand {
                    temp_channel_id: channel_id.clone(),
                    funding_amount: 1000,
                }),
            ))
            .expect("node_a alive");

        node_a
            .expect_event(|event| match event {
                NetworkServiceEvent::ChannelCreated(peer_id, channel_id) => {
                    println!("A channel ({:?}) to {:?} create", channel_id, peer_id);
                    assert_eq!(peer_id, &node_b.peer_id);
                    true
                }
                _ => false,
            })
            .await;

        node_b
            .expect_event(|event| match event {
                NetworkServiceEvent::ChannelCreated(peer_id, channel_id) => {
                    println!("A channel ({:?}) to {:?} create", channel_id, peer_id);
                    assert_eq!(peer_id, &node_a.peer_id);
                    true
                }
                _ => false,
            })
            .await;
    }

    #[test]
    fn test_verify_fixed_tx() {
        use ckb_types::prelude::IntoTransactionView;

        let mut context = get_commitment_lock_context().write().unwrap();
        context.context.set_capture_debug(true);
        // These three transactions are are respectively funding tx, commitment tx
        // and revocation tx that tries to revoke a commitment tx.
        let funding_tx = "b50000000c000000b1000000a50000001c0000002000000024000000280000002c00000099000000000000000000000000000000000000006d0000000800000065000000100000001800000065000000dc050000000000004d00000010000000300000003100000079c1c32b392db873b38b9c76c3fcd4b2858e53698c004a7c23a08997db457ebf011800000066756e64696e67207472616e73616374696f6e20746573740c000000080000000000000004000000";
        let commitment_tx = "260200000c000000da010000ce0100001c00000020000000b8000000bc000000ec000000ba0100000000000004000000e568e3de32f5b76dbc0be25d79766dcdb58db2235d89f81a8bc814ea3f0e34230000000000f35610d6419f54554eccb5c2d2b5b372c9968c53fb6235e434c0cf0c766406e60000000000f70d0547fe8e1cbcca65b00d5f8c48306d0c314aa0bea8111992e27baea28b280000000000d827a2c78f8cf89f39b63b66a878f4537b7601021e1c2dba8534edef68229591000000000000000000010000000000000000000000396a405a3fc223ce968a7e6cd7907fcdb9c18d0d40eed8c7ee5d75060d6c646900000000ce0000000c0000006d00000061000000100000001800000061000000e8030000000000004900000010000000300000003100000079c1c32b392db873b38b9c76c3fcd4b2858e53698c004a7c23a08997db457ebf01140000009a480101f0c18c6ef7dbcdf54b6278212be5138961000000100000001800000061000000f40100000000000049000000100000003000000031000000c5421e17391f3acfb8093c02a4c434d6d9a812abb7ae59270ee680e1ab318fcf011400000051dcdb6df954a4108e46ce2dba4e677298af9028140000000c0000001000000000000000000000004c000000080000004000000068c8433bd151fa8d30f819d46d67835bcfba9d3ed5784ab7c07e0de3203393dcc137f3b7f80e74e49b01160d31ce558a8c8d21da315d95882620221f2c60350d";
        let revocation_tx_1 = "df0100000c00000061010000550100001c00000020000000b8000000bc000000ec000000490100000000000004000000e568e3de32f5b76dbc0be25d79766dcdb58db2235d89f81a8bc814ea3f0e34230000000000f35610d6419f54554eccb5c2d2b5b372c9968c53fb6235e434c0cf0c766406e60000000000f70d0547fe8e1cbcca65b00d5f8c48306d0c314aa0bea8111992e27baea28b280000000000d827a2c78f8cf89f39b63b66a878f4537b7601021e1c2dba8534edef682295910000000000000000000100000000000000000000000793996ad02bd5d606c8c6d2796530d25264f6ba0c3e8ca3cbf6d5300427f5d7010000005d000000080000005500000010000000180000005500000014000000000000003d00000010000000300000003100000079c1c32b392db873b38b9c76c3fcd4b2858e53698c004a7c23a08997db457ebf010800000077686174657665720c00000008000000000000007e00000008000000720000000a00000000000080f2aa0645547c88cd9ed0934c57f113acc4dca6c4ba781a397bbb0499b92db1a9b001bd32fb2437f0ff1bde73b64d5884297d6383ae16bd07ea1b6ca0537d233325a2e6482845b31ce27ffc61fd95d9696af95a05c44547902ea4ea95e109778934d90386c0bc8275d201";
        let revocation_tx_2 = "df0100000c00000061010000550100001c00000020000000b8000000bc000000ec000000490100000000000004000000e568e3de32f5b76dbc0be25d79766dcdb58db2235d89f81a8bc814ea3f0e34230000000000f35610d6419f54554eccb5c2d2b5b372c9968c53fb6235e434c0cf0c766406e60000000000f70d0547fe8e1cbcca65b00d5f8c48306d0c314aa0bea8111992e27baea28b280000000000d827a2c78f8cf89f39b63b66a878f4537b7601021e1c2dba8534edef682295910000000000000000000100000000000000000000000793996ad02bd5d606c8c6d2796530d25264f6ba0c3e8ca3cbf6d5300427f5d7010000005d000000080000005500000010000000180000005500000014000000000000003d00000010000000300000003100000079c1c32b392db873b38b9c76c3fcd4b2858e53698c004a7c23a08997db457ebf010800000077686174657665720c00000008000000000000007e00000008000000720000000a00000000000080f2aa0645547c88cd9ed0934c57f113acc4dca6c4ba781a397bbb0499b92db1a9b001bd32fb2437f0ffba865a25ccf672437b7041d0d5c6733a72d8051dee7ba9686671bee606c23b9f71a401a354abe792e821947dd353bca4a090ae4373d997624eeedfe2041ce18d01";
        for (i, tx) in [funding_tx, commitment_tx, revocation_tx_1, revocation_tx_2]
            .iter()
            .enumerate()
        {
            dbg!("Processing tx", i);
            let tx = Transaction::from_slice(hex::decode(tx).unwrap().as_slice())
                .unwrap()
                .into_view();
            let result = context.context.verify_tx(&tx, 10_000_000);
            dbg!("Verifying tx", &tx);
            let mock_tx = context.context.dump_tx(&tx);
            dbg!(
                &result,
                &tx,
                &serde_json::to_writer_pretty(
                    std::fs::File::create(format!("foo{i}.json")).unwrap(),
                    &mock_tx.unwrap()
                )
            );
            assert!(result.is_ok());
            for outpoint in tx.output_pts_iter() {
                let index: u32 = outpoint.index().unpack();
                let (cell, cell_data) = tx.output_with_data(index as usize).unwrap();
                dbg!(
                    "Creating cell with celloutput and data",
                    &outpoint,
                    &cell,
                    &cell_data
                );
                context
                    .context
                    .create_cell_with_out_point(outpoint, cell, cell_data);
            }
        }
    }
}
