use crate::fiber::{
    channel::{
        AddTlcCommand, ChannelActorStateStore, ChannelCommand, ChannelCommandWithId, ChannelState,
        RemoveTlcCommand, ShutdownCommand, UpdateCommand,
    },
    hash_algorithm::HashAlgorithm,
    network::{AcceptChannelCommand, OpenChannelCommand, SendPaymentCommand},
    serde_utils::{EntityHex, U128Hex, U32Hex, U64Hex},
    types::{Hash256, LockTime, Pubkey, RemoveTlcFail, RemoveTlcFulfill},
    NetworkActorCommand, NetworkActorMessage,
};
use crate::{handle_actor_call, handle_actor_cast, log_and_error};

use ckb_jsonrpc_types::Script;
use ckb_types::{core::FeeRate, packed::OutPoint};
use jsonrpsee::{
    core::async_trait,
    proc_macros::rpc,
    types::{error::CALL_EXECUTION_FAILED_CODE, ErrorObjectOwned},
};
use ractor::{call, ActorRef};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::cmp::Reverse;
use tentacle::secio::PeerId;

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct OpenChannelParams {
    #[serde_as(as = "DisplayFromStr")]
    peer_id: PeerId,
    #[serde_as(as = "U128Hex")]
    funding_amount: u128,
    public: Option<bool>,
    funding_udt_type_script: Option<Script>,
    shutdown_script: Option<Script>,
    #[serde_as(as = "Option<U64Hex>")]
    commitment_fee_rate: Option<u64>,
    #[serde_as(as = "Option<U64Hex>")]
    funding_fee_rate: Option<u64>,
    #[serde_as(as = "Option<U64Hex>")]
    tlc_locktime_expiry_delta: Option<u64>,
    #[serde_as(as = "Option<U128Hex>")]
    tlc_min_value: Option<u128>,
    #[serde_as(as = "Option<U128Hex>")]
    tlc_max_value: Option<u128>,
    #[serde_as(as = "Option<U128Hex>")]
    tlc_fee_proportional_millionths: Option<u128>,
    #[serde_as(as = "Option<U128Hex>")]
    max_tlc_value_in_flight: Option<u128>,
    #[serde_as(as = "Option<U64Hex>")]
    max_tlc_number_in_flight: Option<u64>,
}

#[derive(Clone, Serialize)]
pub(crate) struct OpenChannelResult {
    temporary_channel_id: Hash256,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct AcceptChannelParams {
    temporary_channel_id: Hash256,
    #[serde_as(as = "U128Hex")]
    funding_amount: u128,
    shutdown_script: Option<Script>,
}

#[derive(Clone, Serialize)]
pub(crate) struct AcceptChannelResult {
    channel_id: Hash256,
}

// TODO @quake remove this unnecessary pub(crate) struct and rpc after refactoring
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct CommitmentSignedParams {
    channel_id: Hash256,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub(crate) struct ListChannelsParams {
    #[serde_as(as = "Option<DisplayFromStr>")]
    peer_id: Option<PeerId>,
}

#[derive(Clone, Serialize)]
pub(crate) struct ListChannelsResult {
    channels: Vec<Channel>,
}

#[serde_as]
#[derive(Clone, Serialize)]
pub(crate) struct Channel {
    channel_id: Hash256,
    is_public: bool,
    #[serde_as(as = "Option<EntityHex>")]
    channel_outpoint: Option<OutPoint>,
    #[serde_as(as = "DisplayFromStr")]
    peer_id: PeerId,
    funding_udt_type_script: Option<Script>,
    state: ChannelState,
    #[serde_as(as = "U128Hex")]
    local_balance: u128,
    #[serde_as(as = "U128Hex")]
    offered_tlc_balance: u128,
    #[serde_as(as = "U128Hex")]
    remote_balance: u128,
    #[serde_as(as = "U128Hex")]
    received_tlc_balance: u128,
    #[serde_as(as = "U64Hex")]
    created_at: u64,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct AddTlcParams {
    channel_id: Hash256,
    #[serde_as(as = "U128Hex")]
    amount: u128,
    payment_hash: Hash256,
    expiry: LockTime,
    hash_algorithm: Option<HashAlgorithm>,
}

#[serde_as]
#[derive(Clone, Serialize)]
pub(crate) struct AddTlcResult {
    #[serde_as(as = "U64Hex")]
    tlc_id: u64,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct RemoveTlcParams {
    channel_id: Hash256,
    #[serde_as(as = "U64Hex")]
    tlc_id: u64,
    reason: RemoveTlcReason,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum RemoveTlcReason {
    RemoveTlcFulfill {
        payment_preimage: Hash256,
    },
    RemoveTlcFail {
        #[serde_as(as = "U32Hex")]
        error_code: u32,
    },
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct ShutdownChannelParams {
    channel_id: Hash256,
    close_script: Script,
    force: Option<bool>,
    #[serde_as(as = "U64Hex")]
    fee_rate: u64,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateChannelParams {
    channel_id: Hash256,
    enabled: Option<bool>,
    #[serde_as(as = "Option<U64Hex>")]
    tlc_locktime_expiry_delta: Option<u64>,
    #[serde_as(as = "Option<U128Hex>")]
    tlc_minimum_value: Option<u128>,
    #[serde_as(as = "Option<U128Hex>")]
    tlc_maximum_value: Option<u128>,
    #[serde_as(as = "Option<U128Hex>")]
    tlc_fee_proportional_millionths: Option<u128>,
}

#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SendPaymentCommandParams {
    // the identifier of the payment target
    target_pubkey: Option<Pubkey>,

    // the amount of the payment
    #[serde_as(as = "Option<U128Hex>")]
    amount: Option<u128>,

    // The hash to use within the payment's HTLC
    // FIXME: this should be optional when AMP is enabled
    payment_hash: Option<Hash256>,

    // The CLTV delta from the current height that should be used to set the timelock for the final hop
    #[serde_as(as = "Option<U64Hex>")]
    final_cltv_delta: Option<u64>,

    // the encoded invoice to send to the recipient
    invoice: Option<String>,

    // the payment timeout in seconds, if the payment is not completed within this time, it will be cancelled
    #[serde_as(as = "Option<U64Hex>")]
    timeout: Option<u64>,

    // the maximum fee amounts in shannons that the sender is willing to pay
    #[serde_as(as = "Option<U128Hex>")]
    max_fee_amount: Option<u128>,

    // max parts for the payment, only used for multi-part payments
    #[serde_as(as = "Option<U64Hex>")]
    max_parts: Option<u64>,

    // keysend payment
    keysend: Option<bool>,

    // udt type script for the payment
    udt_type_script: Option<Script>,
}

#[derive(Clone, Serialize)]
pub(crate) struct SendPaymentResult {
    payment_hash: Hash256,
}
#[rpc(server)]
trait ChannelRpc {
    #[method(name = "open_channel")]
    async fn open_channel(
        &self,
        params: OpenChannelParams,
    ) -> Result<OpenChannelResult, ErrorObjectOwned>;

    #[method(name = "accept_channel")]
    async fn accept_channel(
        &self,
        params: AcceptChannelParams,
    ) -> Result<AcceptChannelResult, ErrorObjectOwned>;

    #[method(name = "list_channels")]
    async fn list_channels(
        &self,
        params: ListChannelsParams,
    ) -> Result<ListChannelsResult, ErrorObjectOwned>;

    #[method(name = "commitment_signed")]
    async fn commitment_signed(
        &self,
        params: CommitmentSignedParams,
    ) -> Result<(), ErrorObjectOwned>;

    #[method(name = "add_tlc")]
    async fn add_tlc(&self, params: AddTlcParams) -> Result<AddTlcResult, ErrorObjectOwned>;

    #[method(name = "remove_tlc")]
    async fn remove_tlc(&self, params: RemoveTlcParams) -> Result<(), ErrorObjectOwned>;

    #[method(name = "shutdown_channel")]
    async fn shutdown_channel(&self, params: ShutdownChannelParams)
        -> Result<(), ErrorObjectOwned>;

    #[method(name = "update_channel")]
    async fn update_channel(&self, params: UpdateChannelParams) -> Result<(), ErrorObjectOwned>;

    #[method(name = "send_payment")]
    async fn send_payment(
        &self,
        params: SendPaymentCommandParams,
    ) -> Result<SendPaymentResult, ErrorObjectOwned>;
}

pub(crate) struct ChannelRpcServerImpl<S> {
    actor: ActorRef<NetworkActorMessage>,
    store: S,
}

impl<S> ChannelRpcServerImpl<S> {
    pub(crate) fn new(actor: ActorRef<NetworkActorMessage>, store: S) -> Self {
        ChannelRpcServerImpl { actor, store }
    }
}

#[async_trait]
impl<S> ChannelRpcServer for ChannelRpcServerImpl<S>
where
    S: ChannelActorStateStore + Send + Sync + 'static,
{
    async fn open_channel(
        &self,
        params: OpenChannelParams,
    ) -> Result<OpenChannelResult, ErrorObjectOwned> {
        let message = |rpc_reply| {
            NetworkActorMessage::Command(NetworkActorCommand::OpenChannel(
                OpenChannelCommand {
                    peer_id: params.peer_id.clone(),
                    funding_amount: params.funding_amount,
                    public: params.public.unwrap_or(false),
                    shutdown_script: params.shutdown_script.clone().map(|s| s.into()),
                    funding_udt_type_script: params
                        .funding_udt_type_script
                        .clone()
                        .map(|s| s.into()),
                    commitment_fee_rate: params.commitment_fee_rate,
                    funding_fee_rate: params.funding_fee_rate,
                    tlc_locktime_expiry_delta: params.tlc_locktime_expiry_delta,
                    tlc_min_value: params.tlc_min_value,
                    tlc_max_value: params.tlc_max_value,
                    tlc_fee_proportional_millionths: params.tlc_fee_proportional_millionths,
                    max_tlc_value_in_flight: params.max_tlc_value_in_flight,
                    max_tlc_number_in_flight: params.max_tlc_number_in_flight,
                },
                rpc_reply,
            ))
        };
        handle_actor_call!(self.actor, message, params).map(|response| OpenChannelResult {
            temporary_channel_id: response.channel_id,
        })
    }

    async fn accept_channel(
        &self,
        params: AcceptChannelParams,
    ) -> Result<AcceptChannelResult, ErrorObjectOwned> {
        let message = |rpc_reply| {
            NetworkActorMessage::Command(NetworkActorCommand::AcceptChannel(
                AcceptChannelCommand {
                    temp_channel_id: params.temporary_channel_id,
                    funding_amount: params.funding_amount,
                    shutdown_script: params.shutdown_script.clone().map(|s| s.into()),
                },
                rpc_reply,
            ))
        };

        handle_actor_call!(self.actor, message, params).map(|response| AcceptChannelResult {
            channel_id: response.new_channel_id,
        })
    }

    async fn list_channels(
        &self,
        params: ListChannelsParams,
    ) -> Result<ListChannelsResult, ErrorObjectOwned> {
        let mut channels: Vec<_> = self
            .store
            .get_active_channel_states(params.peer_id)
            .into_iter()
            .filter_map(|(peer_id, channel_id, _state)| {
                self.store
                    .get_channel_actor_state(&channel_id)
                    .map(|state| Channel {
                        channel_id,
                        is_public: state.is_public(),
                        channel_outpoint: state.get_funding_transaction_outpoint(),
                        peer_id,
                        funding_udt_type_script: state
                            .funding_udt_type_script
                            .clone()
                            .map(Into::into),
                        state: state.state,
                        local_balance: state.get_local_balance(),
                        remote_balance: state.get_remote_balance(),
                        offered_tlc_balance: state.get_offered_tlc_balance(),
                        received_tlc_balance: state.get_received_tlc_balance(),
                        created_at: state.get_created_at_in_microseconds(),
                    })
            })
            .collect();
        // Sort by created_at in descending order
        channels.sort_by_key(|channel| Reverse(channel.created_at));
        Ok(ListChannelsResult { channels })
    }

    async fn commitment_signed(
        &self,
        params: CommitmentSignedParams,
    ) -> Result<(), ErrorObjectOwned> {
        let message = NetworkActorMessage::Command(NetworkActorCommand::ControlFiberChannel(
            ChannelCommandWithId {
                channel_id: params.channel_id,
                command: ChannelCommand::CommitmentSigned(),
            },
        ));
        handle_actor_cast!(self.actor, message, params)
    }

    async fn add_tlc(&self, params: AddTlcParams) -> Result<AddTlcResult, ErrorObjectOwned> {
        let message = |rpc_reply| -> NetworkActorMessage {
            NetworkActorMessage::Command(NetworkActorCommand::ControlFiberChannel(
                ChannelCommandWithId {
                    channel_id: params.channel_id,
                    command: ChannelCommand::AddTlc(
                        AddTlcCommand {
                            amount: params.amount,
                            preimage: None,
                            payment_hash: Some(params.payment_hash),
                            expiry: params.expiry,
                            hash_algorithm: params.hash_algorithm.unwrap_or_default(),
                            onion_packet: vec![],
                            previous_tlc: None,
                        },
                        rpc_reply,
                    ),
                },
            ))
        };
        handle_actor_call!(self.actor, message, params).map(|response| AddTlcResult {
            tlc_id: response.tlc_id,
        })
    }

    async fn remove_tlc(&self, params: RemoveTlcParams) -> Result<(), ErrorObjectOwned> {
        let message = |rpc_reply| -> NetworkActorMessage {
            NetworkActorMessage::Command(NetworkActorCommand::ControlFiberChannel(
                ChannelCommandWithId {
                    channel_id: params.channel_id,
                    command: ChannelCommand::RemoveTlc(
                        RemoveTlcCommand {
                            id: params.tlc_id,
                            reason: match params.reason {
                                RemoveTlcReason::RemoveTlcFulfill { payment_preimage } => {
                                    crate::fiber::types::RemoveTlcReason::RemoveTlcFulfill(
                                        RemoveTlcFulfill { payment_preimage },
                                    )
                                }
                                RemoveTlcReason::RemoveTlcFail { error_code } => {
                                    crate::fiber::types::RemoveTlcReason::RemoveTlcFail(
                                        RemoveTlcFail { error_code },
                                    )
                                }
                            },
                        },
                        rpc_reply,
                    ),
                },
            ))
        };

        handle_actor_call!(self.actor, message, params)
    }

    async fn shutdown_channel(
        &self,
        params: ShutdownChannelParams,
    ) -> Result<(), ErrorObjectOwned> {
        let message = |rpc_reply| -> NetworkActorMessage {
            NetworkActorMessage::Command(NetworkActorCommand::ControlFiberChannel(
                ChannelCommandWithId {
                    channel_id: params.channel_id,
                    command: ChannelCommand::Shutdown(
                        ShutdownCommand {
                            close_script: params.close_script.clone().into(),
                            fee_rate: FeeRate::from_u64(params.fee_rate),
                            force: params.force.unwrap_or(false),
                        },
                        rpc_reply,
                    ),
                },
            ))
        };
        handle_actor_call!(self.actor, message, params)
    }

    async fn update_channel(&self, params: UpdateChannelParams) -> Result<(), ErrorObjectOwned> {
        let message = |rpc_reply| -> NetworkActorMessage {
            NetworkActorMessage::Command(NetworkActorCommand::ControlFiberChannel(
                ChannelCommandWithId {
                    channel_id: params.channel_id,
                    command: ChannelCommand::Update(
                        UpdateCommand {
                            enabled: params.enabled,
                            tlc_locktime_expiry_delta: params.tlc_locktime_expiry_delta,
                            tlc_minimum_value: params.tlc_minimum_value,
                            tlc_maximum_value: params.tlc_maximum_value,
                            tlc_fee_proportional_millionths: params.tlc_fee_proportional_millionths,
                        },
                        rpc_reply,
                    ),
                },
            ))
        };
        handle_actor_call!(self.actor, message, params)
    }

    async fn send_payment(
        &self,
        params: SendPaymentCommandParams,
    ) -> Result<SendPaymentResult, ErrorObjectOwned> {
        let message = |rpc_reply| -> NetworkActorMessage {
            NetworkActorMessage::Command(NetworkActorCommand::SendPayment(
                SendPaymentCommand {
                    target_pubkey: params.target_pubkey,
                    amount: params.amount,
                    payment_hash: params.payment_hash,
                    final_cltv_delta: params.final_cltv_delta,
                    invoice: params.invoice.clone(),
                    timeout: params.timeout,
                    max_fee_amount: params.max_fee_amount,
                    max_parts: params.max_parts,
                    keysend: params.keysend,
                    udt_type_script: params.udt_type_script.clone().map(|s| s.into()),
                },
                rpc_reply,
            ))
        };
        handle_actor_call!(self.actor, message, params).map(|response| SendPaymentResult {
            payment_hash: response.payment_hash,
        })
    }
}
