use ckb_sdk::CkbRpcClient;
use ckb_types::{core::TransactionView, packed, prelude::*};
use log::debug;
use ractor::{
    concurrency::{sleep, Duration},
    Actor, ActorProcessingErr, ActorRef, RpcReplyPort,
};

use crate::ckb::chain::CommitmentLockContext;

use super::{funding::FundingContext, CkbChainConfig, FundingError, FundingRequest, FundingTx};

pub struct CkbChainActor {}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct CkbChainState {
    config: CkbChainConfig,
    secret_key: secp256k1::SecretKey,
    funding_source_lock_script: packed::Script,
    ctx: CommitmentLockContext,
}

#[derive(Debug, Clone)]
pub struct TraceTxRequest {
    pub tx_hash: packed::Byte32,
    // How many confirmations required to consider the transaction committed.
    pub confirmations: u64,
}

#[derive(Debug)]
pub enum CkbChainMessage {
    Fund(
        FundingTx,
        FundingRequest,
        RpcReplyPort<Result<FundingTx, FundingError>>,
    ),
    Sign(FundingTx, RpcReplyPort<Result<FundingTx, FundingError>>),
    SendTx(TransactionView),
    TraceTx(TraceTxRequest, RpcReplyPort<ckb_jsonrpc_types::Status>),
}

#[ractor::async_trait]
impl Actor for CkbChainActor {
    type Msg = CkbChainMessage;
    type State = CkbChainState;
    type Arguments = (CkbChainConfig, CommitmentLockContext);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        (config, ctx): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let secret_key = config.read_secret_key()?;

        let secp = secp256k1::Secp256k1::new();
        let pub_key = secret_key.public_key(&secp);
        let pub_key_hash = ckb_hash::blake2b_256(pub_key.serialize());
        let funding_source_lock_script = ctx.get_secp256k1_lock_script(&pub_key_hash[0..20]);
        log::info!(
            "[{}] funding lock args: {}",
            myself.get_name().unwrap_or_default(),
            funding_source_lock_script.args()
        );

        Ok(CkbChainState {
            config,
            secret_key,
            funding_source_lock_script,
            ctx,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        use CkbChainMessage::*;
        match message {
            Fund(tx, request, reply_port) => {
                let context = state.build_funding_context(&request);
                if !reply_port.is_closed() {
                    tokio::task::block_in_place(move || {
                        let result = tx.fulfill(request, context);
                        if !reply_port.is_closed() {
                            reply_port.send(result).expect("reply ok");
                        }
                    });
                }
            }
            Sign(tx, reply_port) => {
                if !reply_port.is_closed() {
                    let secret_key = state.secret_key.clone();
                    let rpc_url = state.config.rpc_url.clone();
                    tokio::task::block_in_place(move || {
                        let result = tx.sign(secret_key, rpc_url);
                        if !reply_port.is_closed() {
                            reply_port.send(result).expect("reply ok");
                        }
                    });
                }
            }
            SendTx(tx) => {
                let rpc_url = state.config.rpc_url.clone();
                tokio::task::block_in_place(move || {
                    let ckb_client = CkbRpcClient::new(&rpc_url);
                    if let Err(err) = ckb_client.send_transaction(tx.data().into(), None) {
                        log::error!(
                            "[{}] send transaction failed: {:?}",
                            myself.get_name().unwrap_or_default(),
                            err
                        );
                    }
                });
            }
            TraceTx(
                TraceTxRequest {
                    tx_hash,
                    confirmations,
                },
                reply_port,
            ) => {
                log::info!(
                    "[{}] trace transaction {} with {} confs",
                    myself.get_name().unwrap_or_default(),
                    tx_hash,
                    confirmations
                );
                // TODO: Need a better way to trace the transaction.
                while !reply_port.is_closed() {
                    let actor_name = myself.get_name().unwrap_or_default();
                    let rpc_url = state.config.rpc_url.clone();
                    let tx_hash = tx_hash.clone();
                    let status = tokio::task::block_in_place(move || {
                        let ckb_client = CkbRpcClient::new(&rpc_url);
                        match ckb_client.get_transaction_status(tx_hash.unpack()) {
                            Ok(resp) => match resp.tx_status.status {
                                ckb_jsonrpc_types::Status::Committed => {
                                    match ckb_client.get_tip_block_number() {
                                        Ok(tip_number) => {
                                            let tip_number: u64 = tip_number.into();
                                            let commit_number: u64 = resp
                                                .tx_status
                                                .block_number
                                                .unwrap_or_default()
                                                .into();
                                            debug!(
                                                "Tracing tx, current tip_number: {}, tx commit_number: {}, num of confirmation required: {}, is transaction committed: {}",
                                                tip_number, commit_number, confirmations, tip_number >= commit_number + confirmations
                                            );
                                            (tip_number >= commit_number + confirmations)
                                                .then_some(ckb_jsonrpc_types::Status::Committed)
                                        }
                                        Err(err) => {
                                            log::error!(
                                                "[{}] get tip block number failed: {:?}",
                                                actor_name,
                                                err
                                            );
                                            None
                                        }
                                    }
                                }
                                ckb_jsonrpc_types::Status::Rejected => {
                                    Some(ckb_jsonrpc_types::Status::Rejected)
                                }
                                _ => None,
                            },
                            Err(err) => {
                                log::error!(
                                    "[{}] get transaction status failed: {:?}",
                                    actor_name,
                                    err
                                );
                                None
                            }
                        }
                    });
                    match status {
                        Some(status) => {
                            if !reply_port.is_closed() {
                                reply_port.send(status).expect("reply ok");
                            }
                            return Ok(());
                        }
                        None => sleep(Duration::from_secs(5)).await,
                    }
                }
            }
        }
        Ok(())
    }
}

impl CkbChainState {
    fn build_funding_context(&self, request: &FundingRequest) -> FundingContext {
        FundingContext {
            secret_key: self.secret_key.clone(),
            rpc_url: self.config.rpc_url.clone(),
            funding_source_lock_script: self.funding_source_lock_script.clone(),
            funding_cell_lock_script: request.script.clone(),
        }
    }
}

#[cfg(test)]
pub struct MockChainActor;

#[cfg(test)]
#[ractor::async_trait]
impl Actor for MockChainActor {
    type Msg = CkbChainMessage;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _: ActorRef<Self::Msg>,
        _: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _: ActorRef<Self::Msg>,
        _: Self::Msg,
        _: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}
