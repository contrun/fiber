use fiber::{store::migration::Migration, Error};
use indicatif::ProgressBar;
use rocksdb::ops::Iterate;
use rocksdb::ops::Put;
use rocksdb::DB;
use std::sync::Arc;
use tracing::debug;

use fiber::fiber::channel::*;
use fiber::fiber::types::*;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::time::SystemTime;

const MIGRATION_DB_VERSION: &str = "20250112205923";

pub use fiber_v020::fiber::channel::ChannelActorState as ChannelActorStateV020;
pub use fiber_v021::fiber::channel::ChannelActorState as ChannelActorStateV021;

pub struct MigrationObj {
    version: String,
}

impl MigrationObj {
    pub fn new() -> Self {
        Self {
            version: MIGRATION_DB_VERSION.to_string(),
        }
    }
}

#[serde_as]
#[derive(Clone, Serialize, Deserialize)]
pub struct MyChannelActorState {
    pub state: ChannelState,
    // The data below are only relevant if the channel is public.
    pub public_channel_info: Option<PublicChannelInfo>,

    pub local_tlc_info: ChannelTlcInfo,
    pub remote_tlc_info: Option<ChannelTlcInfo>,

    // The local public key used to establish p2p network connection.
    pub local_pubkey: Pubkey,
    // The remote public key used to establish p2p network connection.
    pub remote_pubkey: Pubkey,

    pub id: Hash256,
    #[serde_as(as = "Option<EntityHex>")]
    pub funding_tx: Option<Transaction>,

    pub funding_tx_confirmed_at: Option<(H256, u32, u64)>,

    #[serde_as(as = "Option<EntityHex>")]
    pub funding_udt_type_script: Option<Script>,

    // Is this channel initially inbound?
    // An inbound channel is one where the counterparty is the funder of the channel.
    pub is_acceptor: bool,

    // TODO: consider transaction fee while building the commitment transaction.
    // The invariant here is that the sum of `to_local_amount` and `to_remote_amount`
    // should be equal to the total amount of the channel.
    // The changes of both `to_local_amount` and `to_remote_amount`
    // will always happen after a revoke_and_ack message is sent/received.
    // This means that while calculating the amounts for commitment transactions,
    // processing add_tlc command and messages, we need to take into account that
    // the amounts are not decremented/incremented yet.

    // The amount of CKB/UDT that we own in the channel.
    // This value will only change after we have resolved a tlc.
    pub to_local_amount: u128,
    // The amount of CKB/UDT that the remote owns in the channel.
    // This value will only change after we have resolved a tlc.
    pub to_remote_amount: u128,

    // these two amounts used to keep the minimal ckb amount for the two parties
    // TLC operations will not affect these two amounts, only used to keep the commitment transactions
    // to be valid, so that any party can close the channel at any time.
    // Note: the values are different for the UDT scenario
    pub local_reserved_ckb_amount: u64,
    pub remote_reserved_ckb_amount: u64,

    // The commitment fee rate is used to calculate the fee for the commitment transactions.
    // The side who want to submit the commitment transaction will pay fee
    pub commitment_fee_rate: u64,

    // The delay time for the commitment transaction, this value is set by the initiator of the channel.
    // It must be a relative EpochNumberWithFraction in u64 format.
    pub commitment_delay_epoch: u64,

    // The fee rate used for funding transaction, the initiator may set it as `funding_fee_rate` option,
    // if it's not set, DEFAULT_FEE_RATE will be used as default value, two sides will use the same fee rate
    pub funding_fee_rate: u64,

    // Signer is used to sign the commitment transactions.
    pub signer: InMemorySigner,

    // Cached channel public keys for easier of access.
    pub local_channel_public_keys: ChannelBasePublicKeys,

    // Commitment numbers that are used to derive keys.
    // This value is guaranteed to be 0 when channel is just created.
    pub commitment_numbers: CommitmentNumbers,

    pub local_constraints: ChannelConstraints,
    pub remote_constraints: ChannelConstraints,

    // Below are fields that are only usable after the channel is funded,
    // (or at some point of the state).

    // all the TLC related information
    pub tlc_state: TlcState,

    // The remote and local lock script for close channel, they are setup during the channel establishment.
    #[serde_as(as = "Option<EntityHex>")]
    pub remote_shutdown_script: Option<Script>,
    #[serde_as(as = "EntityHex")]
    pub local_shutdown_script: Script,

    // Basically the latest remote nonce sent by the peer with the CommitmentSigned message,
    // but we will only update this field after we have sent a RevokeAndAck to the peer.
    // With above guarantee, we can be sure the results of the sender obtaining its latest local nonce
    // and the receiver obtaining its latest remote nonce are the same.
    #[serde_as(as = "Option<PubNonceAsBytes>")]
    pub last_committed_remote_nonce: Option<PubNonce>,

    // While handling peer's CommitmentSigned message, we will build a RevokeAndAck message,
    // and reply this message to the peer. The nonce used to build the RevokeAndAck message is
    // an older one sent by the peer. We will read this nonce from the field `last_committed_remote_nonce`
    // The new nonce contained in the CommitmentSigned message
    // will be saved to `last_committed_remote_nonce` field when this process finishes successfully.
    // The problem is in some abnormal cases, the may not be able to successfully send the RevokeAndAck.
    // But we have overwritten the `last_committed_remote_nonce` field with the new nonce.
    // While reestablishing the channel, we need to use the old nonce to build the RevokeAndAck message.
    // This is why we need to save the old nonce in this field.
    #[serde_as(as = "Option<PubNonceAsBytes>")]
    pub last_commitment_signed_remote_nonce: Option<PubNonce>,

    // While building a CommitmentSigned message, we use the latest remote nonce (the `last_committed_remote_nonce` above)
    // to partially sign the commitment transaction. This nonce is also needed for the RevokeAndAck message
    // returned from the peer. We need to save this nonce because the counterparty may send other nonces during
    // the period when our CommitmentSigned is sent and the counterparty's RevokeAndAck is received.
    // This field is used to keep the nonce used by the unconfirmed CommitmentSigned. When we receive a
    // RevokeAndAck from the peer, we will use this nonce to validate the RevokeAndAck message.
    #[serde_as(as = "Option<PubNonceAsBytes>")]
    pub last_revoke_and_ack_remote_nonce: Option<PubNonce>,

    // The latest commitment transaction we're holding,
    // it can be broadcasted to blockchain by us to force close the channel.
    #[serde_as(as = "Option<EntityHex>")]
    pub latest_commitment_transaction: Option<Transaction>,

    // All the commitment point that are sent from the counterparty.
    // We need to save all these points to derive the keys for the commitment transactions.
    // The length of this vector is at most the maximum number of flighting tlcs.
    pub remote_commitment_points: Vec<(u64, Pubkey)>,
    pub remote_channel_public_keys: Option<ChannelBasePublicKeys>,

    // The shutdown info for both local and remote, they are setup by the shutdown command or message.
    pub local_shutdown_info: Option<ShutdownInfo>,
    pub remote_shutdown_info: Option<ShutdownInfo>,

    // A flag to indicate whether the channel is reestablishing, we won't process any messages until the channel is reestablished.
    pub reestablishing: bool,

    pub created_at: SystemTime,
}

impl Migration for MigrationObj {
    fn migrate(
        &self,
        db: Arc<DB>,
        _pb: Arc<dyn Fn(u64) -> ProgressBar + Send + Sync>,
    ) -> Result<Arc<DB>, Error> {
        eprintln!("MigrationObj::migrate ...........");

        const CHANNEL_ACTOR_STATE_PREFIX: u8 = 0;
        let prefix = vec![CHANNEL_ACTOR_STATE_PREFIX];

        for (k, v) in db
            .prefix_iterator(prefix.as_slice())
            .take_while(move |(col_key, _)| col_key.starts_with(prefix.as_slice()))
        {
            debug!(
                key = hex::encode(k.as_ref()),
                value = hex::encode(v.as_ref()),
                "Obtained old channel state"
            );
            let old_channel_state: ChannelActorStateV020 =
                bincode::deserialize(&v).expect("deserialize to old channel state");

            let mut all_remote_nonces = old_channel_state.remote_nonces.clone();
            all_remote_nonces.sort_by(|a, b| b.0.cmp(&a.0));
            let last_committed_remote_nonce =
                all_remote_nonces.get(0).map(|(_, nonce)| nonce).cloned();

            // Depending on whether the receiver has received our RevokeAndAck message or not,
            // we need to set different last_revoke_and_ack_remote_nonce.
            // 1. The receiver has received our RevokeAndAck message.
            //    In this case, the last_revoke_and_ack_remote_nonce should be the remote nonce
            //    with the largest commitment number.
            // 2. The receiver has not received our RevokeAndAck message.
            //    In this case, the last_revoke_and_ack_remote_nonce should be the remote nonce
            //    with the second largest commitment number.
            // We can't determine which case is true unless we receive a Reestablish message from the peer,
            // which contains the remote's view on the last commitment number.
            // So this migration is not perfect, but it's the best we can do.
            let last_revoke_and_ack_remote_nonce =
                all_remote_nonces.get(0).map(|(_, nonce)| nonce).cloned();

            let serialized = rmp_serde::to_vec(&old_channel_state).unwrap();
            let mut new_channel_state: MyChannelActorState =
                rmp_serde::from_slice(&serialized).expect("deserialize to new state");
            let new_channel_state_bytes =
                bincode::serialize(&new_channel_state).expect("serialize to new channel state");

            // We don't really want to override data in tests.
            panic!("SUCESS!!!!!!!!!!");
            db.put(k, new_channel_state_bytes)
                .expect("save new channel state");
        }
        Ok(db)
    }

    fn version(&self) -> &str {
        &self.version
    }
}
