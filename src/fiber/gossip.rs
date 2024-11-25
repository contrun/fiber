use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    num::NonZeroUsize,
};

use ckb_hash::blake2b_256;
use ckb_jsonrpc_types::{Status, TxStatus};
use ckb_types::packed::OutPoint;
use lru::LruCache;
use ractor::{
    async_trait as rasync_trait, call_t,
    concurrency::{timeout, Duration},
    Actor, ActorCell, ActorProcessingErr, ActorRef, ActorRuntime,
};
use secp256k1::Message;
use tentacle::{
    async_trait as tasync_trait,
    builder::MetaBuilder,
    bytes::Bytes,
    context::{ProtocolContext, ProtocolContextMutRef, SessionContext},
    secio::PeerId,
    service::{ProtocolHandle, ProtocolMeta, ServiceAsyncControl},
    traits::ServiceProtocol,
    SessionId,
};
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace, warn};

use crate::{
    ckb::{CkbChainMessage, GetBlockTimestampRequest, TraceTxRequest, TraceTxResponse},
    fiber::{network::DEFAULT_CHAIN_ACTOR_TIMEOUT, types::secp256k1_instance},
    now_timestamp, unwrap_or_return, Error,
};

use super::{
    network::{check_chain_hash, get_chain_hash, GossipMessageWithPeerId, GOSSIP_PROTOCOL_ID},
    types::{
        BroadcastMessage, BroadcastMessageID, BroadcastMessageQuery, BroadcastMessageQueryFlags,
        BroadcastMessageWithTimestamp, BroadcastMessagesFilter, BroadcastMessagesFilterResult,
        ChannelAnnouncement, ChannelUpdate, Cursor, GetBroadcastMessages,
        GetBroadcastMessagesResult, GossipMessage, NodeAnnouncement, Pubkey,
        QueryBroadcastMessages, QueryBroadcastMessagesResult,
    },
};

const MAX_NUM_OF_BROADCAST_MESSAGES: u16 = 1000;
pub(crate) const DEFAULT_NUM_OF_BROADCAST_MESSAGE: u16 = 100;

const NUM_SIMULTANEOUS_GET_REQUESTS: usize = 1;
const NUM_PEERS_TO_RECEIVE_BROADCASTS: usize = 3;
const GET_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub trait GossipMessageStore {
    /// The implementors should guarantee that the returned messages are sorted by timestamp in the ascending order.
    fn get_broadcast_messages_iter(
        &self,
        after_cursor: &Cursor,
    ) -> impl IntoIterator<Item = BroadcastMessageWithTimestamp>;

    fn get_broadcast_messages(
        &self,
        after_cursor: &Cursor,
        count: Option<u16>,
    ) -> Vec<BroadcastMessageWithTimestamp> {
        self.get_broadcast_messages_iter(after_cursor)
            .into_iter()
            .take(count.unwrap_or(DEFAULT_NUM_OF_BROADCAST_MESSAGE as u16) as usize)
            .collect()
    }

    fn query_broadcast_messages<I: IntoIterator<Item = BroadcastMessageQuery>>(
        &self,
        queries: I,
    ) -> (Vec<BroadcastMessageWithTimestamp>, Vec<u16>) {
        let mut results = Vec::new();
        let mut missing = Vec::new();
        for (index, query) in queries.into_iter().enumerate() {
            if let Some(message) = self.query_broadcast_message(query) {
                results.push(message);
            } else {
                missing.push(index as u16);
            }
        }
        (results, missing)
    }

    fn query_broadcast_message(
        &self,
        query: BroadcastMessageQuery,
    ) -> Option<BroadcastMessageWithTimestamp> {
        match query.flags {
            BroadcastMessageQueryFlags::ChannelAnnouncement => self
                .get_latest_channel_announcement(&query.channel_outpoint)
                .map(|(timestamp, channel_announcement)| {
                    BroadcastMessageWithTimestamp::ChannelAnnouncement(
                        timestamp,
                        channel_announcement,
                    )
                }),
            BroadcastMessageQueryFlags::ChannelUpdateNode1 => self
                .get_latest_channel_update(&query.channel_outpoint, true)
                .map(|channel_update| BroadcastMessageWithTimestamp::ChannelUpdate(channel_update)),
            BroadcastMessageQueryFlags::ChannelUpdateNode2 => self
                .get_latest_channel_update(&query.channel_outpoint, false)
                .map(|channel_update| BroadcastMessageWithTimestamp::ChannelUpdate(channel_update)),

            BroadcastMessageQueryFlags::NodeAnnouncementNode1
            | BroadcastMessageQueryFlags::NodeAnnouncementNode2 => self
                .get_latest_channel_announcement(&query.channel_outpoint)
                .and_then(|(_, channel_announcement)| {
                    let node = if query.flags == BroadcastMessageQueryFlags::NodeAnnouncementNode1 {
                        &channel_announcement.node1_id
                    } else {
                        &channel_announcement.node2_id
                    };
                    self.get_latest_node_announcement(node)
                        .map(|m| BroadcastMessageWithTimestamp::NodeAnnouncement(m))
                }),
        }
    }

    fn save_broadcast_message(&self, message: BroadcastMessageWithTimestamp) {
        match message {
            BroadcastMessageWithTimestamp::ChannelAnnouncement(timestamp, channel_announcement) => {
                self.save_channel_announcement(timestamp, channel_announcement)
            }
            BroadcastMessageWithTimestamp::ChannelUpdate(channel_update) => {
                self.save_channel_update(channel_update)
            }
            BroadcastMessageWithTimestamp::NodeAnnouncement(node_announcement) => {
                self.save_node_announcement(node_announcement)
            }
        }
    }

    fn save_channel_announcement(&self, timestamp: u64, channel_announcement: ChannelAnnouncement);

    fn save_channel_update(&self, channel_update: ChannelUpdate);

    fn save_node_announcement(&self, node_announcement: NodeAnnouncement);

    fn get_broadcast_message_with_cursor(
        &self,
        cursor: &Cursor,
    ) -> Option<BroadcastMessageWithTimestamp>;

    fn get_latest_broadcast_message_cursor(&self) -> Option<Cursor>;

    fn get_latest_channel_announcement_timestamp(&self, outpoint: &OutPoint) -> Option<u64>;

    fn get_latest_channel_update_timestamp(
        &self,
        outpoint: &OutPoint,
        is_node1: bool,
    ) -> Option<u64>;

    fn get_latest_node_announcement_timestamp(&self, pk: &Pubkey) -> Option<u64>;

    fn get_latest_channel_announcement(
        &self,
        outpoint: &OutPoint,
    ) -> Option<(u64, ChannelAnnouncement)> {
        self.get_latest_channel_announcement_timestamp(outpoint)
            .and_then(|timestamp| {
                 self.get_broadcast_message_with_cursor(&Cursor::new(
                    timestamp,
                    BroadcastMessageID::ChannelAnnouncement(outpoint.clone()),
                )).and_then(|message| match message {
                    BroadcastMessageWithTimestamp::ChannelAnnouncement(
                        _,
                        channel_announcement,
                    ) => Some((timestamp, channel_announcement)),
                    _ => panic!(
                        "get_latest_channel_announcement returned non-ChannelAnnouncement message from db: channel outpoint {:?}, message {:?}", outpoint, message
                    ),
                })
            })
    }

    fn get_latest_channel_update(
        &self,
        outpoint: &OutPoint,
        is_node1: bool,
    ) -> Option<ChannelUpdate> {
        self.get_latest_channel_update_timestamp(outpoint, is_node1)
            .and_then(|timestamp| {
                 self.get_broadcast_message_with_cursor(&Cursor::new(
                    timestamp,
                    BroadcastMessageID::ChannelUpdate(outpoint.clone()),
                )).and_then(|message| match message {
                    BroadcastMessageWithTimestamp::ChannelUpdate(channel_update) => Some(channel_update),
                    _ => panic!("get_latest_channel_update returned non-ChannelUpdate message from db: channel outpoint {:?}, is_node1 {:?}, message {:?}", outpoint, is_node1, message),
                })
            })
    }

    fn get_latest_node_announcement(&self, pk: &Pubkey) -> Option<NodeAnnouncement> {
        self.get_latest_node_announcement_timestamp(pk).and_then(|timestamp| {
            self.get_broadcast_message_with_cursor(&Cursor::new(
                timestamp,
                BroadcastMessageID::NodeAnnouncement(pk.clone()),
            )).and_then(|message|
                    match message {
                    BroadcastMessageWithTimestamp::NodeAnnouncement(node_announcement) => Some(node_announcement),
                    _ => panic!("get_lastest_node_announcement returned non-NodeAnnouncement message from db: pk {:?}, message {:?}", pk, message),
                    }
                )
            }
        )
    }
}

pub(crate) enum GossipActorMessage {
    /// Network events to be processed by this actor.
    PeerConnected(PeerId, Pubkey, SessionContext),
    PeerDisconnected(PeerId, SessionContext),

    // The function of TickNetworkMaintenance is to maintain the network state.
    // Currently it will do the following things:
    // 1. Check inflight requests to see if they are timed out. If so, resend the request to other peers.
    // 2. Check if we have sufficient number of peers to receive broadcasts. If not, connect more peers.
    // 3. Check if there are any pending broadcast messages. If so, broadcast them to the network.
    TickNetworkMaintenance,

    // Process BroadcastMessage from the network. This is mostly used for testing.
    // In production, we process GossipMessage from the network.
    ProcessBroadcastMessage(BroadcastMessage),
    // Broadcast a BroadcastMessage created by us to the network.
    BroadcastMessage(BroadcastMessage),
    // Received GossipMessage from a peer
    GossipMessage(GossipMessageWithPeerId),
}

pub(crate) struct GossipActor<S> {
    _phantom: std::marker::PhantomData<S>,
}

impl<S> GossipActor<S> {
    fn new() -> Self {
        Self {
            _phantom: Default::default(),
        }
    }
}

// This is used to prevent sending duplicate messages to the same peer.
// It may report false negatives (i.e. we may have sent some messages, but it reports that we haven't).
// Even though we wasted some bandwidth, it is not a big deal.
// It will never report false positives (i.e. we haven't sent a message, but it reports that we have).
// So we will never miss any messages.
struct SentMessagesCache {
    cache: LruCache<u64, ()>,
}

impl Default for SentMessagesCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SentMessagesCache {
    fn new() -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(8092).unwrap()),
        }
    }

    fn get_message_hash(message: &BroadcastMessage) -> u64 {
        let mut s = DefaultHasher::new();
        message.hash(&mut s);
        s.finish()
    }

    fn insert(&mut self, message: &BroadcastMessage) {
        self.cache.put(Self::get_message_hash(message), ());
    }

    fn insert_iter<'a, I: IntoIterator<Item = &'a BroadcastMessage>>(&mut self, messages: I) {
        for message in messages {
            self.insert(&message);
        }
    }

    fn contains(&self, message: &BroadcastMessage) -> bool {
        self.cache.contains(&Self::get_message_hash(message))
    }
}

#[derive(Default)]
struct PeerState {
    // The filter is a cursor that the peer sent to us. We will only send messages to the peer
    // that are newer than the cursor. If the peer has not sent us a filter, we will not actively
    // send messages to the peer. If the peer sends us a new filter, we will update this field,
    // and send all the messages after the cursor to the peer immediately.
    filter: Option<Cursor>,
    // Any messages with cursor >= biggest_cursor_sent is definitely not sent to the peer.
    // We may have sent (or may haven't) some message with cursor < biggest_cursor_sent to the peer.
    // That depends on if the message arrives at our side in order or not.
    // If it arrived out of order, i.e. we received a message with cursor < biggest_cursor_sent later,
    // we may have not sent the message to the peer. We will use SentMessagesCache to query if
    // we have sent the message to the peer. If we haven't, we will send the message to the peer.
    biggest_cursor_sent: Cursor,
    // A cache of sent messages to prevent sending duplicate messages to the same peer.
    // This only applies to BroadcastMessagesFilterResult.
    // That means
    // 1. When the peer requests messages via GetBroadcastMessages or QueryBroadcastMessages,
    // we will always send the whole result.
    // 2. Even if we've already sent peer messages via GetBroadcastMessagesResult or
    // QueryBroadcastMessagesResult, we will still send them again with BroadcastMessagesFilterResult.
    sent_messages_cache: SentMessagesCache,
}

pub(crate) struct GossipActorState<S> {
    store: S,
    chain_actor: ActorRef<CkbChainMessage>,
    control: ServiceAsyncControl,
    next_request_id: u64,
    // We sent a GetBroadcastMessages request to a peer, and we are waiting for the response.
    // The key is (peer_id, request_id), and the value is the timestamp of the request and the cursor.
    inflight_gets: HashMap<(PeerId, u64), (u64, Cursor)>,
    // Whether the node is syncing with peers. If this is true, we will send GetBroadcastMessages
    // requests to peers to sync with them. Otherwise, we will only send BroadcastMessagesFilter
    // requests to peers and wait for them to send us BroadcastMessagesFilterResult.
    is_syncing: bool,
    // Messages that are pending to be broadcasted to the network.
    pending_broadcast_messages: HashMap<BroadcastMessage, u64>,
    peer_session_map: HashMap<PeerId, SessionId>,
    peer_states: HashMap<PeerId, PeerState>,
    // The last BroadcastMessagesFilter we sent to a peer. We will maintain
    // a number of filters for each peer, so that we can receive the latest
    // messages from the peer.
    my_filter_map: HashMap<PeerId, Cursor>,
}

impl<S> GossipActorState<S>
where
    S: GossipMessageStore + Send + Sync + 'static,
{
    fn get_peer_session(&self, peer_id: &PeerId) -> Option<SessionId> {
        self.peer_session_map.get(peer_id).cloned()
    }

    fn get_latest_cursor(&self) -> Cursor {
        self.store
            .get_latest_broadcast_message_cursor()
            .unwrap_or_default()
    }

    async fn verify_and_save_broadcast_message(
        &mut self,
        message: BroadcastMessage,
    ) -> Result<BroadcastMessageWithTimestamp, Error> {
        if let Some(timestamp) = self.pending_broadcast_messages.get(&message) {
            return Ok((message, *timestamp).into());
        }
        let message_id = message.id();
        let verified_message = verify_broadcast_message(message, &self.store, &self.chain_actor)
            .await
            .map_err(|error| {
                error!(
                    "Failed to process broadcast message with id {:?}: {:?}",
                    message_id, &error
                );
                error
            })?;

        self.store.save_broadcast_message(verified_message.clone());
        self.pending_broadcast_messages.insert(
            verified_message.clone().into(),
            verified_message.timestamp(),
        );
        Ok(verified_message)
    }

    async fn send_message_to_session(
        &self,
        session_id: SessionId,
        message: GossipMessage,
    ) -> crate::Result<()> {
        self.control
            .send_message_to(session_id, GOSSIP_PROTOCOL_ID, message.to_molecule_bytes())
            .await?;
        Ok(())
    }

    async fn send_message_to_peer(
        &self,
        peer_id: &PeerId,
        message: GossipMessage,
    ) -> crate::Result<()> {
        match self.get_peer_session(peer_id) {
            Some(session_id) => self.send_message_to_session(session_id, message).await,
            None => Err(Error::PeerNotFound(peer_id.clone())),
        }
    }

    fn get_and_increment_request_id(&mut self) -> u64 {
        let id = self.next_request_id;
        self.next_request_id += 1;
        id
    }

    async fn send_get_broadcast_messages(&mut self, peer_id: &PeerId) {
        let id = self.get_and_increment_request_id();
        let cursor = self.get_latest_cursor();
        self.inflight_gets
            .insert((peer_id.clone(), id), (now_timestamp(), cursor.clone()));
        let message = GossipMessage::GetBroadcastMessages(GetBroadcastMessages {
            id,
            chain_hash: get_chain_hash(),
            after_cursor: cursor.clone(),
            count: DEFAULT_NUM_OF_BROADCAST_MESSAGE,
        });
        if let Err(error) = self.send_message_to_peer(peer_id, message).await {
            error!(
                "Failed to send GetBroadcastMessages to peer {:?}: {:?}",
                &peer_id, error
            );
        }
    }

    async fn send_broadcast_message_filter(&mut self, peer_id: &PeerId) {
        let cursor = self.get_latest_cursor();
        let message = GossipMessage::BroadcastMessagesFilter(BroadcastMessagesFilter {
            chain_hash: get_chain_hash(),
            after_cursor: cursor.clone(),
        });
        debug!(
            "Sending BroadcastMessagesFilter to peer {:?}: {:?}",
            &peer_id, &message
        );
        if let Err(error) = self.send_message_to_peer(peer_id, message).await {
            error!(
                "Failed to send BroadcastMessagesFilter to peer {:?}: {:?}",
                &peer_id, error
            );
        }
        self.my_filter_map.insert(peer_id.clone(), cursor);
    }
}

pub(crate) struct GossipProtocolHandle {
    actor: ActorRef<GossipActorMessage>,
    sender: Option<oneshot::Sender<ServiceAsyncControl>>,
}

async fn verify_broadcast_message<S: GossipMessageStore>(
    message: BroadcastMessage,
    store: &S,
    chain: &ActorRef<CkbChainMessage>,
) -> Result<BroadcastMessageWithTimestamp, Error> {
    match message {
        BroadcastMessage::ChannelAnnouncement(channel_announcement) => {
            let timestamp =
                verify_channel_announcement(&channel_announcement, store, chain).await?;
            Ok(BroadcastMessageWithTimestamp::ChannelAnnouncement(
                timestamp,
                channel_announcement,
            ))
        }
        BroadcastMessage::ChannelUpdate(channel_update) => {
            verify_channel_update(&channel_update, store)?;
            Ok(BroadcastMessageWithTimestamp::ChannelUpdate(channel_update))
        }
        BroadcastMessage::NodeAnnouncement(node_announcement) => {
            verify_node_announcement(&node_announcement, store)?;
            Ok(BroadcastMessageWithTimestamp::NodeAnnouncement(
                node_announcement,
            ))
        }
    }
}

async fn verify_channel_announcement<S: GossipMessageStore>(
    channel_announcement: &ChannelAnnouncement,
    store: &S,
    chain: &ActorRef<CkbChainMessage>,
) -> Result<u64, Error> {
    debug!(
        "Verifying channel announcement message: {:?}",
        &channel_announcement
    );
    if let Some((timestamp, announcement)) =
        store.get_latest_channel_announcement(&channel_announcement.channel_outpoint)
    {
        if announcement == *channel_announcement {
            return Ok(timestamp);
        } else {
            return Err(Error::InvalidParameter(format!(
                "Channel announcement message already exists but mismatched: {:?}, existing: {:?}",
                &channel_announcement, &announcement
            )));
        }
    }
    let message = channel_announcement.message_to_sign();
    if channel_announcement.node1_id == channel_announcement.node2_id {
        return Err(Error::InvalidParameter(format!(
            "Channel announcement node had a channel with itself: {:?}",
            &channel_announcement
        )));
    }
    let (node1_signature, node2_signature, ckb_signature) = match (
        &channel_announcement.node1_signature,
        &channel_announcement.node2_signature,
        &channel_announcement.ckb_signature,
    ) {
        (Some(node1_signature), Some(node2_signature), Some(ckb_signature)) => {
            (node1_signature, node2_signature, ckb_signature)
        }
        _ => {
            return Err(Error::InvalidParameter(format!(
                "Channel announcement message signature verification failed, some signatures are missing: {:?}",
                &channel_announcement
            )));
        }
    };

    if !node1_signature.verify(&channel_announcement.node1_id, &message) {
        return Err(Error::InvalidParameter(format!(
            "Channel announcement message signature verification failed for node 1: {:?}, message: {:?}, signature: {:?}, pubkey: {:?}",
            &channel_announcement,
            &message,
            &node1_signature,
            &channel_announcement.node1_id
        )));
    }

    if !node2_signature.verify(&channel_announcement.node2_id, &message) {
        return Err(Error::InvalidParameter(format!(
            "Channel announcement message signature verification failed for node 2: {:?}, message: {:?}, signature: {:?}, pubkey: {:?}",
            &channel_announcement,
            &message,
            &node2_signature,
            &channel_announcement.node2_id
        )));
    }

    debug!(
        "Node signatures in channel announcement message verified: {:?}",
        &channel_announcement
    );

    let (tx, block_hash) = match call_t!(
        chain,
        CkbChainMessage::TraceTx,
        DEFAULT_CHAIN_ACTOR_TIMEOUT,
        TraceTxRequest {
            tx_hash: channel_announcement.channel_outpoint.tx_hash(),
            confirmations: 1,
        }
    ) {
        Ok(TraceTxResponse {
            tx: Some(tx),
            status:
                TxStatus {
                    status: Status::Committed,
                    block_hash: Some(block_hash),
                    ..
                },
        }) => (tx, block_hash),
        err => {
            return Err(Error::InvalidParameter(format!(
                "Channel announcement transaction {:?} not found or not confirmed, result is: {:?}",
                &channel_announcement.channel_outpoint.tx_hash(),
                err
            )));
        }
    };

    debug!("Channel announcement transaction found: {:?}", &tx);

    let pubkey = channel_announcement.ckb_key.serialize();
    let pubkey_hash = &blake2b_256(pubkey.as_slice())[0..20];
    match tx.inner.outputs.first() {
        None => {
            return Err(Error::InvalidParameter(format!(
                "On-chain transaction found but no output: {:?}",
                &channel_announcement
            )));
        }
        Some(output) => {
            if output.lock.args.as_bytes() != pubkey_hash {
                return Err(Error::InvalidParameter(format!(
                    "On-chain transaction found but pubkey hash mismatched: on chain hash {:?}, pub key ({:?}) hash {:?}",
                    &output.lock.args.as_bytes(),
                    hex::encode(pubkey),
                    &pubkey_hash
                )));
            }
            let capacity: u128 = u64::from(output.capacity).into();
            if channel_announcement.udt_type_script.is_none()
                && capacity != channel_announcement.capacity
            {
                return Err(Error::InvalidParameter(format!(
                    "On-chain transaction found but capacity mismatched: on chain capacity {:?}, channel capacity {:?}",
                    &output.capacity, &channel_announcement.capacity
                )));
            }
            capacity
        }
    };

    if let Err(err) = secp256k1_instance().verify_schnorr(
        ckb_signature,
        &Message::from_digest(message),
        &channel_announcement.ckb_key,
    ) {
        return Err(Error::InvalidParameter(format!(
            "Channel announcement message signature verification failed for ckb: {:?}, message: {:?}, signature: {:?}, pubkey: {:?}, error: {:?}",
            &channel_announcement,
            &message,
            &ckb_signature,
            &channel_announcement.ckb_key,
            &err
        )));
    }

    debug!(
        "All signatures in channel announcement message verified: {:?}",
        &channel_announcement
    );

    let timestamp: u64 = match call_t!(
        chain,
        CkbChainMessage::GetBlockTimestamp,
        DEFAULT_CHAIN_ACTOR_TIMEOUT,
        GetBlockTimestampRequest::from_block_hash(block_hash.clone())
    ) {
        Ok(Ok(Some(timestamp))) => timestamp,
        Ok(Ok(None)) => {
            return Err(Error::InternalError(anyhow::anyhow!(
                "Unable to find block {:?} for channel outpoint {:?}",
                &block_hash,
                &channel_announcement.channel_outpoint
            )));
        }
        Ok(Err(err)) => {
            return Err(Error::CkbRpcError(err));
        }
        Err(err) => {
            return Err(Error::InternalError(anyhow::Error::new(err).context(
                format!(
                    "Error while trying to obtain block {:?} for channel outpoint {:?}",
                    block_hash, channel_announcement.channel_outpoint
                ),
            )));
        }
    };

    debug!(
        "Saving channel announcement after obtained block timestamp for transaction {:?}: {}",
        &channel_announcement.channel_outpoint, timestamp
    );

    Ok(timestamp)
}

fn verify_channel_update<S: GossipMessageStore>(
    channel_update: &ChannelUpdate,
    store: &S,
) -> Result<(), Error> {
    if let Some(BroadcastMessageWithTimestamp::ChannelUpdate(existing)) =
        store.get_broadcast_message_with_cursor(&channel_update.cursor())
    {
        if existing == *channel_update {
            return Ok(());
        } else {
            return Err(Error::InvalidParameter(format!(
                "Channel update message already exists but mismatched: {:?}, existing: {:?}",
                &channel_update, &existing
            )));
        }
    }
    let message = channel_update.message_to_sign();

    let signature = match channel_update.signature {
        Some(ref signature) => signature,
        None => {
            return Err(Error::InvalidParameter(format!(
                "Channel update message signature verification failed (signature not found): {:?}",
                &channel_update
            )));
        }
    };
    match store.get_latest_channel_announcement(&channel_update.channel_outpoint) {
        Some((_, channel_announcement)) => {
            let pubkey = if channel_update.is_update_of_node_1() {
                channel_announcement.node1_id
            } else {
                channel_announcement.node2_id
            };
            debug!(
                "Verifying channel update message signature: {:?}, pubkey: {:?}, message: {:?}",
                &channel_update, &pubkey, &message
            );
            if !signature.verify(&pubkey, &message) {
                return Err(Error::InvalidParameter(format!(
                    "Channel update message signature verification failed (invalid signature): {:?}",
                    &channel_update
                )));
            }
        }
        None => {
            // TODO: It is possible that the channel update message is received before the channel announcement message.
            // In this case, we should store the channel update message and verify it later when the channel announcement message is received.
            return Err(Error::InvalidParameter(format!(
                "Failed to process channel update because channel announcement not found: {:?}",
                &channel_update
            )));
        }
    }
    Ok(())
}

fn verify_node_announcement<S: GossipMessageStore>(
    node_announcement: &NodeAnnouncement,
    store: &S,
) -> Result<(), Error> {
    if let Some(BroadcastMessageWithTimestamp::NodeAnnouncement(announcement)) =
        store.get_broadcast_message_with_cursor(&node_announcement.cursor())
    {
        if announcement == *node_announcement {
            return Ok(());
        } else {
            return Err(Error::InvalidParameter(format!(
                "Node announcement message already exists but mismatched: {:?}, existing: {:?}",
                &node_announcement, &announcement
            )));
        }
    }
    let message = node_announcement.message_to_sign();
    match node_announcement.signature {
        Some(ref signature) if signature.verify(&node_announcement.node_id, &message) => {
            debug!(
                "Node announcement message verified: {:?}",
                &node_announcement
            );
        }
        _ => {
            return Err(Error::InvalidParameter(format!(
                "Node announcement message signature verification failed: {:?}",
                &node_announcement
            )));
        }
    }
    Ok(())
}

impl GossipProtocolHandle {
    pub(crate) async fn new<S: GossipMessageStore + Send + Sync + 'static>(
        name: Option<String>,
        gossip_network_maintenance_interval: Duration,
        store: S,
        chain_actor: ActorRef<CkbChainMessage>,
        supervisor: ActorCell,
    ) -> Self {
        let (sender, receiver) = oneshot::channel();

        let (actor, _handle) = ActorRuntime::spawn_linked_instant(
            name,
            GossipActor::new(),
            (
                receiver,
                gossip_network_maintenance_interval,
                store,
                chain_actor,
            ),
            supervisor,
        )
        .expect("start gossip actor");
        Self {
            actor,
            sender: Some(sender),
        }
    }

    pub(crate) fn actor(&self) -> &ActorRef<GossipActorMessage> {
        &self.actor
    }

    pub(crate) fn create_meta(self) -> ProtocolMeta {
        MetaBuilder::new()
            .id(GOSSIP_PROTOCOL_ID)
            .service_handle(move || {
                let handle = Box::new(self);
                ProtocolHandle::Callback(handle)
            })
            .build()
    }
}

#[rasync_trait]
impl<S> Actor for GossipActor<S>
where
    S: GossipMessageStore + Send + Sync + 'static,
{
    type Msg = GossipActorMessage;
    type State = GossipActorState<S>;
    type Arguments = (
        oneshot::Receiver<ServiceAsyncControl>,
        Duration,
        S,
        ActorRef<CkbChainMessage>,
    );

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        (rx, network_maintenance_interval, store, chain_actor): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let control = timeout(Duration::from_secs(1), rx)
            .await
            .expect("received control timely")
            .expect("receive control");
        debug!("Gossip actor received service control");

        let _ = myself.send_after(Duration::from_millis(500), || {
            GossipActorMessage::TickNetworkMaintenance
        });
        let _ = myself.send_interval(network_maintenance_interval, || {
            GossipActorMessage::TickNetworkMaintenance
        });
        let state = Self::State {
            store,
            chain_actor,
            control,
            next_request_id: 0,
            inflight_gets: Default::default(),
            is_syncing: true,
            pending_broadcast_messages: Default::default(),
            peer_session_map: Default::default(),
            peer_states: Default::default(),
            my_filter_map: Default::default(),
        };
        Ok(state)
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            GossipActorMessage::PeerConnected(peer_id, pubkey, session) => {
                if state.peer_session_map.contains_key(&peer_id) {
                    warn!(
                        "Repeated connection from {:?} for gossip protocol",
                        &peer_id
                    );
                    return Ok(());
                }
                debug!(
                    "Saving gossip peer pubkey and session: peer {:?}, pubkey {:?}, session {:?}",
                    &peer_id, &pubkey, &session.id
                );
                state.peer_session_map.insert(peer_id.clone(), session.id);
                state
                    .peer_states
                    .insert(peer_id.clone(), Default::default());
            }
            GossipActorMessage::PeerDisconnected(peer_id, session) => {
                debug!(
                    "Peer disconnected: peer {:?}, session {:?}",
                    &peer_id, &session.id
                );
                let _ = state.peer_session_map.remove(&peer_id);
            }
            GossipActorMessage::ProcessBroadcastMessage(message) => {
                if let Err(error) = state
                    .verify_and_save_broadcast_message(message.clone())
                    .await
                {
                    error!(
                        "Failed to process broadcast message {:?}: {:?}",
                        &message, &error
                    );
                }
            }
            GossipActorMessage::BroadcastMessage(message) => {
                debug!("Trying to broadcast message: {:?}", &message);
                match state
                    .verify_and_save_broadcast_message(message.clone())
                    .await
                {
                    Ok(broadcast_message) => {
                        // This is our own message, so we will broadcast it to all peers immediately.
                        for (peer, session) in &state.peer_session_map {
                            let peer_cursor =
                                state.peer_states.get(peer).and_then(|s| s.filter.as_ref());
                            match peer_cursor {
                                Some(cursor) if cursor < &broadcast_message.cursor() => {
                                    state
                                        .send_message_to_session(
                                            *session,
                                            GossipMessage::BroadcastMessagesFilterResult(
                                                broadcast_message
                                                    .create_broadcast_messages_filter_result(),
                                            ),
                                        )
                                        .await?;
                                }
                                _ => {
                                    debug!(
                                        "Ignoring broadcast message for peer {:?}: {:?} as its cursor is {:?}",
                                        peer, &broadcast_message, peer_cursor
                                    );
                                }
                            }
                        }
                    }
                    Err(error) => {
                        // This should never happen because we have already verified the message before broadcasting.
                        // But it is possible that we failed to obtain the block timestamp for the message.
                        error!(
                            "Failed to verify and save broadcast message {:?}: {:?}",
                            &message, &error
                        );
                        return Ok(());
                    }
                };
            }
            GossipActorMessage::TickNetworkMaintenance => {
                debug!("Network maintenance ticked, current state: num of peers: {}, inflight requests: {}, is syncing: {}",
                    state.peer_session_map.len(), state.inflight_gets.len(), state.is_syncing);

                let now = now_timestamp();

                state
                    .inflight_gets
                    .retain(|_, (v, _)| now - *v < GET_REQUEST_TIMEOUT.as_millis() as u64);

                let current_peers = state
                    .inflight_gets
                    .keys()
                    .map(|p| p.0.clone())
                    .collect::<Vec<_>>();
                let current_num_peers = state.inflight_gets.len();
                if current_num_peers < NUM_SIMULTANEOUS_GET_REQUESTS && state.is_syncing {
                    let peers = state
                        .peer_session_map
                        .keys()
                        .filter(|p| !current_peers.contains(p))
                        .take(NUM_SIMULTANEOUS_GET_REQUESTS - current_num_peers)
                        .cloned()
                        .collect::<Vec<_>>();

                    for peer_id in peers {
                        state.send_get_broadcast_messages(&peer_id).await;
                    }
                }

                state
                    .my_filter_map
                    .retain(|k, _| state.peer_session_map.contains_key(k));

                let current_num_peers = state.my_filter_map.len();
                if current_num_peers < NUM_PEERS_TO_RECEIVE_BROADCASTS && !state.is_syncing {
                    let peers = state
                        .peer_session_map
                        .keys()
                        .filter(|p| !state.my_filter_map.contains_key(p))
                        .take(NUM_PEERS_TO_RECEIVE_BROADCASTS - current_num_peers)
                        .cloned()
                        .collect::<Vec<_>>();

                    for peer_id in peers {
                        state.send_broadcast_message_filter(&peer_id).await;
                    }
                }

                // Save the pending_broadcast_messages to a new variable and clear the original one.
                let pending_messages = std::mem::take(&mut state.pending_broadcast_messages);
                let mut broadcast_messages = pending_messages
                    .into_iter()
                    .map(|(m, t)| BroadcastMessageWithTimestamp::from((m, t)))
                    .collect::<Vec<BroadcastMessageWithTimestamp>>();
                broadcast_messages.sort_by_key(|m| m.cursor());

                trace!(
                    "Trying to rebroadcast pending messages to peers: {:?}",
                    &broadcast_messages
                );
                for (peer, session) in &state.peer_session_map {
                    let (filter_cursor, peer_state) = match state
                        .peer_states
                        .get(peer)
                        .and_then(|s| s.filter.as_ref().map(|f| (f, s)))
                    {
                        Some((filter, state)) => (filter, state),
                        _ => {
                            continue;
                        }
                    };
                    let messages = broadcast_messages
                        .iter()
                        .filter_map(|m| {
                            let c = m.cursor();
                            let m = BroadcastMessage::from(m.clone());
                            (&c > filter_cursor || !peer_state.sent_messages_cache.contains(&m))
                                .then_some(m)
                        })
                        .collect::<Vec<_>>();
                    for chunk in messages.chunks(DEFAULT_NUM_OF_BROADCAST_MESSAGE as usize) {
                        state
                            .send_message_to_session(
                                *session,
                                GossipMessage::BroadcastMessagesFilterResult(
                                    BroadcastMessagesFilterResult {
                                        messages: chunk.to_vec(),
                                    },
                                ),
                            )
                            .await?;
                    }
                }
            }
            GossipActorMessage::GossipMessage(GossipMessageWithPeerId { peer_id, message }) => {
                match message {
                    GossipMessage::BroadcastMessagesFilter(BroadcastMessagesFilter {
                        chain_hash,
                        after_cursor,
                    }) => {
                        check_chain_hash(&chain_hash)?;
                        let mut biggest_cursor_sent = after_cursor.clone();
                        let mut sent_messages_cache = SentMessagesCache::new();
                        // Immediately send existing messages after the cursor in the store to the peer.
                        loop {
                            let messages = state.store.get_broadcast_messages(
                                &biggest_cursor_sent,
                                Some(DEFAULT_NUM_OF_BROADCAST_MESSAGE),
                            );
                            match messages.last() {
                                Some(last_message) => {
                                    biggest_cursor_sent = last_message.cursor();
                                }
                                None => {
                                    break;
                                }
                            }
                            let messages = messages
                                .into_iter()
                                .map(Into::into)
                                .collect::<Vec<BroadcastMessage>>();
                            sent_messages_cache.insert_iter(&messages);
                            let result = GossipMessage::BroadcastMessagesFilterResult(
                                BroadcastMessagesFilterResult { messages },
                            );
                            if let Err(error) = state.send_message_to_peer(&peer_id, result).await {
                                error!(
                                    "Failed to send BroadcastMessagesFilterResult to peer {:?}: {:?}",
                                    &peer_id, error
                                );
                                break;
                            }
                        }
                        state.peer_states.insert(
                            peer_id,
                            PeerState {
                                filter: Some(after_cursor),
                                biggest_cursor_sent,
                                sent_messages_cache,
                            },
                        );
                    }
                    GossipMessage::BroadcastMessagesFilterResult(
                        BroadcastMessagesFilterResult { messages },
                    ) => {
                        for message in messages {
                            let _ = state.verify_and_save_broadcast_message(message).await;
                        }
                    }
                    GossipMessage::GetBroadcastMessages(get_broadcast_messages) => {
                        check_chain_hash(&get_broadcast_messages.chain_hash)?;
                        if get_broadcast_messages.count > MAX_NUM_OF_BROADCAST_MESSAGES {
                            warn!(
                                "Received GetBroadcastMessages with too many messages: {:?}",
                                get_broadcast_messages.count
                            );
                            return Ok(());
                        }
                        let id = get_broadcast_messages.id;
                        let messages = state.store.get_broadcast_messages(
                            &get_broadcast_messages.after_cursor,
                            Some(get_broadcast_messages.count as u16),
                        );
                        let result =
                            GossipMessage::GetBroadcastMessagesResult(GetBroadcastMessagesResult {
                                id,
                                messages: messages.into_iter().map(|m| m.into()).collect(),
                            });
                        if let Err(error) = state.send_message_to_peer(&peer_id, result).await {
                            error!(
                                "Failed to send GetBroadcastMessagesResult to peer {:?}: {:?}",
                                &peer_id, error
                            );
                        }
                    }
                    GossipMessage::GetBroadcastMessagesResult(GetBroadcastMessagesResult {
                        id,
                        messages,
                    }) => {
                        if messages.is_empty() {
                            state.is_syncing = false;
                            let _ = myself.send_message(GossipActorMessage::TickNetworkMaintenance);
                            return Ok(());
                        }
                        let current_cursor = state.get_latest_cursor();
                        for message in messages {
                            // TODO: handle invalid messages.
                            let _ = state.verify_and_save_broadcast_message(message).await;
                        }
                        state.inflight_gets.remove(&(peer_id, id));
                        if current_cursor != state.get_latest_cursor() {
                            // Immediately send another TickNetworkMaintenance to start syncing with the next cursor.
                            let _ = myself.send_message(GossipActorMessage::TickNetworkMaintenance);
                        }
                    }
                    GossipMessage::QueryBroadcastMessages(QueryBroadcastMessages {
                        id,
                        chain_hash,
                        queries,
                    }) => {
                        check_chain_hash(&chain_hash)?;
                        if queries.len() > MAX_NUM_OF_BROADCAST_MESSAGES as usize {
                            warn!(
                                "Received QueryBroadcastMessages with too many queries: {:?}",
                                queries.len()
                            );
                            return Ok(());
                        }
                        let (results, missing_queries) =
                            state.store.query_broadcast_messages(queries);
                        let result = GossipMessage::QueryBroadcastMessagesResult(
                            QueryBroadcastMessagesResult {
                                id,
                                messages: results.into_iter().map(|m| m.into()).collect(),
                                missing_queries: missing_queries,
                            },
                        );
                        if let Err(error) = state.send_message_to_peer(&peer_id, result).await {
                            error!(
                                "Failed to send QueryBroadcastMessagesResult to peer {:?}: {:?}",
                                &peer_id, error
                            );
                        }
                    }
                    GossipMessage::QueryBroadcastMessagesResult(QueryBroadcastMessagesResult {
                        id,
                        messages,
                        missing_queries,
                    }) => {
                        let is_finished = missing_queries.is_empty();
                        for message in messages {
                            let _ = state.verify_and_save_broadcast_message(message).await;
                        }
                        // TODO: mark requests corresponding to id as finished
                        // TODO: if not finished, send another QueryBroadcastMessages.
                    }
                }
            }
        }

        Ok(())
    }
}

#[tasync_trait]
impl ServiceProtocol for GossipProtocolHandle {
    async fn init(&mut self, context: &mut ProtocolContext) {
        let sender = self
            .sender
            .take()
            .expect("service control sender set and init called once");
        if let Err(_) = sender.send(context.control().clone()) {
            panic!("Failed to send service control");
        }
    }

    async fn connected(&mut self, context: ProtocolContextMutRef<'_>, version: &str) {
        info!(
            "proto id [{}] open on session [{}], address: [{}], type: [{:?}], version: {}",
            context.proto_id,
            context.session.id,
            context.session.address,
            context.session.ty,
            version
        );

        if let Some(remote_pubkey) = context.session.remote_pubkey.clone() {
            let remote_peer_id = PeerId::from_public_key(&remote_pubkey);
            let _ = self.actor.send_message(GossipActorMessage::PeerConnected(
                remote_peer_id,
                remote_pubkey.into(),
                context.session.clone(),
            ));
        } else {
            warn!("Peer connected without remote pubkey {:?}", context.session);
        }
    }

    async fn disconnected(&mut self, context: ProtocolContextMutRef<'_>) {
        info!(
            "proto id [{}] close on session [{}], address: [{}], type: [{:?}]",
            context.proto_id, context.session.id, &context.session.address, &context.session.ty
        );

        match context.session.remote_pubkey.as_ref() {
            Some(remote_pubkey) => {
                let remote_peer_id = PeerId::from_public_key(&remote_pubkey);
                let _ = self
                    .actor
                    .send_message(GossipActorMessage::PeerDisconnected(
                        remote_peer_id,
                        context.session.clone(),
                    ));
            }
            None => {
                unreachable!("Received message without remote pubkey");
            }
        }
    }

    async fn received(&mut self, context: ProtocolContextMutRef<'_>, data: Bytes) {
        let message = unwrap_or_return!(GossipMessage::from_molecule_slice(&data), "parse message");
        match context.session.remote_pubkey.as_ref() {
            Some(pubkey) => {
                let peer_id = PeerId::from_public_key(pubkey);
                let _ = self.actor.send_message(GossipActorMessage::GossipMessage(
                    GossipMessageWithPeerId { peer_id, message },
                ));
            }
            None => {
                unreachable!("Received message without remote pubkey");
            }
        }
    }

    async fn notify(&mut self, _context: &mut ProtocolContext, _token: u64) {}
}
