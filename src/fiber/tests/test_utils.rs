use ckb_types::packed::OutPoint;
use ckb_types::{core::TransactionView, packed::Byte32};
use ractor::{call, Actor, ActorRef};
use rand::Rng;
use secp256k1::{rand, PublicKey, Secp256k1, SecretKey};
use std::{
    collections::HashMap,
    env,
    ffi::OsStr,
    mem::ManuallyDrop,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};
use tempfile::TempDir as OldTempDir;
use tentacle::{multiaddr::MultiAddr, secio::PeerId};
use tokio::sync::RwLock as TokioRwLock;
use tokio::{
    select,
    sync::{mpsc, OnceCell},
    time::sleep,
};

use crate::fiber::gossip::{GossipMessageStore, DEFAULT_NUM_OF_BROADCAST_MESSAGE};
use crate::fiber::network::{AcceptChannelCommand, OpenChannelCommand};
use crate::fiber::types::{
    BroadcastMessageID, BroadcastMessageWithTimestamp, ChannelAnnouncement, ChannelUpdate, Cursor,
    NodeAnnouncement, Privkey,
};
use crate::{
    actors::{RootActor, RootActorMessage},
    ckb::tests::test_utils::{
        get_tx_from_hash, submit_tx, trace_tx, trace_tx_hash, MockChainActor,
    },
    ckb::CkbChainMessage,
    fiber::channel::{ChannelActorState, ChannelActorStateStore, ChannelState},
    fiber::graph::NetworkGraphStateStore,
    fiber::graph::PaymentSession,
    fiber::graph::{ChannelInfo, NetworkGraph, NodeInfo},
    fiber::network::{
        NetworkActor, NetworkActorCommand, NetworkActorMessage, NetworkActorStartArguments,
        NetworkActorStateStore, PersistentNetworkActorState,
    },
    fiber::types::{Hash256, Pubkey},
    invoice::{CkbInvoice, InvoiceError, InvoiceStore},
    tasks::{new_tokio_cancellation_token, new_tokio_task_tracker},
    FiberConfig, NetworkServiceEvent,
};

static RETAIN_VAR: &str = "TEST_TEMP_RETAIN";

#[derive(Debug)]
pub struct TempDir(ManuallyDrop<OldTempDir>);

impl TempDir {
    fn new<S: AsRef<OsStr>>(prefix: S) -> Self {
        Self(ManuallyDrop::new(
            OldTempDir::with_prefix(prefix).expect("create temp directory"),
        ))
    }

    pub fn to_str(&self) -> &str {
        self.0.path().to_str().expect("path to str")
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        self.0.path()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let retain = env::var(RETAIN_VAR);
        if retain.is_ok() {
            println!(
                "Keeping temp directory {:?}, as environment variable {RETAIN_VAR} set",
                self.as_ref()
            );
        } else {
            println!(
                "Deleting temp directory {:?}. To keep this directory, set environment variable {RETAIN_VAR} to anything",
                self.as_ref()
            );
            unsafe {
                ManuallyDrop::drop(&mut self.0);
            }
        }
    }
}

pub fn init_tracing() {
    use std::sync::Once;

    static INIT: Once = Once::new();

    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .pretty()
            .init();
    });
}

static ROOT_ACTOR: OnceCell<ActorRef<RootActorMessage>> = OnceCell::const_new();

pub async fn get_test_root_actor() -> ActorRef<RootActorMessage> {
    Actor::spawn(
        Some("test root actor".to_string()),
        RootActor {},
        (new_tokio_task_tracker(), new_tokio_cancellation_token()),
    )
    .await
    .expect("start test root actor")
    .0
}

pub fn generate_keypair() -> (SecretKey, PublicKey) {
    let secp = Secp256k1::new();
    let secret_key = SecretKey::new(&mut rand::thread_rng());
    let public_key = PublicKey::from_secret_key(&secp, &secret_key);
    (secret_key, public_key)
}

pub fn generate_seckey() -> SecretKey {
    SecretKey::new(&mut rand::thread_rng())
}

pub fn generate_pubkey() -> PublicKey {
    let secp = Secp256k1::new();
    let secret_key = SecretKey::new(&mut rand::thread_rng());
    let public_key = PublicKey::from_secret_key(&secp, &secret_key);
    public_key
}

pub fn gen_sha256_hash() -> Hash256 {
    let mut rng = rand::thread_rng();
    let mut result = [0u8; 32];
    rng.fill(&mut result[..]);
    result.into()
}

pub fn get_fiber_config<P: AsRef<Path>>(base_dir: P, node_name: Option<&str>) -> FiberConfig {
    let base_dir = base_dir.as_ref();
    FiberConfig {
        announced_node_name: node_name
            .or(base_dir.file_name().unwrap().to_str())
            .map(Into::into),
        announce_listening_addr: Some(true),
        base_dir: Some(PathBuf::from(base_dir)),
        // This config is needed for the timely processing of gossip messages.
        // Without this, some tests may fail due to the delay in processing gossip messages.
        gossip_network_maintenance_interval_ms: Some(50),
        // This config is needed for the timely processing of gossip messages.
        // Without this, some tests may fail due to the delay in processing gossip messages.
        gossip_store_maintenance_interval_ms: Some(50),
        auto_accept_channel_ckb_funding_amount: Some(0), // Disable auto accept for unit tests
        ..Default::default()
    }
}

pub struct NetworkNode {
    /// The base directory of the node, will be deleted after this struct dropped.
    pub base_dir: Arc<TempDir>,
    pub node_name: Option<String>,
    pub store: MemoryStore,
    pub fiber_config: FiberConfig,
    pub listening_addrs: Vec<MultiAddr>,
    pub network_actor: ActorRef<NetworkActorMessage>,
    pub network_graph: Arc<TokioRwLock<NetworkGraph<MemoryStore>>>,
    pub chain_actor: ActorRef<CkbChainMessage>,
    pub private_key: Privkey,
    pub peer_id: PeerId,
    pub event_emitter: mpsc::Receiver<NetworkServiceEvent>,
}

impl NetworkNode {
    pub fn get_node_address(&self) -> &MultiAddr {
        &self.listening_addrs[0]
    }

    pub fn get_private_key(&self) -> &Privkey {
        &self.private_key
    }

    pub fn get_public_key(&self) -> Pubkey {
        self.private_key.pubkey()
    }
}

pub struct NetworkNodeConfig {
    base_dir: Arc<TempDir>,
    node_name: Option<String>,
    store: MemoryStore,
    fiber_config: FiberConfig,
}

impl NetworkNodeConfig {
    pub fn builder() -> NetworkNodeConfigBuilder {
        NetworkNodeConfigBuilder::new()
    }
}

pub struct NetworkNodeConfigBuilder {
    base_dir: Option<Arc<TempDir>>,
    node_name: Option<String>,
    store: Option<MemoryStore>,
    // We may generate a FiberConfig based on the base_dir and node_name,
    // but allow user to override it.
    fiber_config_updater: Option<Box<dyn FnOnce(&mut FiberConfig) + 'static>>,
}

impl NetworkNodeConfigBuilder {
    pub fn new() -> Self {
        Self {
            base_dir: None,
            node_name: None,
            store: None,
            fiber_config_updater: None,
        }
    }

    pub fn base_dir(mut self, base_dir: Arc<TempDir>) -> Self {
        self.base_dir = Some(base_dir);
        self
    }

    pub fn base_dir_prefix(self, prefix: &str) -> Self {
        self.base_dir(Arc::new(TempDir::new(prefix)))
    }

    pub fn node_name(mut self, node_name: Option<String>) -> Self {
        self.node_name = node_name;
        self
    }

    pub fn store(mut self, store: MemoryStore) -> Self {
        self.store = Some(store);
        self
    }

    pub fn fiber_config_updater(
        mut self,
        updater: impl FnOnce(&mut FiberConfig) + 'static,
    ) -> Self {
        self.fiber_config_updater = Some(Box::new(updater));
        self
    }

    pub fn build(self) -> NetworkNodeConfig {
        let base_dir = self
            .base_dir
            .clone()
            .unwrap_or_else(|| Arc::new(TempDir::new("fnn-test")));
        let node_name = self.node_name.clone();
        let store = self.store.clone().unwrap_or_default();
        let fiber_config = get_fiber_config(base_dir.as_ref(), node_name.as_deref());
        let mut config = NetworkNodeConfig {
            base_dir,
            node_name,
            store,
            fiber_config,
        };
        if let Some(updater) = self.fiber_config_updater {
            updater(&mut config.fiber_config);
        }
        config
    }
}

pub async fn establish_channel_between_nodes(
    node_a: &mut NetworkNode,
    node_b: &mut NetworkNode,
    node_a_funding_amount: u128,
    node_b_funding_amount: u128,
    public: bool,
) -> (Hash256, TransactionView) {
    let message = |rpc_reply| {
        NetworkActorMessage::Command(NetworkActorCommand::OpenChannel(
            OpenChannelCommand {
                peer_id: node_b.peer_id.clone(),
                public,
                shutdown_script: None,
                funding_amount: node_a_funding_amount,
                funding_udt_type_script: None,
                commitment_fee_rate: None,
                commitment_delay_epoch: None,
                funding_fee_rate: None,
                tlc_expiry_delta: None,
                tlc_min_value: None,
                tlc_max_value: None,
                tlc_fee_proportional_millionths: None,
                max_tlc_number_in_flight: None,
                max_tlc_value_in_flight: None,
            },
            rpc_reply,
        ))
    };
    let open_channel_result = call!(node_a.network_actor, message)
        .expect("node_a alive")
        .expect("open channel success");

    node_b
        .expect_event(|event| match event {
            NetworkServiceEvent::ChannelPendingToBeAccepted(peer_id, channel_id) => {
                println!("A channel ({:?}) to {:?} create", &channel_id, peer_id);
                assert_eq!(peer_id, &node_a.peer_id);
                true
            }
            _ => false,
        })
        .await;
    let message = |rpc_reply| {
        NetworkActorMessage::Command(NetworkActorCommand::AcceptChannel(
            AcceptChannelCommand {
                temp_channel_id: open_channel_result.channel_id,
                funding_amount: node_b_funding_amount,
                shutdown_script: None,
            },
            rpc_reply,
        ))
    };
    let accept_channel_result = call!(node_b.network_actor, message)
        .expect("node_b alive")
        .expect("accept channel success");
    let new_channel_id = accept_channel_result.new_channel_id;

    let funding_tx_outpoint = node_a
        .expect_to_process_event(|event| match event {
            NetworkServiceEvent::ChannelReady(peer_id, channel_id, funding_tx_outpoint) => {
                println!(
                    "A channel ({:?}) to {:?} is now ready",
                    &channel_id, &peer_id
                );
                assert_eq!(peer_id, &node_b.peer_id);
                assert_eq!(channel_id, &new_channel_id);
                Some(funding_tx_outpoint.clone())
            }
            _ => None,
        })
        .await;

    node_b
        .expect_event(|event| match event {
            NetworkServiceEvent::ChannelReady(peer_id, channel_id, _funding_tx_hash) => {
                println!(
                    "A channel ({:?}) to {:?} is now ready",
                    &channel_id, &peer_id
                );
                assert_eq!(peer_id, &node_a.peer_id);
                assert_eq!(channel_id, &new_channel_id);
                true
            }
            _ => false,
        })
        .await;

    let funding_tx = node_a
        .get_tx_from_hash(funding_tx_outpoint.tx_hash())
        .await
        .expect("tx found");
    (new_channel_id, funding_tx)
}

impl NetworkNode {
    pub async fn new() -> Self {
        Self::new_with_node_name_opt(None).await
    }

    pub async fn new_with_node_name(node_name: &str) -> Self {
        let config = NetworkNodeConfigBuilder::new()
            .node_name(Some(node_name.to_string()))
            .build();
        Self::new_with_config(config).await
    }

    pub async fn new_with_node_name_opt(node_name: Option<String>) -> Self {
        let config = NetworkNodeConfigBuilder::new().node_name(node_name).build();
        Self::new_with_config(config).await
    }

    pub async fn new_with_config(config: NetworkNodeConfig) -> Self {
        let NetworkNodeConfig {
            base_dir,
            node_name,
            store,
            fiber_config,
        } = config;

        let _span = tracing::info_span!("NetworkNode", node_name = &node_name).entered();

        let root = ROOT_ACTOR.get_or_init(get_test_root_actor).await.clone();
        let (event_sender, mut event_receiver) = mpsc::channel(10000);

        let chain_actor = Actor::spawn_linked(None, MockChainActor::new(), (), root.get_cell())
            .await
            .expect("start mock chain actor")
            .0;

        let secret_key: Privkey = fiber_config
            .read_or_generate_secret_key()
            .expect("must generate key")
            .into();
        let public_key = secret_key.pubkey();

        let network_graph = Arc::new(TokioRwLock::new(NetworkGraph::new(
            store.clone(),
            public_key.into(),
        )));

        let network_actor = Actor::spawn_linked(
            Some(format!("network actor at {}", base_dir.to_str())),
            NetworkActor::new(
                event_sender,
                chain_actor.clone(),
                store.clone(),
                network_graph.clone(),
            ),
            NetworkActorStartArguments {
                config: fiber_config.clone(),
                tracker: new_tokio_task_tracker(),
                channel_subscribers: Default::default(),
                default_shutdown_script: Default::default(),
            },
            root.get_cell(),
        )
        .await
        .expect("start network actor")
        .0;

        #[allow(clippy::never_loop)]
        let (peer_id, _listening_addr, announced_addrs) = loop {
            select! {
                Some(NetworkServiceEvent::NetworkStarted(peer_id, listening_addr, announced_addrs)) = event_receiver.recv() => {
                    break (peer_id, listening_addr, announced_addrs);
                }
                _ = sleep(Duration::from_secs(5)) => {
                    panic!("Failed to start network actor");
                }
            }
        };

        println!(
            "Network node started for peer_id {:?} in directory {:?}",
            &peer_id,
            base_dir.as_ref()
        );

        Self {
            base_dir,
            node_name,
            store,
            fiber_config,
            listening_addrs: announced_addrs,
            network_actor,
            network_graph,
            chain_actor,
            private_key: secret_key.into(),
            peer_id,
            event_emitter: event_receiver,
        }
    }

    pub fn get_node_config(&self) -> NetworkNodeConfig {
        NetworkNodeConfig {
            base_dir: self.base_dir.clone(),
            node_name: self.node_name.clone(),
            store: self.store.clone(),
            fiber_config: self.fiber_config.clone(),
        }
    }

    pub async fn start(&mut self) {
        let config = self.get_node_config();
        let new = Self::new_with_config(config).await;
        *self = new;
    }

    pub async fn stop(&mut self) {
        self.network_actor
            .stop(Some("stopping actor on request".to_string()));
        let my_peer_id = self.peer_id.clone();
        self.expect_event(
            |event| matches!(event, NetworkServiceEvent::NetworkStopped(id) if id == &my_peer_id),
        )
        .await;
    }

    pub async fn restart(&mut self) {
        self.stop().await;
        // Tentacle shutdown may require some time to propagate to other nodes.
        // If we start the node immediately, other nodes may deem our new connection
        // as a duplicate connection and report RepeatedConnection error.
        // And we will receive `ProtocolSelectError` error from tentacle.
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        tracing::debug!("Node stopped, restarting");
        self.start().await;
    }

    pub async fn new_n_interconnected_nodes<const N: usize>() -> [Self; N] {
        let mut nodes: Vec<NetworkNode> = Vec::with_capacity(N);
        for i in 0..N {
            let new = Self::new_with_config(
                NetworkNodeConfigBuilder::new()
                    .node_name(Some(format!("node-{}", i)))
                    .base_dir_prefix(&format!("fnn-test-node-{}-", i))
                    .build(),
            )
            .await;
            for node in nodes.iter_mut() {
                node.connect_to(&new).await;
            }
            nodes.push(new);
        }
        match nodes.try_into() {
            Ok(nodes) => nodes,
            Err(_) => unreachable!(),
        }
    }

    pub async fn new_2_nodes_with_established_channel(
        node_a_funding_amount: u128,
        node_b_funding_amount: u128,
        public: bool,
    ) -> (NetworkNode, NetworkNode, Hash256, TransactionView) {
        let [mut node_a, mut node_b] = NetworkNode::new_n_interconnected_nodes().await;

        let (channel_id, funding_tx) = establish_channel_between_nodes(
            &mut node_a,
            &mut node_b,
            node_a_funding_amount,
            node_b_funding_amount,
            public,
        )
        .await;

        (node_a, node_b, channel_id, funding_tx)
    }

    // Create n nodes and connect them. The config_gen function
    // (function that creates a NetworkNodeConfig from an index)
    // will be called to generate the config for each node.
    pub async fn new_n_interconnected_nodes_with_config(
        n: usize,
        config_gen: impl Fn(usize) -> NetworkNodeConfig,
    ) -> Vec<Self> {
        let mut nodes: Vec<NetworkNode> = Vec::with_capacity(n);
        for i in 0..n {
            let new = Self::new_with_config(config_gen(i)).await;
            for node in nodes.iter_mut() {
                node.connect_to(&new).await;
            }
            nodes.push(new);
        }
        nodes
    }

    pub async fn connect_to_nonblocking(&mut self, other: &Self) {
        let peer_addr = other.listening_addrs[0].clone();
        println!(
            "Trying to connect to {:?} from {:?}",
            other.listening_addrs, &self.listening_addrs
        );

        self.network_actor
            .send_message(NetworkActorMessage::new_command(
                NetworkActorCommand::ConnectPeer(peer_addr.clone()),
            ))
            .expect("self alive");
    }

    pub async fn connect_to(&mut self, other: &Self) {
        self.connect_to_nonblocking(other).await;
        let peer_id = &other.peer_id;
        self.expect_event(
            |event| matches!(event, NetworkServiceEvent::PeerConnected(id, _addr) if id == peer_id),
        )
        .await;
    }

    pub async fn expect_to_process_event<F, T>(&mut self, event_processor: F) -> T
    where
        F: Fn(&NetworkServiceEvent) -> Option<T>,
    {
        loop {
            select! {
                event = self.event_emitter.recv() => {
                    match event {
                        None => panic!("Event emitter unexpectedly stopped"),
                        Some(event) => {
                            println!("Recevied event when waiting for specific event: {:?}", &event);
                            if let Some(r) = event_processor(&event) {
                                println!("Event ({:?}) matching filter received, exiting waiting for event loop", &event);
                                return r;
                            }
                        }
                    }
                }
                _ = sleep(Duration::from_secs(5)) => {
                    panic!("Waiting for event timeout");
                }
            }
        }
    }

    pub async fn expect_event<F>(&mut self, event_filter: F)
    where
        F: Fn(&NetworkServiceEvent) -> bool,
    {
        self.expect_to_process_event(|event| if event_filter(event) { Some(()) } else { None })
            .await;
    }

    pub async fn submit_tx(&mut self, tx: TransactionView) -> ckb_jsonrpc_types::Status {
        submit_tx(self.chain_actor.clone(), tx).await
    }

    pub async fn trace_tx(&mut self, tx: TransactionView) -> ckb_jsonrpc_types::Status {
        trace_tx(self.chain_actor.clone(), tx).await
    }

    pub async fn trace_tx_hash(&mut self, tx_hash: Byte32) -> ckb_jsonrpc_types::Status {
        trace_tx_hash(self.chain_actor.clone(), tx_hash).await
    }

    pub async fn get_tx_from_hash(
        &mut self,
        tx_hash: Byte32,
    ) -> Result<TransactionView, anyhow::Error> {
        get_tx_from_hash(self.chain_actor.clone(), tx_hash).await
    }

    pub fn get_network_graph(&self) -> &Arc<TokioRwLock<NetworkGraph<MemoryStore>>> {
        &self.network_graph
    }

    pub async fn with_network_graph<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&NetworkGraph<MemoryStore>) -> T,
    {
        let graph = self.get_network_graph().read().await;
        f(&*graph)
    }

    pub async fn get_network_graph_nodes(&self) -> Vec<NodeInfo> {
        self.with_network_graph(|graph| graph.nodes().into_iter().cloned().collect())
            .await
    }

    pub async fn get_network_graph_node(&self, pubkey: &Pubkey) -> Option<NodeInfo> {
        self.with_network_graph(|graph| graph.get_node(pubkey).cloned())
            .await
    }

    pub async fn get_network_graph_channels(&self) -> Vec<ChannelInfo> {
        self.with_network_graph(|graph| graph.channels().into_iter().cloned().collect())
            .await
    }

    pub async fn get_network_graph_channel(&self, channel_id: &OutPoint) -> Option<ChannelInfo> {
        self.with_network_graph(|graph| {
            tracing::debug!("Getting channel info for {:?}", channel_id);
            tracing::debug!(
                "Channels: {:?}",
                graph.channels().into_iter().collect::<Vec<_>>()
            );
            graph.get_channel(channel_id).cloned()
        })
        .await
    }
}

#[derive(Clone, Default)]
pub struct MemoryStore {
    network_actor_sate_map: Arc<RwLock<HashMap<PeerId, PersistentNetworkActorState>>>,
    channel_actor_state_map: Arc<RwLock<HashMap<Hash256, ChannelActorState>>>,
    channels_map: Arc<RwLock<HashMap<OutPoint, ChannelInfo>>>,
    nodes_map: Arc<RwLock<HashMap<Pubkey, NodeInfo>>>,
    // The bool value in the key is to distinguish between channel updates of node1 or node2.
    // It always is set to true for all other messages.
    // This is to avoid overwriting the channel update of node2 with the channel update of node1.
    gossip_messages_map:
        Arc<RwLock<HashMap<(BroadcastMessageID, bool), BroadcastMessageWithTimestamp>>>,
    payment_sessions: Arc<RwLock<HashMap<Hash256, PaymentSession>>>,
    invoice_store: Arc<RwLock<HashMap<Hash256, CkbInvoice>>>,
    invoice_hash_to_preimage: Arc<RwLock<HashMap<Hash256, Hash256>>>,
}

impl NetworkActorStateStore for MemoryStore {
    fn get_network_actor_state(&self, id: &PeerId) -> Option<PersistentNetworkActorState> {
        self.network_actor_sate_map.read().unwrap().get(id).cloned()
    }

    fn insert_network_actor_state(&self, id: &PeerId, state: PersistentNetworkActorState) {
        self.network_actor_sate_map
            .write()
            .unwrap()
            .insert(id.clone(), state);
    }
}

impl MemoryStore {
    fn save_broadcast_message(&self, message: BroadcastMessageWithTimestamp) {
        tracing::debug!("Saving broadcast message {:?}", &message);
        let is_node_1 = match &message {
            BroadcastMessageWithTimestamp::ChannelUpdate(msg) if msg.is_update_of_node_2() => false,
            _ => true,
        };
        let key = (message.message_id(), is_node_1);
        match self.gossip_messages_map.read().unwrap().get(&key) {
            Some(old) if old.timestamp() > message.timestamp() => {
                return;
            }
            _ => {}
        }
        self.gossip_messages_map
            .write()
            .unwrap()
            .insert(key, message);
    }
}

impl GossipMessageStore for MemoryStore {
    fn get_broadcast_messages_iter(
        &self,
        after_cursor: &Cursor,
    ) -> impl IntoIterator<Item = BroadcastMessageWithTimestamp> {
        let mut v: Vec<_> = self
            .gossip_messages_map
            .read()
            .unwrap()
            .values()
            .filter_map(|msg| (after_cursor < &msg.cursor()).then_some(msg.clone()))
            .collect();
        v.sort_by(|a, b| a.cursor().cmp(&b.cursor()));
        v
    }

    fn get_broadcast_messages(
        &self,
        after_cursor: &Cursor,
        count: Option<u16>,
    ) -> Vec<BroadcastMessageWithTimestamp> {
        let mut messages = self
            .gossip_messages_map
            .read()
            .unwrap()
            .values()
            .filter_map(|msg| (after_cursor < &msg.cursor()).then_some(msg.clone()))
            .take(count.unwrap_or(DEFAULT_NUM_OF_BROADCAST_MESSAGE as u16) as usize)
            .collect::<Vec<_>>();
        messages.sort_by(|a, b| a.cursor().cmp(&b.cursor()));
        messages
    }

    fn save_channel_announcement(&self, timestamp: u64, channel_announcement: ChannelAnnouncement) {
        self.save_broadcast_message(BroadcastMessageWithTimestamp::ChannelAnnouncement(
            timestamp,
            channel_announcement,
        ));
    }

    fn save_channel_update(&self, channel_update: ChannelUpdate) {
        self.save_broadcast_message(BroadcastMessageWithTimestamp::ChannelUpdate(channel_update));
    }

    fn save_node_announcement(&self, node_announcement: NodeAnnouncement) {
        self.save_broadcast_message(BroadcastMessageWithTimestamp::NodeAnnouncement(
            node_announcement,
        ));
    }

    fn get_broadcast_message_with_cursor(
        &self,
        cursor: &Cursor,
    ) -> Option<BroadcastMessageWithTimestamp> {
        let timestamp = cursor.timestamp;
        let map = self.gossip_messages_map.read().unwrap();
        // It is possible that the cursor is for a channel update of node2.
        let key1 = (cursor.message_id.clone(), true);
        let key2 = (cursor.message_id.clone(), false);
        // It is possible that the user is querying an older message and we may have replaced the
        // older message with a new one.
        let get = |key| {
            map.get(&key)
                .and_then(|d| (d.timestamp() == timestamp).then_some(d))
                .cloned()
        };
        get(key1).or_else(|| get(key2))
    }

    fn get_latest_broadcast_message_cursor(&self) -> Option<Cursor> {
        let map = self.gossip_messages_map.read().unwrap();
        map.iter()
            .map(|((k, _), v)| Cursor::new(v.timestamp(), k.clone()))
            .max()
    }

    fn get_latest_channel_announcement_timestamp(&self, outpoint: &OutPoint) -> Option<u64> {
        self.gossip_messages_map
            .read()
            .unwrap()
            .get(&(
                BroadcastMessageID::ChannelAnnouncement(outpoint.clone()),
                true,
            ))
            .map(|msg| msg.timestamp())
    }

    fn get_latest_channel_update_timestamp(
        &self,
        outpoint: &OutPoint,
        is_node1: bool,
    ) -> Option<u64> {
        self.gossip_messages_map
            .read()
            .unwrap()
            .get(&(
                BroadcastMessageID::ChannelUpdate(outpoint.clone()),
                is_node1,
            ))
            .map(|msg| msg.timestamp())
    }

    fn get_latest_node_announcement_timestamp(&self, pk: &Pubkey) -> Option<u64> {
        self.gossip_messages_map
            .read()
            .unwrap()
            .get(&(BroadcastMessageID::NodeAnnouncement(pk.clone()), true))
            .map(|msg| msg.timestamp())
    }
}

impl NetworkGraphStateStore for MemoryStore {
    fn get_payment_session(&self, id: Hash256) -> Option<PaymentSession> {
        self.payment_sessions.read().unwrap().get(&id).cloned()
    }

    fn insert_payment_session(&self, session: PaymentSession) {
        self.payment_sessions
            .write()
            .unwrap()
            .insert(session.payment_hash(), session);
    }
}

impl ChannelActorStateStore for MemoryStore {
    fn get_channel_actor_state(&self, id: &Hash256) -> Option<ChannelActorState> {
        self.channel_actor_state_map
            .read()
            .unwrap()
            .get(id)
            .cloned()
    }

    fn insert_channel_actor_state(&self, state: ChannelActorState) {
        self.channel_actor_state_map
            .write()
            .unwrap()
            .insert(state.id, state);
    }

    fn delete_channel_actor_state(&self, id: &Hash256) {
        self.channel_actor_state_map.write().unwrap().remove(id);
    }

    fn get_channel_ids_by_peer(&self, peer_id: &PeerId) -> Vec<Hash256> {
        self.channel_actor_state_map
            .read()
            .unwrap()
            .values()
            .filter_map(|state| {
                if peer_id == &state.get_remote_peer_id() {
                    Some(state.id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn get_channel_states(&self, peer_id: Option<PeerId>) -> Vec<(PeerId, Hash256, ChannelState)> {
        let map = self.channel_actor_state_map.read().unwrap();
        let values = map.values();
        match peer_id {
            Some(peer_id) => values
                .filter_map(|state| {
                    if peer_id == state.get_remote_peer_id() {
                        Some((state.get_remote_peer_id(), state.id, state.state.clone()))
                    } else {
                        None
                    }
                })
                .collect(),
            None => values
                .map(|state| {
                    (
                        state.get_remote_peer_id(),
                        state.id.clone(),
                        state.state.clone(),
                    )
                })
                .collect(),
        }
    }
}

impl InvoiceStore for MemoryStore {
    fn get_invoice(&self, id: &Hash256) -> Option<CkbInvoice> {
        self.invoice_store.read().unwrap().get(id).cloned()
    }

    fn insert_invoice(
        &self,
        invoice: CkbInvoice,
        preimage: Option<Hash256>,
    ) -> Result<(), InvoiceError> {
        let id = invoice.payment_hash();
        if let Some(preimage) = preimage {
            self.invoice_hash_to_preimage
                .write()
                .unwrap()
                .insert(*id, preimage);
        }
        self.invoice_store.write().unwrap().insert(*id, invoice);
        Ok(())
    }

    fn get_invoice_preimage(&self, hash: &Hash256) -> Option<Hash256> {
        self.invoice_hash_to_preimage
            .read()
            .unwrap()
            .get(hash)
            .cloned()
    }

    fn get_invoice_status(&self, _id: &Hash256) -> Option<crate::invoice::CkbInvoiceStatus> {
        unimplemented!()
    }

    fn update_invoice_status(
        &self,
        _id: &Hash256,
        _status: crate::invoice::CkbInvoiceStatus,
    ) -> Result<(), InvoiceError> {
        unimplemented!()
    }
}

#[tokio::test]
async fn test_connect_to_other_node() {
    let mut node_a = NetworkNode::new().await;
    let node_b = NetworkNode::new().await;
    node_a.connect_to(&node_b).await;
}

#[tokio::test]
async fn test_restart_network_node() {
    let mut node = NetworkNode::new().await;
    node.restart().await;
}
