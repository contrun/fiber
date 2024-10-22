use super::test_utils::{init_tracing, NetworkNode};
use crate::{
    fiber::{
        graph::{ChannelInfo, NetworkGraphStateStore},
        network::get_chain_hash,
        tests::channel::test_create_public_channel,
        types::{
            ChannelAnnouncement, ChannelUpdate, FiberBroadcastMessage, FiberMessage,
            NodeAnnouncement, Privkey, Pubkey,
        },
        NetworkActorCommand, NetworkActorEvent, NetworkActorMessage,
    },
    NetworkServiceEvent,
};
use ckb_hash::blake2b_256;
use ckb_jsonrpc_types::Status;
use ckb_types::{
    core::TransactionView,
    packed::{CellOutput, ScriptBuilder},
};
use ckb_types::{
    packed::OutPoint,
    prelude::{Builder, Entity, Pack},
};
use std::str::FromStr;
use tentacle::{multiaddr::MultiAddr, secio::PeerId};

fn get_test_priv_key() -> Privkey {
    Privkey::from_slice(&[42u8; 32])
}

fn get_test_pub_key() -> Pubkey {
    get_test_priv_key().pubkey()
}

fn get_test_peer_id() -> PeerId {
    let pub_key = get_test_pub_key().into();
    PeerId::from_public_key(&pub_key)
}

fn create_fake_channel_announcement_mesage(
    priv_key: Privkey,
    capacity: u64,
    outpoint: OutPoint,
) -> ChannelAnnouncement {
    let x_only_pub_key = priv_key.x_only_pub_key();
    let sk1 = Privkey::from([1u8; 32]);
    let sk2 = Privkey::from([2u8; 32]);

    let mut announcement = ChannelAnnouncement::new_unsigned(
        &sk1.pubkey(),
        &sk2.pubkey(),
        outpoint,
        get_chain_hash(),
        &x_only_pub_key,
        capacity as u128,
        None,
    );
    let message = announcement.message_to_sign();

    announcement.ckb_signature = Some(priv_key.sign_schnorr(message));
    announcement.node1_signature = Some(sk1.sign(message));
    announcement.node2_signature = Some(sk2.sign(message));
    announcement
}

fn create_fake_node_announcement_mesage_version1() -> NodeAnnouncement {
    let priv_key = get_test_priv_key();
    let node_name = "fake node";
    let addresses =
        vec!["/ip4/1.1.1.1/tcp/8346/p2p/QmaFDJb9CkMrXy7nhTWBY5y9mvuykre3EzzRsCJUAVXprZ"]
            .iter()
            .map(|x| MultiAddr::from_str(x).expect("valid multiaddr"))
            .collect();
    let version = 1;
    NodeAnnouncement::new(node_name.into(), addresses, &priv_key, version, 0)
}

fn create_fake_node_announcement_mesage_version2() -> NodeAnnouncement {
    let priv_key = get_test_priv_key();
    let node_name = "fake node";
    let addresses =
        vec!["/ip4/1.1.1.1/tcp/8346/p2p/QmaFDJb9CkMrXy7nhTWBY5y9mvuykre3EzzRsCJUAVXprZ"]
            .iter()
            .map(|x| MultiAddr::from_str(x).expect("valid multiaddr"))
            .collect();
    let version = 2;
    NodeAnnouncement::new(node_name.into(), addresses, &priv_key, version, 0)
}

fn create_fake_node_announcement_mesage_version3() -> NodeAnnouncement {
    let priv_key = get_test_priv_key();
    let node_name = "fake node";
    let addresses =
        vec!["/ip4/1.1.1.1/tcp/8346/p2p/QmaFDJb9CkMrXy7nhTWBY5y9mvuykre3EzzRsCJUAVXprZ"]
            .iter()
            .map(|x| MultiAddr::from_str(x).expect("valid multiaddr"))
            .collect();
    let version = 3;
    NodeAnnouncement::new(node_name.into(), addresses, &priv_key, version, 0)
}

// Manually mark syncing done to avoid waiting for the syncing process.
async fn new_synced_node(name: &str) -> NetworkNode {
    let mut node = NetworkNode::new_with_node_name(name).await;
    node.network_actor
        .send_message(NetworkActorMessage::Command(
            NetworkActorCommand::MarkSyncingDone,
        ))
        .expect("send message to network actor");

    node.expect_event(|c| matches!(c, NetworkServiceEvent::SyncingCompleted))
        .await;
    node
}

#[tokio::test]
async fn test_sync_channel_announcement_on_startup() {
    init_tracing();

    let mut node1 = new_synced_node("node1").await;
    let mut node2 = NetworkNode::new_with_node_name("node2").await;

    let capacity = 42;
    let priv_key: Privkey = get_test_priv_key();
    let pubkey = priv_key.x_only_pub_key().serialize();
    let pubkey_hash = &blake2b_256(pubkey.as_slice())[0..20];
    let tx = TransactionView::new_advanced_builder()
        .output(
            CellOutput::new_builder()
                .capacity(capacity.pack())
                .lock(ScriptBuilder::default().args(pubkey_hash.pack()).build())
                .build(),
        )
        .output_data(vec![0u8; 8].pack())
        .build();
    let outpoint = tx.output_pts()[0].clone();
    let channel_announcement =
        create_fake_channel_announcement_mesage(priv_key, capacity, outpoint);

    assert_eq!(node1.submit_tx(tx.clone()).await, Status::Committed);

    node1
        .network_actor
        .send_message(NetworkActorMessage::Event(NetworkActorEvent::PeerMessage(
            get_test_peer_id(),
            FiberMessage::BroadcastMessage(FiberBroadcastMessage::ChannelAnnouncement(
                channel_announcement.clone(),
            )),
        )))
        .expect("send message to network actor");

    node1.connect_to(&node2).await;

    assert_eq!(node2.submit_tx(tx.clone()).await, Status::Committed);
    node2
        .expect_event(|c| matches!(c, NetworkServiceEvent::SyncingCompleted))
        .await;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let channels = node2.store.get_channels(None);
    assert!(!channels.is_empty());
}

async fn create_a_channel() -> (NetworkNode, ChannelInfo, Privkey, Privkey, Privkey) {
    init_tracing();

    let mut node1 = new_synced_node("node1").await;
    let capacity = 42;
    let priv_key: Privkey = get_test_priv_key();
    let pubkey = priv_key.x_only_pub_key().serialize();
    let pubkey_hash = &blake2b_256(pubkey.as_slice())[0..20];
    let tx = TransactionView::new_advanced_builder()
        .output(
            CellOutput::new_builder()
                .capacity(capacity.pack())
                .lock(ScriptBuilder::default().args(pubkey_hash.pack()).build())
                .build(),
        )
        .output_data(vec![0u8; 8].pack())
        .build();
    let outpoint = tx.output_pts()[0].clone();
    let x_only_pub_key = priv_key.x_only_pub_key();
    let sk1 = Privkey::from([1u8; 32]);
    let pk1 = sk1.pubkey();
    let sk2 = Privkey::from([2u8; 32]);
    let pk2 = sk2.pubkey();

    let mut channel_announcement = ChannelAnnouncement::new_unsigned(
        &pk1,
        &pk2,
        outpoint,
        get_chain_hash(),
        &x_only_pub_key,
        capacity as u128,
        None,
    );
    let message = channel_announcement.message_to_sign();

    channel_announcement.ckb_signature = Some(priv_key.sign_schnorr(message));
    channel_announcement.node1_signature = Some(sk1.sign(message));
    channel_announcement.node2_signature = Some(sk2.sign(message));
    node1
        .network_actor
        .send_message(NetworkActorMessage::Event(NetworkActorEvent::PeerMessage(
            get_test_peer_id(),
            FiberMessage::BroadcastMessage(FiberBroadcastMessage::ChannelAnnouncement(
                channel_announcement.clone(),
            )),
        )))
        .expect("send message to network actor");

    assert_eq!(node1.submit_tx(tx.clone()).await, Status::Committed);

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let channels = node1.store.get_channels(None);
    assert_eq!(channels.len(), 1);
    let channel_info = channels.into_iter().next().unwrap();
    assert_eq!(&channel_info.announcement_msg, &channel_announcement);

    (node1, channel_info, priv_key, sk1, sk2)
}

#[tokio::test]
async fn test_node1_node2_channel_update() {
    let (node, channel_info, _priv_key, sk1, sk2) = create_a_channel().await;

    let create_channel_update = |version: u64, message_flags: u32, key: Privkey| {
        let mut channel_update = ChannelUpdate::new_unsigned(
            get_chain_hash(),
            channel_info.announcement_msg.channel_outpoint.clone(),
            version,
            message_flags,
            0,
            42,
            0,
            0,
            10,
        );

        channel_update.signature = Some(key.sign(channel_update.message_to_sign()));
        node.network_actor
            .send_message(NetworkActorMessage::Event(NetworkActorEvent::PeerMessage(
                get_test_peer_id(),
                FiberMessage::BroadcastMessage(FiberBroadcastMessage::ChannelUpdate(
                    channel_update.clone(),
                )),
            )))
            .expect("send message to network actor");
        channel_update
    };

    let channel_update_of_node1 = create_channel_update(2, 0, sk1);
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let new_channel_info = node
        .store
        .get_channels(Some(channel_info.announcement_msg.channel_outpoint.clone()));
    assert_eq!(new_channel_info.len(), 1);
    assert_eq!(
        new_channel_info[0]
            .node2_to_node1
            .as_ref()
            .unwrap()
            .last_update_message,
        channel_update_of_node1
    );
    assert_eq!(new_channel_info[0].node1_to_node2, None);

    let channel_update_of_node2 = create_channel_update(3, 1, sk2);
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let new_channel_info = node
        .store
        .get_channels(Some(channel_info.announcement_msg.channel_outpoint.clone()));
    assert_eq!(new_channel_info.len(), 1);
    assert_eq!(
        new_channel_info[0]
            .node2_to_node1
            .as_ref()
            .unwrap()
            .last_update_message,
        channel_update_of_node1
    );
    assert_eq!(
        new_channel_info[0]
            .node1_to_node2
            .as_ref()
            .unwrap()
            .last_update_message,
        channel_update_of_node2
    );
}

#[tokio::test]
async fn test_channel_update_version() {
    let (node, channel_info, _priv_key, sk1, _sk2) = create_a_channel().await;

    let create_channel_update = |version: u64, key: &Privkey| {
        let mut channel_update = ChannelUpdate::new_unsigned(
            get_chain_hash(),
            channel_info.announcement_msg.channel_outpoint.clone(),
            version,
            0,
            0,
            42,
            0,
            0,
            10,
        );

        channel_update.signature = Some(key.sign(channel_update.message_to_sign()));
        node.network_actor
            .send_message(NetworkActorMessage::Event(NetworkActorEvent::PeerMessage(
                get_test_peer_id(),
                FiberMessage::BroadcastMessage(FiberBroadcastMessage::ChannelUpdate(
                    channel_update.clone(),
                )),
            )))
            .expect("send message to network actor");
        channel_update
    };

    let channel_update_2 = create_channel_update(2, &sk1);
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let new_channel_info = node
        .store
        .get_channels(Some(channel_info.announcement_msg.channel_outpoint.clone()));
    assert_eq!(new_channel_info.len(), 1);
    assert_eq!(
        new_channel_info[0]
            .node2_to_node1
            .as_ref()
            .unwrap()
            .last_update_message,
        channel_update_2
    );

    // Old channel update will not replace the new one.
    let _channel_update_1 = create_channel_update(1, &sk1);
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let new_channel_info = node
        .store
        .get_channels(Some(channel_info.announcement_msg.channel_outpoint.clone()));
    assert_eq!(new_channel_info.len(), 1);
    assert_eq!(
        new_channel_info[0]
            .node2_to_node1
            .as_ref()
            .unwrap()
            .last_update_message,
        channel_update_2
    );

    // New channel update will replace the old one.
    let channel_update_3 = create_channel_update(3, &sk1);
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let new_channel_info = node
        .store
        .get_channels(Some(channel_info.announcement_msg.channel_outpoint.clone()));
    assert_eq!(new_channel_info.len(), 1);
    assert_eq!(
        new_channel_info[0]
            .node2_to_node1
            .as_ref()
            .unwrap()
            .last_update_message,
        channel_update_3
    );
}

#[tokio::test]
async fn test_sync_node_announcement_version() {
    init_tracing();

    let node = new_synced_node("node").await;
    let test_pub_key = get_test_pub_key();
    let test_peer_id = get_test_peer_id();

    node.network_actor
        .send_message(NetworkActorMessage::Event(NetworkActorEvent::PeerMessage(
            test_peer_id.clone(),
            FiberMessage::BroadcastMessage(FiberBroadcastMessage::NodeAnnouncement(
                create_fake_node_announcement_mesage_version2(),
            )),
        )))
        .expect("send message to network actor");

    // Wait for the broadcast message to be processed.
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let node_info = node.store.get_nodes(Some(test_pub_key));
    match node_info.first() {
        Some(n) if n.anouncement_msg.version == 2 => {}
        _ => panic!(
            "Must have version 2 announcement message, found {:?}",
            &node_info
        ),
    }

    node.network_actor
        .send_message(NetworkActorMessage::Event(NetworkActorEvent::PeerMessage(
            test_peer_id.clone(),
            FiberMessage::BroadcastMessage(FiberBroadcastMessage::NodeAnnouncement(
                create_fake_node_announcement_mesage_version1(),
            )),
        )))
        .expect("send message to network actor");

    // Wait for the broadcast message to be processed.
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let node_info = node.store.get_nodes(Some(test_pub_key));
    match node_info.first() {
        Some(n) if n.anouncement_msg.version == 2 => {}
        _ => panic!(
            "Must have version 2 announcement message, found {:?}",
            &node_info
        ),
    }

    node.network_actor
        .send_message(NetworkActorMessage::Event(NetworkActorEvent::PeerMessage(
            test_peer_id.clone(),
            FiberMessage::BroadcastMessage(FiberBroadcastMessage::NodeAnnouncement(
                create_fake_node_announcement_mesage_version3(),
            )),
        )))
        .expect("send message to network actor");
    // Wait for the broadcast message to be processed.
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let node_info = node.store.get_nodes(Some(test_pub_key));
    match node_info.first() {
        Some(n) if n.anouncement_msg.version == 3 => {}
        _ => panic!(
            "Must have version 3 announcement message, found {:?}",
            &node_info
        ),
    }
}

// Test that we can sync the network graph with peers.
// We will first create a node and announce a fake node announcement to the network.
// Then we will create another node and connect to the first node.
// We will see if the second node has the fake node announcement.
#[tokio::test]
async fn test_sync_node_announcement_on_startup() {
    init_tracing();

    let mut node1 = new_synced_node("node1").await;
    let mut node2 = NetworkNode::new_with_node_name("node2").await;
    let test_pub_key = get_test_pub_key();
    let test_peer_id = get_test_peer_id();

    node1
        .network_actor
        .send_message(NetworkActorMessage::Event(NetworkActorEvent::PeerMessage(
            test_peer_id.clone(),
            FiberMessage::BroadcastMessage(FiberBroadcastMessage::NodeAnnouncement(
                create_fake_node_announcement_mesage_version1(),
            )),
        )))
        .expect("send message to network actor");

    node1.connect_to(&node2).await;

    node2
        .expect_event(|c| matches!(c, NetworkServiceEvent::SyncingCompleted))
        .await;

    // Wait for the broadcast message to be processed.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let node = node1.store.get_nodes(Some(test_pub_key));
    assert!(!node.is_empty());

    let node = node2.store.get_nodes(Some(test_pub_key));
    assert!(!node.is_empty());
}

// Test that we can sync the network graph with peers.
// We will first create a node and announce a fake node announcement to the network.
// Then we will create another node and connect to the first node.
// We will see if the second node has the fake node announcement.
#[tokio::test]
async fn test_sync_node_announcement_after_restart() {
    init_tracing();

    let [mut node1, mut node2] = NetworkNode::new_n_interconnected_nodes().await;

    node1
        .expect_event(|c| matches!(c, NetworkServiceEvent::SyncingCompleted))
        .await;
    node2
        .expect_event(|c| matches!(c, NetworkServiceEvent::SyncingCompleted))
        .await;

    node2.stop().await;

    let test_pub_key = get_test_pub_key();
    let test_peer_id = get_test_peer_id();
    node1
        .network_actor
        .send_message(NetworkActorMessage::Event(NetworkActorEvent::PeerMessage(
            test_peer_id.clone(),
            FiberMessage::BroadcastMessage(FiberBroadcastMessage::NodeAnnouncement(
                create_fake_node_announcement_mesage_version1(),
            )),
        )))
        .expect("send message to network actor");

    node2.start().await;
    node2.connect_to(&node1).await;

    node2
        .expect_event(|c| matches!(c, NetworkServiceEvent::SyncingCompleted))
        .await;

    // Wait for the broadcast message to be processed.
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let node = node1.store.get_nodes(Some(test_pub_key));
    assert!(!node.is_empty());

    let node = node2.store.get_nodes(Some(test_pub_key));
    assert!(!node.is_empty());
}

// This test will dump all the messages received by the peer.
#[tokio::test]
async fn dump_peer_messages() {
    test_create_public_channel();
}
