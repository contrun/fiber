use super::network::{get_chain_hash, SendPaymentCommand};
use super::path::NodeHeap;
use super::types::Pubkey;
use super::types::{ChannelAnnouncement, ChannelUpdate, Hash256, NodeAnnouncement};
use crate::fiber::channel::CHANNEL_DISABLED_FLAG;
use crate::fiber::path::{NodeHeapElement, ProbabilityEvaluator};
use crate::fiber::types::PaymentHopData;
use crate::invoice::CkbInvoice;
use ckb_jsonrpc_types::JsonBytes;
use ckb_types::packed::{OutPoint, Script};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;
use thiserror::Error;
use tracing::log::error;
use tracing::{debug, info, warn};

const DEFAULT_MIN_PROBABILITY: f64 = 0.01;

#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Details about a node in the network, known from the network announcement.
pub struct NodeInfo {
    pub node_id: Pubkey,

    // The time when the node was last updated. This is the time of processing the message,
    // not the time of the NodeAnnouncement itself.
    pub timestamp: u64,

    pub anouncement_msg: NodeAnnouncement,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelInfo {
    pub funding_tx_block_number: u64,
    pub funding_tx_index: u32,
    pub announcement_msg: ChannelAnnouncement,
    pub node1_to_node2: Option<ChannelUpdateInfo>,
    pub node2_to_node1: Option<ChannelUpdateInfo>,
    // The time that the channel was announced to the network.
    pub timestamp: u64,
}

impl ChannelInfo {
    pub fn out_point(&self) -> OutPoint {
        self.announcement_msg.channel_outpoint.clone()
    }

    pub fn node1(&self) -> Pubkey {
        self.announcement_msg.node1_id
    }

    pub fn node2(&self) -> Pubkey {
        self.announcement_msg.node2_id
    }

    pub fn channel_annoucement_timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn node1_to_node2_channel_update_flags(&self) -> u8 {
        1
    }

    pub fn node2_to_node1_channel_update_flags(&self) -> u8 {
        0
    }

    pub fn channel_update_node1_to_node2_timestamp(&self) -> Option<u64> {
        self.node1_to_node2.as_ref().map(|x| x.timestamp)
    }

    pub fn channel_update_node2_to_node1_timestamp(&self) -> Option<u64> {
        self.node2_to_node1.as_ref().map(|x| x.timestamp)
    }

    pub fn channel_last_update_time(&self) -> Option<u64> {
        self.node1_to_node2
            .as_ref()
            .map(|n| n.timestamp)
            .max(self.node2_to_node1.as_ref().map(|n| n.timestamp))
    }

    // Whether this channel is explicitly disabled in either direction.
    // TODO: we currently deem a channel as disabled if one direction is disabled.
    // Is it possible that one direction is disabled while the other is not?
    pub fn is_explicitly_disabled(&self) -> bool {
        dbg!(self.node1_to_node2.as_ref(), self.node2_to_node1.as_ref());
        match (&self.node1_to_node2, &self.node2_to_node1) {
            (Some(update1), _) if !update1.enabled => true,
            (_, Some(update2)) if !update2.enabled => true,
            _ => false,
        }
    }

    pub fn capacity(&self) -> u128 {
        self.announcement_msg.capacity
    }

    pub fn funding_tx_block_number(&self) -> u64 {
        self.funding_tx_block_number
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelUpdateInfo {
    // The version is a number that represents the newness of the channel update.
    // It is set by the node that sends the channel update. Larger number means newer update.
    pub version: u64,
    // The timestamp is the time when the channel update was received by the node.
    pub timestamp: u64,
    /// Whether the channel can be currently used for payments (in this one direction).
    pub enabled: bool,
    /// The difference in CLTV values that you must have when routing through this channel.
    pub cltv_expiry_delta: u64,
    /// The minimum value, which must be relayed to the next hop via the channel
    pub htlc_minimum_value: u128,
    /// The maximum value which may be relayed to the next hop via the channel.
    pub htlc_maximum_value: u128,
    pub fee_rate: u64,
    /// Most recent update for the channel received from the network
    /// Mostly redundant with the data we store in fields explicitly.
    /// Everything else is useful only for sending out for initial routing sync.
    /// Not stored if contains excess data to prevent DoS.
    pub last_update_message: ChannelUpdate,
}

#[derive(Clone, Debug)]
pub struct NetworkGraph<S> {
    source: Pubkey,
    channels: HashMap<OutPoint, ChannelInfo>,
    // This is the best height of the network graph, every time the
    // node restarts, we will try to sync the graph from this height - ASSUME_MAX_CHANNEL_HEIGHT_GAP.
    // We assume that we have already synced the graph up to this height - ASSUME_MAX_CHANNEL_HEIGHT_GAP.
    best_height: u64,
    // Similar to the best_height, this is the last update time of the network graph.
    // We assume that we have already synced the graph up to this time - ASSUME_MAX_MESSAGE_TIMESTAMP_GAP.
    last_update_timestamp: u64,
    nodes: HashMap<Pubkey, NodeInfo>,
    store: S,
    chain_hash: Hash256,
}

#[derive(Error, Debug)]
pub enum GraphError {
    #[error("Graph error: {0}")]
    Amount(String),
    #[error("PathFind error: {0}")]
    PathFind(String),
    #[error("Graph other error: {0}")]
    Other(String),
}

#[derive(Clone, Debug)]
pub struct PathEdge {
    pub target: Pubkey,
    pub channel_outpoint: OutPoint,
}

impl<S> NetworkGraph<S>
where
    S: NetworkGraphStateStore + Clone + Send + Sync + 'static,
{
    pub fn new(store: S, source: Pubkey) -> Self {
        let mut network_graph = Self {
            source,
            best_height: 0,
            last_update_timestamp: 0,
            channels: HashMap::new(),
            nodes: HashMap::new(),
            store,
            chain_hash: get_chain_hash(),
        };
        network_graph.load_from_store();
        network_graph
    }

    pub fn chain_hash(&self) -> Hash256 {
        self.chain_hash
    }

    pub(crate) fn load_from_store(&mut self) {
        let channels = self.store.get_channels(None);
        for channel in channels.iter() {
            if self.best_height < channel.funding_tx_block_number() {
                self.best_height = channel.funding_tx_block_number();
            }
            if self.last_update_timestamp < channel.timestamp {
                self.last_update_timestamp = channel.timestamp;
            }
            if let Some(channel_update) = channel.node1_to_node2.as_ref() {
                if self.last_update_timestamp < channel_update.timestamp {
                    self.last_update_timestamp = channel_update.timestamp;
                }
            }
            if let Some(channel_update) = channel.node2_to_node1.as_ref() {
                if self.last_update_timestamp < channel_update.timestamp {
                    self.last_update_timestamp = channel_update.timestamp;
                }
            }
            self.channels.insert(channel.out_point(), channel.clone());
        }
        let nodes = self.store.get_nodes(None);
        for node in nodes.iter() {
            if self.last_update_timestamp < node.timestamp {
                self.last_update_timestamp = node.timestamp;
            }
            self.nodes.insert(node.node_id, node.clone());
        }
    }

    pub fn get_best_height(&self) -> u64 {
        self.best_height
    }

    pub fn get_last_update_timestamp(&self) -> u64 {
        self.last_update_timestamp
    }

    pub(crate) fn process_node_announcement(&mut self, node_announcement: NodeAnnouncement) {
        let node_id = node_announcement.node_id;
        let node_info = NodeInfo {
            node_id,
            timestamp: std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64,
            anouncement_msg: node_announcement,
        };
        self.add_node(node_info);
    }

    pub fn add_node(&mut self, node_info: NodeInfo) {
        debug!("Adding node to network graph: {:?}", node_info);

        let node_id = node_info.node_id;
        if let Some(old_node) = self.nodes.get(&node_id) {
            if old_node.anouncement_msg.version > node_info.anouncement_msg.version {
                warn!(
                    "Ignoring adding an outdated node info because old node version {} > new node version {}, new node info {:?}, existing node {:?}",
                    old_node.anouncement_msg.version, node_info.anouncement_msg.version,
                    &node_info, &old_node
                );
                return;
            } else if old_node.anouncement_msg.version == node_info.anouncement_msg.version {
                debug!("Repeatedly adding node info, ignoring: {:?}", node_info);
                return;
            }
        }
        if self.last_update_timestamp < node_info.timestamp {
            self.last_update_timestamp = node_info.timestamp;
        }
        self.nodes.insert(node_id, node_info.clone());
        self.store.insert_node(node_info);
    }

    // TODO: If we are syncing with the peers for newest graph, we should
    // not process channels here. Because if the node may restart while syncing is
    // is still ongoing, the next time when the node starts, it may falsely believe
    // that we have already processed channels before the height of this channel.
    pub fn add_channel(&mut self, channel_info: ChannelInfo) {
        assert_ne!(channel_info.node1(), channel_info.node2());
        debug!("Adding channel to network graph: {:?}", channel_info);
        if self.best_height < channel_info.funding_tx_block_number {
            self.best_height = channel_info.funding_tx_block_number;
        }
        if self.last_update_timestamp < channel_info.timestamp {
            self.last_update_timestamp = channel_info.timestamp;
        }
        match self.channels.get(&channel_info.out_point()) {
            Some(channel) => {
                // If the channel already exists, we don't need to update it
                // FIXME: if other fields is different, we should consider it as malioucious and ban the node?
                if channel.node1_to_node2.is_some() || channel.node2_to_node1.is_some() {
                    debug!("channel already exists, ignoring: {:?}", &channel_info);
                    return;
                }
            }
            None => {
                debug!(
                    "Channel not found, saving it to database {:?}",
                    &channel_info
                );
            }
        }
        if let Some(node) = self.nodes.get(&channel_info.node1()) {
            self.store.insert_node(node.clone());
        } else {
            // It is possible that the node announcement is after broadcasted after the channel announcement.
            // So don't just ignore the channel even if we didn't find the node info here.
            warn!("Node1 not found for channel {:?}", &channel_info);
        }
        if let Some(node) = self.nodes.get(&channel_info.node2()) {
            self.store.insert_node(node.clone());
        } else {
            warn!("Node2 not found for channel {:?}", &channel_info);
        }

        let outpoint = channel_info.out_point();
        self.channels.insert(outpoint.clone(), channel_info.clone());
        self.store.insert_channel(channel_info);
        debug!("Successfully added channel {:?}", outpoint);
    }

    pub fn nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes.values()
    }

    pub fn get_nodes_with_params(
        &self,
        limit: usize,
        after: Option<JsonBytes>,
    ) -> (Vec<NodeInfo>, JsonBytes) {
        self.store.get_nodes_with_params(limit, after, None)
    }

    pub fn get_channels_with_params(
        &self,
        limit: usize,
        after: Option<JsonBytes>,
    ) -> (Vec<ChannelInfo>, JsonBytes) {
        self.store.get_channels_with_params(limit, after, None)
    }

    pub fn get_node(&self, node_id: Pubkey) -> Option<&NodeInfo> {
        self.nodes.get(&node_id)
    }

    pub fn channels(&self) -> impl Iterator<Item = &ChannelInfo> {
        self.channels.values()
    }

    pub fn get_channel(&self, outpoint: &OutPoint) -> Option<&ChannelInfo> {
        self.channels.get(outpoint)
    }

    pub fn get_channels_by_peer(&self, node_id: Pubkey) -> impl Iterator<Item = &ChannelInfo> {
        self.channels
            .values()
            .filter(move |channel| channel.node1() == node_id || channel.node2() == node_id)
    }

    pub fn get_channels_within_block_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> (impl Iterator<Item = &ChannelInfo>, u64, bool) {
        (
            self.channels.values().filter(move |channel| {
                channel.funding_tx_block_number >= start_block
                    && channel.funding_tx_block_number < end_block
            }),
            end_block,
            self.channels.is_empty()
                || self
                    .channels
                    .values()
                    .any(|channel| channel.funding_tx_block_number >= end_block),
        )
    }

    pub fn process_channel_update(&mut self, update: ChannelUpdate) -> Result<(), GraphError> {
        debug!("Processing channel update: {:?}", &update);
        let channel_outpoint = &update.channel_outpoint;
        let Some(channel) = self.channels.get_mut(channel_outpoint) else {
            return Err(GraphError::Other("channel not found".to_string()));
        };
        debug!(
            "Found channel {:?} for channel update {:?}",
            &channel, &update
        );
        let update_info = if update.message_flags & 1 == 1 {
            &mut channel.node1_to_node2
        } else {
            &mut channel.node2_to_node1
        };

        if let Some(info) = update_info {
            if update.version <= info.version {
                // update.version == info.version happens most possibly because we received the
                // broadcast many times. Don't emit too many logs in that case.
                if update.version < info.version {
                    warn!(
                        "Ignoring updating with an outdated channel update {:?} for channel {:?}, current update info: {:?}",
                        &update, channel_outpoint, &info
                    );
                }
                return Ok(());
            }
        }
        let disabled = update.channel_flags & CHANNEL_DISABLED_FLAG == CHANNEL_DISABLED_FLAG;

        *update_info = Some(ChannelUpdateInfo {
            version: update.version,
            timestamp: std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64,
            enabled: !disabled,
            cltv_expiry_delta: update.tlc_locktime_expiry_delta,
            htlc_minimum_value: update.tlc_minimum_value,
            htlc_maximum_value: update.tlc_maximum_value,
            fee_rate: update.tlc_fee_proportional_millionths as u64,
            last_update_message: update.clone(),
        });

        self.store.insert_channel(channel.to_owned());
        debug!(
            "Processed channel update: channel {:?}, update {:?}",
            &channel, &update
        );
        if disabled {
            self.channels.remove(channel_outpoint);
        }
        Ok(())
    }

    pub fn check_chain_hash(&self, chain_hash: Hash256) -> bool {
        self.chain_hash == chain_hash
    }

    pub fn get_node_inbounds(
        &self,
        node_id: Pubkey,
    ) -> impl Iterator<Item = (Pubkey, &ChannelInfo, &ChannelUpdateInfo)> {
        self.channels.values().filter_map(move |channel| {
            if let Some(info) = channel.node1_to_node2.as_ref() {
                if info.enabled && channel.node2() == node_id {
                    return Some((channel.node1(), channel, info));
                }
            }

            if let Some(info) = channel.node2_to_node1.as_ref() {
                if info.enabled && channel.node1() == node_id {
                    return Some((channel.node2(), channel, info));
                }
            }
            None
        })
    }

    pub fn get_source_pubkey(&self) -> Pubkey {
        self.source
    }

    pub fn calculate_fee(&self, amount: u128, fee_proportational_millionths: u128) -> u128 {
        let fee = fee_proportational_millionths * amount;
        let base_fee = fee / 1_000_000;
        let remainder = fee % 1_000_000;
        if remainder > 0 {
            base_fee + 1
        } else {
            base_fee
        }
    }

    #[cfg(test)]
    pub fn reset(&mut self) {
        self.channels.clear();
        self.nodes.clear();
    }

    pub fn init_payment_session(&self, payment_request: SendPaymentCommand) -> PaymentSession {
        let payment_session = PaymentSession::new(payment_request, 3);
        if let Some(session) = self
            .store
            .get_payment_session(payment_session.payment_hash())
        {
            return session;
        } else {
            self.store.insert_payment_session(payment_session.clone());
            payment_session
        }
    }

    /// Returns a list of `PaymentHopData` for all nodes in the route, including the origin and the target node.
    pub fn build_route(
        &self,
        payment_request: SendPaymentCommand,
    ) -> Result<Vec<PaymentHopData>, GraphError> {
        let source = self.get_source_pubkey();
        let (target, amount, payment_hash, preimage, udt_type_script) = payment_request
            .check_valid()
            .map_err(|e| GraphError::Other(format!("payment request is invalid {:?}", e)))?;
        let invoice = payment_request
            .invoice
            .map(|x| x.parse::<CkbInvoice>().unwrap());
        let hash_algorithm = invoice
            .as_ref()
            .and_then(|x| x.hash_algorithm().copied())
            .unwrap_or_default();

        info!(
            "build_route source: {:?} target: {:?} amount: {:?}, payment_hash: {:?}",
            source, target, amount, payment_hash
        );

        let route = self.find_route(
            source,
            target,
            amount,
            payment_request.max_fee_amount,
            udt_type_script,
        )?;
        assert!(!route.is_empty());

        let mut current_amount = amount;
        let mut current_expiry = 0;
        let mut onion_infos = vec![];
        for i in (0..route.len()).rev() {
            let is_last = i == route.len() - 1;
            let (next_hop, next_channel_outpoint) = if is_last {
                (None, None)
            } else {
                (
                    Some(route[i + 1].target),
                    Some(route[i + 1].channel_outpoint.clone()),
                )
            };
            let (fee, expiry) = if is_last {
                (0, 0)
            } else {
                let channel_info = self
                    .get_channel(&route[i + 1].channel_outpoint)
                    .expect("channel not found");
                let channel_update = &if channel_info.node1() == route[i + 1].target {
                    channel_info.node2_to_node1.as_ref()
                } else {
                    channel_info.node1_to_node2.as_ref()
                }
                .expect("channel_update is none");
                let fee_rate = channel_update.fee_rate;
                let fee = self.calculate_fee(current_amount, fee_rate as u128);
                let expiry = channel_update.cltv_expiry_delta;
                (fee, expiry)
            };

            // make sure the final hop's amount is the same as the payment amount
            // the last hop will check the amount from TLC and the amount from the onion packet
            onion_infos.push(PaymentHopData {
                amount: current_amount,
                payment_hash,
                next_hop,
                tlc_hash_algorithm: hash_algorithm,
                expiry: current_expiry,
                channel_outpoint: next_channel_outpoint,
                preimage: if is_last { preimage } else { None },
            });
            current_amount += fee;
            current_expiry += expiry;
        }
        // Add the first hop as the instruction for the current node, so the logic for send HTLC can be reused.
        let next_hop = if !route.is_empty() {
            Some(route[0].target)
        } else {
            None
        };
        onion_infos.push(PaymentHopData {
            amount: current_amount,
            payment_hash,
            next_hop,
            tlc_hash_algorithm: hash_algorithm,
            expiry: current_expiry,
            channel_outpoint: Some(route[0].channel_outpoint.clone()),
            preimage: None,
        });
        onion_infos.reverse();
        assert_eq!(onion_infos.len(), route.len() + 1);
        assert_eq!(onion_infos[route.len()].amount, amount);
        Ok(onion_infos)
    }

    // the algorithm works from target-to-source to find the shortest path
    pub fn find_route(
        &self,
        source: Pubkey,
        target: Pubkey,
        amount: u128,
        max_fee_amount: Option<u128>,
        udt_type_script: Option<Script>,
    ) -> Result<Vec<PathEdge>, GraphError> {
        let started_time = std::time::Instant::now();
        let nodes_len = self.nodes.len();
        let mut result = vec![];
        let mut nodes_visited = 0;
        let mut edges_expanded = 0;
        let mut nodes_heap = NodeHeap::new(nodes_len);
        let mut distances = HashMap::<Pubkey, NodeHeapElement>::new();

        if amount == 0 {
            return Err(GraphError::Amount(
                "Amount must be greater than 0".to_string(),
            ));
        }

        if source == target {
            return Err(GraphError::PathFind(
                "source and target are the same".to_string(),
            ));
        }
        let Some(source_node) = self.nodes.get(&source) else {
            return Err(GraphError::PathFind(format!(
                "source node not found: {:?}",
                &source
            )));
        };
        let Some(_target_node) = self.nodes.get(&target) else {
            return Err(GraphError::PathFind(format!(
                "target node not found: {:?}",
                &target
            )));
        };
        // initialize the target node
        nodes_heap.push(NodeHeapElement {
            node_id: target,
            weight: 0,
            distance: 0,
            amount_received: amount,
            fee_charged: 0,
            probability: 1.0,
            next_hop: None,
            incoming_cltv_height: 0,
        });
        loop {
            nodes_visited += 1;
            let Some(cur_hop) = nodes_heap.pop() else {
                break;
            };

            if cur_hop.node_id == source {
                break;
            }

            for (from, channel_info, channel_update) in self.get_node_inbounds(cur_hop.node_id) {
                edges_expanded += 1;
                // if charge inbound fees for exit hop
                let fee_rate = channel_update.fee_rate;
                let next_hop_received_amount = cur_hop.amount_received;
                let fee = self.calculate_fee(next_hop_received_amount, fee_rate as u128);
                let amount_to_send = next_hop_received_amount + fee;

                debug!(
                    "fee_rate: {:?} next_hop_received_amount: {:?}, fee: {:?} amount_to_send: {:?} channel_capacity: {:?} htlc_max_value: {:?}",
                    fee_rate, next_hop_received_amount, fee, amount_to_send, channel_info.capacity(), channel_update.htlc_maximum_value
                );

                if udt_type_script != channel_info.announcement_msg.udt_type_script {
                    continue;
                }

                // if the amount to send is greater than the amount we have, skip this edge
                if let Some(max_fee_amount) = max_fee_amount {
                    if amount_to_send > amount + max_fee_amount {
                        debug!(
                            "amount_to_send: {:?} is greater than sum_amount sum_amount: {:?}",
                            amount_to_send,
                            amount + max_fee_amount
                        );
                        continue;
                    }
                }
                // check to make sure the current hop can send the amount
                // if `htlc_maximum_value` equals 0, it means there is no limit
                if amount_to_send > channel_info.capacity()
                    || (channel_update.htlc_maximum_value != 0
                        && amount_to_send > channel_update.htlc_maximum_value)
                {
                    debug!(
                        "amount_to_send is greater than channel capacity: {:?} capacity: {:?}, htlc_max_value: {:?}",
                        amount_to_send,
                        channel_info.capacity(),
                        channel_update.htlc_maximum_value
                    );
                    continue;
                }
                if amount_to_send < channel_update.htlc_minimum_value {
                    debug!(
                        "amount_to_send is less than htlc_minimum_value: {:?} min_value: {:?}",
                        amount_to_send, channel_update.htlc_minimum_value
                    );
                    continue;
                }
                let incomming_cltv = cur_hop.incoming_cltv_height
                    + if from == source {
                        0
                    } else {
                        channel_update.cltv_expiry_delta
                    };

                let probability = cur_hop.probability
                    * ProbabilityEvaluator::evaluate_probability(
                        from,
                        cur_hop.node_id,
                        amount_to_send,
                        channel_info.capacity(),
                    );

                if probability < DEFAULT_MIN_PROBABILITY {
                    debug!("probability is too low: {:?}", probability);
                    continue;
                }
                debug!("probability: {:?}", probability);
                let agg_weight =
                    self.edge_weight(amount_to_send, fee, channel_update.cltv_expiry_delta);
                let weight = cur_hop.weight + agg_weight;
                let distance = self.calculate_distance_based_probability(probability, weight);

                if let Some(node) = distances.get(&from) {
                    if distance >= node.distance {
                        continue;
                    }
                }
                // info!("weight: {:?} dist: {:?} fee: {:?}", weight, dist, fee);
                // TODO: update weight and distance here
                let node: NodeHeapElement = NodeHeapElement {
                    node_id: from,
                    weight,
                    distance,
                    amount_received: amount_to_send,
                    incoming_cltv_height: incomming_cltv,
                    fee_charged: fee,
                    probability,
                    next_hop: Some((cur_hop.node_id, channel_info.out_point())),
                };
                distances.insert(node.node_id, node.clone());
                nodes_heap.push_or_fix(node);
            }
        }

        let mut current = source_node.node_id;
        while current != target {
            if let Some(elem) = distances.get(&current) {
                let next_hop = elem.next_hop.as_ref().expect("next_hop is none");
                result.push(PathEdge {
                    target: next_hop.0,
                    channel_outpoint: next_hop.1.clone(),
                });
                current = next_hop.0;
            } else {
                break;
            }
        }

        info!(
            "get_route: nodes visited: {}, edges expanded: {}, time: {:?}",
            nodes_visited,
            edges_expanded,
            started_time.elapsed()
        );
        if result.is_empty() || current != target {
            return Err(GraphError::PathFind("no path found".to_string()));
        }
        Ok(result)
    }

    fn edge_weight(&self, amount: u128, fee: u128, cltv_expiry_delta: u64) -> u128 {
        let risk_factor: u128 = 15;
        let time_lock_penalty = amount * cltv_expiry_delta as u128 * (risk_factor / 1000000000);
        fee + time_lock_penalty
    }

    fn calculate_distance_based_probability(&self, probability: f64, weight: u128) -> u128 {
        // FIXME: set this to configurable parameters
        let weight = weight as f64;
        let time_pref = 0.5_f64;
        let default_attemp_cost = 0.1_f64;
        let penalty = default_attemp_cost * (1.0 / (0.5 - time_pref / 2.0) - 1.0);
        weight as u128 + (penalty / probability) as u128
    }
}

pub trait NetworkGraphStateStore {
    fn get_channels(&self, outpoint: Option<OutPoint>) -> Vec<ChannelInfo>;
    fn get_nodes(&self, peer_id: Option<Pubkey>) -> Vec<NodeInfo>;
    fn get_nodes_with_params(
        &self,
        limit: usize,
        after: Option<JsonBytes>,
        node_id: Option<Pubkey>,
    ) -> (Vec<NodeInfo>, JsonBytes);
    fn get_channels_with_params(
        &self,
        limit: usize,
        after: Option<JsonBytes>,
        outpoint: Option<OutPoint>,
    ) -> (Vec<ChannelInfo>, JsonBytes);
    fn insert_channel(&self, channel: ChannelInfo);
    fn insert_node(&self, node: NodeInfo);
    fn get_payment_session(&self, payment_hash: Hash256) -> Option<PaymentSession>;
    fn insert_payment_session(&self, session: PaymentSession);
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PaymentSession {
    pub command: SendPaymentCommand,
    pub retried_times: u32,
    pub last_error: Option<String>,
    pub try_limit: u32,
}

impl PaymentSession {
    pub fn new(command: SendPaymentCommand, try_limit: u32) -> Self {
        Self {
            command,
            retried_times: 0,
            last_error: None,
            try_limit,
        }
    }

    pub fn payment_hash(&self) -> Hash256 {
        self.command.payment_hash()
    }
}
