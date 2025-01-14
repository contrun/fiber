# Network graph 重构

## 需求

我们需要一个整个 fiber 网络的拓扑状态图来实现寻找和规划支付路径。构建支付路径的时候，我们需要综合考虑如下信息。
- public channel 的广播信息
- 我们拥有的 channel 的当前资金分配
- 支付的金额
- channel 的支付历史
- 节点的连接历史

目前我们是通过构建一个 network graph 来实现寻找支付路径。这样的一个 network graph 应该具备如下功能：

- 查询某条广播信息（包含 channel announcement, channel update 和 node announcement）
- 查询某个时间范围内的广播消息
- 处理不完整的广播消息（比如在某些情况下，channel update 比 channel announcement 先被接收，这个时候我们可能找不到对应的 channel 的更多信息）
- 支持 private channel（private channel 状态信息并不能通过处理广播信息得到，我们需要直接和 channel actor 交互）
- 支持使用精确的余额信息构建更加健壮的支付通道（这个功能和上面的支持 private channel 功能是正交的，我们可以知道自己拥有的 public channel 的精确余额信息），为了方便，我们称知道完整资金分配的通道为 transparent 通道，不知道完整资金分配的通道为 opaque 通道
- 支持标注某些节点或者通道出现了临时或者永久性失败（有些通道或者节点可能出现临时性错误，这些错误不会在广播消息中体现出来）

## 当前实现

### 当前实现的概述

下面是目前 network graph 的主要数据结构。其中 `NetworkGraph` 是一个保存在内存中的 network graph, `NetworkGraphStateStore` 是抽象出来的用来持久化 graph 数据的 trait. 在每次启动的时候，我们会先使用 `NetworkGraphStateStore` 得到所有的 channel 或者节点，然后生成得到一个内存中的数据结构 `NetworkGraph` （[函数 `load_from_store`](https://github.com/nervosnetwork/fiber/blob/a482523eb014b4db8b7e8793c7dbb6b80025ba62/src/fiber/graph.rs#L193) ）. 任何 `NetworkGraph` 的改动最后都会调用一下 `NetworkGraphStateStore` 的 `insert_*` （如 `insert_channel`） 函数以持久化数据。普通的操作流程都是直接读取保存在内存里面的 `NetworkGraph` 数据。

```rust
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
    // when we restarting a node, we will reconnect to these peers
    connected_peer_addresses: HashMap<PeerId, Multiaddr>,
    nodes: HashMap<Pubkey, NodeInfo>,
    store: S,
    chain_hash: Hash256,
}
```

```rust
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
```


```rust
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelInfo {
    pub funding_tx_block_number: u64,
    pub funding_tx_index: u32,
    pub announcement_msg: ChannelAnnouncement,
    pub one_to_two: Option<ChannelUpdateInfo>,
    pub two_to_one: Option<ChannelUpdateInfo>,
    // The time that the channel was announced to the network.
    pub timestamp: u64,
}
```

```rust
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
```

### 当前实现存在的问题

现在 Network graph 的实现存在几个问题。
- 字段设计没有考虑能够知道所有 channel 余额信息的情况，因此可能出现某个通道里面节点A没有这么多资金了，但是还是构建出的支付路径里面还是使用了这个通道的情况。
- 现在完全没有考虑 private channel， 因此可能出现 A B 之间明明有一个通道，但是没有找到支付路径的情况。
- 所有的 channel 和 node 信息都保存在内存里面了，并且 `NodeInfo` 和 `ChannelInfo` 的会包含原始的 channel announcement, channel update, node announcement 信息，这里可能占用较多的内存。
- 可能无法高效查询某个时间段内的信息。我们现在都是通过枚举 `NetworkGraph` 里面的 `channels` 和 `nodes` 然后对照时间戳得到的数据列表。之后会使用 cursor 的方式来实现分页，当前方案显然不太理想。

## 重构

### 将所有验证过之后的广播信息直接保存到硬盘

我们会增加一个一个 column family（方便起见称这个 column family 为 `broadcast_messages`）来保存广播信息，这个 column family 的 key 的设计和 [fiber gossip 消息的改进](https://www.notion.so/cryptape/fiber-gossip-1348f0d3781e806bad30e075c3638154) 一样。也就是使用 `Byte45` 作为一个 key, 它由 timestamp (8 bytes in big-endian) 和 union id (1 byte), 以及 message id (36 bytes) 拼接组成, message id 的定义如下:

```
NodeAnnouncement: node_id 33 bytes padding 3 zero bytes
ChannelAnnouncement: channel_outpoint, 36 bytes
ChannelUpdate: channel_outpoint, 36 bytes
```

使用这种方式保存广播信息的一个缺点是，我们没法方便的查询某条特定的广播信息。比如某个节点向我们发起请求，要查询某个 channel 的 announcement, 因为我们不知道这个 channel 的 announcement 的时间戳，所以不知道上面 column family `broadcast_messages` 的 key。解决这个问题的方法有

- 我们直接保存一份冗余的数据，创建一个 `latest_broadcast_messages` column family。这个 column family 的 key 不包含时间戳，其余和 `broadcast_messages` 保持一致。
- 创建一个 `latest_broadcast_messages` column family，它和 `broadcast_messages` 都不直接保存数据，而是一个指向真正数据的指针。
- 直接在启动的时候读取时间戳到内存，在查询原始广播信息的时候，先从内存里面读取出时间戳，然后构建完整的 key. 这个方法的缺点是可能需要保存较多的数据到内存。

前两个方法的额外的优势是，在启动的时候，我们不再需要读取所有的广播信息，来获取最新的 channel 状态。

### 更改 network graph 的边的定义

[当前实现](https://github.com/nervosnetwork/fiber/blob/a482523eb014b4db8b7e8793c7dbb6b80025ba62/src/fiber/graph.rs#L436)里面，我们在寻找到某个节点的 channel 的时候，我们直接返回了 `(Pubkey, &ChannelInfo, &ChannelUpdateInfo)`，

```rust
    pub fn get_node_inbounds(
        &self,
        node_id: Pubkey,
    ) -> impl Iterator<Item = (Pubkey, &ChannelInfo, &ChannelUpdateInfo)>
```

这些数据结构代表的是 public, opaque（不知道资金分配的）的 channel 信息。我们需要把这个返回的数据结构改成下面的 `DirectedChannelInfo`

```rust
/// All the channel information for a payment from `from` to `to`.
pub struct DirectedChannelInfo {
    pub from: Pubkey,
    pub to: Pubkey,
    pub udt_type_script: Option<Script>,
    pub channel_outpoint: OutPoint,
    pub channel_info: PublicOrPrivateChannelInfo,
}

/// The liquidity (with direction) of a channel. This may be the upper bound of the liquidity of the channel
/// (normally obtained from broadcasted channel capacity) or the precise liquidity of the channel
/// (normally we get this information because this is a channel owned by ourselves).
#[derive(Debug)]
pub enum LiquidityInfo {
    /// The upper bound of the liquidity of the channel. This is the maximum amount that can be sent through the channel.
    /// Normally we can only send a fraction of this amount through the channel. This amount is normally obtained from
    /// the channel update messages.
    UpperBound(u128),
    /// The precise liquidity of the channel. It is guaranteed that a payment of this amount can be sent through the channel.
    /// We know this amount because we own the channel and we know the exact balance of the channel.
    Precise(u128),
}

/// Directed information about a channel that is public and can be used for routing.
/// We normally get this information from the broadcasted channel update messages.
/// It normally contains imprecise information about the channel (e.g. the balance of
/// one party in a channel is not known, only the total capacity of the channel is known).
pub struct PublicChannelInfo {
    // We may (if, for example, we own the channel) or we may not know the exact balance of the channel.
    pub liquidity_info: LiquidityInfo,
    pub htlc_expiry_delta: u64,
    pub htlc_minimum_value: u128,
    pub htlc_maximum_value: u128,
    pub fee_rate: u64,
}

/// Directed information about a channel that is private and cannot be used for routing.
/// This channel is normally owned by the node itself and is used for receiving payments.
pub struct PrivateChannelInfo {
    /// The precise balance of the channel.
    pub balance: u128,
}

/// Directed information about a channel that can be either public or private.
pub enum PublicOrPrivateChannelInfo {
    Public(PublicChannelInfo),
    Private(PrivateChannelInfo),
}
```

这里 `DirectedChannelInfo` 可能返回 public 或者 private channel。private channel 不能用于 forward tlc, 它只能接收支付。这里的主要使用场景是，A B 之间创建了一个通道，A 想要给 B 直接转账。如果 graph 里面的某条边是一个 public channel，一般来说，整个支付通道里面的第一跳的具体资金信息 sender 是清楚的，所以我们可以把第一跳的流通性信息 `LiquidityInfo` 设置为 `LiquidityInfo::Precise(balance)` （其中 `balance` 为 sender 拥有的具体金额），其他的中间跳我们可能知道具体的资金信息也可能不知道（比如如果中间某个其他节点也是我们拥有的，那我们可能也知道其他两个通道的资金信息，未来我们可以支持给用户 api，让他告诉我们现在具体的通道资金信息），我们可以更具需求设置 `LiquidityInfo`。把边的定义改成这个可以满足 public/private, transparent/opaque channel 的转账需求。

### 重构 `ChannelInfo`

首先，我们会区分对待是否知道具体余额信息的通道，分别创建一个 `OpaqueChannelInfo` 和 `TransparentChannelInfo`。这里 `OpaqueChannelInfo` 是指的我们仅能通过广播信息知道他的最新状态的 channel，`TransparentChannelInfo` 一般是我们自己拥有的 channel, 我们对它的资金分配非常清楚。


#### `OpaqueChannelInfo`

`OpaqueChannelInfo` 相较于之前 `ChannelInfo` 的主要改动是，首先，这些信息的字段里面不再包含完整的广播信息，其次，这些信息里面会增加某些字段用来描述本地看到的 channel node 信息（比如增加一个上次尝试使用 channel 失败的时间和金额）。比如新的 `OpaqueChannelInfo` 可以定义为

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct OpaqueChannelInfo {
    pub features: u64,
    pub channel_outpoint: OutPoint,

    pub node1_id: Pubkey,
    pub node2_id: Pubkey,
    // The total capacity of the channel.
    pub capacity: u128,
    // UDT script
    pub udt_type_script: Option<Script>,
    pub node1_to_node2: Option<ChannelUpdateInfo>,
    pub node2_to_node1: Option<ChannelUpdateInfo>,
    // The time that the channel was announced to the network.
    pub timestamp: u64,

    // Some other fields to determine the health of this channel.
    pub failed_payment_attempts: Vec<(u128, u64)>,
}
```

从实现上来说，有一部分信息是从广播消息里面获取到的（如上面的 `channel_outpoint` ），有一部分信息是本地看到的 channel 信息（比如上面的 `failed_payment_attempts` ）。

#### `TransparentChannelInfo`

这里主要的问题是要保持和这里的数据和 channel actor 的最新状态一致。这里有两种方法更新数据。

- 第一种方式是直接保存 channel actor 的 reference, 需要用的时候我们就直接发个消息给 channel actor, 然后等待 channel actor 的回复。
- 第二种方式是每次 channel actor 更新的时候，都发出一个事件，然后更新 graph 里面的信息。因为不同组件数据的更新时间不一样，可能会出现细微的数据不一致。

之前和 yukang 这个细节，他比较倾向于使用第二种方案。

## 其他可选方案

## 待解决问题
