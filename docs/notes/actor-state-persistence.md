# Actor State Persistence

One common problem of updating actor states is that we need to ensure the actor state is always persisted to disk atomically. For example, if we want to update a channel actor upon receiving `AddTlc` message. Then we don't want part of the `ChannelActorState` (e.g. `pending_received_tlcs`) to be updated while other part of `ChannelActorState` (e.g. `next_receiving_tlc_id`) remains unchanged. Because, the node may crash next minute. The next time when we load actor state from the disk, it can become inconsistent.

In order to tackle the problem of atomic channel state update. We can wrap the implementation of our actor a little bit.

For example, assume that we are now implementing `Actor` for `ActorWithState`.

```rust
#[async_trait]
impl Actor for ActorWithState {
    type Msg = ActorMessage;
    type State = ActorState;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let channel_state = read_channel_state_from_disk();
        // There are some operations needed to process after the state update.
        // E.g. if a we are handling a RemoveTlcCommand, then we need to send a
        // RemoveTlc to the peer, so that the peer can update their state corespondingly.
        for op in channel_state.operations() {
            channel_state.process_op(op);
        }
        Ok(channel_state)
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // TODO: maybe some smart copy on write tricks to make cloning of
        // an old state cheaper.
        let old_state = state.clone();
        // Here we need to save all the post-saving operations to the state.
        // An simple example of these post-saving operations is sending the peer a message.
        // We delay the executation of these operations to archive atomicity.
        self.normal_handle(message, state);
        if old_state != state {
            // Up until now, all the state updates are volatine.
            atomically_persist_state(state);
        }
        // There are some operations needed to process after the state update.
        // E.g. if a we are handling a RemoveTlcCommand, then we need to send a
        // RemoveTlc to the peer, so that the peer can update their state corespondingly.
        for op in channel_state.operations() {
            channel_state.process_op(op);
        }
    }
}
```

Note here, the messages in the mailbox are not guaranteed to be processed. The node may crash any time while processing the messages. Even when we're executing the `process_op`. So all the operation in `process_op` must be idempotent. If that is true, then the messages are guaranteed to processed atomically and the state is persisted atomically.

Another problem is that even if the message is processed atomically on our side, the peer may crashed while processing any other related message (e.g. we successfully processed `RemoveTlcCommand` and removed this TLC from our state, the peer crashed while processing the corresponding `RemoveTlc` messsage, or it may never receive this message at all). In this case, we need to guarantee that a message is indeed delivered to the peer at least once. This requires [at least once peer message processing](./peer-message-processing-guarantees.md).