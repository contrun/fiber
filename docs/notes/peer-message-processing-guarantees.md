# Peer message processing guarantee

As peers may crash at any time, the two parties of a channel may have different view our current channel state. In order to keep them in sync, we need a way to resume channel processing after the counterparty crashed or just lost their Internet connection.

One thing we definitely should do is save the related message even if we think we have deliver the message to the peer. This is because even if we know for sure the message is delivered, we don't know if that message is processed normally by the peer immediately (as we don't have a ACK mechanism in our protocol). For example, Imagine we have sent a `RemoveTlc` message and our p2p network stack tells us the message is successfully delivered, we still have to save this message to our channel state. It is not until we receive some message that confirms the peer has received and processed this message can we remove this message from our state (e.g. in the `RemoveTlc` case, if the peer signed a commitment without this TLC, it must have processed this message). Otherwise, we need to resend this message on the resumption of connection to the peer. The time when we can definitely remove the message is dependent on the message type.


Below the client indicates its intention of removing some TLC from the state by sending a `RemoveTlcCommand` to the `ChannelActor`. The `ChannelActor` should save this operation to the state first.

```rust
impl ChannelActorState {
    fn process_remove_tlc_command(&mut self, command: RemoveTlcCommand) {
        let remove_tlc_message = PcnMessage::RemoveTlc {
            
        };
        let operation = Operation::SendPcnMessage(remove_tlc_message);
        self.append_operation(operation);
    }
}
```

Upon peer connection resumption, the actor should iterate over all pending operations, and re-execute the operation (in our case, resend the message). This should be continued indefinitely until we know for sure, this message is processed by the peer (e.g. a commitment is signed by the peer without this TLC).

The message processing workflow here should be accompanied with [atomic actor state persistence](./actor-state-persistence.md).