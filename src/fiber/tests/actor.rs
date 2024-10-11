use ractor::{
    async_trait as rasync_trait, concurrency::JoinHandle, Actor, ActorProcessingErr, ActorRef,
};
use std::{mem::ManuallyDrop, sync::Arc};
use tokio::sync::{mpsc, Mutex, MutexGuard};
use tokio_condvar::Condvar;

// 1. introspector sends the message to the actor, notifies the actor to process the message and starts to wait for the message to be handled.
// 2. actor exits the waiting for message receiving, processes the message, updates the states, notifies the introspector, waits for the introspector's signal to continue processing the next message.
// 3. introspector awakes from sleeping, introspects the changed states, and sends the next message to the actor. The loop continues.
pub struct ActorTestHarness<M, S> {
    // Mutex and Condvar to synchronize the test harness with the actor.
    mutex: Arc<Mutex<bool>>,
    condvar: Arc<Condvar>,
    mediator: ActorRef<M>,
    state_sender: mpsc::Sender<ManuallyDrop<S>>,
}

trait ActorWithTestHarness<M, S>: Actor {
    fn with_test_harness(
        self,
        harness: ActorTestHarness<<Self as Actor>::Msg, <Self as Actor>::State>,
    ) -> Self;

    fn get_test_harness(
        &self,
    ) -> Option<&ActorTestHarness<<Self as Actor>::Msg, <Self as Actor>::State>>;

    fn get_mediator(&self) -> Option<&ActorRef<<Self as Actor>::Msg>> {
        self.get_test_harness().map(|harness| &harness.mediator)
    }
}

impl<M, S> ActorTestHarness<M, Box<S>> {
    pub async unsafe fn send_boxed_state_reference(&self, state: Box<S>) -> Box<S> {
        let state_copy = unsafe { Box::from_raw(&*state as *const S as *mut S) };
        self.state_sender
            .clone()
            .send(ManuallyDrop::new(state_copy))
            .await
            .expect("Failed to send state");
        state
    }
}

impl<M, S> ActorTestHarness<M, S> {
    pub async fn wait_to_handle_message(&self) -> MutexGuard<'_, bool> {
        println!("ActorTestHarness: Waiting to handle message");
        let lock = self.mutex.as_ref();
        let mut can_start_handling_message = lock.lock().await;
        while !*can_start_handling_message {
            println!("ActorTestHarness: Waiting for message handling loop");
            can_start_handling_message = self.condvar.wait(can_start_handling_message).await;
        }
        println!("ActorTestHarness: Handling message");
        return can_start_handling_message;
    }

    pub async fn notify_message_handled(
        &self,
        mut can_start_handling_message: MutexGuard<'_, bool>,
    ) {
        *can_start_handling_message = false;
        self.condvar.notify_one();
    }
}

pub struct Introspector<A, S> {
    mutex: Arc<Mutex<bool>>,
    condvar: Arc<Condvar>,
    _phantom: std::marker::PhantomData<A>,
    _phantom2: std::marker::PhantomData<S>,
}

impl<A, S> Introspector<A, S> {
    pub fn new() -> Self {
        Self {
            mutex: Arc::new(Mutex::new(false)),
            condvar: Arc::new(Condvar::new()),
            _phantom: std::marker::PhantomData,
            _phantom2: std::marker::PhantomData,
        }
    }

    pub async fn wait_to_send_message(&self) -> MutexGuard<'_, bool> {
        println!("Introspector: Waiting to send message");
        let lock = self.mutex.as_ref();
        lock.lock().await
    }

    pub async fn notify_and_wait_for_message_handling(
        &self,
        mut can_start_handling_message: MutexGuard<'_, bool>,
    ) {
        println!("Introspector: Notifying message handling");
        *can_start_handling_message = true;
        self.condvar.notify_one();
        while *can_start_handling_message {
            println!("Introspector: Waiting for message handled loop");
            can_start_handling_message = self.condvar.wait(can_start_handling_message).await;
        }
        println!("Introspector: Message handled");
    }
}

struct IntrospectorArguments<A, AA, AS, AM, I, S> {
    actor: A,
    arguments: AA,
    state_initializer: Box<dyn Fn(&I, &mut AS) -> S>,
    state_updater: Option<Box<dyn Fn(&I, AM, &mut AS, &mut S) -> ()>>,
    state_finalizer: Option<Box<dyn Fn(&I, &mut AS, S) -> ()>>,
}

impl<A: Actor, I, S>
    IntrospectorArguments<A, <A as Actor>::Arguments, <A as Actor>::State, <A as Actor>::Msg, I, S>
{
    fn new(
        actor: A,
        arguments: <A as Actor>::Arguments,
        state_initializer: impl Fn(&I, &mut <A as Actor>::State) -> S,
    ) -> Self {
        Self {
            actor,
            arguments,
            state_initializer: Box::new(state_initializer),
            state_updater: None,
            state_finalizer: None,
        }
    }

    fn with_state_updater(
        self,
        state_updater: impl Fn(&I, <A as Actor>::Msg, &mut <A as Actor>::State, &mut S) -> (),
    ) -> Self {
        Self {
            state_updater: Some(Box::new(state_updater)),
            ..self
        }
    }

    fn with_state_finalizer(
        self,
        state_finalizer: impl Fn(&I, &mut <A as Actor>::State, S) -> (),
    ) -> Self {
        Self {
            state_finalizer: Some(Box::new(state_finalizer)),
            ..self
        }
    }
}

#[rasync_trait]
impl<A, S> Actor for Introspector<A, S>
where
    A: ActorWithTestHarness<<A as Actor>::Msg, <A as Actor>::State>,
    <A as Actor>::Msg: Clone,
    IntrospectorArguments<
        A,
        <A as Actor>::Arguments,
        <A as Actor>::State,
        <A as Actor>::Msg,
        Self,
        S,
    >: ractor::State + Sync,
    S: ractor::State + Sync,
{
    type Msg = <A as Actor>::Msg;
    type Arguments = IntrospectorArguments<
        A,
        <A as Actor>::Arguments,
        <A as Actor>::State,
        <A as Actor>::Msg,
        Self,
        S,
    >;
    type State = (
        S,
        Option<
            Box<
                dyn Fn(&Self, <Self as Actor>::Msg, &mut <A as Actor>::State, &mut S) -> ()
                    + Send
                    + Sync,
            >,
        >,
        Option<Box<dyn Fn(&Self, &mut <A as Actor>::State, S) -> () + Send + Sync>>,
        ManuallyDrop<<A as Actor>::State>,
        ActorRef<Self::Msg>,
        JoinHandle<()>,
    );

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        my_arguments: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let IntrospectorArguments {
            actor,
            arguments,
            state_initializer,
            state_updater,
            state_finalizer,
        } = my_arguments;
        let (sender, mut receiver) = mpsc::channel(1);
        let actor = actor.with_test_harness(ActorTestHarness {
            mutex: self.mutex.clone(),
            condvar: self.condvar.clone(),
            mediator: myself.clone(),
            state_sender: sender,
        });
        let (actor, handle) = Actor::spawn_linked(None, actor, arguments, myself.get_cell())
            .await
            .expect("start actor");
        let boxed_state = receiver.recv().await.expect("recv boxed state");
        let state = state_initializer(self, &mut *boxed_state);
        Ok((
            state,
            state_updater,
            state_finalizer,
            ManuallyDrop::new(boxed_state),
            actor,
            handle,
        ))
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let guard = self.wait_to_send_message().await;
        let (our_state, state_updater, _state_finalizer, their_state, actor, _handle) = state;
        actor
            .send_message(message.clone())
            .expect("Failed to send message");
        self.notify_and_wait_for_message_handling(guard).await;
        if let Some(state_updater) = self.state_updater {
            state_updater(self, message, their_state.as_mut(), our_state);
        }
        Ok(())
    }
}

mod tests {
    use ractor::cast;

    use super::*;

    pub struct PingPong {
        harness: Option<ActorTestHarness<Message, State>>,
    }

    #[derive(Debug, Clone)]
    pub enum Message {
        Ping,
        Pong,
    }

    impl Message {
        // retrieve the next message in the sequence
        fn next(&self) -> Self {
            match self {
                Self::Ping => Self::Pong,
                Self::Pong => Self::Ping,
            }
        }
    }

    type State = Box<u8>;

    type Arguments = ();

    impl ActorWithTestHarness<Message, State> for PingPong {
        fn with_test_harness(self, harness: ActorTestHarness<Message, State>) -> Self {
            Self {
                harness: Some(harness),
            }
        }

        fn get_test_harness(&self) -> Option<&ActorTestHarness<Message, State>> {
            self.harness.as_ref()
        }
    }

    #[rasync_trait]
    // the implementation of our actor's "logic"
    impl Actor for PingPong {
        type Msg = Message;
        type State = State;
        type Arguments = Arguments;

        async fn pre_start(
            &self,
            myself: ActorRef<Self::Msg>,
            _: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            let myself = self.get_mediator().cloned().unwrap_or(myself);

            // startup the event processing
            cast!(myself, Message::Ping)?;

            let state = Box::new(0u8);

            match self.get_test_harness() {
                Some(harness) => Ok(unsafe { harness.send_boxed_state_reference(state).await }),
                None => Ok(state),
            }
        }

        async fn handle(
            &self,
            myself: ActorRef<Self::Msg>,
            message: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            let myself = self.get_mediator().cloned().unwrap_or(myself);
            let state = state.as_mut();
            let guard = match self.get_test_harness() {
                Some(harness) => Some((harness, harness.wait_to_handle_message().await)),
                None => None,
            };
            if *state < 10u8 {
                println!("{:?}", &message);
                cast!(myself, message.next())?;
                *state += 1;
            } else {
                println!("PingPong: Exiting");
                myself.stop(None);
            }
            if let Some((harness, guard)) = guard {
                harness.notify_message_handled(guard).await;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_ping_pong_with_harness() {
        let introspector = Introspector::new();
        let actor = PingPong { harness: None };
        let (_actor, handle) = Actor::spawn(None, introspector, (actor, ()))
            .await
            .expect("start introspector");
        handle.await.expect("introspector should not fail");
    }

    #[tokio::test]
    async fn test_ping_pong_without_harness() {
        let actor = PingPong { harness: None };
        let (_actor, handle) = Actor::spawn(None, actor, ()).await.expect("start actor");
        handle.await.expect("actor should not fail");
    }
}
