use ractor::{
    async_trait as rasync_trait, concurrency::JoinHandle, Actor, ActorProcessingErr, ActorRef,
    SpawnErr,
};
use std::{mem::ManuallyDrop, ops::DerefMut, sync::Arc};
use tokio::sync::{mpsc, Mutex, MutexGuard};
use tokio_condvar::Condvar;

// 1. inspector sends the message to the actor, notifies the actor to process the message and starts to wait for the message to be handled.
// 2. actor exits the waiting for message receiving, processes the message, updates the states, notifies the inspector, waits for the inspector's signal to continue processing the next message.
// 3. inspector awakes from sleeping, introspects the changed states, and sends the next message to the actor. The loop continues.
pub struct ActorTestHarness<M, S, T> {
    // Mutex and Condvar to synchronize the test harness with the actor.
    mutex: Arc<Mutex<bool>>,
    condvar: Arc<Condvar>,
    mediator: ActorRef<M>,
    state_sender: mpsc::Sender<T>,
    phantom: std::marker::PhantomData<(M, S)>,
}

pub trait ActorWithTestHarness<M, S, T>: Actor<Msg = M, State = S> {
    fn with_test_harness(self, harness: ActorTestHarness<M, S, T>) -> Self;

    fn get_test_harness(&self) -> Option<&ActorTestHarness<M, S, T>>;

    fn get_mediator(&self) -> Option<&ActorRef<M>> {
        self.get_test_harness().map(|harness| &harness.mediator)
    }
}

trait LeakReference: Sized {
    type Target: Sized + DerefMut<Target = Self>;
    fn leak(self) -> (Self, Self::Target);
}

impl<S> LeakReference for Box<S> {
    type Target = ManuallyDrop<Self>;

    fn leak(self) -> (Self, Self::Target) {
        let copy =
            ManuallyDrop::new(unsafe { Box::from_raw(&*self.as_ref() as *const S as *mut S) });
        (self, copy)
    }
}

impl<S> LeakReference for Arc<S> {
    type Target = Self;

    fn leak(self) -> (Self, Self) {
        (self.clone(), self)
    }
}

impl<M, S, T> ActorTestHarness<M, S, T>
where
    S: LeakReference<Target = T>,
{
    pub async fn leak_state(&self, state: S) -> S {
        let (state, state_copy) = state.leak();
        self.state_sender
            .clone()
            .send(state_copy)
            .await
            .expect("Failed to send state");
        state
    }
}

impl<M, S, T> ActorTestHarness<M, S, T> {
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

pub struct Inspector<A, S> {
    mutex: Arc<Mutex<bool>>,
    condvar: Arc<Condvar>,
    _phantom: std::marker::PhantomData<A>,
    _phantom2: std::marker::PhantomData<S>,
}

// We will actually never access the actor state from multiple threads, so we can safely implement Sync for the inspector.
unsafe impl<A: Actor, S: ractor::State> Sync for Inspector<A, S> {}

impl<A, S> Inspector<A, S> {
    pub fn new() -> Self {
        Self {
            mutex: Arc::new(Mutex::new(false)),
            condvar: Arc::new(Condvar::new()),
            _phantom: std::marker::PhantomData,
            _phantom2: std::marker::PhantomData,
        }
    }

    pub async fn wait_to_send_message(&self) -> MutexGuard<'_, bool> {
        println!("inspector: Waiting to send message");
        let lock = self.mutex.as_ref();
        lock.lock().await
    }

    pub async fn notify_and_wait_for_message_handling(
        &self,
        mut can_start_handling_message: MutexGuard<'_, bool>,
    ) {
        println!("inspector: Notifying message handling");
        *can_start_handling_message = true;
        self.condvar.notify_one();
        while *can_start_handling_message {
            println!("inspector: Waiting for message handled loop");
            can_start_handling_message = self.condvar.wait(can_start_handling_message).await;
        }
        println!("inspector: Message handled");
    }
}

pub trait InspectorPlugin {
    type ActorState: ractor::State;
    type ActorMessage: ractor::Message;

    fn actor_started(&mut self, actor_state: &mut Self::ActorState);
    fn message_handled(&mut self, actor_state: &mut Self::ActorState, message: Self::ActorMessage);
    fn actor_stopped(&mut self, actor_state: &mut Self::ActorState);
}

pub struct InspectorPluginNoop<S, M> {
    _phantom: std::marker::PhantomData<(S, M)>,
}

impl<S, M> InspectorPluginNoop<S, M> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S, M> InspectorPlugin for InspectorPluginNoop<S, M>
where
    S: ractor::State,
    M: ractor::Message,
{
    type ActorState = S;
    type ActorMessage = M;

    fn actor_started(&mut self, _actor_state: &mut Self::ActorState) {}

    fn message_handled(
        &mut self,
        _actor_state: &mut Self::ActorState,
        _message: Self::ActorMessage,
    ) {
    }

    fn actor_stopped(&mut self, _actor_state: &mut Self::ActorState) {}
}

pub struct InspectorPluginDumper<S, M> {
    _phantom: std::marker::PhantomData<(S, M)>,
}

impl<S, M> InspectorPluginDumper<S, M> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S, M> InspectorPlugin for InspectorPluginDumper<S, M>
where
    S: ractor::State + std::fmt::Debug,
    M: ractor::Message + std::fmt::Debug,
{
    type ActorState = S;
    type ActorMessage = M;

    fn actor_started(&mut self, actor_state: &mut Self::ActorState) {
        println!(
            "InspectorPluginDumper: Actor started with state {:?}",
            actor_state
        );
    }

    fn message_handled(&mut self, actor_state: &mut Self::ActorState, message: Self::ActorMessage) {
        println!(
            "InspectorPluginDumper: Message handled {:?} with final state {:?}",
            message, actor_state
        );
    }

    fn actor_stopped(&mut self, actor_state: &mut Self::ActorState) {
        println!(
            "InspectorPluginDumper: Actor stopped with state {:?}",
            actor_state
        );
    }
}

impl<M, S, T, A, P> Inspector<A, P>
where
    A: ActorWithTestHarness<M, S, T>,
    A: Actor<Msg = M, State = S>,
    M: Clone + ractor::Message,
    T: ractor::State,
    S: LeakReference<Target = T>,
    P: InspectorPlugin<ActorState = S, ActorMessage = M> + ractor::State,
{
    pub async fn start(
        actor: A,
        arguments: <A as Actor>::Arguments,
        plugin: P,
    ) -> Result<(ActorRef<<A as Actor>::Msg>, JoinHandle<()>), SpawnErr> {
        let inspector = Self::new();
        Actor::spawn(None, inspector, (actor, arguments, plugin)).await
    }
}

#[rasync_trait]
impl<M, S, T, A, P> Actor for Inspector<A, P>
where
    A: ActorWithTestHarness<M, S, T>,
    A: Actor<Msg = M, State = S>,
    M: Clone + ractor::Message,
    T: ractor::State,
    S: LeakReference<Target = T>,
    P: InspectorPlugin<ActorState = S, ActorMessage = M> + ractor::State,
{
    type Msg = M;
    type Arguments = (A, <A as Actor>::Arguments, P);
    type State = (P, T, ActorRef<M>, JoinHandle<()>);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        arguments: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let (actor, arguments, mut plugin) = arguments;
        let (sender, mut receiver) = mpsc::channel(1);
        let actor = actor.with_test_harness(ActorTestHarness {
            mutex: self.mutex.clone(),
            condvar: self.condvar.clone(),
            mediator: myself.clone(),
            state_sender: sender,
            phantom: std::marker::PhantomData,
        });
        let (actor, handle) = Actor::spawn_linked(None, actor, arguments, myself.get_cell())
            .await
            .expect("start actor");
        let mut actor_state = receiver.recv().await.expect("recv boxed state");
        plugin.actor_started(actor_state.deref_mut());
        Ok((plugin, actor_state, actor, handle))
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let guard = self.wait_to_send_message().await;
        let (plugin, their_state, actor, _handle) = state;
        actor
            .send_message(message.clone())
            .expect("Failed to send message");
        self.notify_and_wait_for_message_handling(guard).await;
        plugin.message_handled(their_state.deref_mut(), message);
        Ok(())
    }
}

mod tests {
    use ractor::cast;

    use super::*;
    #[derive(Debug, Clone)]
    pub enum Message {
        Ping,
        Pong,
    }

    type State = Box<u8>;

    type TransformedState = ManuallyDrop<State>;

    type Arguments = ();

    type Harness = ActorTestHarness<Message, State, TransformedState>;

    impl Message {
        // retrieve the next message in the sequence
        fn next(&self) -> Self {
            match self {
                Self::Ping => Self::Pong,
                Self::Pong => Self::Ping,
            }
        }
    }

    pub struct PingPong {
        harness: Option<Harness>,
    }

    impl ActorWithTestHarness<Message, State, TransformedState> for PingPong {
        fn with_test_harness(self, harness: Harness) -> Self {
            Self {
                harness: Some(harness),
            }
        }

        fn get_test_harness(&self) -> Option<&Harness> {
            self.harness.as_ref()
        }
    }

    #[rasync_trait]
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

            cast!(myself, Message::Ping)?;

            let state = Box::new(0u8);

            match self.get_test_harness() {
                Some(harness) => Ok(unsafe { harness.leak_state(state).await }),
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
    async fn test_ping_pong_without_harness() {
        let actor = PingPong { harness: None };
        let arguments = ();
        let (_actor, handle) = Actor::spawn(None, actor, arguments)
            .await
            .expect("start actor");
        handle.await.expect("actor should not fail");
    }

    #[tokio::test]
    async fn test_ping_pong_with_noop() {
        let actor = PingPong { harness: None };
        let arguments = ();
        let plugin = InspectorPluginNoop::new();
        let (_actor, handle) = Inspector::start(actor, arguments, plugin)
            .await
            .expect("start inspector");
        handle.await.expect("inspector should not fail");
    }

    #[tokio::test]
    async fn test_ping_pong_with_dumper() {
        let actor = PingPong { harness: None };
        let arguments = ();
        let plugin = InspectorPluginDumper::new();
        let (_actor, handle) = Inspector::start(actor, arguments, plugin)
            .await
            .expect("start inspector");
        handle.await.expect("inspector should not fail");
    }
}
