use fnn::cch::CchMessage;
use fnn::ckb::contracts::init_contracts_context;
use fnn::fiber::graph::NetworkGraph;
use fnn::store::Store;
use fnn::watchtower::{WatchtowerActor, WatchtowerMessage};
use ractor::Actor;
use tokio::sync::{mpsc, RwLock};
use tokio::{select, signal};
use tracing::{debug, error, info, info_span, trace};
use tracing_subscriber::field::MakeExt;
use tracing_subscriber::{fmt, EnvFilter};

use core::default::Default;
use fnn::actors::RootActor;
use fnn::ckb::CkbChainActor;
use fnn::fiber::channel::ChannelSubscribers;
use fnn::tasks::{
    cancel_tasks_and_wait_for_completion, new_tokio_cancellation_token, new_tokio_task_tracker,
};
use fnn::{start_cch, start_ldk, start_network, start_rpc, Config};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::fmt::format;

#[tokio::main]
pub async fn main() {
    // ractor will set "id" for each actor:
    // https://github.com/slawlor/ractor/blob/67d657e4cdcb8884a9ccc9b758704cbb447ac163/ractor/src/actor/mod.rs#L701
    // here we map it with the node prefix
    let node_formatter = format::debug_fn(|writer, field, value| {
        if field.name() == "id" {
            write!(
                writer,
                "{}: {:?} on {}",
                field,
                value,
                fnn::get_node_prefix()
            )
        } else {
            write!(writer, "{}: {:?}", field, value)
        }
    })
    .delimited(", ");

    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .pretty()
        .fmt_fields(node_formatter)
        .init();

    info!("Starting node with git version {}", fnn::get_git_versin());

    let _span = info_span!("node", node = fnn::get_node_prefix()).entered();

    let config = Config::parse();
    debug!("Parsed config: {:?}", &config);

    if let Some(ldk_config) = config.ldk {
        info!("Starting ldk");
        start_ldk(ldk_config).await;
    }

    let tracker = new_tokio_task_tracker();
    let token = new_tokio_cancellation_token();
    let root_actor = RootActor::start(tracker, token).await;

    let store = Store::new(config.fiber.as_ref().unwrap().store_path());
    let subscribers = ChannelSubscribers::default();

    let (fiber_command_sender, network_graph, public_key) = match config.fiber {
        Some(fiber_config) => {
            // TODO: this is not a super user friendly error message which has actionable information
            // for the user to fix the error and start the node.
            let ckb_config = config.ckb.expect("ckb service is required for ckb service. \
            Add ckb service to the services list in the config file and relevant configuration to the ckb section of the config file.");
            let node_public_key = fiber_config.public_key();

            let _ = init_contracts_context(fiber_config.network, Some(&ckb_config));

            let ckb_actor = Actor::spawn_linked(
                Some("ckb".to_string()),
                CkbChainActor {},
                ckb_config.clone(),
                root_actor.get_cell(),
            )
            .await
            .expect("start ckb actor")
            .0;

            const CHANNEL_SIZE: usize = 4000;
            let (event_sender, mut event_receiver) = mpsc::channel(CHANNEL_SIZE);

            let network_graph = Arc::new(RwLock::new(NetworkGraph::new(
                store.clone(),
                node_public_key.clone().into(),
            )));

            info!("Starting fiber");
            let network_actor = start_network(
                fiber_config,
                ckb_actor,
                event_sender,
                new_tokio_task_tracker(),
                root_actor.get_cell(),
                store.clone(),
                subscribers.clone(),
                network_graph.clone(),
            )
            .await;

            let watchtower_actor = Actor::spawn_linked(
                Some("watchtower".to_string()),
                WatchtowerActor::new(store.clone()),
                ckb_config,
                root_actor.get_cell(),
            )
            .await
            .expect("start watchtower actor")
            .0;

            // every 60 seconds, check if there are any channels that submitted a commitment transaction
            // TODO: move interval to config file
            watchtower_actor
                .send_interval(Duration::from_secs(60), || WatchtowerMessage::PeriodicCheck);

            new_tokio_task_tracker().spawn(async move {
                let token = new_tokio_cancellation_token();
                loop {
                    select! {
                        event = event_receiver.recv() => {
                            match event {
                                None => {
                                    trace!("Event receiver completed, stopping event processing service");
                                    break;
                                }
                                Some(event) => {
                                    let _ = watchtower_actor.send_message(WatchtowerMessage::NetworkServiceEvent(event));
                                }
                            }
                        }
                        _ = token.cancelled() => {
                            debug!("Cancellation received, stopping event processing service");
                            break;
                        }
                    }
                }
                debug!("Event processing service exited");
            });

            (
                Some(network_actor),
                Some(network_graph),
                Some(node_public_key),
            )
        }
        None => (None, None, None),
    };

    let cch_actor = match config.cch {
        Some(cch_config) => {
            info!("Starting cch");
            let ignore_startup_failure = cch_config.ignore_startup_failure;
            match start_cch(
                cch_config,
                new_tokio_task_tracker(),
                new_tokio_cancellation_token(),
                root_actor.get_cell(),
                fiber_command_sender.clone(),
            )
            .await
            {
                Err(err) => {
                    if ignore_startup_failure {
                        info!("Cross-chain service failed to start and is ignored by the config option ignore_startup_failure: {}", err);
                        None
                    } else {
                        error!("Cross-chain service failed to start: {}", err);
                        return;
                    }
                }
                Ok(actor) => {
                    subscribers.pending_received_tlcs_subscribers.subscribe(
                        actor.clone(),
                        |tlc_notification| {
                            Some(CchMessage::PendingReceivedTlcNotification(tlc_notification))
                        },
                    );
                    subscribers.settled_tlcs_subscribers.subscribe(
                        actor.clone(),
                        |tlc_notification| {
                            Some(CchMessage::SettledTlcNotification(tlc_notification))
                        },
                    );

                    Some(actor)
                }
            }
        }
        None => None,
    };

    // Start rpc service
    let rpc_server_handle = match config.rpc {
        Some(config) => {
            if fiber_command_sender.is_none() && cch_actor.is_none() {
                error!("Rpc service requires ckb and cch service to be started. Exiting.");
                return;
            }

            info!("Starting rpc");
            let handle = start_rpc(
                config,
                fiber_command_sender,
                cch_actor,
                store,
                network_graph.unwrap(),
                public_key,
            )
            .await;
            Some(handle)
        }
        None => None,
    };

    signal::ctrl_c().await.expect("Failed to listen for event");
    info!("Received Ctrl-C, shutting down");
    if let Some(handle) = rpc_server_handle {
        handle.stop().unwrap();
        handle.stopped().await;
    }
    cancel_tasks_and_wait_for_completion().await;
}
