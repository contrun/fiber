use ckb_hash::blake2b_256;
use core::default::Default;
use fnn::actors::RootActor;
use fnn::cch::CchMessage;
use fnn::ckb::contracts::{get_script_by_contract, init_contracts_context, Contract};
use fnn::ckb::CkbChainActor;
use fnn::fiber::graph::NetworkGraph;
use fnn::fiber::{channel::ChannelSubscribers, NetworkActorCommand, NetworkActorMessage};
use fnn::store::Store;
use fnn::tasks::{
    cancel_tasks_and_wait_for_completion, new_tokio_cancellation_token, new_tokio_task_tracker,
};
use fnn::watchtower::{WatchtowerActor, WatchtowerMessage};
use fnn::{start_cch, start_network, start_rpc, Config};
use ractor::Actor;
use secp256k1::Secp256k1;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tentacle::multiaddr::Multiaddr;
use tokio::sync::{mpsc, RwLock};
use tokio::{select, signal};
use tracing::{debug, error, info, info_span, trace};
use tracing_subscriber::{field::MakeExt, fmt, fmt::format, EnvFilter};

#[tokio::main]
pub async fn main() {
    // ractor will set "id" for each actor:
    // https://github.com/slawlor/ractor/blob/67d657e4cdcb8884a9ccc9b758704cbb447ac163/ractor/src/actor/mod.rs#L701
    // here we map it with the node prefix
    let node_formatter = format::debug_fn(|writer, field, value| {
        let prefix = if field.name() == "id" {
            let r = fnn::get_node_prefix();
            if !r.is_empty() {
                format!(" on {}", r)
            } else {
                "".to_string()
            }
        } else {
            "".to_string()
        };
        write!(writer, "{}: {:?}{}", field, value, prefix)
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

    debug!("Starting root actor");
    let root_actor =
        RootActor::start(new_tokio_task_tracker(), new_tokio_cancellation_token()).await;

    let store = Store::new(config.fiber.as_ref().unwrap().store_path());
    let subscribers = ChannelSubscribers::default();

    let (fiber_command_sender, network_graph) = match config.fiber.clone() {
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

            let bootnodes = fiber_config.bootnode_addrs.clone();

            let network_graph = Arc::new(RwLock::new(NetworkGraph::new(
                store.clone(),
                node_public_key.clone().into(),
            )));

            let secret_key = ckb_config.read_secret_key().unwrap();
            let secp = Secp256k1::new();
            let pubkey_hash = blake2b_256(secret_key.public_key(&secp).serialize());
            let default_shutdown_script =
                get_script_by_contract(Contract::Secp256k1Lock, &pubkey_hash[0..20]);

            info!("Starting fiber service");
            let network_actor = start_network(
                fiber_config,
                ckb_actor,
                event_sender,
                new_tokio_task_tracker(),
                root_actor.get_cell(),
                store.clone(),
                subscribers.clone(),
                network_graph.clone(),
                default_shutdown_script,
            )
            .await;

            for bootnode in bootnodes {
                let addr = Multiaddr::from_str(&bootnode).expect("valid bootnode");
                let command = NetworkActorCommand::ConnectPeer(addr);
                network_actor
                    .send_message(NetworkActorMessage::new_command(command))
                    .expect("ckb actor alive")
            }

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

            debug!("Starting event processing service");
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

            (Some(network_actor), Some(network_graph))
        }
        None => (None, None),
    };

    let cch_actor = match config.cch {
        Some(cch_config) => {
            info!("Starting cch service");
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
        Some(rpc_config) => {
            if fiber_command_sender.is_none() && cch_actor.is_none() {
                error!("Rpc service requires ckb and cch service to be started. Exiting.");
                return;
            }

            info!("Starting rpc service");
            let handle = start_rpc(
                rpc_config,
                config.fiber,
                fiber_command_sender,
                cch_actor,
                store,
                network_graph.unwrap(),
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
        debug!("Waiting for rpc server to stop");
        handle.stopped().await;
        debug!("Rpc server stopped");
    }
    cancel_tasks_and_wait_for_completion().await;
}
