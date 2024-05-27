use ckb_pcn_node::ckb::chain::CommitmentLockContext;
use ckb_pcn_node::ckb::config::CkbNetwork;
use ckb_pcn_node::invoice::start_invoice;
use ckb_pcn_node::rpc::InvoiceCommandWithReply;
use ckb_pcn_node::store::Store;
use log::{debug, error, info};
use ractor::Actor;
use tentacle::multiaddr::Multiaddr;
use tokio::sync::mpsc;
use tokio::{select, signal};

use std::str::FromStr;

use ckb_pcn_node::actors::RootActor;
use ckb_pcn_node::cch::CchCommand;
use ckb_pcn_node::ckb::{NetworkActorCommand, NetworkActorMessage};
use ckb_pcn_node::ckb_chain::CkbChainActor;
use ckb_pcn_node::tasks::{
    cancel_tasks_and_wait_for_completion, new_tokio_cancellation_token, new_tokio_task_tracker,
};
use ckb_pcn_node::{start_cch, start_ckb, start_ldk, start_rpc, Config};

#[tokio::main]
pub async fn main() {
    let mut builder = env_logger::builder();
    match std::env::var("LOG_SURFFIX") {
        Ok(log_surffix) => {
            info!("Setting log surffix to: {}", &log_surffix);
            builder.format_suffix(log_surffix.leak());
        }
        Err(_) => {}
    }
    builder.init();

    let config = Config::parse();
    debug!("Parsed config: {:?}", &config);

    if let Some(ldk_config) = config.ldk {
        info!("Starting ldk");
        start_ldk(ldk_config).await;
    }

    let tracker = new_tokio_task_tracker();
    let token = new_tokio_cancellation_token();
    let root_actor = RootActor::start(tracker, token).await;

    let store = Store::new(config.ckb.as_ref().unwrap().store_path());

    let ckb_command_sender = match config.ckb {
        Some(ckb_config) => {
            // TODO: this is not a super user friendly error message which has actionable information
            // for the user to fix the error and start the node.
            let ckb_chain_config = config.ckb_chain.expect("ckb-chain service is required for ckb service. Add ckb-chain service to the services list in the config file and relevant configuration to the ckb_chain section of the config file.");

            let network = ckb_config.network.unwrap_or(CkbNetwork::Dev);
            let ctx = CommitmentLockContext::initialize(network);

            let ckb_chain_actor = Actor::spawn_linked(
                Some("ckb-chain".to_string()),
                CkbChainActor {},
                (ckb_chain_config, ctx.clone()),
                root_actor.get_cell(),
            )
            .await
            .expect("start ckb-chain actor")
            .0;

            const CHANNEL_SIZE: usize = 4000;
            let (event_sender, mut event_receiver) = mpsc::channel(CHANNEL_SIZE);

            let bootnodes = ckb_config.bootnode_addrs.clone();

            info!("Starting ckb");
            let ckb_actor = start_ckb(
                ckb_config,
                ckb_chain_actor,
                event_sender,
                new_tokio_task_tracker(),
                root_actor.get_cell(),
                store.clone(),
            )
            .await;

            for bootnode in bootnodes {
                let addr = Multiaddr::from_str(&bootnode).expect("valid bootnode");
                let command = NetworkActorCommand::ConnectPeer(addr);
                ckb_actor
                    .send_message(NetworkActorMessage::new_command(command))
                    .expect("ckb actor alive")
            }

            new_tokio_task_tracker().spawn(async move {
                let token = new_tokio_cancellation_token();
                loop {
                    select! {
                        event = event_receiver.recv() => {
                            match event {
                                None => {
                                    debug!("Event receiver completed, stopping event processing service");
                                    break;
                                }
                                Some(event) => {
                                    debug!("Received event from ckb service: {:?}", event);
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

            Some(ckb_actor)
        }
        None => None,
    };

    let cch_command_sender = match config.cch {
        Some(cch_config) => {
            const CHANNEL_SIZE: usize = 4000;
            let (command_sender, command_receiver) = mpsc::channel::<CchCommand>(CHANNEL_SIZE);
            info!("Starting cch");
            start_cch(
                cch_config,
                command_receiver,
                new_tokio_cancellation_token(),
                new_tokio_task_tracker(),
            )
            .await;
            Some(command_sender)
        }
        None => None,
    };

    let invoice_command_sender = {
        const CHANNEL_SIZE: usize = 4000;
        let (command_sender, command_receiver) =
            mpsc::channel::<InvoiceCommandWithReply>(CHANNEL_SIZE);
        info!("Starting cch");
        start_invoice(
            command_receiver,
            new_tokio_cancellation_token(),
            new_tokio_task_tracker(),
            store,
        )
        .await;
        Some(command_sender)
    };

    // Start rpc service
    if let Some(rpc_config) = config.rpc {
        if ckb_command_sender.is_none()
            && cch_command_sender.is_none()
            && invoice_command_sender.is_none()
        {
            error!("Rpc service requires ckb, chh and invoice service to be started. Exiting.");
            return;
        }

        let shutdown_signal = async {
            let token = new_tokio_cancellation_token();
            token.cancelled().await;
        };
        new_tokio_task_tracker().spawn(async move {
            start_rpc(
                rpc_config,
                ckb_command_sender,
                cch_command_sender,
                invoice_command_sender,
                shutdown_signal,
            )
            .await;
        });
    };

    signal::ctrl_c().await.expect("Failed to listen for event");
    info!("Received Ctrl-C, shutting down");
    cancel_tasks_and_wait_for_completion().await;
}
