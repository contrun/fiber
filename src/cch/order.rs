use std::fmt;

use lightning_invoice::Bolt11Invoice;
use lnd_grpc_tonic_client::routerrpc;
use ractor::{Actor, ActorRef};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use thiserror::Error;
use tokio::select;

use crate::{
    cch::actor::BTC_PAYMENT_TIMEOUT_SECONDS,
    fiber::{
        serde_utils::{U128Hex, U64Hex},
        types::Hash256,
        NetworkActorMessage,
    },
    invoice::CkbInvoice,
    store::subscription::{InvoiceState, InvoiceUpdate, PaymentState, PaymentUpdate},
};

use super::{
    actor::{CchState, LndConnectionInfo},
    CchMessage,
};

#[derive(Debug)]
pub enum StateTransitionEvent {
    InvoiceUpdate(InvoiceState),
    PaymentUpdate(PaymentState),
}

pub struct InvalidStateTransition {
    pub previous_in_state: InvoiceState,
    pub previous_out_state: PaymentState,
    pub state_transition_event: StateTransitionEvent,
    pub error: CchStateError,
}

impl fmt::Debug for InvalidStateTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "An error occurred when trying to make state transition from ({:?}, {:?}) by event {:?}: {}",
            self.previous_in_state, self.previous_out_state, self.state_transition_event, self.error
        )
    }
}

#[derive(Error, Debug)]
pub enum CchOrderError {
    #[error("Invalid state transition: {0:?}")]
    InvalidStateTransition(InvalidStateTransition),
}

/// The status of a cross-chain hub order, will update as the order progresses.
#[derive(Debug, Copy, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CchOrderStatus {
    /// Order is created and the first half has not received complete payment yet.
    Pending = 0,
    /// HTLC in the first half is accepted.
    FirstHalfAccepted = 1,
    /// There's an outgoing payment in flight for the second half.
    SecondHalfInFlight = 2,
    /// The second half payment is succeeded.
    SecondHalfSucceeded = 3,
    /// The first half payment is succeeded.
    FirstHalfSucceeded = 4,
    /// Order is failed.
    Failed = 5,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CchOrder {
    // The payment hash of the order
    pub payment_hash: Hash256,
    pub payment_preimage: Option<Hash256>,
    // Seconds since epoch when the order is created
    #[serde_as(as = "U64Hex")]
    pub created_at: u64,
    // Seconds after timestamp that the order expires
    #[serde_as(as = "U64Hex")]
    pub expires_after: u64,

    #[serde_as(as = "U128Hex")]
    /// Amount required to pay in Satoshis via wrapped BTC, including the fee for the cross-chain hub
    pub amount_sats: u128,
    #[serde_as(as = "U128Hex")]
    pub fee_sats: u128,

    pub in_invoice: CchInvoice,
    pub out_invoice: CchInvoice,
    pub in_state: InvoiceState,
    pub out_state: PaymentState,
}

impl CchOrder {
    pub fn new(
        payment_hash: Hash256,
        created_at: u64,
        expires_after: u64,
        amount_sats: u128,
        fee_sats: u128,
        in_invoice: CchInvoice,
        out_invoice: CchInvoice,
    ) -> Self {
        Self {
            payment_hash,
            payment_preimage: None,
            created_at,
            expires_after,
            amount_sats,
            fee_sats,
            in_invoice,
            out_invoice,
            in_state: InvoiceState::Open,
            out_state: PaymentState::Created,
        }
    }

    pub fn status(&self) -> Result<CchOrderStatus, CchStateError> {
        let status = match (self.in_state, self.out_state) {
            (InvoiceState::Cancelled | InvoiceState::Expired, _) => CchOrderStatus::Failed,
            (_, PaymentState::Failed) => CchOrderStatus::Failed,
            (InvoiceState::Open, PaymentState::Created) => CchOrderStatus::Pending,
            (InvoiceState::Open, _) => {
                return Err(format!(
                    "The second payment has a state too new for a just open first payment: {:?}",
                    self.out_state
                ))
            }
            (
                InvoiceState::Received {
                    amount: _amount,
                    is_finished,
                },
                PaymentState::Created,
            ) => {
                if is_finished {
                    CchOrderStatus::FirstHalfAccepted
                } else {
                    CchOrderStatus::Pending
                }
            }
            (
                InvoiceState::Received {
                    amount: _amount,
                    is_finished,
                },
                PaymentState::Inflight,
            ) => {
                if !is_finished {
                    return Err("The second payment should be inflight when the first one is unfinished".to_string());
                }
                CchOrderStatus::SecondHalfInFlight
            }
            (InvoiceState::Received { .. }, PaymentState::Success { .. }) => {
                CchOrderStatus::SecondHalfSucceeded
            }
            (InvoiceState::Paid, PaymentState::Success { .. }) => {
                CchOrderStatus::SecondHalfSucceeded
            }
            (InvoiceState::Paid, _) => {
                return Err(format!(
                    "The first payment succeeded while the second payment has state (should have been succeeded or failed): {:?}",
                    self.out_state
                ))
            }
        };
        Ok(status)
    }

    // Try to save the invoice update and update the state of the order.
    // If the state transition is invalid, return an error, else the
    // old status and the new status will be returned.
    fn try_save_invoice_update(
        &mut self,
        invoice_update: CchInvoiceUpdate,
    ) -> Result<(CchOrderStatus, CchOrderStatus), CchOrderError> {
        let old_status = self.status().expect("status is valid");
        if self.in_invoice.is_fiber() != invoice_update.is_fiber {
            return Err(CchOrderError::InvalidStateTransition(
                InvalidStateTransition {
                    previous_in_state: self.in_state,
                    previous_out_state: self.out_state,
                    state_transition_event: StateTransitionEvent::InvoiceUpdate(
                        invoice_update.update.state,
                    ),
                    error: "The invoice update is for the wrong network".to_string(),
                },
            ));
        }
        self.in_state = invoice_update.update.state;
        match self.status() {
            Err(error) => Err(CchOrderError::InvalidStateTransition(
                InvalidStateTransition {
                    previous_in_state: self.in_state,
                    previous_out_state: self.out_state,
                    state_transition_event: StateTransitionEvent::InvoiceUpdate(
                        invoice_update.update.state,
                    ),
                    error,
                },
            )),
            Ok(new_status) => Ok((old_status, new_status)),
        }
    }

    async fn handle_invoice_update(
        &mut self,
        invoice_update: CchInvoiceUpdate,
        lnd_connection: LndConnectionInfo,
        network_actor: &ActorRef<NetworkActorMessage>,
    ) -> Result<(), CchOrderError> {
        tracing::trace!(invoice_update = ?invoice_update, "Cch received invoice update");
        let (old_status, new_status) = self.try_save_invoice_update(invoice_update)?;
        if old_status == new_status {
            return Ok(());
        }

        if let CchOrderStatus::FirstHalfAccepted = new_status {
            match &self.out_invoice {
                CchInvoice::Lightning(invoice) => {
                    let out_invoice = invoice.to_string();
                    let req = routerrpc::SendPaymentRequest {
                        payment_request: out_invoice,
                        timeout_seconds: BTC_PAYMENT_TIMEOUT_SECONDS,
                        ..Default::default()
                    };
                    tracing::debug!("[inbounding tlc] SendPaymentRequest: {:?}", req);

                    let mut client = lnd_connection.create_router_client().await?;
                    // TODO: set a fee
                    let mut stream = client.send_payment_v2(req).await?.into_inner();
                    // Wait for the first message then quit
                    select! {
                        payment_result_opt = stream.next() => {
                            tracing::debug!("[inbounding tlc] payment result: {:?}", payment_result_opt);
                            if let Some(Ok(payment)) = payment_result_opt {
                                self.handle_payment_update(state, CchPaymentUpdate {
                                    is_fiber: false,
                                    update: payment.try_into()?
                                }).await?;
                            }
                        }
                        _ = self.token.cancelled() => {
                            tracing::debug!("Cancellation received, shutting down cch service");
                            return Ok(());
                        }
                    }
                }
                CchInvoice::Fiber(fiber_invoice) => {
                    tracing::debug!(
                        payment_hash = ?invoice_update.hash,
                        "Sending payment to fiber node because we received payment from LND",
                    );
                    let message = |rpc_reply| -> NetworkActorMessage {
                        NetworkActorMessage::Command(NetworkActorCommand::SendPayment(
                            SendPaymentCommand {
                                invoice: Some(fiber_invoice.to_string()),
                                ..Default::default()
                            },
                            rpc_reply,
                        ))
                    };

                    // TODO: handle payment failure here.
                    let tlc_response = call!(self.network_actor, message)
                        .expect("call actor")
                        .map_err(|msg| anyhow!(msg))?;
                    // TODO: handle payment failure here.
                    if tlc_response.status == PaymentSessionStatus::Failed {
                        order.out_state = PaymentState::Failed;
                    }
                }
            }
        };

        Ok(())
    }

    async fn handle_payment_update(
        &self,
        state: &mut CchState,
        payment_update: CchPaymentUpdate,
    ) -> Result<()> {
        let CchPaymentUpdate {
            is_fiber,
            update: payment_update,
        } = payment_update;
        tracing::trace!(is_fiber = is_fiber, payment_update = ?payment_update, "Cch received payment update");
        let payment_hash = payment_update.hash;

        let mut order = match state.orders_db.get_cch_order(&payment_hash).await {
            Err(CchDbError::NotFound(_)) => return Ok(()),
            Err(err) => return Err(err.into()),
            Ok(order) => order,
        };

        order.out_state = payment_update.state;
        match (&order.in_state, &order.out_state) {
            (
                InvoiceState::Received {
                    is_finished: true, ..
                },
                PaymentState::Success { preimage },
            ) => {
                let preimage = *preimage;
                order.payment_preimage = Some(preimage);
                match &order.in_invoice {
                    CchInvoice::Lightning(_) => {
                        tracing::debug!(
                            hash = ?payment_hash,
                            "Settling lightning invoice on payment success",
                        );
                        let req = invoicesrpc::SettleInvoiceMsg {
                            preimage: preimage.as_ref().to_vec(),
                        };
                        let mut client = state.lnd_connection.create_invoices_client().await?;
                        let resp = client.settle_invoice(req).await?.into_inner();
                        tracing::debug!("[settled tlc] SettleInvoiceResp: {:?}", resp);
                        // TODO: settle_invoice response actually contains no useful information.
                        // We need to check the invoice state to see if it's settled.
                    }
                    CchInvoice::Fiber(_) => {
                        tracing::debug!(
                            hash = ?payment_hash,
                            "Settling fiber invoice on payment success",
                        );
                        let message = move |rpc_reply| -> NetworkActorMessage {
                            NetworkActorMessage::Command(NetworkActorCommand::SettleInvoice(
                                payment_hash,
                                preimage,
                                rpc_reply,
                            ))
                        };

                        call!(&self.network_actor, message)
                            .expect("call actor")
                            .map_err(|msg| anyhow!(msg))?;
                    }
                }
            }
            (_, PaymentState::Failed) => {
                // TODO: handle payment failure
            }
            _ => {
                // TODO: handle other states
            }
        }

        state.orders_db.update_cch_order(order).await?;

        Ok(())
    }
}

pub type CchStateError = String;

pub type FiberInvoiceUpdate = InvoiceUpdate;
pub type FiberPaymentUpdate = PaymentUpdate;
pub type LightningInvoiceUpdate = InvoiceUpdate;
pub type LightningPaymentUpdate = PaymentUpdate;

#[derive(Debug)]
pub struct CchInvoiceUpdate {
    pub is_fiber: bool,
    pub update: InvoiceUpdate,
}

pub struct CchPaymentUpdate {
    pub is_fiber: bool,
    pub update: PaymentUpdate,
}

/// A cross-chain hub invoice, which can be either a lightning network invoice or a fiber network invoice.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CchInvoice {
    /// A lightning network invoice
    Lightning(#[serde_as(as = "DisplayFromStr")] Bolt11Invoice),
    /// A fiber network invoice
    Fiber(#[serde_as(as = "DisplayFromStr")] CkbInvoice),
}

impl CchInvoice {
    pub fn is_fiber(&self) -> bool {
        matches!(self, CchInvoice::Fiber(_))
    }
}

pub struct CchOrderActor {
    pub cch_actor: ActorRef<CchMessage>,
}
