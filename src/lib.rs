mod config;
pub use config::Config;

pub mod ckb_chain;

pub mod ldk;
pub use ldk::{start_ldk, LdkConfig};
pub mod ckb;
pub use ckb::{start_ckb, CkbConfig, NetworkServiceEvent};
pub mod cch;
pub use cch::{start_cch, CchConfig};

pub mod rpc;
pub use rpc::{start_rpc, RpcConfig, RpcError};
pub mod invoice;
pub mod store;

mod errors;
pub use errors::{Error, Result};

pub mod actors;

pub mod tasks;

pub mod macros {
    #[macro_export]
    macro_rules! unwrap_or_return {
        ($expr:expr, $msg:expr) => {
            match $expr {
                Ok(val) => val,
                Err(err) => {
                    error!("{}: {:?}", $msg, err);
                    return;
                }
            }
        };
        ($expr:expr) => {
            match $expr {
                Ok(val) => val,
                Err(err) => {
                    error!("{:?}", err);
                    return;
                }
            }
        };
    }
}
