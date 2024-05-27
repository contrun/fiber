use std::{fs, path::PathBuf};

use ckb_sdk::NetworkType;
use clap::ValueEnum;
use clap_serde_derive::{
    clap::{self},
    ClapSerde,
};
use serde::Deserialize;

use crate::Result;

// See comment in `LdkConfig` for why do we need to specify both name and long,
// and prefix them with `ckb-`/`CKB_`.
#[derive(ClapSerde, Debug, Clone)]
pub struct CkbConfig {
    /// ckb base directory
    #[arg(
        name = "CKB_BASE_DIR",
        long = "ckb-base-dir",
        env,
        help = "base directory for ckb [default: $BASE_DIR/ckb]"
    )]
    pub(crate) base_dir: Option<PathBuf>,

    /// listening port for ckb payment channel network
    #[arg(name = "CKB_LISTENING_PORT", long = "ckb-listening-port", env)]
    pub(crate) listening_port: u16,

    /// addresses to be announced to payment channel network (separated by `,`)
    #[arg(name = "CKB_ANNOUNCED_LISTEN_ADDRS", long = "ckb-announced-listen-addrs", env, value_parser, num_args = 0.., value_delimiter = ',')]
    pub(crate) announced_listen_addrs: Vec<String>,

    /// bootstrap node addresses to be connected at startup (separated by `,`)
    #[arg(name = "CKB_BOOTNODES_ADDRS", long = "ckb-bootnodes-addrs", env, value_parser, num_args = 0.., value_delimiter = ',')]
    pub bootnode_addrs: Vec<String>,

    /// node name to be announced to lightning network
    #[arg(
        name = "CKB_ANNOUNCED_NODE_NAME",
        long = "ckb-announced-node-name",
        env
    )]
    pub(crate) announced_node_name: String,

    /// name of the network to use (can be any of `mocknet`/`mainnet`/`testnet`/`staging`/`dev`)
    #[arg(name = "CKB_NETWORK", long = "ckb-network", env)]
    pub network: Option<CkbNetwork>,
}

// Basically ckb_sdk::types::NetworkType. But we added a `Mocknet` variant.
// And we can't use `ckb_sdk::types::NetworkType` directly because it is not `ValueEnum`.
#[derive(Debug, Clone, Copy, ValueEnum, Deserialize, PartialEq, Eq)]
pub enum CkbNetwork {
    Mocknet,
    Mainnet,
    Testnet,
    Staging,
    Dev,
}

impl From<CkbNetwork> for Option<NetworkType> {
    fn from(network: CkbNetwork) -> Self {
        match network {
            CkbNetwork::Mocknet => None,
            CkbNetwork::Mainnet => Some(NetworkType::Mainnet),
            CkbNetwork::Testnet => Some(NetworkType::Testnet),
            CkbNetwork::Staging => Some(NetworkType::Staging),
            CkbNetwork::Dev => Some(NetworkType::Dev),
        }
    }
}

impl CkbConfig {
    pub fn base_dir(&self) -> &PathBuf {
        self.base_dir.as_ref().expect("have set base dir")
    }

    pub fn create_base_dir(&self) -> Result<()> {
        if !self.base_dir().exists() {
            fs::create_dir_all(self.base_dir()).map_err(Into::into)
        } else {
            Ok(())
        }
    }

    pub fn read_or_generate_secret_key(&self) -> Result<super::KeyPair> {
        self.create_base_dir()?;
        super::key::KeyPair::read_or_generate(&self.base_dir().join("sk")).map_err(Into::into)
    }

    pub fn store_path(&self) -> PathBuf {
        let path = self.base_dir().join("store");
        if !path.exists() {
            fs::create_dir_all(&path).expect("create store directory");
        }
        path
    }
}
