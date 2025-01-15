use fiber::{
    fiber::config::{
        DEFAULT_TLC_EXPIRY_DELTA, DEFAULT_TLC_FEE_PROPORTIONAL_MILLIONTHS, DEFAULT_TLC_MAX_VALUE,
        DEFAULT_TLC_MIN_VALUE,
    },
    now_timestamp_as_millis_u64,
    store::migration::Migration,
    Error,
};
use indicatif::ProgressBar;
use rocksdb::ops::Iterate;
use rocksdb::ops::Put;
use rocksdb::DB;
use std::sync::Arc;
use tracing::info;

const MIGRATION_DB_VERSION: &str = "20250115051223";

pub use fiber_v021::fiber::channel::ChannelActorState as ChannelActorStateOld;
pub use fiber_v022::fiber::channel::ChannelActorState as ChannelActorStateNew;
pub use fiber_v022::fiber::channel::ChannelTlcInfo as ChannelTlcInfoNew;

use crate::util::convert;

pub struct MigrationObj {
    version: String,
}

impl MigrationObj {
    pub fn new() -> Self {
        Self {
            version: MIGRATION_DB_VERSION.to_string(),
        }
    }
}

impl Migration for MigrationObj {
    fn migrate(
        &self,
        db: Arc<DB>,
        _pb: Arc<dyn Fn(u64) -> ProgressBar + Send + Sync>,
    ) -> Result<Arc<DB>, Error> {
        info!(
            "MigrationObj::migrate to {} ...........",
            MIGRATION_DB_VERSION
        );

        const CHANNEL_ACTOR_STATE_PREFIX: u8 = 0;
        let prefix = vec![CHANNEL_ACTOR_STATE_PREFIX];

        for (k, v) in db
            .prefix_iterator(prefix.as_slice())
            .take_while(move |(col_key, _)| col_key.starts_with(prefix.as_slice()))
        {
            let old_channel_state: ChannelActorStateOld =
                bincode::deserialize(&v).expect("deserialize to old channel state");

            let mut new_channel_state: ChannelActorStateNew = convert(&old_channel_state);
            new_channel_state.local_tlc_info = {
                let now_timestamp = now_timestamp_as_millis_u64();
                match old_channel_state.public_channel_info {
                    Some(ref info) => ChannelTlcInfoNew {
                        timestamp: now_timestamp,
                        enabled: info.enabled,
                        tlc_fee_proportional_millionths: info.tlc_fee_proportional_millionths,
                        tlc_expiry_delta: info.tlc_expiry_delta,
                        tlc_minimum_value: info.tlc_min_value,
                        tlc_maximum_value: DEFAULT_TLC_MAX_VALUE,
                    },
                    None => ChannelTlcInfoNew {
                        timestamp: now_timestamp,
                        enabled: true,
                        tlc_fee_proportional_millionths: DEFAULT_TLC_FEE_PROPORTIONAL_MILLIONTHS,
                        tlc_expiry_delta: DEFAULT_TLC_EXPIRY_DELTA,
                        tlc_minimum_value: DEFAULT_TLC_MIN_VALUE,
                        tlc_maximum_value: DEFAULT_TLC_MAX_VALUE,
                    },
                }
            };
            new_channel_state.remote_tlc_info = None;

            let new_channel_state_bytes =
                bincode::serialize(&new_channel_state).expect("serialize to new channel state");

            db.put(k, new_channel_state_bytes)
                .expect("save new channel state");
        }
        Ok(db)
    }

    fn version(&self) -> &str {
        &self.version
    }
}
