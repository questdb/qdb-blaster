use std::{collections::HashMap, time::Duration};

use serde::Deserialize;

use crate::col::{ColName, ColType};

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub debug: bool,
    pub database: Connection,
    pub tables: HashMap<String, Table>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Connection {
    pub ilp: String,
    pub pgsql: String,
}

#[derive(Debug, Deserialize)]
pub struct Table {
    pub schema: Vec<(ColName, ColType)>,
    pub designated_ts: String,
    pub send: SendSettings,
}

#[derive(Debug, Deserialize)]
pub struct SendSettings {
    #[serde(with = "humantime_serde_vec")]
    pub batch_pause: (Duration, Duration), // from ["1s", "5s"]

    pub batch_size: (u32, u32), // from [min, max]

    pub parallel_senders: u16,
    pub tot_rows: u64,
    pub batches_connection_keepalive: u16,
}

mod humantime_serde_vec {
    use humantime::parse_duration;
    use serde::{self, Deserialize, Deserializer};
    use std::time::Duration;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<(Duration, Duration), D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw: [String; 2] = Deserialize::deserialize(deserializer)?;
        let parse = |s: &str| parse_duration(s).map_err(serde::de::Error::custom);
        Ok((parse(&raw[0])?, parse(&raw[1])?))
    }
}
