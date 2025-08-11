use serde::Deserialize;

pub type ColName = String;

#[derive(Debug, Deserialize, Clone)]
pub enum ColType {
    Symbol,
    Timestamp,
    Long,
    Double,
}
