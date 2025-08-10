use serde::Deserialize;

pub type ColName = String;

#[derive(Debug, Deserialize)]
pub enum ColType {
    Symbol,
    Timestamp,
    Long,
    Double,
}
