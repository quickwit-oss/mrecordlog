use std::collections::BTreeMap;

use serde::Serialize;

#[derive(Default, Serialize, Debug)]
pub struct QueueSummary {
    pub start: u64,
    pub end: Option<u64>,
    pub file_number: Option<u64>,
}

#[derive(Default, Serialize)]
pub struct QueuesSummary {
    pub queues: BTreeMap<String, QueueSummary>,
}
