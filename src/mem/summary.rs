use std::collections::BTreeMap;

#[derive(Default, Debug)]
pub struct QueueSummary {
    pub start: u64,
    pub end: Option<u64>,
    pub file_number: Option<u64>,
}

#[derive(Default)]
pub struct QueuesSummary {
    pub queues: BTreeMap<String, QueueSummary>,
}
