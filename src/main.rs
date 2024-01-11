use std::error::Error;
use std::path::Path;

use mrecordlog::MultiRecordLog;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let multi_record_log = MultiRecordLog::open(Path::new(".")).await?;
    for queue in multi_record_log.list_queues() {
        println!("queue {queue}");
        let mut range = multi_record_log.range(queue, ..).unwrap();
        let first = range.next().map(|(position, _)| position);
        let last = range.last().map(|(position, _)| position);
        println!("{first:?}..{last:?}");
    }
    Ok(())
}
