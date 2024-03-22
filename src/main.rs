use std::error::Error;
use std::path::Path;

use mrecordlog::MultiRecordLog;

fn main() -> Result<(), Box<dyn Error>> {
    let multi_record_log = MultiRecordLog::open(Path::new("."))?;
    let summary = multi_record_log.summary();
    for (queue, summary) in summary.queues {
        println!("{}", queue);
        println!("{summary:?}");
    }
    Ok(())
}
