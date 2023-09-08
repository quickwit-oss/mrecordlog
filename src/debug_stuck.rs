use std::error::Error;
use std::path::Path;

use mrecordlog::MultiRecordLog;
use tokio::io::{stdin, AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let buf_reader = BufReader::new(stdin());
    let mut lines = buf_reader.lines();
    let path = std::env::args().skip(1).next().expect("no path provided");
    let mut multi_record_log = MultiRecordLog::open(Path::new(&path)).await?;
    println!(
        "{:<32}: {:<24} {}",
        "queue name", "file name", "number of documents"
    );
    for (queue, file, count) in multi_record_log.debug_info() {
        println!("{queue:<32}: {file:<24} {count}");
    }
    Ok(())
}
