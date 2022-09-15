use std::error::Error;
use std::path::Path;

use mrecordlog::MultiRecordLog;
use tokio::io::{stdin, AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let buf_reader = BufReader::new(stdin());
    let mut lines = buf_reader.lines();
    let mut multi_record_log = MultiRecordLog::open(Path::new(".")).await?;
    if !multi_record_log.queue_exists("q") {
        multi_record_log.create_queue("q").await?;
    }
    for (_line, payload) in multi_record_log.range("q", ..).unwrap() {
        let line_utf8 = std::str::from_utf8(payload).unwrap();
        println!("BEFORE {line_utf8}");
    }
    while let Some(line) = lines.next_line().await? {
        multi_record_log
            .append_record("q", None, line.as_bytes())
            .await?;
    }
    Ok(())
}
