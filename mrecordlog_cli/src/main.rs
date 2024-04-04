use std::path::{Path, PathBuf};

use mrecordlog::MultiRecordLog;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
enum Command {
    Summary {
        #[structopt(short)]
        wal_path: PathBuf,
    },
    Read {
        #[structopt(short)]
        wal_path: PathBuf,
        #[structopt(short)]
        queue_name: String,
    },
}

fn run_summary(path: &Path) -> anyhow::Result<()> {
    let multi_record_log = MultiRecordLog::open(path)?;
    let summary = multi_record_log.summary();
    for (queue, summary) in summary.queues {
        println!("{}", queue);
        println!("{summary:?}");
    }
    Ok(())
}

fn run_read_queue(path: &Path, queue_name: &str) -> anyhow::Result<()> {
    let multi_record_log = MultiRecordLog::open(path)?;
    for record in multi_record_log.range(queue_name, ..)? {
        let Ok(payload_str) = std::str::from_utf8(&record.payload) else {
            eprintln!("Payload is not utf8: {:?}", record.payload);
            continue;
        };
        println!("{payload_str}");
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let command = Command::from_args();
    match command {
        Command::Summary { wal_path } => {
            run_summary(&wal_path)?;
        }
        Command::Read {
            queue_name,
            wal_path,
        } => {
            run_read_queue(&wal_path, &queue_name)?;
        }
    }
    Ok(())
}
