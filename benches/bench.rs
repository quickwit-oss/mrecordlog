use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use mrecordlog::MultiRecordLog;

fn bench_single_size(size: usize, count: usize, loop_count: usize) {
    let tempdir = tempfile::tempdir().unwrap();
    let mut record_log = MultiRecordLog::open(tempdir.path()).unwrap();
    record_log.create_queue("q1").unwrap();

    let record = vec![0; size];

    for _ in 0..loop_count {
        record_log
            .append_records("q1", None, std::iter::repeat(&record[..]).take(count))
            .unwrap();
    }
}

fn insert_throughput(c: &mut Criterion) {
    let record_sizes: [usize; 2] = [1 << 8, 1 << 14];
    let record_counts: [usize; 3] = [1, 16, 256];
    let bytes_written: usize = 1 << 22;

    let mut group = c.benchmark_group("insert speed");
    group.throughput(criterion::Throughput::Bytes(bytes_written as _));

    for record_size in record_sizes {
        for record_count in record_counts {
            if record_size * record_count > bytes_written {
                continue;
            }
            let loop_count = bytes_written / record_count / record_size;

            group.bench_with_input(
                BenchmarkId::new(
                    "bench_append_throughput",
                    format!("size={},count={}", record_size, record_count),
                ),
                &(record_size, record_count, loop_count),
                |b, (record_size, record_count, loop_count)| {
                    b.iter(|| bench_single_size(*record_size, *record_count, *loop_count));
                },
            );
        }
    }
}

criterion_group!(benches, insert_throughput);
criterion_main!(benches);
