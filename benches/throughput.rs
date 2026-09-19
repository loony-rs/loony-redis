use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use protocol::{parse_frame, serialize_frame, write_frame_into, Frame};
use storage::Store;

// ── Protocol parsing ───────────────────────────────────────────────────────

fn bench_parse(c: &mut Criterion) {
    let mut g = c.benchmark_group("parse");

    let set_cmd  = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let get_cmd  = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    let ping_cmd = b"*1\r\n$4\r\nPING\r\n";

    g.throughput(Throughput::Elements(1));

    g.bench_function("SET", |b| {
        b.iter(|| parse_frame(std::hint::black_box(set_cmd)).unwrap().unwrap())
    });
    g.bench_function("GET", |b| {
        b.iter(|| parse_frame(std::hint::black_box(get_cmd)).unwrap().unwrap())
    });
    g.bench_function("PING", |b| {
        b.iter(|| parse_frame(std::hint::black_box(ping_cmd)).unwrap().unwrap())
    });

    g.finish();
}

// ── Protocol serialisation ─────────────────────────────────────────────────

fn bench_serialize(c: &mut Criterion) {
    let mut g = c.benchmark_group("serialize");

    let ok_frame      = Frame::ok();
    let bulk_frame    = Frame::bulk_str("hello world");
    let int_frame     = Frame::Integer(42);
    let null_frame    = Frame::null_bulk();
    let array_frame   = Frame::array(vec![
        Frame::bulk_str("SET"),
        Frame::bulk_str("key"),
        Frame::bulk_str("value"),
    ]);

    g.bench_function("ok",        |b| b.iter(|| serialize_frame(std::hint::black_box(&ok_frame))));
    g.bench_function("bulk",      |b| b.iter(|| serialize_frame(std::hint::black_box(&bulk_frame))));
    g.bench_function("integer",   |b| b.iter(|| serialize_frame(std::hint::black_box(&int_frame))));
    g.bench_function("null_bulk", |b| b.iter(|| serialize_frame(std::hint::black_box(&null_frame))));
    g.bench_function("array_3",   |b| b.iter(|| serialize_frame(std::hint::black_box(&array_frame))));

    g.finish();
}

// ── Pipelined serialisation (write_frame_into) ─────────────────────────────

fn bench_pipeline_serialize(c: &mut Criterion) {
    let mut g = c.benchmark_group("pipeline_serialize");

    // Simulate 32 pipelined SET responses — this is the redis-benchmark default.
    let ok_frame = Frame::ok();
    let pipeline_depth = 32usize;

    g.throughput(Throughput::Elements(pipeline_depth as u64));
    g.bench_function(
        BenchmarkId::new("batch", pipeline_depth),
        |b| {
            let mut buf = BytesMut::with_capacity(pipeline_depth * 8);
            b.iter(|| {
                buf.clear();
                for _ in 0..pipeline_depth {
                    write_frame_into(&mut buf, std::hint::black_box(&ok_frame));
                }
                std::hint::black_box(&buf);
            })
        },
    );

    g.finish();
}

// ── Storage layer (raw DashMap ops) ───────────────────────────────────────

fn bench_store(c: &mut Criterion) {
    let mut g = c.benchmark_group("store");

    let store = Store::new();
    let key   = "bench_key".to_string();
    let val   = Bytes::from_static(b"bench_value");

    // Seed one key for GET benchmarks.
    store.set(key.clone(), storage::Value::String(val.clone()), None);

    g.throughput(Throughput::Elements(1));

    g.bench_function("set", |b| {
        b.iter(|| {
            store.set(
                std::hint::black_box(key.clone()),
                storage::Value::String(std::hint::black_box(val.clone())),
                None,
            )
        })
    });

    g.bench_function("get_hit", |b| {
        b.iter(|| store.get(std::hint::black_box(&key)))
    });

    g.bench_function("get_miss", |b| {
        b.iter(|| store.get(std::hint::black_box("no_such_key")))
    });

    g.finish();
}

// ── Round-trip: parse → store SET → serialize OK ──────────────────────────

fn bench_roundtrip_set(c: &mut Criterion) {
    let raw_set = b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
    let store   = Store::new();
    let ok      = Frame::ok();

    c.bench_function("roundtrip_set", |b| {
        b.iter(|| {
            let (frame, _) = parse_frame(std::hint::black_box(raw_set))
                .unwrap()
                .unwrap();
            // Simulate the SET command's store write.
            if let Frame::Array(Some(ref args)) = frame {
                if let (Some(Frame::Bulk(Some(k))), Some(Frame::Bulk(Some(v)))) =
                    (args.get(1), args.get(2))
                {
                    store.set(
                        String::from_utf8_lossy(k).into_owned(),
                        storage::Value::String(v.clone()),
                        None,
                    );
                }
            }
            serialize_frame(std::hint::black_box(&ok))
        })
    });
}

criterion_group!(
    benches,
    bench_parse,
    bench_serialize,
    bench_pipeline_serialize,
    bench_store,
    bench_roundtrip_set,
);
criterion_main!(benches);
