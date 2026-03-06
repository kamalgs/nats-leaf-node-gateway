// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use nats_server::sub_list::{subject_matches, SubList, Subscription};

fn bench_subject_matches(c: &mut Criterion) {
    let mut group = c.benchmark_group("subject_matches");

    group.bench_function("exact", |b| {
        b.iter(|| subject_matches("foo.bar.baz", "foo.bar.baz"));
    });

    group.bench_function("star_wildcard", |b| {
        b.iter(|| subject_matches("foo.*.baz", "foo.bar.baz"));
    });

    group.bench_function("gt_wildcard", |b| {
        b.iter(|| subject_matches("foo.>", "foo.bar.baz.qux"));
    });

    group.bench_function("no_match", |b| {
        b.iter(|| subject_matches("foo.bar.baz", "foo.bar.qux"));
    });

    group.finish();
}

fn bench_sublist_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("sublist_match");

    for count in [10, 100, 1000] {
        let mut sl = SubList::new();
        for i in 0..count {
            sl.insert(Subscription {
                conn_id: i,
                sid: 1,
                subject: format!("test.subject.{i}"),
                queue: None,
            });
        }
        // Add a wildcard that matches
        sl.insert(Subscription {
            conn_id: 9999,
            sid: 1,
            subject: "test.subject.*".to_string(),
            queue: None,
        });

        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("{count}_subs"), |b| {
            b.iter(|| sl.match_subject("test.subject.42"));
        });
    }

    group.finish();
}

fn bench_publish_local(c: &mut Criterion) {
    use std::sync::Arc;
    use tokio::runtime::Runtime;
    use tokio::sync::RwLock;

    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("publish_local");
    group.throughput(Throughput::Elements(1));

    // Simulate the publish hot path: lock subs, match, lock conns, send
    let subs = Arc::new(RwLock::new({
        let mut sl = SubList::new();
        sl.insert(Subscription {
            conn_id: 1,
            sid: 1,
            subject: "test.>".to_string(),
            queue: None,
        });
        sl
    }));

    group.bench_function("lock_and_match", |b| {
        b.to_async(&rt).iter(|| {
            let subs = subs.clone();
            async move {
                let subs = subs.read().await;
                let _matches = subs.match_subject("test.subject.foo");
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_subject_matches, bench_sublist_match, bench_publish_local);
criterion_main!(benches);
