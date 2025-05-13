use criterion::{Criterion, criterion_group, criterion_main};
use pprof::criterion::{Output, PProfProfiler};
use rdf2hdt::builder::{Options, build_hdt};
use std::time::Duration;
use tempfile::tempdir;

fn generate(c: &mut Criterion) {
    // ######### NOTE ###########
    // requires tests/resources/taxonomy-nodes.nq, download via 'make init'
    // ##########################
    let tmp_dir: tempfile::TempDir = tempdir().unwrap();
    let fname = format!("{}/rdf.hdt", tmp_dir.as_ref().display());
    let test_hdt = fname.as_str();
    let _ = std::fs::remove_file(test_hdt);
    let source_rdf = "tests/resources/taxonomy-nodes.nq".to_string();

    let mut group = c.benchmark_group("create HDT from NQ file");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1090));
    group.bench_function("hdt create", |b| {
        b.iter(|| build_hdt(vec![source_rdf.clone()], test_hdt, Options::default()).unwrap());
    });
    group.finish();

    let _ = tmp_dir.close();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Protobuf))
        .warm_up_time(Duration::from_millis(1));
    targets = generate
}
criterion_main!(benches);
