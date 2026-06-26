// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! Before/after benchmark for the non-N-Triples conversion path.
//!
//! `from_triples (streaming)` is the current path: parse with oxrdfio and stream
//! triples straight into the HDT dictionary. `nt_tempfile (baseline)` reproduces
//! the previous path: parse, serialize to an intermediate N-Triples temp file,
//! then rebuild with `hdt`'s parallel N-Triples reader. Running both in one
//! `cargo bench` shows whether `from_triples`' single-threaded interning costs
//! anything versus the old serialize + parallel-reparse round trip.
//!
//! Input defaults to `tests/resources/taxonomy-nodes.nq` (≈23.5M triples, 4.7 GB;
//! fetch via `make init`). Point `RDF2HDT_BENCH_INPUT` at a smaller `.nq`/`.ttl`
//! file for a quick A/B; the benchmark is skipped if the input is absent.

use criterion::{Criterion, criterion_group, criterion_main};
use oxrdf::TripleRef;
use oxrdfio::{RdfFormat, RdfParser, RdfSerializer};
use pprof::criterion::{Output, PProfProfiler};
use rdf2hdt::builder::build_hdt;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::time::Duration;
use tempfile::tempdir;

const IO_BUF: usize = 1 << 20;

fn format_for(source: &Path) -> RdfFormat {
    source
        .extension()
        .and_then(|e| e.to_str())
        .and_then(RdfFormat::from_extension)
        .unwrap_or(RdfFormat::NQuads)
}

/// Baseline: the pre-change conversion path. Parse `source` with oxrdfio,
/// serialize every triple to an intermediate N-Triples temp file, then build the
/// HDT with `hdt`'s parallel N-Triples reader and write it out. Kept here (not in
/// the library) purely as the "before" reference for the A/B above.
fn build_via_nt_tempfile(source: &Path, out_hdt: &Path) {
    let tmp = tempfile::Builder::new().suffix(".nt").tempfile().unwrap();
    {
        let mut writer = BufWriter::with_capacity(IO_BUF, tmp.reopen().unwrap());
        let reader = BufReader::with_capacity(IO_BUF, std::fs::File::open(source).unwrap());
        let base = url::Url::from_file_path(std::fs::canonicalize(source).unwrap()).unwrap();
        let mut serializer =
            RdfSerializer::from_format(RdfFormat::NTriples).for_writer(writer.by_ref());
        let quads = RdfParser::from_format(format_for(source))
            .with_base_iri(base.as_str())
            .unwrap()
            .for_reader(reader);
        for q in quads {
            let q = q.unwrap();
            serializer
                .serialize_triple(TripleRef {
                    subject: q.subject.as_ref(),
                    predicate: q.predicate.as_ref(),
                    object: q.object.as_ref(),
                })
                .unwrap();
        }
        serializer.finish().unwrap();
        writer.flush().unwrap();
    }
    let hdt = hdt::Hdt::read_nt(tmp.path()).unwrap();
    let mut out = BufWriter::with_capacity(IO_BUF, std::fs::File::create(out_hdt).unwrap());
    hdt.write(&mut out).unwrap();
    out.flush().unwrap();
}

fn generate(c: &mut Criterion) {
    let source = std::env::var("RDF2HDT_BENCH_INPUT")
        .unwrap_or_else(|_| "tests/resources/taxonomy-nodes.nq".to_owned());
    let source = Path::new(&source);
    if !source.exists() {
        eprintln!(
            "skipping HDT benchmark: input {} not found \
             (run `make init`, or set RDF2HDT_BENCH_INPUT to an existing .nq/.ttl file)",
            source.display()
        );
        return;
    }

    let tmp_dir = tempdir().unwrap();
    let new_hdt = tmp_dir.path().join("new.hdt");
    let old_hdt = tmp_dir.path().join("old.hdt");
    let source_arg = [source.to_str().expect("non-UTF-8 input path")];

    let mut group = c.benchmark_group("RDF to HDT");
    group.sample_size(10);
    // Target only; criterion still takes the 10-sample minimum even if a single
    // build of the full taxonomy file overruns it. Shrink the input via
    // RDF2HDT_BENCH_INPUT for a faster run.
    group.measurement_time(Duration::from_secs(120));

    group.bench_function("from_triples (streaming)", |b| {
        b.iter(|| build_hdt(&source_arg, &new_hdt).unwrap());
    });
    group.bench_function("nt_tempfile (baseline)", |b| {
        b.iter(|| build_via_nt_tempfile(source, &old_hdt));
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
