// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::builder::Error;
use bzip2::bufread::MultiBzDecoder;
use flate2::bufread::MultiGzDecoder;
use log::{debug, error, warn};
use oxrdfio::{RdfFormat, RdfParseError, RdfParser, ReaderQuadParser};
use std::fs::File;
use std::io::Write;
use std::{
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
};
use url::Url;

const IO_BUF: usize = 1 << 20;

fn open_rdf_reader(file: &Path) -> io::Result<Box<dyn Read>> {
    let fp = File::open(file)?;
    let buffered = BufReader::with_capacity(IO_BUF, fp);
    match file.extension().and_then(|e| e.to_str()) {
        Some(e) if e.eq_ignore_ascii_case("gz") => Ok(Box::new(BufReader::with_capacity(
            IO_BUF,
            MultiGzDecoder::new(buffered),
        ))),
        Some(e) if e.eq_ignore_ascii_case("bz2") => Ok(Box::new(BufReader::with_capacity(
            IO_BUF,
            MultiBzDecoder::new(buffered),
        ))),
        _ => Ok(Box::new(buffered)),
    }
}

fn rdf_format_from_path(file: &Path) -> io::Result<RdfFormat> {
    let format_path: PathBuf = match file.extension().and_then(|e| e.to_str()) {
        Some(e) if e.eq_ignore_ascii_case("gz") || e.eq_ignore_ascii_case("bz2") => {
            file.with_extension("")
        }
        _ => file.to_path_buf(),
    };

    let ext = format_path
        .extension()
        .and_then(|e| e.to_str())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("file {} has no usable extension", file.display()),
            )
        })?;
    if ext.eq_ignore_ascii_case("owl") {
        return Ok(RdfFormat::RdfXml);
    }
    RdfFormat::from_extension(ext).ok_or_else(|| {
        error!("unrecognized file extension for {}", file.display());
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unrecognized file extension for {}", file.display()),
        )
    })
}

/// Parse one or more non-N-Triples RDF files with oxrdfio and build an HDT
/// directly from the parsed triples, streaming them into the dictionary instead
/// of serializing to an intermediate N-Triples temp file and reparsing it.
///
/// The triple stream is lazy: [`hdt::Hdt::from_triples`] interns each term and
/// drops the source `[String; 3]` before pulling the next triple, so peak memory
/// holds the HDT dictionary/triples but not a second full copy of the terms.
///
/// `from_triples` takes `Item = [S; 3]`, not `Result`, so the first parse error
/// is stashed by the adapter (which then ends the stream) and returned here once
/// `from_triples` has unwound the iterator. This preserves oxrdfio's detailed
/// syntax diagnostics — note `hdt`'s own N-Triples reader would instead panic on
/// malformed input.
pub(crate) fn convert_to_hdt<P: AsRef<Path>>(file_paths: &[P]) -> Result<hdt::Hdt, Error> {
    // Header dataset IRI: a deterministic file:// URI from the first input
    // (the old path labelled it with a random temp-file path).
    let dataset_iri = match file_paths.first() {
        Some(f) => file_base_iri(f.as_ref())?.to_string(),
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no files provided to convert",
            )
            .into());
        }
    };

    let timer = std::time::Instant::now();
    let mut first_err: Option<Error> = None;
    let triples = file_paths
        .iter()
        .flat_map(|f| file_triples(f.as_ref()))
        .map_while(|r| match r {
            Ok(triple) => Some(triple),
            Err(e) => {
                first_err = Some(e);
                None
            }
        });

    let result = hdt::Hdt::from_triples(triples, &dataset_iri);
    // `triples` — and with it the &mut borrow of `first_err` held by the adapter
    // — is fully consumed and dropped inside `from_triples`, so the stash is
    // readable again here. A stashed parse error takes priority over any error
    // `from_triples` may report from the truncated stream.
    if let Some(e) = first_err {
        return Err(e);
    }
    let hdt = result?;
    debug!("RDF parse + HDT build time: {:?}", timer.elapsed());
    Ok(hdt)
}

/// Lazily yield every triple in `file` as HDT dictionary strings. Per-file setup
/// failures (open / format detection / base IRI) surface as a single terminal
/// `Err` item, so the caller stops the whole stream on the first error just as
/// the old sequential conversion did.
fn file_triples(file: &Path) -> Box<dyn Iterator<Item = Result<[String; 3], Error>>> {
    let parser = match open_parser(file) {
        Ok(p) => p,
        Err(e) => return Box::new(std::iter::once(Err(e))),
    };
    debug!("streaming {} into HDT dictionary", file.display());

    let file = file.to_path_buf();
    let mut warned = false;
    Box::new(parser.map(move |q| {
        let q = q.map_err(|e| parse_error(&file, e))?;
        if !warned && q.graph_name != oxrdf::GraphName::DefaultGraph {
            warned = true;
            warn!(
                "HDT does not support named graphs, merging triples for {}",
                file.display()
            );
        }
        Ok([
            term_string(q.subject.into()),
            q.predicate.into_string(),
            term_string(q.object),
        ])
    }))
}

/// Open `file` (transparently decompressing `.gz`/`.bz2`) and configure an
/// oxrdfio parser for its syntax, resolving relative IRIs against the file's
/// `file://` URI.
fn open_parser(file: &Path) -> Result<ReaderQuadParser<Box<dyn Read>>, Error> {
    let reader = open_rdf_reader(file).map_err(|e| {
        error!("Error opening file {}: {e:?}", file.display());
        e
    })?;
    let rdf_format = rdf_format_from_path(file)?;
    let base_iri = file_base_iri(file)?;
    Ok(RdfParser::from_format(rdf_format)
        .with_base_iri(base_iri.as_str())
        .map_err(|e| Error::Parse(Box::new(e)))?
        .for_reader(reader))
}

/// The `file://` base IRI used to resolve relative IRIs while parsing `file`.
fn file_base_iri(file: &Path) -> Result<Url, Error> {
    let abs_path = std::fs::canonicalize(file)?;
    Url::from_file_path(&abs_path).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("cannot build file:// URI for {}", file.display()),
        )
        .into()
    })
}

/// Translate an oxrdfio parse error for `file` into our error type, logging it
/// the same way the previous N-Triples conversion path did.
fn parse_error(file: &Path, e: RdfParseError) -> Error {
    match e {
        RdfParseError::Io(v) => {
            error!("Error reading file {}: {v}", file.display());
            Error::Io(v)
        }
        RdfParseError::Syntax(syn_err) => {
            error!("syntax error for RDF file {}: {syn_err}", file.display());
            Error::Parse(Box::new(syn_err))
        }
    }
}

/// Render an oxrdf term in the HDT dictionary string form expected by
/// [`hdt::Hdt::from_triples`]: bare IRIs (no angle brackets), literals keeping
/// their quotes / language tag / `^^<datatype>`, and blank nodes as `_:id`. This
/// is exactly the form `hdt`'s N-Triples reader derives internally (and that
/// `Hdt::triples_all` returns), so the resulting HDT is identical to the old
/// serialize-and-reparse path.
fn term_string(t: oxrdf::Term) -> String {
    match t {
        oxrdf::Term::NamedNode(n) => n.into_string(),
        oxrdf::Term::BlankNode(b) => b.to_string(),
        oxrdf::Term::Literal(l) => l.to_string(),
    }
}

pub(crate) fn concat_nt<P: AsRef<Path>>(
    file_paths: &[P],
    mut output_file: std::fs::File,
) -> Result<(), Error> {
    for file in file_paths {
        let file = file.as_ref();
        let mut source = std::fs::File::open(file).map_err(|e| {
            error!("Error opening file {}: {e:?}", file.display());
            e
        })?;
        io::copy(&mut source, &mut output_file)?;
        output_file.write_all(b"\n")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::collections::HashSet;

    fn count_via_convert<P: AsRef<Path>>(input: P) -> usize {
        convert_to_hdt(&[input])
            .expect("convert")
            .triples_all()
            .count()
    }

    /// apple.ttl has 9 distinct triples. Build it through the streaming path and
    /// confirm both the count and that terms land in HDT dictionary-string form
    /// (bare IRIs, typed literal keeping its quotes and `^^<datatype>`).
    #[test]
    fn ttl_to_hdt() -> Result<(), Error> {
        let hdt = convert_to_hdt(&["tests/resources/apple.ttl"])?;
        let triples: HashSet<[String; 3]> = hdt
            .triples_all()
            .map(|[s, p, o]| [s.to_string(), p.to_string(), o.to_string()])
            .collect();
        assert_eq!(triples.len(), 9);
        assert!(triples.contains(&[
            "http://example.org/apple#Apple".to_owned(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_owned(),
            "http://example.org/apple#Fruit".to_owned(),
        ]));
        assert!(triples.contains(&[
            "http://example.org/apple#Apple".to_owned(),
            "http://example.org/apple#isOrganic".to_owned(),
            "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>".to_owned(),
        ]));
        Ok(())
    }

    #[test]
    fn gzipped_ttl_input() -> Result<(), Error> {
        let tmp = tempfile::tempdir()?;
        let gz_path = tmp.path().join("apple.ttl.gz");
        let source = std::fs::read("tests/resources/apple.ttl")?;
        let mut enc =
            flate2::write::GzEncoder::new(File::create(&gz_path)?, flate2::Compression::default());
        enc.write_all(&source)?;
        enc.finish()?;
        assert_eq!(count_via_convert(&gz_path), 9);
        Ok(())
    }

    #[test]
    fn bzipped_ttl_input() -> Result<(), Error> {
        let tmp = tempfile::tempdir()?;
        let bz_path = tmp.path().join("apple.ttl.bz2");
        let source = std::fs::read("tests/resources/apple.ttl")?;
        let mut enc =
            bzip2::write::BzEncoder::new(File::create(&bz_path)?, bzip2::Compression::default());
        enc.write_all(&source)?;
        enc.finish()?;
        assert_eq!(count_via_convert(&bz_path), 9);
        Ok(())
    }

    #[test]
    fn owl_as_rdfxml() -> Result<(), Error> {
        let tmp = tempfile::tempdir()?;
        let owl_path = tmp.path().join("tiny.owl");
        std::fs::write(
            &owl_path,
            r#"<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
  <rdf:Description rdf:about="http://example.org/a">
    <rdf:type rdf:resource="http://example.org/Thing"/>
  </rdf:Description>
</rdf:RDF>"#,
        )?;
        assert_eq!(count_via_convert(&owl_path), 1);
        Ok(())
    }

    /// A syntactically invalid file must surface a parse error rather than panic,
    /// exercising the error-stashing adapter.
    #[test]
    fn syntax_error_is_reported() -> Result<(), Error> {
        let tmp = tempfile::tempdir()?;
        let bad = tmp.path().join("bad.ttl");
        // Subject and predicate but no object before the `.` — a Turtle syntax error.
        std::fs::write(&bad, "<http://example.org/s> <http://example.org/p> .\n")?;
        let err = convert_to_hdt(&[&bad]).expect_err("expected a parse error");
        assert!(matches!(err, Error::Parse(_)), "got {err:?}");
        Ok(())
    }
}
