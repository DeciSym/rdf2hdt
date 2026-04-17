// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::builder::Error;
use bzip2::bufread::MultiBzDecoder;
use flate2::bufread::MultiGzDecoder;
use log::{debug, error, warn};
use oxrdfio::RdfSerializer;
use oxrdfio::{
    RdfFormat::{self, NTriples},
    RdfParseError, RdfParser,
};
use std::fs::File;
use std::io::Write;
use std::{
    io::{self, BufReader, BufWriter, Read},
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

pub(crate) fn convert_to_nt<P: AsRef<Path>>(
    file_paths: &[P],
    output_file: std::fs::File,
) -> Result<(), Error> {
    let mut dest_writer = BufWriter::with_capacity(IO_BUF, output_file);
    for file in file_paths {
        let file = file.as_ref();
        let source_reader = open_rdf_reader(file).map_err(|e| {
            error!("Error opening file {}: {e:?}", file.display());
            e
        })?;

        debug!("converting {} to nt format", file.display());

        let mut serializer = RdfSerializer::from_format(NTriples).for_writer(dest_writer.by_ref());
        let v = std::time::Instant::now();
        let rdf_format = rdf_format_from_path(file)?;
        let abs_path = std::fs::canonicalize(file)?;
        let base_iri = Url::from_file_path(&abs_path).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("cannot build file:// URI for {}", file.display()),
            )
        })?;
        let quads = RdfParser::from_format(rdf_format)
            .with_base_iri(base_iri.as_str())
            .map_err(|e| Error::Parse(Box::new(e)))?
            .for_reader(source_reader);
        let mut warned = false;
        for q in quads {
            let q = match q {
                Ok(v) => v,
                Err(e) => match e {
                    RdfParseError::Io(v) => {
                        error!("Error reading file {}: {v}", file.display());
                        return Err(v.into());
                    }
                    RdfParseError::Syntax(syn_err) => {
                        error!("syntax error for RDF file {}: {syn_err}", file.display());
                        return Err(Error::Parse(Box::new(syn_err)));
                    }
                },
            };
            if !warned && q.graph_name != oxrdf::GraphName::DefaultGraph {
                warned = true;
                warn!(
                    "HDT does not support named graphs, merging triples for {}",
                    file.display()
                );
            }
            serializer.serialize_triple(oxrdf::TripleRef {
                subject: q.subject.as_ref(),
                predicate: q.predicate.as_ref(),
                object: q.object.as_ref(),
            })?
        }

        serializer.finish()?;
        debug!("RDF to NTriple convert time: {:?}", v.elapsed());
    }
    dest_writer.flush()?;
    Ok(())
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

    #[test]
    fn test_rdf() {
        let tmp_file = tempfile::Builder::new().suffix(".nt").tempfile().expect("");
        assert!(
            (convert_to_nt(
                &["tests/resources/apple.ttl"],
                tmp_file.reopen().expect("error opening tmp file")
            ))
            .is_ok()
        );
        let source_reader = BufReader::new(tmp_file.reopen().expect("error opening tmp file"));
        let quads = RdfParser::from_format(NTriples)
            .for_reader(source_reader)
            .collect::<Result<Vec<_>, _>>();

        assert!(quads.is_ok());
        assert_eq!(quads.unwrap().len(), 9)
    }

    fn count_triples_in(nt_file: &tempfile::NamedTempFile) -> usize {
        let reader = BufReader::new(nt_file.reopen().expect("error opening tmp file"));
        RdfParser::from_format(NTriples)
            .for_reader(reader)
            .collect::<Result<Vec<_>, _>>()
            .expect("parse nt")
            .len()
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

        let out = tempfile::Builder::new().suffix(".nt").tempfile()?;
        convert_to_nt(&[&gz_path], out.reopen()?)?;
        assert_eq!(count_triples_in(&out), 9);
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

        let out = tempfile::Builder::new().suffix(".nt").tempfile()?;
        convert_to_nt(&[&bz_path], out.reopen()?)?;
        assert_eq!(count_triples_in(&out), 9);
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

        let out = tempfile::Builder::new().suffix(".nt").tempfile()?;
        convert_to_nt(&[&owl_path], out.reopen()?)?;
        assert_eq!(count_triples_in(&out), 1);
        Ok(())
    }
}
