// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::builder::Error;
use log::{debug, error, warn};
use oxrdfio::RdfSerializer;
use oxrdfio::{
    RdfFormat::{self, NTriples},
    RdfParseError, RdfParser,
};
use std::io::Write;
use std::{
    io::{self, BufReader, BufWriter},
    path::Path,
};
use url::Url;

pub(crate) fn convert_to_nt<P: AsRef<Path>>(
    file_paths: &[P],
    output_file: std::fs::File,
) -> Result<(), Error> {
    let mut dest_writer = BufWriter::new(output_file);
    for file in file_paths {
        let file = file.as_ref();
        let source = std::fs::File::open(file).map_err(|e| {
            error!("Error opening file {}: {e:?}", file.display());
            e
        })?;
        let source_reader = BufReader::new(source);

        debug!("converting {} to nt format", file.display());

        let mut serializer = RdfSerializer::from_format(NTriples).for_writer(dest_writer.by_ref());
        let v = std::time::Instant::now();
        let ext = file.extension().and_then(|e| e.to_str()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("file {} has no usable extension", file.display()),
            )
        })?;
        let rdf_format = RdfFormat::from_extension(ext).ok_or_else(|| {
            error!("unrecognized file extension for {}", file.display());
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unrecognized file extension for {}", file.display()),
            )
        })?;
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
}
