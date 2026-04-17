// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::rdf_reader::{concat_nt, convert_to_nt};
use log::{debug, error};
use std::{
    fmt,
    fs::OpenOptions,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Hdt(hdt::hdt::Error),
    Parse(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Hdt(e) => write!(f, "HDT error: {e}"),
            Error::Parse(e) => write!(f, "parse error: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Hdt(e) => Some(e),
            Error::Parse(e) => Some(e.as_ref()),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<hdt::hdt::Error> for Error {
    fn from(e: hdt::hdt::Error) -> Self {
        Error::Hdt(e)
    }
}

pub fn build_hdt<P: AsRef<Path>, Q: AsRef<Path>>(inputs: &[P], dest: Q) -> Result<hdt::Hdt, Error> {
    if inputs.is_empty() {
        error!("no files provided");
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "no files provided to convert").into(),
        );
    }

    let timer = std::time::Instant::now();
    let all_nt = inputs.iter().all(|p| {
        p.as_ref()
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("nt"))
    });

    let (nt_path, _tmp_guard): (PathBuf, Option<tempfile::NamedTempFile>) =
        match (all_nt, inputs.len()) {
            (true, 1) => (inputs[0].as_ref().to_path_buf(), None),
            (true, _) => {
                let tmp = tempfile::Builder::new().suffix(".nt").tempfile()?;
                concat_nt(inputs, tmp.reopen()?)?;
                (tmp.path().to_path_buf(), Some(tmp))
            }
            _ => {
                let tmp = tempfile::Builder::new().suffix(".nt").tempfile()?;
                convert_to_nt(inputs, tmp.reopen()?)?;
                (tmp.path().to_path_buf(), Some(tmp))
            }
        };

    let converted_hdt = hdt::Hdt::read_nt(&nt_path)?;

    debug!("HDT build time: {:?}", timer.elapsed());

    let out_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dest.as_ref())?;
    let mut writer = BufWriter::with_capacity(1 << 20, out_file);
    converted_hdt.write(&mut writer)?;
    writer.flush()?;

    debug!("Total execution time: {:?}", timer.elapsed());
    Ok(converted_hdt)
}

#[cfg(test)]
mod tests {

    use super::*;
    use walkdir::WalkDir;

    fn run_sparql_suite(suite: &str) -> Result<(), Error> {
        let suite_dir = format!("tests/resources/rdf-tests/sparql/{suite}");
        assert!(
            std::path::Path::new(&suite_dir).exists(),
            "{suite_dir} not found — run `git submodule update --init` to fetch rdf-tests"
        );

        let input_files = find_ttl_files(&suite_dir);
        assert!(
            !input_files.is_empty(),
            "no .ttl files found under {suite_dir}"
        );

        let tmp = tempfile::tempdir()?;
        let mut failures = Vec::new();

        for f in &input_files {
            let p = std::path::Path::new(f);
            let parent_name = p
                .parent()
                .and_then(|x| x.file_name())
                .and_then(|x| x.to_str());
            let file_name = p.file_name().and_then(|n| n.to_str());

            if file_name == Some("manifest.ttl") || parent_name == Some(suite) {
                continue;
            }

            let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("out");
            let out = tmp
                .path()
                .join(format!("{}_{}.hdt", parent_name.unwrap_or("root"), stem));

            if let Err(e) = build_hdt(std::slice::from_ref(f), &out) {
                failures.push(format!("{f}: build: {e}"));
                continue;
            }

            let reader = match std::fs::File::open(&out) {
                Ok(file) => std::io::BufReader::new(file),
                Err(e) => {
                    failures.push(format!("{f}: open: {e}"));
                    continue;
                }
            };
            if let Err(e) = hdt::Hdt::read(reader) {
                failures.push(format!("{f}: read: {e}"));
            }
        }

        assert!(
            failures.is_empty(),
            "{} conversion failure(s) in {suite}:\n{}",
            failures.len(),
            failures.join("\n")
        );
        Ok(())
    }

    #[test]
    fn sparql10_tests() -> Result<(), Error> {
        run_sparql_suite("sparql10")
    }

    #[test]
    fn sparql11_tests() -> Result<(), Error> {
        run_sparql_suite("sparql11")
    }

    #[test]
    fn sparql12_tests() -> Result<(), Error> {
        run_sparql_suite("sparql12")
    }

    #[test]
    fn multi_nt_concat() -> Result<(), Error> {
        let tmp = tempfile::tempdir()?;
        let a = tmp.path().join("a.nt");
        let b = tmp.path().join("b.nt");
        // `a` intentionally omits a trailing newline to exercise the separator.
        std::fs::write(&a, "<http://ex/a> <http://ex/p> <http://ex/o1> .")?;
        std::fs::write(&b, "<http://ex/b> <http://ex/p> <http://ex/o2> .\n")?;

        let out = tmp.path().join("merged.hdt");
        build_hdt(&[&a, &b], &out)?;

        let reader = std::io::BufReader::new(std::fs::File::open(&out)?);
        hdt::Hdt::read(reader)?;
        Ok(())
    }

    fn find_ttl_files<P: AsRef<std::path::Path>>(dir: P) -> Vec<String> {
        WalkDir::new(dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "ttl"))
            .map(|e| e.path().display().to_string())
            .collect()
    }
}
