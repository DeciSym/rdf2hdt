// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::rdf_reader::convert_to_nt;
use log::{debug, error};
use std::{
    fs::OpenOptions,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

pub fn build_hdt(file_paths: Vec<String>, dest_file: &str) -> Result<hdt::Hdt, hdt::hdt::Error> {
    if file_paths.is_empty() {
        error!("no files provided");
        return Err(
            io::Error::new(io::ErrorKind::InvalidData, "no files provided to convert").into(),
        );
    }

    let timer = std::time::Instant::now();
    let is_nt = file_paths.len() == 1
        && Path::new(&file_paths[0])
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("nt"));

    let (nt_path, _tmp_guard): (PathBuf, Option<tempfile::NamedTempFile>) = if is_nt {
        (PathBuf::from(&file_paths[0]), None)
    } else {
        let tmp = tempfile::Builder::new().suffix(".nt").tempfile()?;
        convert_to_nt(file_paths, tmp.reopen()?).map_err(|e| io::Error::other(e.to_string()))?;
        (tmp.path().to_path_buf(), Some(tmp))
    };

    let converted_hdt = hdt::Hdt::read_nt(&nt_path)?;

    debug!("HDT build time: {:?}", timer.elapsed());

    let out_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dest_file)?;
    let mut writer = BufWriter::new(out_file);
    converted_hdt.write(&mut writer)?;
    writer.flush()?;

    debug!("Total execution time: {:?}", timer.elapsed());
    Ok(converted_hdt)
}

#[cfg(test)]
mod tests {

    use super::*;
    use walkdir::WalkDir;

    fn run_sparql_suite(suite: &str) -> hdt::hdt::Result<()> {
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
                .join(format!("{}_{}.hdt", parent_name.unwrap_or("root"), stem,));
            let out_str = out
                .to_str()
                .expect("tempdir path should be valid UTF-8 on test platforms");

            if let Err(e) = build_hdt(vec![f.to_string()], out_str) {
                failures.push(format!("{f}: {e}"));
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
    fn sparql10_tests() -> hdt::hdt::Result<()> {
        run_sparql_suite("sparql10")
    }

    #[test]
    fn sparql11_tests() -> hdt::hdt::Result<()> {
        run_sparql_suite("sparql11")
    }

    #[test]
    fn sparql12_tests() -> hdt::hdt::Result<()> {
        run_sparql_suite("sparql12")
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
