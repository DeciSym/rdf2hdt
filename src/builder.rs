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

    #[test]
    fn sparql10_tests() -> hdt::hdt::Result<()> {
        let input_files = find_ttl_files("tests/resources/rdf-tests/sparql/sparql10");
        for f in &input_files {
            if f.ends_with("manifest.ttl")
                || std::path::Path::new(f)
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    == "sparql10"
            {
                continue;
            }
            let hdt_file_path = format!(
                "tests/resources/generated/nt/sparql10/{}/{}",
                std::path::Path::new(f)
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap(),
                std::path::Path::new(f)
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replace(".ttl", ".hdt")
            );
            std::fs::create_dir_all(std::path::Path::new(&hdt_file_path).parent().unwrap())?;

            if build_hdt(vec![f.to_string()], &hdt_file_path).is_ok() {
                assert!(std::path::Path::new(&hdt_file_path).exists())
            }
        }
        Ok(())
    }

    #[test]
    fn sparql11_tests() -> hdt::hdt::Result<()> {
        let input_files = find_ttl_files("tests/resources/rdf-tests/sparql/sparql11");
        for f in &input_files {
            if f.ends_with("manifest.ttl")
                || std::path::Path::new(f)
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    == "sparql11"
            {
                continue;
            }
            let hdt_file_path = format!(
                "tests/resources/generated/nt/sparql11/{}/{}",
                std::path::Path::new(f)
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap(),
                std::path::Path::new(f)
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replace(".ttl", ".hdt")
            );
            std::fs::create_dir_all(std::path::Path::new(&hdt_file_path).parent().unwrap())?;

            if build_hdt(vec![f.to_string()], &hdt_file_path).is_ok() {
                assert!(std::path::Path::new(&hdt_file_path).exists())
            }
        }
        Ok(())
    }

    #[test]
    fn sparql12_tests() -> hdt::hdt::Result<()> {
        let input_files = find_ttl_files("tests/resources/rdf-tests/sparql/sparql12");
        for f in &input_files {
            if f.ends_with("manifest.ttl")
                || std::path::Path::new(f)
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    == "sparql12"
            {
                continue;
            }
            let hdt_file_path = format!(
                "tests/resources/generated/nt/sparql12/{}/{}",
                std::path::Path::new(f)
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap(),
                std::path::Path::new(f)
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replace(".ttl", ".hdt")
            );
            std::fs::create_dir_all(std::path::Path::new(&hdt_file_path).parent().unwrap())?;

            if build_hdt(vec![f.to_string()], &hdt_file_path).is_ok() {
                assert!(std::path::Path::new(&hdt_file_path).exists())
            }
        }
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
