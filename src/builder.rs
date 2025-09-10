// Copyright (c) 2025, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

use crate::rdf_reader::convert_to_nt;
use log::{debug, error};
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
};

pub fn build_hdt(file_paths: Vec<String>, dest_file: &str) -> Result<hdt::Hdt, hdt::hdt::Error> {
    if file_paths.is_empty() {
        error!("no files provided");
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "no files provided to convert",
        )
        .into());
    }

    let timer = std::time::Instant::now();
    // TODO
    // implement an RDF reader trait
    // 1. for larger datasets, read from source files everytime since storing all triples in memory may OOM kill process
    // 2. build Vec<Triple> in memory from source files
    let nt_file = if file_paths.len() == 1 && file_paths[0].ends_with(".nt") {
        file_paths[0].clone()
    } else {
        let tmp_file = tempfile::Builder::new()
            .disable_cleanup(true)
            .suffix(".nt")
            .tempfile()?;
        convert_to_nt(file_paths, tmp_file.reopen()?).expect("failed to convert file to NT");
        tmp_file.path().to_str().unwrap().to_string()
    };

    let converted_hdt = hdt::Hdt::read_nt(std::path::Path::new(&nt_file))?;

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

            if let Ok(_) = build_hdt(vec![f.to_string()], &hdt_file_path) {
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

            if let Ok(_) = build_hdt(vec![f.to_string()], &hdt_file_path) {
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

            if let Ok(_) = build_hdt(vec![f.to_string()], &hdt_file_path) {
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
