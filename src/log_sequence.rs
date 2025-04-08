// Copyright (c) 2024-2025, Decisym, LLC

use crate::common::save_u32_vec;
use hdt::containers::vbyte::encode_vbyte;
use std::{
    collections::BTreeSet,
    error::Error,
    fs::File,
    io::{BufWriter, Write},
};

/// Represents a compressed LogSequence2 sequence for storage
pub struct LogSequence2 {
    compressed_terms: Vec<u8>,
    offsets: Vec<u32>, // Stores positions of terms
    num_terms: usize,
}

impl LogSequence2 {
    /// Compress a sorted vector of terms using prefix compression
    pub fn compress(set: &BTreeSet<String>) -> Result<Self, Box<dyn Error>> {
        let mut terms: Vec<String> = set.iter().to_owned().cloned().collect();
        terms.sort(); // Ensure lexicographic order
        // println!("{:?}", terms);
        let mut compressed_terms = Vec::new();
        let mut offsets = Vec::new();
        let mut last_term = "";

        let num_terms = terms.len();
        let block_size = 16; // Every 16th term is stored fully
        for (i, term) in terms.iter().enumerate() {
            if i % block_size == 0 {
                offsets.push(compressed_terms.len() as u32);
                compressed_terms.extend_from_slice(term.as_bytes());
                // Every block stores a full term
            } else {
                let common_prefix_len = last_term
                    .chars()
                    .zip(term.chars())
                    .take_while(|(a, b)| a == b)
                    .count();
                compressed_terms.extend_from_slice(&encode_vbyte(common_prefix_len));
                compressed_terms.extend_from_slice(term[common_prefix_len..].as_bytes());
            };

            compressed_terms.push(0); // Null separator

            last_term = term;
        }
        offsets.push(compressed_terms.len() as u32);

        Ok(Self {
            compressed_terms,
            offsets,
            num_terms,
        })
    }

    /// Save the LogSequence2Rust structure to a file
    pub fn save(&self, dest_writer: &mut BufWriter<File>) -> Result<(), Box<dyn Error>> {
        let crc = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut hasher = crc.digest();
        // libhdt/src/libdcs/CSD_PFC.cpp::save()
        // save type
        let seq_type: [u8; 1] = [2];
        let _ = dest_writer.write(&seq_type)?;
        hasher.update(&seq_type);

        // // Save sizes
        let mut buf: Vec<u8> = vec![];
        buf.extend_from_slice(&encode_vbyte(self.num_terms));
        buf.extend_from_slice(&encode_vbyte(self.compressed_terms.len()));
        buf.extend_from_slice(&encode_vbyte(16));
        let _ = dest_writer.write(&buf)?;
        hasher.update(&buf);
        let checksum = hasher.finalize();
        let _ = dest_writer.write(&checksum.to_le_bytes())?;

        // // Write number of terms
        save_u32_vec(&self.offsets, dest_writer, 32)?;

        // Write packed data
        let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut hasher = crc.digest();
        let _ = dest_writer.write(&self.compressed_terms)?;
        hasher.update(&self.compressed_terms);
        // println!("{}", String::from_utf8_lossy(&self.compressed_terms));
        let checksum = hasher.finalize();
        let _ = dest_writer.write(&checksum.to_le_bytes())?;

        Ok(())
    }
}
