# rdf2hdt
Library for converting RDF data to HDT

This is a Rust-based tool that converts RDF data into HDT format. It uses the `oxrdfio` crate for RDF parsing and conversion, 
and then generates and saves the data as HDT. Implementation is based on the [HDT specification](https://www.w3.org/submissions/2011/SUBM-HDT-20110330) 
and the output HDT is intended to be consumed by one of [hdt crate](https://github.com/KonradHoeffner/hdt), [hdt-cpp](https://github.com/rdfhdt/hdt-cpp),
or [hdt-java](https://github.com/rdfhdt/hdt-java).

## Using the rdf2hdt CLI

This library includes a CLI utility for gnerating HDT files from RDF input data. The binary can be built using `cargo build`.

```bash
$ rdf2hdt convert --help
Convert RDF to HDT.

The `convert` command parses RDF files, converts it to RDF triples using `oxrdfio` for parsing and conversion, and then generates and saves the data as HDT.

Usage: rdf2hdt convert [OPTIONS] --output <OUTPUT>

Options:
  -i, --input <INPUT>...
          Path to input RDF file(s).

          Provide the path to one or more RDF files that will be parsed and converted. Support file formats: https://crates.io/crates/oxrdfio

  -o, --output <OUTPUT>
          Path to output file.

          Specify the path to save the generated HDT.

  -v, --verbose...
          Increase logging verbosity

  -b, --block-size <BLOCK_SIZE>
          Block size used during term compression

          Every Nth term will be stored fully while others will only contain everything besides the longest common prefix of the last Nth term

          [default: 16]

  -q, --quiet...
          Decrease logging verbosity

  -h, --help
          Print help (see a summary with '-h')
```

## Using the build_hdt library

HDT files can be generated directly in Rust.

```rust
use rdf2hdt::hdt::{buld_hdt, Options};

let result = build_hdt(
  vec!["tests/resources/apple.ttl".to_string()],
  "output.hdt",
  Options::default(),
)?;
```

## License

TBD