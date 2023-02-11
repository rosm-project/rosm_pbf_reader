# rosm_pbf_reader

[![Crates.io](https://img.shields.io/crates/v/rosm_pbf_reader.svg?label=rosm_pbf_reader)](https://crates.io/crates/rosm_pbf_reader)
[![Docs.rs](https://docs.rs/rosm_pbf_reader/badge.svg)](https://docs.rs/rosm_pbf_reader)
[![Build Status](https://github.com/yzsolt/rosm_pbf_reader/workflows/continuous-integration/badge.svg)](https://github.com/yzsolt/rosm_pbf_reader/actions)

A low-level Rust library for parsing OpenStreetMap data in [PBF format](https://wiki.openstreetmap.org/wiki/PBF_Format).

This library provides the smallest possible API to work with OSM PBF files: a blob reader, a block parser and some utilities to read delta or densely encoded data. No other utilities are provided for further data processing (like filtering). There's also no built-in parallelization, however block parsing (which is the most computation-heavy part of the process) can be easily dispatched to multiple threads.

## Features

Since most OSM PBFs are ZLib compressed, ZLib decompression support using [`flate2`](https://crates.io/crates/flate2) is enabled by default. See Cargo's [default feature documentation](https://doc.rust-lang.org/cargo/reference/features.html#the-default-feature) how to disable it.

The library provides a way for the user to support other compression methods by implementing the `Decompressor` trait.

## Examples

- `print_header` is a very simple example showing how to print the header block of an OSM PBF file.
- `count_wikidata` is a more complete example showing multithreaded parsing, tag and dense node reading.

## Similar projects

- [osmpbfreader-rs](https://github.com/TeXitoi/osmpbfreader-rs)
- [osmpbf](https://github.com/b-r-u/osmpbf)
