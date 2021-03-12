# rosm_pbf_reader

A low-level Rust library for parsing OpenStreetMap data in [PBF format](https://wiki.openstreetmap.org/wiki/PBF_Format).

This library provides the smallest possible API to work with OSM PBF files: a blob reader, a block parser and some utilities to read delta or densely encoded data. No other utilities are provided for further data processing (like filtering). There's also no built-in parallelization, however block parsing (which is the most computation-heavy part of the process) can be easily dispatched to multiple threads.

The library uses [quick-protobuf](https://github.com/tafia/quick-protobuf) for fast protobuf parsing with minimal allocations.

## Examples

- `print_header` is a very simple example showing how to print the header block of an OSM PBF file.
- `count_wikidata` is a more complete example showing multithreaded parsing, tag and dense node reading.

## Similar projects

- [osmpbfreader-rs](https://github.com/TeXitoi/osmpbfreader-rs)
- [osmpbf](https://github.com/b-r-u/osmpbf)
