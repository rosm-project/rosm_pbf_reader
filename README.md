# rosm_pbf_reader

A low-level Rust library for parsing [OpenStreetMap data in PBF format](https://wiki.openstreetmap.org/wiki/PBF_Format).

Low-level means that:
 - This library provides the smallest possible API to work with OSM PBF files: a `PbfReader` to iterate on header/data blocks and some utilities to read delta encoded messages and properties. No other utilities are provided for further data processing (like filtering).
 - Most parse error are propagated to the user, the library tries to not panic on its own. It may panic on highly corrupted or invalid inputs though.
 - The library doesn't do any validation on the input data, except for checking blob header/data size.

Parallel reading of a single input PBF is currently not in the scope of this library.

## Dependencies

- [quick-protobuf](https://github.com/tafia/quick-protobuf) for protobuf parsing

## Similar projects

- [osmpbfreader-rs](https://github.com/TeXitoi/osmpbfreader-rs)
- [osmpbf](https://github.com/b-r-u/osmpbf)
