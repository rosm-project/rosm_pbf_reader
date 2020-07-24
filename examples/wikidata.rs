use rosm_pbf_reader::pbf;
use rosm_pbf_reader::{Block, DenseNodeReader, PbfReader, TagReader};

use std::fs::File;

fn print_header_block(block: pbf::HeaderBlock) {
    if let Some(writing_program) = &block.writingprogram {
        println!("Writing program: {}", writing_program);
    }
}

fn print_wikidata_tag(key: &str, value: &str) {
    if key == "wikidata" {
        println!("{}: {}", key, value);
    }
}

fn print_primitive_block(block: pbf::PrimitiveBlock) {
    for group in &block.primitivegroup {
        let string_table = &block.stringtable;

        for way in &group.ways {
            let tags = TagReader::new(&way.keys, &way.vals, string_table);
            for (key, value) in tags {
                print_wikidata_tag(key.unwrap(), value.unwrap());
            }
        }

        if let Some(dense_nodes) = &group.dense {
            let nodes = DenseNodeReader::new(&dense_nodes, string_table);

            for node in nodes {
                for (key, value) in node.tags {
                    print_wikidata_tag(key.unwrap(), value.unwrap());
                }
            }
        }
    }
}

fn main() {
    let pbf_path = std::env::args()
        .nth(1)
        .expect("Expected an OSM PBF file as first argument");

    let file = File::open(pbf_path).unwrap();

    let mut reader = PbfReader::new(file);

    while let Some(result) = reader.read_block() {
        match result {
            Ok(Block::Header(block)) => print_header_block(block),
            Ok(Block::Primitive(block)) => print_primitive_block(block),
            Ok(_) => println!("Skipping unknown block"),
            Err(error) => println!("Error during read: {:?}", error),
        }
    }
}
