use rosm_pbf_reader::{read_blob, Block, BlockParser};

use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pbf_path = std::env::args()
        .nth(1)
        .expect("Expected an OSM PBF file as first argument");

    let mut file = File::open(pbf_path).unwrap();

    let mut block_parser = BlockParser::default();

    while let Some(raw_block) = read_blob(&mut file) {
        let block = block_parser.parse_block(raw_block?)?;

        if let Block::Header(header_block) = block {
            println!("{:#?}", header_block);
            break;
        }
    }

    Ok(())
}
