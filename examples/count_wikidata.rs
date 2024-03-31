use log::{error, info, warn};

use rosm_pbf_reader::dense::{new_dense_tag_reader, DenseNodeReader};
use rosm_pbf_reader::{new_tag_reader, pbf, read_blob, Block, BlockParser, Error, RawBlock};

use std::cell::RefCell;
use std::fs::File;
use std::sync::atomic::{AtomicUsize, Ordering};

use threadpool::ThreadPool;

static WIKIDATA_COUNT: AtomicUsize = AtomicUsize::new(0);

fn process_header_block(block: pbf::HeaderBlock) {
    if let Some(writing_program) = &block.writingprogram {
        info!("Writing program: {}", writing_program);
    }
}

fn process_tag(key: &str, _value: &str) {
    if key == "wikidata" {
        WIKIDATA_COUNT.fetch_add(1, Ordering::SeqCst);
    }
}

fn process_primitive_block(block: pbf::PrimitiveBlock) -> Result<(), Error> {
    for group in &block.primitivegroup {
        let string_table = &block.stringtable;

        for way in &group.ways {
            let tags = new_tag_reader(string_table, &way.keys, &way.vals);
            for (key, value) in tags {
                process_tag(key.unwrap(), value.unwrap());
            }
        }

        if let Some(dense_nodes) = &group.dense {
            let nodes = DenseNodeReader::new(dense_nodes)?;

            for node in nodes {
                let tags = new_dense_tag_reader(string_table, node?.key_value_indices);

                for (key, value) in tags {
                    process_tag(key.unwrap(), value.unwrap());
                }
            }
        }
    }

    Ok(())
}

fn parse_block(block_parser: &mut BlockParser, raw_block: RawBlock) {
    match block_parser.parse_block(raw_block) {
        Ok(block) => match block {
            Block::Header(header_block) => process_header_block(header_block),
            Block::Primitive(primitive_block) => {
                if let Err(error) = process_primitive_block(primitive_block) {
                    error!("Error during processing a primitive block: {error:?}")
                }
            }
            Block::Unknown(unknown_block) => {
                warn!("Skipping unknown block of size {}", unknown_block.len())
            }
        },
        Err(error) => error!("Error during parsing a block: {error:?}"),
    }
}

fn main() {
    let mut builder = env_logger::Builder::from_default_env();
    builder.filter_level(log::LevelFilter::Info);
    builder.init();

    let mut args = std::env::args();

    let pbf_path = args.nth(1).expect("Expected an OSM PBF file as first argument");

    let thread_count: usize = match args.next() {
        Some(s) => s.parse().expect("Expected a thread count as second argument"),
        None => 1,
    };

    let mut file = File::open(pbf_path).unwrap();

    let start = std::time::Instant::now();

    if thread_count == 1 {
        let mut block_parser = BlockParser::default();

        while let Some(result) = read_blob(&mut file) {
            match result {
                Ok(raw_block) => parse_block(&mut block_parser, raw_block),
                Err(error) => error!("Error during reading the next blob: {:?}", error),
            }
        }
    } else {
        let thread_pool = ThreadPool::new(thread_count);

        // Make the parser thread local to reduce memory allocation count
        thread_local!(static BLOCK_PARSER: RefCell<BlockParser> = RefCell::new(BlockParser::default()));

        while let Some(result) = read_blob(&mut file) {
            match result {
                Ok(raw_block) => {
                    thread_pool.execute(move || {
                        BLOCK_PARSER.with(|block_parser| {
                            let mut block_parser = block_parser.borrow_mut();
                            parse_block(&mut block_parser, raw_block);
                        });
                    });
                }
                Err(error) => error!("Error during reading the next blob: {:?}", error),
            }
        }

        thread_pool.join();
    }

    info!("Wikidata tag count: {}", WIKIDATA_COUNT.load(Ordering::SeqCst));
    info!(
        "Finished in {:.2}s on {} thread(s)",
        start.elapsed().as_secs_f64(),
        thread_count
    );
}
