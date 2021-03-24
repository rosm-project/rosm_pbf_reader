use rosm_pbf_reader::pbf;
use rosm_pbf_reader::{read_blob, BlockParser, Block, DenseNodeReader, TagReader};

use std::cell::RefCell;
use std::fs::File;
use std::sync::atomic::{AtomicUsize, Ordering};

use threadpool::ThreadPool;

static WIKIDATA_COUNT: AtomicUsize = AtomicUsize::new(0);

fn process_header_block(block: pbf::HeaderBlock) {
    if let Some(writing_program) = &block.writingprogram {
        println!("Writing program: {}", writing_program);
    }
}

fn process_tag(key: &str, _value: &str) {
    if key == "wikidata" {
        WIKIDATA_COUNT.fetch_add(1, Ordering::SeqCst);
    }
}

fn process_primitive_block(block: pbf::PrimitiveBlock) {
    for group in &block.primitivegroup {
        let string_table = &block.stringtable;

        for way in &group.ways {
            let tags = TagReader::new(&way.keys, &way.vals, string_table);
            for (key, value) in tags {
                process_tag(key.unwrap(), value.unwrap());
            }
        }

        if let Some(dense_nodes) = &group.dense {
            let nodes = DenseNodeReader::new(&dense_nodes, string_table);

            for node in nodes {
                for (key, value) in node.tags {
                    process_tag(key.unwrap(), value.unwrap());
                }
            }
        }
    }
}

fn main() {
    let mut args = std::env::args();

    let pbf_path = args
        .nth(1)
        .expect("Expected an OSM PBF file as first argument");

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
                Ok(raw_block) => {
                    match block_parser.parse_block(raw_block) {
                        Ok(block) => match block {
                            Block::Header(header_block) => process_header_block(header_block),
                            Block::Primitive(primitive_block) => process_primitive_block(primitive_block),
                            Block::Unknown(unknown_block) => println!("Skipping unknown block of size {}", unknown_block.len()),
                        }
                        Err(error) => println!("Error during parsing a block: {:?}", error),
                    }
                }
                Err(error) => println!("Error during reading the next blob: {:?}", error),
            }
        }
    } else {
        let thread_pool = ThreadPool::new(thread_count);

        // Make the parser thread local to reduce memory allocation count
        thread_local!(static BLOCK_PARSER: RefCell<BlockParser> = RefCell::new(BlockParser::default()));

        while let Some(result) = read_blob(&mut file) {
            match result {
                Ok(blob) => {
                    thread_pool.execute(move || {
                        BLOCK_PARSER.with(|block_parser| {
                            let mut block_parser = block_parser.borrow_mut();

                            match block_parser.parse_block(blob) {
                                Ok(block) => match block {
                                    Block::Header(header_block) => process_header_block(header_block),
                                    Block::Primitive(primitive_block) => process_primitive_block(primitive_block),
                                    Block::Unknown(unknown_block) => println!("Skipping unknown block of size {}", unknown_block.len()),
                                }
                                Err(error) => println!("Error during parsing a block: {:?}", error),
                            }
                        });
                    });
                }
                Err(error) => println!("Error during reading the next blob: {:?}", error),
            }
        }

        thread_pool.join();
    }

    println!("Wikidata tag count: {}", WIKIDATA_COUNT.load(Ordering::SeqCst));
    println!("Finished in {:.2}s on {} thread(s)", start.elapsed().as_secs_f64(), thread_count);
}
