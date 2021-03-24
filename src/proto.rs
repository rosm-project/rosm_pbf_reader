#![allow(deprecated)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(unused_imports)]

pub mod fileformat {
    include!(concat!(env!("OUT_DIR"), "/proto/fileformat.rs"));
}

pub mod osmformat {
    include!(concat!(env!("OUT_DIR"), "/proto/osmformat.rs"));
}
