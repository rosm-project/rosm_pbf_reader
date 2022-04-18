use std::io::Result;
use std::path::{Path, PathBuf};

fn main() -> Result<()> {
    let in_dir = PathBuf::from(::std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("proto");
    println!("cargo:rerun-if-changed={}", in_dir.to_str().unwrap());

    let in_files: Vec<_> = ["osmformat.proto", "fileformat.proto"]
        .iter()
        .map(|s| in_dir.join(s))
        .collect();

    for file in &in_files {
        println!("cargo:rerun-if-changed={}", file.to_str().unwrap());
    }

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_dir = Path::new(&out_dir).join("proto");

    if out_dir.exists() {
        std::fs::remove_dir_all(&out_dir).unwrap();
    }

    std::fs::DirBuilder::new().create(&out_dir).unwrap();

    let mut prost_build = prost_build::Config::new();
    prost_build.out_dir(&out_dir);
    prost_build.compile_protos(&in_files, &[in_dir])?;

    Ok(())
}
