//! Compiles `proto/rvs.proto` with protox, a protobuf compiler written in Rust, so the
//! build needs no `protoc` installation.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/rvs.proto");
    let descriptors = protox::compile(["proto/rvs.proto"], ["proto"])?;
    tonic_prost_build::configure().compile_fds(descriptors)?;
    Ok(())
}
