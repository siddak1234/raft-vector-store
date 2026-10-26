//! The ANN-Benchmarks loader (`client::Dataset`).

use std::path::PathBuf;

use client::Dataset;

#[test]
fn a_missing_file_is_an_error_that_names_it() {
    let path = std::env::temp_dir().join("rvs-no-such-dataset.hdf5");
    let err = Dataset::load(&path).expect_err("there is no such file");
    assert!(err.contains("rvs-no-such-dataset.hdf5"), "{err}");
}

#[test]
fn a_file_that_is_not_hdf5_is_an_error() {
    let path = std::env::temp_dir().join(format!("rvs-not-hdf5-{}.hdf5", std::process::id()));
    std::fs::write(&path, b"not an HDF5 file").expect("write");
    let result = Dataset::load(&path);
    let _ = std::fs::remove_file(&path);
    assert!(result.is_err());
}

/// Where `cargo xtask dataset glove-25` puts the file: `$RVS_DATASETS`, or
/// `~/.cache/raft-vector-store/datasets`.
fn glove() -> PathBuf {
    let dir = std::env::var_os("RVS_DATASETS").map_or_else(
        || {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join(".cache/raft-vector-store/datasets")
        },
        PathBuf::from,
    );
    dir.join("glove-25-angular.hdf5")
}

#[test]
#[ignore = "needs the GloVe-25 file: run `cargo xtask dataset glove-25`, then this test with --ignored"]
fn glove_25_has_the_published_shape() {
    let data = Dataset::load(&glove()).expect("load");
    assert_eq!(data.dimension, 25);
    assert_eq!(data.len(), 1_183_514);
    assert_eq!(data.queries(), 10_000);
    assert_eq!(data.vector(0).len(), 25);
    assert_eq!(data.query(0).len(), 25);
    assert!(data.neighbors.iter().all(|n| n.len() == 100));
    assert!(data.distances.iter().all(|d| d.len() == 100));
}
