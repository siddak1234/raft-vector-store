//! ANN-Benchmarks datasets (`docs/PLAN.md` §10): HDF5 files with `train` vectors, `test`
//! queries, and the true `neighbors` of each query, read without the HDF5 C library.

use std::path::Path;

/// A dataset loaded into memory.
#[derive(Debug, Clone)]
pub struct Dataset {
    /// Number of dimensions.
    pub dimension: usize,
    /// Training vectors, row by row; vector `i` has ID `i`.
    pub train: Vec<f32>,
    /// Query vectors, row by row.
    pub test: Vec<f32>,
    /// For each query, the IDs of its 100 true nearest neighbors, closest first.
    pub neighbors: Vec<Vec<u64>>,
    /// The distances of those neighbors.
    pub distances: Vec<Vec<f32>>,
}

impl Dataset {
    /// Reads an ANN-Benchmarks HDF5 file.
    ///
    /// # Errors
    ///
    /// Returns a description of the problem if the file cannot be read.
    pub fn load(path: &Path) -> Result<Dataset, String> {
        let error = |err: hdf5_reader::error::Error| format!("{}: {err}", path.display());
        let file = hdf5_reader::Hdf5File::open(path).map_err(error)?;
        let root = file.root_group().map_err(error)?;
        let train = root.dataset("train").map_err(error)?;
        let shape = train.shape().to_vec();
        let [_, dimension] = shape[..] else {
            return Err(format!("{}: train is not a matrix", path.display()));
        };
        let train = train
            .read_array::<f32>()
            .map_err(error)?
            .into_raw_vec_and_offset()
            .0;
        let test = root
            .dataset("test")
            .map_err(error)?
            .read_array::<f32>()
            .map_err(error)?
            .into_raw_vec_and_offset()
            .0;
        let neighbors_set = root.dataset("neighbors").map_err(error)?;
        let width = neighbors_set.shape()[1] as usize;
        let flat = neighbors_set
            .read_array::<i32>()
            .map_err(error)?
            .into_raw_vec_and_offset()
            .0;
        let neighbors = flat
            .chunks(width)
            .map(|row| row.iter().map(|&id| id as u64).collect())
            .collect();
        let distances = root
            .dataset("distances")
            .map_err(error)?
            .read_array::<f32>()
            .map_err(error)?
            .into_raw_vec_and_offset()
            .0
            .chunks(width)
            .map(<[f32]>::to_vec)
            .collect();
        Ok(Dataset {
            dimension: dimension as usize,
            train,
            test,
            neighbors,
            distances,
        })
    }

    /// Number of training vectors.
    pub fn len(&self) -> usize {
        self.train.len() / self.dimension
    }

    /// Whether there are no training vectors.
    pub fn is_empty(&self) -> bool {
        self.train.is_empty()
    }

    /// Training vector `i`.
    pub fn vector(&self, i: usize) -> &[f32] {
        &self.train[i * self.dimension..(i + 1) * self.dimension]
    }

    /// Query `i`.
    pub fn query(&self, i: usize) -> &[f32] {
        &self.test[i * self.dimension..(i + 1) * self.dimension]
    }

    /// Number of queries.
    pub fn queries(&self) -> usize {
        self.test.len() / self.dimension
    }
}
