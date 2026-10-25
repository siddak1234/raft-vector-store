//! Structured logs: one JSON object per line.

use std::fs::File;
use std::io::{LineWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// A log of JSON lines, shared by the server's threads. Each line gets a `time` field in
/// microseconds since the Unix epoch if it has none.
#[derive(Clone)]
pub struct JsonLog {
    out: Arc<Mutex<Box<dyn Write + Send>>>,
}

impl JsonLog {
    /// A log written to `path`, or to standard error if `path` is `None`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created.
    pub fn open(path: Option<&Path>) -> std::io::Result<JsonLog> {
        let out: Box<dyn Write + Send> = match path {
            Some(path) => Box::new(LineWriter::new(File::create(path)?)),
            None => Box::new(std::io::stderr()),
        };
        Ok(JsonLog {
            out: Arc::new(Mutex::new(out)),
        })
    }

    /// Writes one line.
    pub fn write(&self, value: &serde_json::Value) {
        let mut value = value.clone();
        if let Some(object) = value.as_object_mut() {
            object
                .entry("time")
                .or_insert_with(|| serde_json::json!(rpc::now_micros()));
        }
        let mut out = self.out.lock().expect("no thread panicked while logging");
        let _ = writeln!(out, "{value}");
    }
}
