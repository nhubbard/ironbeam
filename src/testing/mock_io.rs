//! Mock I/O helpers for testing without actual files.
//!
//! This module provides utilities for testing I/O operations using
//! temporary files and in-memory data.

use serde::Serialize;
use std::path::{Path, PathBuf};
use tempfile::{NamedTempFile, TempDir};

/// A temporary file that is automatically deleted when dropped.
///
/// This wrapper provides a convenient way to create temporary files
/// for testing I/O operations.
pub struct TempFilePath {
    #[allow(dead_code)]
    temp_file: NamedTempFile,
    path: PathBuf,
}

impl TempFilePath {
    /// Create a new temporary file.
    ///
    /// # Errors
    ///
    /// Returns an error if the temporary file cannot be created.
    pub fn new() -> std::io::Result<Self> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().to_path_buf();
        Ok(Self { temp_file, path })
    }

    /// Create a new temporary file with a specific extension.
    ///
    /// # Errors
    ///
    /// Returns an error if the temporary file cannot be created.
    pub fn with_extension(extension: &str) -> std::io::Result<Self> {
        let temp_file = tempfile::Builder::new()
            .suffix(&format!(".{extension}"))
            .tempfile()?;
        let path = temp_file.path().to_path_buf();
        Ok(Self { temp_file, path })
    }

    /// Get the path to the temporary file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Default for TempFilePath {
    fn default() -> Self {
        Self::new().expect("Failed to create temporary file")
    }
}

/// A temporary directory that is automatically deleted when dropped.
pub struct TempDirPath {
    #[allow(dead_code)]
    temp_dir: TempDir,
    path: PathBuf,
}

impl TempDirPath {
    /// Create a new temporary directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the temporary directory cannot be created.
    pub fn new() -> std::io::Result<Self> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_path_buf();
        Ok(Self { temp_dir, path })
    }

    /// Get the path to the temporary directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Create a file path within this directory.
    #[must_use]
    pub fn file_path(&self, filename: &str) -> PathBuf {
        self.path.join(filename)
    }
}

impl Default for TempDirPath {
    fn default() -> Self {
        Self::new().expect("Failed to create temporary directory")
    }
}

/// Create a temporary CSV file with the given data.
///
/// # Errors
///
/// Returns an error if the temporary file cannot be created.
///
/// # Example
///
/// ```
/// use rustflow::testing::mock_csv_file;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Record {
///     name: String,
///     age: u32,
/// }
///
/// let data = vec![
///     Record { name: "Alice".to_string(), age: 30 },
///     Record { name: "Bob".to_string(), age: 25 },
/// ];
///
/// let temp_file = mock_csv_file(&data, true).unwrap();
/// // Use temp_file.path() for testing
/// ```
#[cfg(feature = "io-csv")]
pub fn mock_csv_file<T: Serialize>(data: &[T], with_header: bool) -> std::io::Result<TempFilePath> {
    use csv::Writer;

    let temp = TempFilePath::with_extension("csv")?;
    let mut writer = if with_header {
        Writer::from_path(temp.path())?
    } else {
        Writer::from_writer(std::fs::File::create(temp.path())?)
    };

    for record in data {
        writer.serialize(record)?;
    }

    writer.flush()?;
    Ok(temp)
}

/// Create a temporary JSON Lines file with the given data.
///
/// # Errors
///
/// If the temporary file is unable to be created, an error is returned.
///
/// # Example
///
/// ```
/// use rustflow::testing::mock_jsonl_file;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Record {
///     id: u32,
///     value: String,
/// }
///
/// let data = vec![
///     Record { id: 1, value: "foo".to_string() },
///     Record { id: 2, value: "bar".to_string() },
/// ];
///
/// let temp_file = mock_jsonl_file(&data).unwrap();
/// // Use temp_file.path() for testing
/// ```
#[cfg(feature = "io-jsonl")]
pub fn mock_jsonl_file<T: Serialize>(data: &[T]) -> std::io::Result<TempFilePath> {
    use std::io::Write;

    let temp = TempFilePath::with_extension("jsonl")?;
    let mut file = std::fs::File::create(temp.path())?;

    for record in data {
        let json = serde_json::to_string(record)?;
        writeln!(file, "{json}")?;
    }

    file.flush()?;
    Ok(temp)
}

/// Read the contents of a CSV file for assertion.
///
/// # Errors
///
/// Returns an error if the read operation fails.
///
/// # Example
///
/// ```no_run
/// use rustflow::testing::read_csv_output;
/// use serde::Deserialize;
///
/// #[derive(Deserialize, Debug, PartialEq)]
/// struct Record {
///     name: String,
///     age: u32,
/// }
///
/// let records: Vec<Record> = read_csv_output("output.csv").unwrap();
/// assert_eq!(records.len(), 2);
/// ```
#[cfg(feature = "io-csv")]
pub fn read_csv_output<T: serde::de::DeserializeOwned, P: AsRef<Path>>(
    path: P,
) -> std::io::Result<Vec<T>> {
    use csv::Reader;

    let mut reader = Reader::from_path(path)?;
    let mut records = Vec::new();

    for result in reader.deserialize() {
        records.push(result?);
    }

    Ok(records)
}

/// Read the contents of a JSON Lines file for assertion.
///
/// # Errors
///
/// Returns an error if the read operation fails.
///
/// # Example
///
/// ```no_run
/// use rustflow::testing::read_jsonl_output;
/// use serde::Deserialize;
///
/// #[derive(Deserialize, Debug, PartialEq)]
/// struct Record {
///     id: u32,
///     value: String,
/// }
///
/// let records: Vec<Record> = read_jsonl_output("output.jsonl").unwrap();
/// assert_eq!(records.len(), 2);
/// ```
#[cfg(feature = "io-jsonl")]
pub fn read_jsonl_output<T: serde::de::DeserializeOwned, P: AsRef<Path>>(
    path: P,
) -> std::io::Result<Vec<T>> {
    use std::io::{BufRead, BufReader};

    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            let record: T = serde_json::from_str(&line)?;
            records.push(record);
        }
    }

    Ok(records)
}

/// Assert that a CSV file contains the expected records.
///
/// # Panics
///
/// Panics if the assertion fails.
///
/// # Example
///
/// ```no_run
/// use rustflow::testing::assert_csv_equals;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, PartialEq, Serialize, Deserialize)]
/// struct Record {
///     name: String,
///     age: u32,
/// }
///
/// let expected = vec![
///     Record { name: "Alice".to_string(), age: 30 },
///     Record { name: "Bob".to_string(), age: 25 },
/// ];
///
/// assert_csv_equals("output.csv", &expected);
/// ```
#[cfg(feature = "io-csv")]
pub fn assert_csv_equals<T, P>(path: P, expected: &[T])
where
    T: serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
    P: AsRef<Path>,
{
    let actual: Vec<T> = read_csv_output(path).expect("Failed to read CSV file");

    assert_eq!(actual.len(), expected.len(), "CSV record count mismatch:\n  Expected: {} records\n  Actual: {} records", expected.len(), actual.len());

    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        assert_eq!(a, e, "CSV record mismatch at index {i}:\n  Expected: {e:?}\n  Actual: {a:?}");
    }
}

/// Assert that a JSON Lines file contains the expected records.
///
/// # Panics
///
/// Panics if the assertion fails.
///
/// # Example
///
/// ```no_run
/// use rustflow::testing::assert_jsonl_equals;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, PartialEq, Serialize, Deserialize)]
/// struct Record {
///     id: u32,
///     value: String,
/// }
///
/// let expected = vec![
///     Record { id: 1, value: "foo".to_string() },
///     Record { id: 2, value: "bar".to_string() },
/// ];
///
/// assert_jsonl_equals("output.jsonl", &expected);
/// ```
#[cfg(feature = "io-jsonl")]
pub fn assert_jsonl_equals<T, P>(path: P, expected: &[T])
where
    T: serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
    P: AsRef<Path>,
{
    let actual: Vec<T> = read_jsonl_output(path).expect("Failed to read JSONL file");

    assert_eq!(actual.len(), expected.len(), "JSONL record count mismatch:\n  Expected: {} records\n  Actual: {} records", expected.len(), actual.len());

    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        assert_eq!(a, e, "JSONL record mismatch at index {i}:\n  Expected: {e:?}\n  Actual: {a:?}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_temp_file_path() {
        let temp = TempFilePath::new().unwrap();
        assert!(temp.path().exists());
    }

    #[test]
    fn test_temp_file_path_with_extension() {
        let temp = TempFilePath::with_extension("csv").unwrap();
        assert_eq!(temp.path().extension().unwrap(), "csv");
    }

    #[test]
    fn test_temp_dir_path() {
        let temp_dir = TempDirPath::new().unwrap();
        assert!(temp_dir.path().exists());
        assert!(temp_dir.path().is_dir());
    }

    #[test]
    fn test_temp_dir_file_path() {
        let temp_dir = TempDirPath::new().unwrap();
        let file_path = temp_dir.file_path("test.txt");
        assert!(file_path.starts_with(temp_dir.path()));
        assert!(file_path.ends_with("test.txt"));
    }

    #[cfg(feature = "io-jsonl")]
    #[test]
    fn test_mock_jsonl_file() {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct TestRecord {
            id: u32,
            value: String,
        }

        let data = vec![
            TestRecord {
                id: 1,
                value: "foo".to_string(),
            },
            TestRecord {
                id: 2,
                value: "bar".to_string(),
            },
        ];

        let temp_file = mock_jsonl_file(&data).unwrap();
        let read_data: Vec<TestRecord> = read_jsonl_output(temp_file.path()).unwrap();

        assert_eq!(read_data, data);
    }

    #[cfg(feature = "io-csv")]
    #[test]
    fn test_mock_csv_file() {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct TestRecord {
            name: String,
            age: u32,
        }

        let data = vec![
            TestRecord {
                name: "Alice".to_string(),
                age: 30,
            },
            TestRecord {
                name: "Bob".to_string(),
                age: 25,
            },
        ];

        let temp_file = mock_csv_file(&data, true).unwrap();
        let read_data: Vec<TestRecord> = read_csv_output(temp_file.path()).unwrap();

        assert_eq!(read_data, data);
    }
}
