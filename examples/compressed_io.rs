//! Example demonstrating pluggable compressed I/O with automatic detection.
//!
//! This example shows:
//! - Transparent compression/decompression based on file extension
//! - Custom codec implementation and registration
//! - Working with compressed data in pipelines
//!
//! Run with: `cargo run --example compressed_io --features compression-gzip`

use anyhow::Result as AnyhowResult;
use ironbeam::io::compression::{
    CompressionCodec, auto_detect_reader, auto_detect_writer, register_codec,
};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_writer};
use std::fs::{File, metadata, remove_file};
use std::io::Result as IOResult;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LogEntry {
    timestamp: u64,
    level: String,
    message: String,
}

// Custom no-op codec for demonstration
struct CustomCodec;

impl CompressionCodec for CustomCodec {
    fn name(&self) -> &'static str {
        "custom"
    }

    fn extensions(&self) -> &[&str] {
        &[".custom"]
    }

    fn magic_bytes(&self) -> Option<&[u8]> {
        None
    }

    fn wrap_reader_dyn(&self, reader: Box<dyn Read>) -> IOResult<Box<dyn Read>> {
        println!("🔧 Custom codec: wrapping reader");
        Ok(reader)
    }

    fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> IOResult<Box<dyn Write>> {
        println!("🔧 Custom codec: wrapping writer");
        Ok(writer)
    }
}

fn main() -> AnyhowResult<()> {
    println!("=== Ironbeam Pluggable Compression Demo ===\n");

    // Sample data
    let logs = vec![
        LogEntry {
            timestamp: 1_234_567_890,
            level: "INFO".to_string(),
            message: "Application started".to_string(),
        },
        LogEntry {
            timestamp: 1_234_567_891,
            level: "WARN".to_string(),
            message: "High memory usage detected".to_string(),
        },
        LogEntry {
            timestamp: 1_234_567_892,
            level: "ERROR".to_string(),
            message: "Connection timeout".to_string(),
        },
    ];

    // 1. Write compressed data (automatic detection from .gz extension)
    #[cfg(feature = "compression-gzip")]
    {
        println!("📝 Writing compressed data to logs.jsonl.gz...");
        let file = File::create("logs.jsonl.gz")?;
        let mut writer = auto_detect_writer(file, "logs.jsonl.gz")?;

        for entry in &logs {
            to_writer(&mut writer, entry)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        drop(writer); // Ensure the encoder finishes before reading

        let size = metadata("logs.jsonl.gz")?.len();
        println!("✅ Compressed file size: {size} bytes\n");

        // 2. Read compressed data (automatic detection)
        println!("📖 Reading compressed data from logs.jsonl.gz...");
        let file = File::open("logs.jsonl.gz")?;
        let reader = auto_detect_reader(file, "logs.jsonl.gz")?;
        let buf_reader = BufReader::new(reader);

        let mut loaded_logs = Vec::new();
        for line in BufRead::lines(buf_reader) {
            let line = line?;
            if !line.trim().is_empty() {
                let entry: LogEntry = from_str(&line)?;
                loaded_logs.push(entry);
            }
        }

        println!("✅ Read {} log entries", loaded_logs.len());
        for (i, entry) in loaded_logs.iter().enumerate() {
            println!("   [{}] {:?}", i + 1, entry);
        }
        println!();

        remove_file("logs.jsonl.gz")?;
    }

    // 3. Register and use custom codec
    println!("🔌 Registering custom codec...");
    register_codec(Arc::new(CustomCodec));
    println!("✅ Custom codec registered\n");

    println!("📝 Writing with custom codec to logs.jsonl.custom...");
    let file = File::create("logs.jsonl.custom")?;
    let mut writer = auto_detect_writer(file, "logs.jsonl.custom")?;

    for entry in &logs {
        to_writer(&mut writer, entry)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    println!("✅ Written with custom codec\n");

    println!("📖 Reading with custom codec from logs.jsonl.custom...");
    let file = File::open("logs.jsonl.custom")?;
    let reader = auto_detect_reader(file, "logs.jsonl.custom")?;
    let buf_reader = BufReader::new(reader);

    let mut custom_logs = Vec::new();
    for line in BufRead::lines(buf_reader) {
        let line = line?;
        if !line.trim().is_empty() {
            let entry: LogEntry = from_str(&line)?;
            custom_logs.push(entry);
        }
    }
    println!(
        "✅ Read {} log entries with custom codec\n",
        custom_logs.len()
    );

    remove_file("logs.jsonl.custom")?;

    println!("=== Demo Complete ===");
    println!("\nKey Features Demonstrated:");
    println!("• Automatic compression detection from file extensions");
    println!("• Transparent decompression on read");
    println!("• Pluggable codec system - users can add their own algorithms");
    println!("• Zero-copy architecture with trait objects");

    Ok(())
}
