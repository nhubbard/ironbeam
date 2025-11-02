//! Example demonstrating pluggable compressed I/O with automatic detection.
//!
//! This example shows:
//! - Transparent compression/decompression based on file extension
//! - Custom codec implementation and registration
//! - Working with compressed data in pipelines
//!
//! Run with: cargo run --example compressed_io --features compression-gzip

use rustflow::io::compression::{auto_detect_reader, auto_detect_writer, register_codec, CompressionCodec};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
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
    fn name(&self) -> &str {
        "custom"
    }

    fn extensions(&self) -> &[&str] {
        &[".custom"]
    }

    fn magic_bytes(&self) -> Option<&[u8]> {
        None
    }

    fn wrap_reader_dyn(&self, reader: Box<dyn Read>) -> std::io::Result<Box<dyn Read>> {
        println!("🔧 Custom codec: wrapping reader");
        Ok(reader)
    }

    fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> std::io::Result<Box<dyn Write>> {
        println!("🔧 Custom codec: wrapping writer");
        Ok(writer)
    }
}

fn main() -> anyhow::Result<()> {
    println!("=== Rustflow Pluggable Compression Demo ===\n");

    // Sample data
    let logs = vec![
        LogEntry {
            timestamp: 1234567890,
            level: "INFO".to_string(),
            message: "Application started".to_string(),
        },
        LogEntry {
            timestamp: 1234567891,
            level: "WARN".to_string(),
            message: "High memory usage detected".to_string(),
        },
        LogEntry {
            timestamp: 1234567892,
            level: "ERROR".to_string(),
            message: "Connection timeout".to_string(),
        },
    ];

    // 1. Write compressed data (automatic detection from .gz extension)
    #[cfg(feature = "compression-gzip")]
    {
        println!("📝 Writing compressed data to logs.jsonl.gz...");
        let file = std::fs::File::create("logs.jsonl.gz")?;
        let mut writer = auto_detect_writer(file, "logs.jsonl.gz")?;

        for entry in &logs {
            serde_json::to_writer(&mut writer, entry)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        drop(writer); // Ensure encoder finishes before reading

        let size = std::fs::metadata("logs.jsonl.gz")?.len();
        println!("✅ Compressed file size: {} bytes\n", size);

        // 2. Read compressed data (automatic detection)
        println!("📖 Reading compressed data from logs.jsonl.gz...");
        let file = std::fs::File::open("logs.jsonl.gz")?;
        let reader = auto_detect_reader(file, "logs.jsonl.gz")?;
        let buf_reader = std::io::BufReader::new(reader);

        let mut loaded_logs = Vec::new();
        for line in std::io::BufRead::lines(buf_reader) {
            let line = line?;
            if !line.trim().is_empty() {
                let entry: LogEntry = serde_json::from_str(&line)?;
                loaded_logs.push(entry);
            }
        }

        println!("✅ Read {} log entries", loaded_logs.len());
        for (i, entry) in loaded_logs.iter().enumerate() {
            println!("   [{}] {:?}", i + 1, entry);
        }
        println!();

        std::fs::remove_file("logs.jsonl.gz")?;
    }

    // 3. Register and use custom codec
    println!("🔌 Registering custom codec...");
    register_codec(Arc::new(CustomCodec));
    println!("✅ Custom codec registered\n");

    println!("📝 Writing with custom codec to logs.jsonl.custom...");
    let file = std::fs::File::create("logs.jsonl.custom")?;
    let mut writer = auto_detect_writer(file, "logs.jsonl.custom")?;

    for entry in &logs {
        serde_json::to_writer(&mut writer, entry)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    println!("✅ Written with custom codec\n");

    println!("📖 Reading with custom codec from logs.jsonl.custom...");
    let file = std::fs::File::open("logs.jsonl.custom")?;
    let reader = auto_detect_reader(file, "logs.jsonl.custom")?;
    let buf_reader = std::io::BufReader::new(reader);

    let mut custom_logs = Vec::new();
    for line in std::io::BufRead::lines(buf_reader) {
        let line = line?;
        if !line.trim().is_empty() {
            let entry: LogEntry = serde_json::from_str(&line)?;
            custom_logs.push(entry);
        }
    }
    println!("✅ Read {} log entries with custom codec\n", custom_logs.len());

    std::fs::remove_file("logs.jsonl.custom")?;

    println!("=== Demo Complete ===");
    println!("\nKey Features Demonstrated:");
    println!("• Automatic compression detection from file extensions");
    println!("• Transparent decompression on read");
    println!("• Pluggable codec system - users can add their own algorithms");
    println!("• Zero-copy architecture with trait objects");

    Ok(())
}
