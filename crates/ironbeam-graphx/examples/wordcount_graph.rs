//! Print the v5 WordCount graph's Beam protobuf shape.
//! Pass an output path to write the encoded pipeline protobuf.

use ironbeam_core::Pipeline;
use ironbeam_graphx::v5::{translate_v5, validate_beam_contract};
use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("lines", vec!["a b".to_owned(), "a c".to_owned()])?
        .flat_map_named("words", |line| {
            line.split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })?
        .map_named("ones", |word| (word, 1_u64))?
        .group_by_key_named("group")?
        .map_named("counts", |(word, values)| {
            (word, values.into_iter().sum::<u64>())
        })?
        .output("result")?;
    let (graph, _) = pipeline.snapshot()?;
    let translated = translate_v5(&pipeline, "example.test/ironbeam:v5", "wordcount-graph-v5")?;
    validate_beam_contract(&graph, &translated)?;
    let components = translated.pipeline.components.as_ref().unwrap();
    println!("graph identity: {}", translated.identity.graph_fingerprint);
    println!("transforms: {}", components.transforms.len());
    println!("collections: {}", components.pcollections.len());
    println!("coders: {}", components.coders.len());
    for key in translated.chain {
        println!("node: {key}");
    }
    if let Some(path) = std::env::args().nth(1) {
        std::fs::write(path, translated.pipeline.encode_to_vec())?;
    }
    Ok(())
}
