//! MongoDB I/O pipeline example (feature: `io-mongodb`).
//!
//! Demonstrates a full pipeline: seed documents into a `people` collection, read them back
//! with a filter via [`ironbeam::read_mongodb`], transform them, and write the results to a
//! second collection via [`ironbeam::PCollection::write_mongodb`].
//!
//! **Requires a running MongoDB server.** By default this connects to
//! `mongodb://localhost:27017`; override with the `MONGODB_URI` environment variable (e.g. to
//! point at a `testcontainers`-managed instance, or a remote server).
//!
//! Run with:
//! ```bash
//! cargo run --example mongodb_pipeline --features io-mongodb
//! ```

use anyhow::Result;

#[cfg(feature = "io-mongodb")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Person {
    id: i64,
    name: String,
    age: i32,
}

#[cfg(feature = "io-mongodb")]
fn main() -> Result<()> {
    use ironbeam::{Pipeline, from_vec, read_mongodb};
    use std::env;

    let uri = env::var("MONGODB_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into());
    let database = "ironbeam_examples";

    println!("🚀 MongoDB Pipeline Example: Person Processing");
    println!("   Connecting to {uri}\n");

    let p = Pipeline::default();

    println!("📥 Seeding the `people` collection...");
    let seed = from_vec(
        &p,
        vec![
            Person {
                id: 1,
                name: "Alice".into(),
                age: 30,
            },
            Person {
                id: 2,
                name: "Bob".into(),
                age: 17,
            },
            Person {
                id: 3,
                name: "Carol".into(),
                age: 45,
            },
            Person {
                id: 4,
                name: "Dave".into(),
                age: 8,
            },
        ],
    );
    let seeded = seed.write_mongodb(&uri, database, "people")?;
    println!("  ✓ Inserted {seeded} people\n");

    println!("📖 Reading + transforming adults (age >= 18)...");
    let adults = read_mongodb::<Person>(
        &p,
        &uri,
        database,
        "people",
        bson::doc! { "age": { "$gte": 18 } },
    )?
    .map(|person: &Person| Person {
        id: person.id,
        name: person.name.to_uppercase(),
        age: person.age,
    });

    println!("💾 Writing results to `adults`...");
    let written = adults.write_mongodb(&uri, database, "adults")?;
    println!("  ✓ Wrote {written} adults\n");

    println!("📊 Final contents of `adults`:");
    let result =
        read_mongodb::<Person>(&p, &uri, database, "adults", bson::doc! {})?.collect_seq()?;
    for person in &result {
        println!("  #{:<3} {:<10} {}", person.id, person.name, person.age);
    }

    println!("\n✅ MongoDB Pipeline Complete!");
    Ok(())
}

#[cfg(not(feature = "io-mongodb"))]
fn main() {
    println!("This example requires the 'io-mongodb' feature.");
    println!("Run with: cargo run --example mongodb_pipeline --features io-mongodb");
}
