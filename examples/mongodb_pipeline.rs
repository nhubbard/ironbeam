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
//! No MongoDB service is available in this crate's CI, so this example first runs a fast (2
//! second timeout) connectivity check and prints a graceful skip message instead of running the
//! demo if no server is reachable, rather than failing (or hanging on the driver's much longer
//! default server-selection timeout).
//!
//! Run with:
//! ```bash
//! cargo run --example mongodb_pipeline --features io-mongodb
//! ```

#[cfg(feature = "io-mongodb")]
use anyhow::Result;

#[cfg(feature = "io-mongodb")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Person {
    id: i64,
    name: String,
    age: i32,
}

/// Quickly check whether a MongoDB server is reachable at `uri`, using a short
/// `server_selection_timeout` instead of the driver's ~30 second default so an unreachable
/// server (e.g. in CI, where none is running) fails fast.
#[cfg(feature = "io-mongodb")]
fn mongodb_reachable(uri: &str) -> Result<bool> {
    use std::time::Duration;

    let rt = tokio::runtime::Runtime::new()?;
    Ok(rt.block_on(async {
        let Ok(mut options) = mongodb::options::ClientOptions::parse(uri).await else {
            return false;
        };
        options.server_selection_timeout = Some(Duration::from_secs(2));

        let Ok(client) = mongodb::Client::with_options(options) else {
            return false;
        };
        client
            .database("admin")
            .run_command(bson::doc! { "ping": 1 })
            .await
            .is_ok()
    }))
}

#[cfg(feature = "io-mongodb")]
fn main() -> Result<()> {
    use ironbeam::{Pipeline, from_vec, read_mongodb};
    use std::env;

    let uri = env::var("MONGODB_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into());
    let database = "ironbeam_examples";

    println!("🚀 MongoDB Pipeline Example: Person Processing");
    println!("   Connecting to {uri}\n");

    if !mongodb_reachable(&uri)? {
        println!("⚠️  No MongoDB server reachable at {uri}; skipping example.");
        println!("   Start a MongoDB server and re-run, optionally setting MONGODB_URI.");
        return Ok(());
    }

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
