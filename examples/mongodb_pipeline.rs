//! Read, transform, and write documents through the v5 MongoDB extension.
//!
//! Run with `cargo run --example mongodb_pipeline --features io-mongodb`.
//! Set `MONGODB_URI` to choose a server; localhost is used by default.

use ironbeam::v5::{
    DirectRunner, Element, JobConfig, Pipeline, mongodb_sink, mongodb_source, prelude::*,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Person {
    id: i64,
    name: String,
    age: i32,
}

impl Element for Person {
    const TYPE_ID: &'static str = "examples/mongodb-person";
    const CODER_ID: &'static str = "examples/mongodb-person-local";
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uri = std::env::var("MONGODB_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let reachable = runtime.block_on(async {
        let Ok(mut options) = mongodb::options::ClientOptions::parse(&uri).await else {
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
    });
    if !reachable {
        println!("No MongoDB server reachable at {uri}; start one and retry.");
        return Ok(());
    }
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let database = format!("ironbeam_example_{timestamp}");
    let seed = Pipeline::default();
    let input = seed.from_vec(
        "seed-people",
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
    )?;
    DirectRunner.run_sink(
        &mongodb_sink(&input, "write-people", &uri, &database, "people")?,
        &JobConfig::default(),
    )?;

    let processing = Pipeline::default();
    let adults = mongodb_source::<Person>(
        &processing,
        "read-adults",
        &uri,
        &database,
        "people",
        bson::doc! { "age": { "$gte": 18 } },
    )?
    .map_named("uppercase-names", |person| Person {
        id: person.id,
        name: person.name.to_uppercase(),
        age: person.age,
    })?;
    DirectRunner.run_sink(
        &mongodb_sink(&adults, "write-adults", &uri, &database, "adults")?,
        &JobConfig::default(),
    )?;

    let verify = Pipeline::default();
    let mut rows =
        mongodb_source::<Person>(&verify, "verify", &uri, &database, "adults", bson::doc! {})?
            .collect_seq()?;
    rows.sort_by_key(|person| person.id);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "ALICE");
    assert_eq!(rows[1].name, "CAROL");
    for person in rows {
        println!("#{:3} {:10} age {}", person.id, person.name, person.age);
    }
    runtime.block_on(async {
        mongodb::Client::with_uri_str(&uri)
            .await?
            .database(&database)
            .drop()
            .await
    })?;
    Ok(())
}
