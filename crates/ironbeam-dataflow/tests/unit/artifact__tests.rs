// Unit tests for src/artifact.rs.
use super::*;
use beam_proto::pipeline_v1::{Components, Environment};
use std::collections::HashMap;

fn docker_env() -> Environment {
    Environment {
        urn: urns::ENV_DOCKER.to_string(),
        payload: Vec::new(),
        display_data: Vec::new(),
        capabilities: Vec::new(),
        dependencies: Vec::new(),
        resource_hints: HashMap::new(),
    }
}

#[test]
fn attach_rewrites_every_environment() {
    let mut envs = HashMap::new();
    envs.insert("env_a".to_string(), docker_env());
    envs.insert("env_b".to_string(), docker_env());
    let mut pipeline = Pipeline {
        components: Some(Components {
            environments: envs,
            ..Default::default()
        }),
        ..Default::default()
    };

    let worker = StagedArtifact {
        url: "gs://bucket/job/ironbeam_worker".to_string(),
        sha256: "abc123".to_string(),
    };
    attach_worker_binary(&mut pipeline, &worker);

    let comps = pipeline.components.as_ref().unwrap();
    assert_eq!(comps.environments.len(), 2);
    for env in comps.environments.values() {
        assert_eq!(env.dependencies.len(), 1);
        let dep = &env.dependencies[0];
        assert_eq!(dep.type_urn, urns::ARTIFACT_TYPE_URL);
        assert_eq!(dep.role_urn, urns::ARTIFACT_ROLE_GO_WORKER_BINARY);
        let payload = ArtifactUrlPayload::decode(dep.type_payload.as_slice()).unwrap();
        assert_eq!(payload.url, "gs://bucket/job/ironbeam_worker");
        assert_eq!(payload.sha256, "abc123");
    }
}

#[test]
fn attach_replaces_existing_dependencies() {
    let stale = ArtifactInformation {
        type_urn: urns::ARTIFACT_TYPE_FILE.to_string(),
        type_payload: b"stale".to_vec(),
        role_urn: "old".to_string(),
        role_payload: Vec::new(),
    };
    let mut env = docker_env();
    env.dependencies = vec![stale];
    let mut envs = HashMap::new();
    envs.insert("env_a".to_string(), env);
    let mut pipeline = Pipeline {
        components: Some(Components {
            environments: envs,
            ..Default::default()
        }),
        ..Default::default()
    };
    attach_worker_binary(
        &mut pipeline,
        &StagedArtifact {
            url: "gs://b/w".into(),
            sha256: "x".into(),
        },
    );
    let env = pipeline
        .components
        .as_ref()
        .unwrap()
        .environments
        .values()
        .next()
        .unwrap();
    assert_eq!(env.dependencies.len(), 1);
    assert_eq!(env.dependencies[0].type_urn, urns::ARTIFACT_TYPE_URL);
}

#[test]
fn attach_noop_when_components_missing() {
    let mut pipeline = Pipeline {
        components: None,
        ..Default::default()
    };
    attach_worker_binary(
        &mut pipeline,
        &StagedArtifact {
            url: "gs://b/w".into(),
            sha256: "x".into(),
        },
    );
    assert!(pipeline.components.is_none());
}
