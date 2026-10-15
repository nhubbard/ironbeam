//! String constants for Beam URNs. Mirrors the URNs defined in
//! `model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/beam_runner_api.proto`
//! under `StandardPTransforms`, `StandardCoders`, `StandardEnvironments`,
//! `StandardRequirements`, and `StandardArtifacts`.
//!
//! Keeping these as plain `&'static str` keeps `graphx` independent of the
//! generated code layout — useful because prost renames enum variants.

// ---- Transforms -----------------------------------------------------------

pub const TRANSFORM_IMPULSE: &str = "beam:transform:impulse:v1";
pub const TRANSFORM_TEST_STREAM: &str = "beam:transform:teststream:v1";
pub const TRANSFORM_SDF_PAIR_WITH_RESTRICTION: &str = "beam:transform:sdf_pair_with_restriction:v1";
pub const TRANSFORM_SDF_SPLIT_AND_SIZE: &str = "beam:transform:sdf_split_and_size_restrictions:v1";
pub const TRANSFORM_SDF_PROCESS_SIZED: &str =
    "beam:transform:sdf_process_sized_element_and_restrictions:v1";
pub const TRANSFORM_PARDO: &str = "beam:transform:pardo:v1";
pub const TRANSFORM_GROUP_BY_KEY: &str = "beam:transform:group_by_key:v1";
pub const TRANSFORM_FLATTEN: &str = "beam:transform:flatten:v1";
pub const TRANSFORM_WINDOW_INTO: &str = "beam:transform:window_into:v1";
pub const TRANSFORM_COMBINE_PER_KEY: &str = "beam:transform:combine_per_key:v1";
pub const TRANSFORM_COMBINE_GLOBALLY: &str = "beam:transform:combine_globally:v1";
pub const TRANSFORM_RESHUFFLE: &str = "beam:transform:reshuffle:v1";

// ---- DoFn (ironbeam-specific) --------------------------------------------

pub const DOFN_IRONBEAM_V5: &str = "beam:dofn:ironbeam:v5";

// ---- Coders ---------------------------------------------------------------

pub const CODER_BYTES: &str = "beam:coder:bytes:v1";
pub const CODER_STRING_UTF8: &str = "beam:coder:string_utf8:v1";
pub const CODER_VARINT: &str = "beam:coder:varint:v1";
pub const CODER_DOUBLE: &str = "beam:coder:double:v1";
pub const CODER_BOOL: &str = "beam:coder:bool:v1";
pub const CODER_ROW: &str = "beam:coder:row:v1";
pub const CODER_KV: &str = "beam:coder:kv:v1";
pub const CODER_ITERABLE: &str = "beam:coder:iterable:v1";
/// Dataflow runner-v2 substitutes this URN for GBK-output iterables so the
/// SDK fetches values via `BeamFnState` instead of inline on the data
/// channel. The harness detects it at plan time; implementing it requires
/// the State API, which is not yet wired.
pub const CODER_STATE_BACKED_ITERABLE: &str = "beam:coder:state_backed_iterable:v1";
pub const CODER_LENGTH_PREFIX: &str = "beam:coder:length_prefix:v1";
pub const CODER_WINDOWED_VALUE: &str = "beam:coder:windowed_value:v1";
pub const CODER_PARAM_WINDOWED_VALUE: &str = "beam:coder:param_windowed_value:v1";
pub const CODER_GLOBAL_WINDOW: &str = "beam:coder:global_window:v1";
pub const CODER_INTERVAL_WINDOW: &str = "beam:coder:interval_window:v1";
pub const CODER_NULLABLE: &str = "beam:coder:nullable:v1";

pub const CODER_IRONBEAM_V5: &str = "beam:coder:ironbeam:element:v5";

// ---- Windowing ------------------------------------------------------------

pub const WINDOW_FN_GLOBAL: &str = "beam:window_fn:global_windows:v1";
pub const WINDOW_FN_FIXED: &str = "beam:window_fn:fixed_windows:v1";
pub const WINDOW_FN_SLIDING: &str = "beam:window_fn:sliding_windows:v1";
pub const WINDOW_FN_SESSIONS: &str = "beam:window_fn:session_windows:v1";

// ---- Environments ---------------------------------------------------------

pub const ENV_DOCKER: &str = "beam:env:docker:v1";
pub const ENV_PROCESS: &str = "beam:env:process:v1";
pub const ENV_EXTERNAL: &str = "beam:env:external:v1";
pub const ENV_DEFAULT: &str = "beam:env:default:v1";

// ---- Artifacts ------------------------------------------------------------

pub const ARTIFACT_TYPE_FILE: &str = "beam:artifact:type:file:v1";
pub const ARTIFACT_TYPE_URL: &str = "beam:artifact:type:url:v1";
pub const ARTIFACT_TYPE_EMBEDDED: &str = "beam:artifact:type:embedded:v1";

pub const ARTIFACT_ROLE_STAGING_TO: &str = "beam:artifact:role:staging_to:v1";
/// The Go SDK's role for its worker binary. We reuse this role for MVP so
/// the stock Go SDK container can exec our Rust binary — the container boot
/// treats the binary opaquely.
pub const ARTIFACT_ROLE_GO_WORKER_BINARY: &str = "beam:artifact:role:go_worker_binary:v1";

// ---- Requirements ---------------------------------------------------------

pub const REQUIREMENT_SDF: &str = "beam:requirement:pardo:splittable_dofn:v1";
pub const REQUIREMENT_STATEFUL: &str = "beam:requirement:pardo:stateful:v1";
pub const REQUIREMENT_FINALIZATION: &str = "beam:requirement:pardo:finalization:v1";
pub const REQUIREMENT_STABLE_INPUT: &str = "beam:requirement:pardo:stable_input:v1";
pub const REQUIREMENT_TIME_SORTED_INPUT: &str = "beam:requirement:pardo:time_sorted_input:v1";
pub const REQUIREMENT_ON_WINDOW_EXPIRATION: &str = "beam:requirement:pardo:on_window_expiration:v1";
