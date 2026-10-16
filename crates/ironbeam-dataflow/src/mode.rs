//! Driver-vs-harness mode detection.
//!
//! The same binary runs on the user's laptop (driver) and inside a Dataflow
//! worker container (harness). Dataflow's container-boot contract passes
//! `--control_endpoint` when starting the worker; its presence in argv is
//! our gate. Mirrors the Go SDK's `sdks/go/container/boot.go` invocation.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Whether the executable submits a job or serves the portable worker.
pub enum Mode {
    /// Submit the pipeline to Dataflow.
    Driver,
    /// Serve the Beam Fn API as a worker.
    Harness,
}

impl Mode {
    /// Portable workers may be launched with only a Provision endpoint,
    /// notably by Beam's process environment in local Prism.
    pub fn detect_portable<I, S>(args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for tok in args {
            let t = tok.as_ref();
            if t == "--control_endpoint"
                || t.starts_with("--control_endpoint=")
                || t == "--provision_endpoint"
                || t.starts_with("--provision_endpoint=")
            {
                return Mode::Harness;
            }
        }
        Mode::Driver
    }

    /// Detect from a stream of argv tokens (skip `argv[0]`).
    pub fn detect<I, S>(args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for tok in args {
            let t = tok.as_ref();
            if t == "--control_endpoint" || t.starts_with("--control_endpoint=") {
                return Mode::Harness;
            }
        }
        Mode::Driver
    }

    /// Detect driver or harness mode from this process's arguments.
    pub fn from_env() -> Self {
        Self::detect(std::env::args().skip(1))
    }
}

#[cfg(test)]
#[path = "../tests/unit/mode__tests.rs"]
mod tests;
