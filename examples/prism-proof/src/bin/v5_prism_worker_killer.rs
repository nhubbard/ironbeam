//! Test-only process wrapper that kills a live Prism worker at a residual cursor.

use std::path::PathBuf;
use std::process::{Command, ExitCode};
use std::time::{Duration, Instant};

fn run() -> anyhow::Result<()> {
    let worker = std::env::var_os("IRONBEAM_V5_SUPERVISOR_WORKER")
        .ok_or_else(|| anyhow::anyhow!("missing worker binary"))?;
    let trigger = PathBuf::from(
        std::env::var_os("IRONBEAM_V5_SUPERVISOR_TRIGGER")
            .ok_or_else(|| anyhow::anyhow!("missing restart trigger"))?,
    );
    let killed = PathBuf::from(
        std::env::var_os("IRONBEAM_V5_SUPERVISOR_KILLED")
            .ok_or_else(|| anyhow::anyhow!("missing kill witness"))?,
    );
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    let mut first = Command::new(&worker).args(&args).spawn()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if trigger.exists() {
            let first_pid = first.id();
            first.kill()?;
            let pending = killed.with_extension("pending");
            std::fs::write(&pending, first_pid.to_string())?;
            std::fs::rename(pending, &killed)?;
            first.wait()?;
            anyhow::bail!("worker {first_pid} terminated after residual source cursor");
        }
        if let Some(status) = first.try_wait()? {
            anyhow::bail!("first worker exited before restart trigger: {status}");
        }
        if Instant::now() >= deadline {
            first.kill()?;
            first.wait()?;
            anyhow::bail!("first worker did not reach residual source cursor");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Prism worker killer: {error:#}");
            ExitCode::FAILURE
        }
    }
}
