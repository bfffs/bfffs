use std::{
    fmt,
    process::{Child, Command},
    thread::sleep,
    time::{Duration, Instant},
};

use assert_cmd::prelude::*;

mod bfffs;
mod bfffsd;

fn bfffs() -> Command {
    Command::cargo_bin("bfffs").unwrap()
}

fn bfffsd() -> Command {
    Command::cargo_bin("bfffsd").unwrap()
}

/// A wrapper for the bfffsd process that kills on Drop
struct Bfffsd(Child);

impl Drop for Bfffsd {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl From<Child> for Bfffsd {
    fn from(child: Child) -> Self {
        Self(child)
    }
}

/// Skip a test.
// Copied from nix.  Sure would be nice if the test harness knew about "skipped"
// tests as opposed to "passed" or "failed".
#[macro_export]
macro_rules! skip {
    ($($reason: expr),+) => {
        use ::std::io::{self, Write};

        let stderr = io::stderr();
        let mut handle = stderr.lock();
        writeln!(handle, $($reason),+).unwrap();
        return;
    }
}

/// Skip the test if we don't have the ability to mount fuse file systems.
// Copied from nix.
#[cfg(target_os = "freebsd")]
#[macro_export]
macro_rules! require_fusefs {
    ($name: expr) => {
        use ::sysctl::CtlValue;
        use nix::unistd::Uid;

        if (!Uid::current().is_root() &&
            CtlValue::Int(0) == ::sysctl::value("vfs.usermount").unwrap()) ||
            !::std::path::Path::new("/dev/fuse").exists()
        {
            skip!(
                "{} requires the ability to mount fusefs. Skipping test.",
                $name
            );
        }
    };
}

#[derive(Clone, Copy, Debug)]
pub struct WaitForError;

impl fmt::Display for WaitForError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "timeout waiting for condition")
    }
}

impl std::error::Error for WaitForError {}

/// Wait for a limited amount of time for the given condition to be true.
fn waitfor<C>(timeout: Duration, condition: C) -> Result<(), WaitForError>
where
    C: Fn() -> bool,
{
    let start = Instant::now();
    loop {
        if condition() {
            break Ok(());
        }
        if start.elapsed() > timeout {
            break (Err(WaitForError));
        }
        sleep(Duration::from_millis(50));
    }
}