use std::{
    process::{Child, Command},
    thread::sleep,
    time::{Duration, Instant},
};

use assert_cmd::prelude::*;
use thiserror::Error;

pub fn bfffs() -> Command {
    Command::cargo_bin("bfffs").unwrap()
}

pub fn bfffsd() -> Command {
    Command::cargo_bin("bfffsd").unwrap()
}

/// A wrapper for the bfffsd process that kills on Drop
pub struct Bfffsd(Child);

impl Bfffsd {
    // It's actually used by the benchmarks, which are compiled separately.
    #[allow(dead_code)]
    pub fn id(&self) -> u32 {
        self.0.id()
    }
}

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
    () => {
        use nix::unistd::Uid;
        use sysctl::Sysctl as _;

        if (!Uid::current().is_root() &&
            ::sysctl::CtlValue::Int(0) ==
                ::sysctl::Ctl::new(&"vfs.usermount")
                    .unwrap()
                    .value()
                    .unwrap()) ||
            !::std::path::Path::new("/dev/fuse").exists()
        {
            skip!(
                "{} requires the ability to mount fusefs. Skipping test.",
                concat!(::std::module_path!(), "::", function_name!())
            );
        }
    };
}

#[derive(Clone, Copy, Debug, Error)]
#[error("timeout waiting for condition")]
pub struct WaitForError;

/// Wait for a limited amount of time for the given condition to be true.
pub fn waitfor<C>(timeout: Duration, condition: C) -> Result<(), WaitForError>
where
    C: Fn() -> bool,
{
    let start = Instant::now();
    loop {
        if condition() {
            break Ok(());
        }
        if start.elapsed() > timeout {
            break Err(WaitForError);
        }
        sleep(Duration::from_millis(50));
    }
}
