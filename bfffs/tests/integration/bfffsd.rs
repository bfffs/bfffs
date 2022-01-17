use assert_cmd::prelude::*;

use super::bfffsd;

#[test]
fn help() {
    bfffsd().arg("-h").assert().success();
}
