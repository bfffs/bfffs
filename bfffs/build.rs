fn main() {
    // Avoid unnecessary re-building.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/pool_create_parser.lalrpop");

    // Workaround a lalrpop bug
    // https://github.com/lalrpop/lalrpop/issues/892
    println!("cargo::rustc-check-cfg=cfg(rustfmt)");

    lalrpop::process_root().unwrap();
}
