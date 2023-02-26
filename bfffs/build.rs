fn main() {
    // Avoid unnecessary re-building.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/pool_create_parser.lalrpop");

    lalrpop::process_root().unwrap();
}
