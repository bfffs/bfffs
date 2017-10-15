// vim: tw=80

fn main() {
    println!("cargo:rustc-link-search=native=/usr/local/lib");
    println!("cargo:rustc-link-lib=isal");
}
