use nix::sys::utsname::uname;

fn main() {
    // Avoid unnecessary re-building.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo::rustc-check-cfg=cfg(have_fspacectl)");

    let utsname = uname().unwrap();
    // XXX: this only checks kernel version, so it doesn't work properly in a
    // jail.  Checking userland would be more accurate, but that would pull in
    // bindgen with all its many dependencies.
    if utsname.sysname() == "FreeBSD" {
        let major = utsname.release()
            .to_str()
            .unwrap()
            .split('.')
            .next()
            .unwrap()
            .parse::<i32>()
            .unwrap();
        if major >= 14 {
            println!("cargo:rustc-cfg=have_fspacectl");
        }
    }
}
