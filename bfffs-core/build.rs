use nix::sys::utsname::uname;

fn main() {
    // Avoid unnecessary re-building.
    println!("cargo:rerun-if-changed=build.rs");

    let utsname = uname();
    if utsname.sysname() == "FreeBSD" {
        let major = utsname.release()
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
