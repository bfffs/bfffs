use nix::sys::utsname::uname;

fn main() {
    // Avoid unnecessary re-building.
    println!("cargo:rerun-if-changed=build.rs");

    let utsname = uname().unwrap();
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
