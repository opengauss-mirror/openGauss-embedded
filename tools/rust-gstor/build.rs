use std::process::Command;
use std::env;

fn main() {
    println!("pre build ...");
    let pwd = Command::new("pwd").output().expect("get pwd error");
    println!("[INFO]: {}", String::from_utf8_lossy(&pwd.stdout));
    let src_dir = env::var("GSTOR_SRC").unwrap();
    let binarylibs = env::var("BINARYLIBS").unwrap();
    Command::new(&format!("{}/build.sh", src_dir)).arg("-3rd").arg(binarylibs).arg("-m").arg("Debug").spawn().expect("build error");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-link-search=native={}", &format!("{}/output/lib", src_dir));
}