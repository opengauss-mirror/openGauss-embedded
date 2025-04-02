#![allow(warnings, unused)]
use std::process::Command;
use std::env;

fn main() {
    println!("pre build ...");
    let pwd = Command::new("pwd").output().expect("get pwd error");
    println!("[INFO]: {}", String::from_utf8_lossy(&pwd.stdout));
    println!("cargo:rerun-if-changed=build.rs");
    // 以下路径要根据IntarkDB编译的具体版本路径修改
    println!("cargo:rustc-link-search=native={}/../../../../../output/release/lib/", String::from_utf8_lossy(&pwd.stdout).replace("\n", ""));
}

