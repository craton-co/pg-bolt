// SPDX-License-Identifier: Apache-2.0

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

fn emit_link_search_if_exists(dir: &Path) -> bool {
    if dir.exists() {
        println!("cargo:rustc-link-search=native={}", dir.display());
        true
    } else {
        false
    }
}

fn parse_cuda_version(name: &OsStr) -> (u32, u32) {
    let s = name.to_string_lossy();
    let digits = s.strip_prefix('v').unwrap_or(&s);
    let mut parts = digits.split('.');
    let major = parts.next().and_then(|p| p.parse::<u32>().ok());
    let minor = parts.next().and_then(|p| p.parse::<u32>().ok());
    match major {
        Some(major) => (major, minor.unwrap_or(0)),
        None => (0, 0),
    }
}

fn discover_cuda_windows(cuda_path: Option<String>) -> bool {
    if let Some(root) = cuda_path {
        if emit_link_search_if_exists(&PathBuf::from(root).join("lib").join("x64")) {
            return true;
        }
    }

    let base_dir = Path::new(r"C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA");
    let Ok(entries) = std::fs::read_dir(base_dir) else {
        return false;
    };

    let mut entries: Vec<_> = entries.flatten().collect();
    entries.sort_by_key(|entry| std::cmp::Reverse(parse_cuda_version(&entry.file_name())));
    for entry in entries {
        if emit_link_search_if_exists(&entry.path().join("lib").join("x64")) {
            return true;
        }
    }
    false
}

fn discover_cuda_unix(cuda_path: Option<String>) -> bool {
    if let Some(root) = cuda_path {
        let path = PathBuf::from(root);
        let lib64 = path.join("lib64");
        if emit_link_search_if_exists(&lib64) {
            emit_link_search_if_exists(&lib64.join("stubs"));
            return true;
        }
        if emit_link_search_if_exists(&path.join("lib")) {
            return true;
        }
    }

    let mut found = false;
    for candidate in &[
        "/usr/local/cuda/lib64",
        "/usr/local/cuda/lib64/stubs",
        "/usr/local/cuda/lib",
        "/usr/lib/x86_64-linux-gnu",
        "/usr/lib/x86_64-linux-gnu/stubs",
        "/opt/cuda/lib64",
        "/opt/cuda/lib64/stubs",
    ] {
        if emit_link_search_if_exists(Path::new(candidate)) {
            found = true;
        }
    }
    found
}

fn main() {
    println!("cargo:rustc-check-cfg=cfg(docsrs)");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_CUDA_STUB");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_CUDA");

    if std::env::var_os("CARGO_FEATURE_CUDA_STUB").is_some()
        || std::env::var_os("CARGO_FEATURE_CUDA").is_none()
    {
        return;
    }

    println!("cargo:rerun-if-env-changed=CUDA_PATH");
    println!("cargo:rerun-if-env-changed=CUDA_HOME");
    let cuda_path = std::env::var("CUDA_PATH")
        .ok()
        .or_else(|| std::env::var("CUDA_HOME").ok())
        .filter(|s| !s.trim().is_empty());

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let cuda_lib_found = if target_os == "windows" {
        discover_cuda_windows(cuda_path)
    } else {
        discover_cuda_unix(cuda_path)
    };

    if !cuda_lib_found {
        println!(
            "cargo:warning=pg_bolt: no CUDA library directory found \
             (checked CUDA_PATH, CUDA_HOME, and standard install paths). Real GPU builds \
             require CUDA Toolkit >= 12; for GPU-less CI, build with \
             `--no-default-features --features cuda-stub`."
        );
    }
}
