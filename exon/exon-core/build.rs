// Copyright 2023 WHERE TRUE Technologies.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::path::{Path, PathBuf};

use std::process::Command;

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn wfa() {
    let repo = Path::new("../wfa2");
    let target = repo.join("lib/libwfa.a");

    if target.exists() {
        return;
    }

    if !repo.exists() {
        let clone_status = Command::new("git")
            .arg("clone")
            .arg("git@github.com:smarco/WFA2-lib.git")
            .arg("../wfa2")
            .status()
            .expect("Failed to clone repository");

        if !clone_status.success() {
            panic!("Failed to clone repository");
        }
    }

    let make_status = Command::new("make")
        .arg("lib_wfa")
        .current_dir(repo)
        .status()
        .expect("Failed to build WFA2");

    if !make_status.success() {
        panic!("Failed to build WFA2");
    }

    wfa();

    // The directory of the WFA libraries, added to the search path.
    println!("cargo:rustc-link-search=../wfa2/lib");
    // Link the `wfa-lib` library.
    println!("cargo:rustc-link-lib=wfa");
    // Also link `omp`.
    println!("cargo:rustc-link-lib=omp");
    // Invalidate the built crate whenever the linked library changes.
    println!("cargo:rerun-if-changed=../wfa2/lib/libwfa.a");

    // 2. Generate bindings.

    let bindings = bindgen::Builder::default()
        // Generate bindings for this header file.
        .header("../wfa2/wavefront/wavefront_align.h")
        // Add this directory to the include path to find included header files.
        .clang_arg("-I../wfa2")
        // Generate bindings for all functions starting with `wavefront_`.
        .allowlist_function("wavefront_.*")
        // Generate bindings for all variables starting with `wavefront_`.
        .allowlist_var("wavefront_.*")
        // Invalidate the built crate whenever any of the included header files
        // changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings_wfa.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings_wfa.rs"))
        .expect("Couldn't write bindings!");
}

fn main() {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    wfa();
}
