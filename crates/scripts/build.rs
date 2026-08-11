//! Stage the model during the build: weights are copied from the
//! tabicl-candle checkout into the target directory beside the binaries
//! (with the pinned DIGESTS from that repo's fixtures), so the built
//! artifact carries them — never git, never a runtime copy, and a
//! container bakes exactly what the build staged. A checkout without
//! converted weights still builds, with a warning: the runtime then
//! requires the workspace to carry its own `weights/`.

use std::path::PathBuf;

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let checkout = manifest.join("../../../tabicl-candle");
    let src = checkout.join("weights");
    let digests = checkout.join("fixtures/DIGESTS");
    println!("cargo:rerun-if-changed={}", src.display());
    println!("cargo:rerun-if-changed={}", digests.display());

    if !src.join("tabicl-regressor.safetensors").exists() || !digests.exists() {
        println!(
            "cargo:warning=no converted weights at {} — building without staged weights; \
             the workspace must carry its own weights/",
            src.display()
        );
        return;
    }
    // target/<profile>/build/<crate>-<hash>/out -> target/<profile>
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let Some(profile_dir) = out.ancestors().nth(3) else {
        return;
    };
    let dst = profile_dir.join("weights");
    if std::fs::create_dir_all(&dst).is_err() {
        return;
    }
    for name in [
        "tabicl-regressor.safetensors",
        "tabicl-regressor.config.json",
        "tabicl-classifier.safetensors",
        "tabicl-classifier.config.json",
    ] {
        let s = src.join(name);
        if s.exists() {
            let _ = std::fs::copy(&s, dst.join(name));
        }
    }
    let _ = std::fs::copy(&digests, dst.join("DIGESTS"));
}
