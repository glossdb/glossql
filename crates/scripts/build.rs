//! Stage the model during the build: weights are copied from the
//! tabicl-candle checkout into the target directory beside the binaries
//! (with the pinned DIGESTS from that repo's fixtures), so the built
//! artifact carries them — never git, never a runtime copy, and a
//! container bakes exactly what the build staged. What lands in the
//! staged directory is hashed against the pinned digests right after
//! the copy — the copy event is the boundary where verification
//! belongs (the runtime loader does not hash), and a failed or stale
//! copy fails the build instead of serving old weights. A checkout
//! without converted weights still builds, with a warning: the runtime
//! then requires the workspace to carry its own `weights/`.
//!
//! With the `embed-weights` feature the regressor is baked into the
//! binary instead: the safetensors bytes are verified here against the
//! pinned digest — the one moment the bytes are fixed — and an include
//! is generated for lib.rs. A release artifact is then one file; that
//! build refuses to proceed without the weights.

use std::path::{Path, PathBuf};

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let checkout = manifest.join("../../../tabicl-candle");
    let src = checkout.join("weights");
    let digests = checkout.join("fixtures/DIGESTS");
    println!("cargo:rerun-if-changed={}", src.display());
    println!("cargo:rerun-if-changed={}", digests.display());

    let embed = std::env::var_os("CARGO_FEATURE_EMBED_WEIGHTS").is_some();
    let complete = src.join("tabicl-regressor.safetensors").exists() && digests.exists();
    if embed {
        if !complete {
            panic!(
                "embed-weights: no converted weights at {} — the release \
                 artifact carries the regressor, so this build cannot \
                 proceed without it (run tabicl-candle's \
                 verify/python/convert_weights.py)",
                src.display()
            );
        }
        embed_regressor(&src, &digests);
    }
    if !complete {
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
    verify_staged(&dst, &digests);
}

/// Hash every safetensors present in the staged directory against the
/// checkout's pinned DIGESTS (the source of truth, not the staged
/// copy). Catches what the silent `fs::copy` loop can leave behind: a
/// failed or partial copy, or a stale file from an earlier build whose
/// source is gone — the runtime loader does not hash, so the copy
/// event is where a mismatch must fail.
fn verify_staged(dst: &Path, digests: &Path) {
    let pinned: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(digests).expect("read DIGESTS"))
            .expect("parse DIGESTS");
    for name in ["regressor", "classifier"] {
        let staged = dst.join(format!("tabicl-{name}.safetensors"));
        if !staged.exists() {
            continue; // never staged (e.g. classifier not converted); resolve_dir decides
        }
        let expected = pinned[name]["sha256"]
            .as_str()
            .unwrap_or_else(|| panic!("DIGESTS pins a {name} sha256"));
        let actual = sha256_hex(&staged);
        assert_eq!(
            actual,
            expected,
            "staged {name} digest mismatch at {} — stale or partial copy; \
             re-run tabicl-candle's verify/python/convert_weights.py or \
             delete the staged weights and rebuild",
            staged.display()
        );
    }
}

fn sha256_hex(path: &Path) -> String {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Verify the regressor against the pinned digest and generate the
/// include lib.rs compiles under `embed-weights`. Verification lives
/// here because the embedded bytes can never change after this build —
/// the runtime check that guards a workspace's copyable weights/ has
/// nothing left to catch.
fn embed_regressor(src: &Path, digests: &Path) {
    let st = src.join("tabicl-regressor.safetensors");
    let config = src.join("tabicl-regressor.config.json");
    let pinned: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(digests).expect("read DIGESTS"))
            .expect("parse DIGESTS");
    let expected = pinned["regressor"]["sha256"]
        .as_str()
        .expect("DIGESTS pins a regressor sha256");
    let actual = sha256_hex(&st);
    assert_eq!(
        actual,
        expected,
        "embed-weights: regressor digest mismatch at {} — re-run \
         tabicl-candle's verify/python/convert_weights.py or restore \
         the pinned weights",
        st.display()
    );

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let generated = format!(
        "pub static REGRESSOR_SAFETENSORS: &[u8] = include_bytes!({:?});\n\
         pub static REGRESSOR_CONFIG: &str = include_str!({:?});\n",
        st.canonicalize().expect("canonicalize safetensors path"),
        config.canonicalize().expect("canonicalize config path"),
    );
    std::fs::write(out.join("embedded_weights.rs"), generated).expect("write embed include");
}
