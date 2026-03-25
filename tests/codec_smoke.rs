use std::fs;
use std::path::{Path, PathBuf};

use haruki_sekai_asset_updater::core::codec::{
    codec_summary, decode_hca_to_wav, export_acb, export_usm, read_usm_metadata, CODEC_BACKEND,
};
use tempfile::tempdir;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn sample_path(name: &str) -> PathBuf {
    repo_root().join("tests").join("files").join(name)
}

fn sha256_hex(path: &Path) -> String {
    use std::io::Read;
    use sha2::Digest;

    let mut file = fs::File::open(path).unwrap();
    let mut hasher = sha2::Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = file.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    hex::encode(hasher.finalize())
}

fn frozen_hash(rel_path: &str) -> &'static str {
    match rel_path {
        "usm/0703.m2v" => "28392362da3cc9f837cbf7db160c423e00b7edc6d1fa3baad4e6252455db5804",
        "acb/se_0126_01.hca" => "9e4f11119803d743191fab904933e4c8a4a79229310592c3368d57f6f1d8c9fe",
        "acb/se_0126_01_BGM.hca" => {
            "34026ca6dff11f4fbba14c82c104b245cce51466b684e97b4f6798d3b294261c"
        }
        "acb/se_0126_01_SCREEN.hca" => {
            "9e4f11119803d743191fab904933e4c8a4a79229310592c3368d57f6f1d8c9fe"
        }
        "acb/se_0126_01_VR.hca" => {
            "9e4f11119803d743191fab904933e4c8a4a79229310592c3368d57f6f1d8c9fe"
        }
        _ => panic!("missing frozen hash for {rel_path}"),
    }
}

#[test]
fn codec_summary_reports_published_backend() {
    let summary = codec_summary();
    assert_eq!(summary.backend, CODEC_BACKEND);
}

#[test]
fn sample_usm_matches_frozen_baseline_if_present() {
    let input = sample_path("0703.usm");
    if !input.exists() {
        return;
    }

    let output_dir = tempdir().unwrap();
    let files = export_usm(&input, output_dir.path()).unwrap();
    assert!(!files.is_empty());

    let output = output_dir.path().join("0703.m2v");
    assert!(output.exists());
    assert_eq!(sha256_hex(&output), frozen_hash("usm/0703.m2v"));

    let metadata = read_usm_metadata(&input).unwrap();
    assert!(metadata.container_filename.is_some());
}

#[test]
fn sample_acb_matches_frozen_baseline_if_present() {
    let input = sample_path("se_0126_01.acb");
    if !input.exists() {
        return;
    }

    let output_dir = tempdir().unwrap();
    let files = export_acb(&input, output_dir.path()).unwrap().unwrap();
    assert_eq!(files.len(), 4);

    for rel in [
        "se_0126_01.hca",
        "se_0126_01_BGM.hca",
        "se_0126_01_SCREEN.hca",
        "se_0126_01_VR.hca",
    ] {
        let output = output_dir.path().join(rel);
        assert!(output.exists(), "missing {}", output.display());
        assert_eq!(sha256_hex(&output), frozen_hash(&format!("acb/{rel}")));
    }
}

#[test]
fn sample_hca_can_decode_to_wav_if_present() {
    let input = sample_path("se_0126_01.acb");
    if !input.exists() {
        return;
    }

    std::thread::Builder::new()
        .name("codec-smoke-hca".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let output_dir = tempdir().unwrap();
            let _ = export_acb(&input, output_dir.path()).unwrap().unwrap();
            let hca = output_dir.path().join("se_0126_01_BGM.hca");
            let wav = output_dir.path().join("se_0126_01_BGM.wav");
            decode_hca_to_wav(&hca, &wav).unwrap();
            assert!(wav.exists());
            let size = fs::metadata(&wav).unwrap().len();
            assert!(size > 44);
        })
        .unwrap()
        .join()
        .unwrap();
}
