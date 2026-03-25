use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::tempdir;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn sample_path(name: &str) -> PathBuf {
    repo_root().join("tests").join("files").join(name)
}

fn write_fake_ffmpeg(script_path: &Path) {
    fs::write(
        script_path,
        "#!/bin/sh\nset -eu\nout=\"\"\nfor arg in \"$@\"; do\n  out=\"$arg\"\ndone\n: > \"$out\"\n",
    )
    .unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(script_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(script_path, perms).unwrap();
    }
}

#[test]
fn usmexport_direct_uses_input_stem_for_mp4_name() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("0703.usm");
    let output_dir = dir.path().join("out");
    let ffmpeg = dir.path().join("fake_ffmpeg.sh");
    fs::write(&input, b"dummy").unwrap();
    write_fake_ffmpeg(&ffmpeg);

    let output = Command::new(env!("CARGO_BIN_EXE_usmexport"))
        .arg("--input")
        .arg(&input)
        .arg("--output-dir")
        .arg(&output_dir)
        .arg("--ffmpeg")
        .arg(&ffmpeg)
        .arg("--direct")
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    assert!(output_dir.join("0703.mp4").exists());
    assert!(!output_dir
        .join("0312_バイオレンストリガー_ゲーム尺.mp4")
        .exists());
}

#[test]
fn usmmeta_exports_pretty_json_for_sample_if_present() {
    let source_usm = sample_path("0703.usm");
    if !source_usm.exists() {
        return;
    }

    let dir = tempdir().unwrap();
    let input = dir.path().join("0703.usm");
    fs::copy(source_usm, &input).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_usmmeta"))
        .arg("--input")
        .arg(&input)
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");

    let metadata_path = dir.path().join("0703.metadata.json");
    let body = fs::read_to_string(metadata_path).unwrap();
    let metadata: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        metadata["input_file"].as_str(),
        Some(input.to_string_lossy().as_ref())
    );
    assert!(metadata["sections"].is_array());
}

#[test]
fn usmexport_extracts_sample_and_converts_with_fake_ffmpeg_if_present() {
    let source_usm = sample_path("0703.usm");
    if !source_usm.exists() {
        return;
    }

    let dir = tempdir().unwrap();
    let input = dir.path().join("0703.usm");
    let output_dir = dir.path().join("out");
    let ffmpeg = dir.path().join("fake_ffmpeg.sh");
    fs::copy(source_usm, &input).unwrap();
    write_fake_ffmpeg(&ffmpeg);

    let output = Command::new(env!("CARGO_BIN_EXE_usmexport"))
        .arg("--input")
        .arg(&input)
        .arg("--output-dir")
        .arg(&output_dir)
        .arg("--ffmpeg")
        .arg(&ffmpeg)
        .arg("--keep-m2v")
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    assert!(output_dir.join("0703.m2v").exists());
    assert!(output_dir.join("0703.mp4").exists());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("extracted "));
    assert!(stdout.contains("converted "));
}
