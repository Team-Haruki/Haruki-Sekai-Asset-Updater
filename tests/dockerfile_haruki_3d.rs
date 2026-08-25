use std::fs;

#[test]
fn dockerfile_keeps_haruki_3d_exporter_external() {
    let dockerfile = fs::read_to_string("Dockerfile").expect("Dockerfile should be readable");

    assert!(
        !dockerfile.contains("AS haruki-3d-exporter-builder"),
        "default Docker image should not build Haruki-3D-Exporter"
    );
    assert!(
        !dockerfile.contains("HARUKI_3D_EXPORTER_REPOSITORY"),
        "default Docker image should not clone the exporter repository"
    );
    assert!(
        !dockerfile
            .contains("FROM mcr.microsoft.com/dotnet/runtime:8.0-bookworm-slim AS dotnet-runtime"),
        "default Docker image should not bundle the .NET runtime for exporter use"
    );
    assert!(
        !dockerfile.contains("/app/bin/Haruki-3D-Exporter"),
        "default Docker image should not install an exporter wrapper"
    );
    assert!(
        !dockerfile.contains("COPY --from=haruki-3d-exporter-builder"),
        "default Docker image should not copy exporter output into the updater image"
    );
}

#[test]
fn example_config_uses_external_haruki_3d_exporter_path() {
    let config = fs::read_to_string("haruki-asset-configs.example.yaml")
        .expect("example config should be readable");

    assert!(
        config.contains("enabled: false"),
        "Haruki 3D export should stay disabled by default"
    );
    assert!(
        config.contains("exporter_path: \"/app/haruki-3d/exporter/Haruki-3D-Exporter\""),
        "example config should point at an externally mounted exporter"
    );
    assert!(
        config.contains("work_dir: \"/app/data/3d-work\""),
        "example config should keep transient 3D bundles under /app/data/3d-work"
    );
    assert!(
        config.contains("output_dir: \"/app/data/3d-output\""),
        "example config should keep Engine runtime output under /app/data/3d-output"
    );
    assert!(
        config.contains("cleanup_work_dir_after_success: true"),
        "example config should clean transient 3D bundle work dirs after success"
    );
    assert!(
        config.contains("cleanup_work_dir_after_failure: true"),
        "example config should clean transient 3D bundle work dirs after failure"
    );
}
