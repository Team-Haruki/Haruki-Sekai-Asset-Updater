use std::fs;

#[test]
fn dockerfile_bundles_haruki_3d_exporter_binary() {
    let dockerfile = fs::read_to_string("Dockerfile").expect("Dockerfile should be readable");

    assert!(
        dockerfile.contains("AS haruki-3d-exporter-builder"),
        "Dockerfile should build Haruki-3D-Exporter in a dedicated stage"
    );
    assert!(
        dockerfile.contains("HARUKI_3D_EXPORTER_REPOSITORY"),
        "Dockerfile should allow CI to choose the exporter repository"
    );
    assert!(
        dockerfile.contains("HARUKI_3D_EXPORTER_BRANCH"),
        "Dockerfile should allow CI to choose the exporter branch or tag"
    );
    assert!(
        dockerfile.contains("/app/bin/Haruki-3D-Exporter"),
        "Dockerfile should install the exporter executable at the config default path"
    );
    assert!(
        dockerfile.contains("FROM mcr.microsoft.com/dotnet/runtime:8.0-bookworm-slim AS dotnet-runtime"),
        "Dockerfile should source the .NET runtime from the official runtime image"
    );
    assert!(
        dockerfile.contains("COPY --from=dotnet-runtime /usr/share/dotnet /usr/share/dotnet"),
        "final image should copy the .NET runtime needed by the exporter"
    );
    assert!(
        dockerfile.contains("COPY --from=haruki-3d-exporter-builder"),
        "final image should copy the exporter output from the exporter build stage"
    );
    assert!(
        !dockerfile.contains("dotnet-runtime-8.0"),
        "final image should not depend on Debian trixie carrying the dotnet runtime package"
    );
}

#[test]
fn example_config_matches_bundled_exporter_path() {
    let config = fs::read_to_string("haruki-asset-configs.example.yaml")
        .expect("example config should be readable");

    assert!(
        config.contains("exporter_path: \"/app/bin/Haruki-3D-Exporter\""),
        "example config should point at the exporter path installed by Dockerfile"
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
