use std::path::{Component, Path, PathBuf};

use crate::PipelineError;

/// Validates an untrusted server-provided bundle path.
pub fn validate_relative_bundle_path(bundle_path: &str) -> Result<&Path, PipelineError> {
    let invalid = |reason: &str| PipelineError::InvalidBundlePath {
        bundle: bundle_path.to_string(),
        reason: reason.to_string(),
    };
    if bundle_path.is_empty() {
        return Err(invalid("path is empty"));
    }
    if bundle_path
        .split('/')
        .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(invalid(
            "empty, current-directory, or parent-directory components are not allowed",
        ));
    }

    let relative = Path::new(bundle_path);
    if relative.is_absolute() {
        return Err(invalid("absolute paths are not allowed"));
    }

    for component in relative.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {
                return Err(invalid("current-directory components are not allowed"))
            }
            Component::ParentDir => {
                return Err(invalid("parent-directory components are not allowed"))
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(invalid("root or prefix components are not allowed"))
            }
        }
    }

    Ok(relative)
}

pub fn raw_bundle_output_path(root: &Path, bundle_path: &str) -> Result<PathBuf, PipelineError> {
    let relative = validate_relative_bundle_path(bundle_path)?;
    let mut path = root.to_path_buf();
    for component in relative.components() {
        if let Component::Normal(value) = component {
            path.push(value);
        }
    }
    if path.extension().and_then(|extension| extension.to_str()) != Some("bundle") {
        path.set_extension("bundle");
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{raw_bundle_output_path, validate_relative_bundle_path};

    #[test]
    fn rejects_paths_that_can_escape_a_trusted_root() {
        for value in ["", "/absolute", "../secret", "a/../secret", "a//b", "./a"] {
            assert!(validate_relative_bundle_path(value).is_err(), "{value}");
        }
    }

    #[test]
    fn appends_the_bundle_extension_to_a_safe_relative_path() {
        let output = raw_bundle_output_path(Path::new("/tmp/root"), "music/short/0001").unwrap();
        assert_eq!(output, Path::new("/tmp/root/music/short/0001.bundle"));
    }
}
