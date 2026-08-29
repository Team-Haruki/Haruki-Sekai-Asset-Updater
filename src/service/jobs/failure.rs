//! Turning a failure message into a typed job failure.

use crate::core::models::{JobFailure, JobFailureKind};

pub(super) fn classify_failure(message: &str) -> JobFailure {
    let lowered = message.to_lowercase();
    let (kind, retryable) = if lowered.contains("timed out") {
        (JobFailureKind::Timeout, true)
    } else if lowered.contains("cancelled") {
        (JobFailureKind::Cancelled, false)
    } else if lowered.contains("http") || lowered.contains("request") || lowered.contains("status")
    {
        (JobFailureKind::Network, true)
    } else if lowered.contains("decrypt") || lowered.contains("msgpack") || lowered.contains("aes")
    {
        (JobFailureKind::Decode, false)
    } else if lowered.contains("s3 upload")
        || lowered.contains("bucket")
        || lowered.contains("storage")
    {
        (JobFailureKind::Storage, true)
    } else if lowered.contains("git") || lowered.contains("chart hash") {
        (JobFailureKind::GitSync, true)
    } else if lowered.contains("assetstudio")
        || lowered.contains("ffmpeg")
        || lowered.contains("media conversion")
        || lowered.contains("export")
    {
        (JobFailureKind::Export, true)
    } else if lowered.contains("config")
        || lowered.contains("missing")
        || lowered.contains("region")
    {
        (JobFailureKind::Configuration, false)
    } else {
        (JobFailureKind::Internal, false)
    };

    JobFailure {
        kind,
        message: message.to_string(),
        retryable,
        at: chrono::Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Failures are classified by scanning the message for keywords, in a fixed
    /// priority order. This pins that order, including where it gets the answer
    /// wrong: an S3 upload failure whose OpenDAL source mentions an HTTP status
    /// is reported as `Network`, because the http/request/status arm is tested
    /// before the storage arm. The kinds are what the API reports, so the
    /// misread is visible to callers.
    #[test]
    fn failure_classification_follows_keyword_priority() {
        assert_eq!(
            classify_failure("operation timed out").kind,
            JobFailureKind::Timeout
        );
        assert_eq!(
            classify_failure("storage upload failed for provider `s3`").kind,
            JobFailureKind::Storage
        );
        assert_eq!(
            classify_failure(
                "storage upload failed for provider `s3` file `a.png`: \
                 Unexpected (permanent), response: HTTP status 403"
            )
            .kind,
            JobFailureKind::Network,
            "the same storage failure classifies differently once its source \
             mentions HTTP -- keyword priority, not error type, decides"
        );
        assert_eq!(
            classify_failure("something nobody anticipated").kind,
            JobFailureKind::Internal
        );
        assert!(!classify_failure("something nobody anticipated").retryable);
    }
}
