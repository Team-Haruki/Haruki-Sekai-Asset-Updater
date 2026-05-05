use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde::Serialize;

use crate::core::config::{ChartHashConfig, GitSigningFormat, RetryConfig};
use crate::core::download_records::DownloadRecord;
use crate::core::errors::GitSyncError;
use crate::core::retry::retry_sync;

#[derive(Debug, Clone, Serialize)]
pub struct ChartHashSyncResult {
    pub output_file: PathBuf,
    pub commit_oid: Option<String>,
    pub branch: Option<String>,
    pub pushed: bool,
}

pub fn collect_chart_hashes(downloaded_assets: &DownloadRecord) -> BTreeMap<String, String> {
    downloaded_assets
        .iter()
        .filter(|(key, _)| key.starts_with("music/music_score"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

pub fn chart_hash_output_path(repository_dir: &Path, region_name: &str) -> PathBuf {
    repository_dir.join(format!("{region_name}_chart_hashes.json"))
}

pub fn sync_chart_hashes(
    config: &ChartHashConfig,
    region_name: &str,
    downloaded_assets: &DownloadRecord,
    proxy: Option<&str>,
    retry: &RetryConfig,
    dry_run: bool,
) -> Result<Option<ChartHashSyncResult>, GitSyncError> {
    if !config.enabled {
        return Ok(None);
    }

    let repository_dir = config
        .repository_dir
        .as_ref()
        .map(PathBuf::from)
        .ok_or(GitSyncError::MissingRepositoryDir)?;
    let chart_hashes = collect_chart_hashes(downloaded_assets);
    if chart_hashes.is_empty() {
        return Ok(None);
    }

    let output_file = chart_hash_output_path(&repository_dir, region_name);
    if dry_run {
        return Ok(Some(ChartHashSyncResult {
            output_file,
            commit_oid: None,
            branch: None,
            pushed: false,
        }));
    }

    let relative_output = PathBuf::from(format!("{region_name}_chart_hashes.json"));

    if let Some(parent) = output_file.parent() {
        std::fs::create_dir_all(parent).map_err(|source| GitSyncError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let data =
        sonic_rs::to_vec_pretty(&chart_hashes).map_err(|source| GitSyncError::Serialize {
            path: output_file.clone(),
            source,
        })?;
    std::fs::write(&output_file, data).map_err(|source| GitSyncError::Io {
        path: output_file.clone(),
        source,
    })?;

    let branch = current_branch(&repository_dir)?;

    if !path_has_changes(&repository_dir, &relative_output)? {
        return Ok(Some(ChartHashSyncResult {
            output_file,
            commit_oid: None,
            branch: Some(branch),
            pushed: false,
        }));
    }

    stage_path(&repository_dir, &relative_output)?;
    let message = format!("Updated {region_name} server chart hashes");
    let author_name = config
        .username
        .as_deref()
        .unwrap_or("Haruki Automation Git Bot");
    let author_email = config.email.as_deref().unwrap_or("no-reply@seiunx.com");
    commit(&repository_dir, config, &message, author_name, author_email)?;
    let oid = head_oid(&repository_dir)?;

    let refspec = format!("refs/heads/{branch}:refs/heads/{branch}");
    push_origin(&repository_dir, &refspec, config, proxy, retry)?;

    Ok(Some(ChartHashSyncResult {
        output_file,
        commit_oid: Some(oid),
        branch: Some(branch),
        pushed: true,
    }))
}

fn run_git(workdir: &Path, args: &[&str], action: &str) -> Result<Output, GitSyncError> {
    let mut command = Command::new("git");
    command.arg("-C").arg(workdir).args(args);
    let output = command.output().map_err(|e| GitSyncError::GitCommand {
        action: action.to_string(),
        message: format!("failed to spawn git: {e}"),
    })?;
    if !output.status.success() {
        return Err(GitSyncError::GitCommand {
            action: action.to_string(),
            message: command_failure_message(&output),
        });
    }
    Ok(output)
}

fn command_failure_message(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let detail = stderr.trim();
    if detail.is_empty() {
        output.status.to_string()
    } else {
        detail.to_string()
    }
}

fn current_branch(workdir: &Path) -> Result<String, GitSyncError> {
    let output = run_git(
        workdir,
        &["symbolic-ref", "--short", "HEAD"],
        "symbolic-ref",
    )
    .map_err(|_| GitSyncError::MissingBranch)?;
    let branch = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if branch.is_empty() {
        return Err(GitSyncError::MissingBranch);
    }
    Ok(branch)
}

fn path_has_changes(workdir: &Path, relative_output: &Path) -> Result<bool, GitSyncError> {
    let mut command = Command::new("git");
    command
        .arg("-C")
        .arg(workdir)
        .args(["status", "--porcelain", "--"])
        .arg(relative_output);
    let output = command.output().map_err(|e| GitSyncError::GitCommand {
        action: "status".to_string(),
        message: format!("failed to spawn git: {e}"),
    })?;
    if !output.status.success() {
        return Err(GitSyncError::GitCommand {
            action: "status".to_string(),
            message: command_failure_message(&output),
        });
    }
    Ok(!output.stdout.is_empty())
}

fn stage_path(workdir: &Path, relative_output: &Path) -> Result<(), GitSyncError> {
    let mut command = Command::new("git");
    command
        .arg("-C")
        .arg(workdir)
        .args(["add", "--"])
        .arg(relative_output);
    let output = command.output().map_err(|e| GitSyncError::GitCommand {
        action: "add".to_string(),
        message: format!("failed to spawn git: {e}"),
    })?;
    if !output.status.success() {
        return Err(GitSyncError::GitCommand {
            action: "add".to_string(),
            message: command_failure_message(&output),
        });
    }
    Ok(())
}

fn commit(
    workdir: &Path,
    config: &ChartHashConfig,
    message: &str,
    author_name: &str,
    author_email: &str,
) -> Result<(), GitSyncError> {
    let mut command = Command::new("git");
    command.arg("-C").arg(workdir);

    if config.sign_commits {
        let (format, program_key) = match config.signing_format {
            GitSigningFormat::Gpg => ("openpgp", "gpg.program"),
            GitSigningFormat::Ssh => ("ssh", "gpg.ssh.program"),
        };
        command.arg("-c").arg(format!("gpg.format={format}"));
        if let Some(program) = config.signing_program.as_deref().filter(|s| !s.is_empty()) {
            command.arg("-c").arg(format!("{program_key}={program}"));
        }
        if let Some(key) = config.signing_key.as_deref().filter(|s| !s.is_empty()) {
            command.arg("-c").arg(format!("user.signingkey={key}"));
        }
        command.arg("commit").arg("-S");
    } else {
        command.arg("commit");
    }

    command
        .arg("-m")
        .arg(message)
        .env("GIT_AUTHOR_NAME", author_name)
        .env("GIT_AUTHOR_EMAIL", author_email)
        .env("GIT_COMMITTER_NAME", author_name)
        .env("GIT_COMMITTER_EMAIL", author_email);

    let output = command.output().map_err(|e| GitSyncError::GitCommand {
        action: "commit".to_string(),
        message: format!("failed to spawn git: {e}"),
    })?;
    if !output.status.success() {
        return Err(GitSyncError::GitCommand {
            action: "commit".to_string(),
            message: command_failure_message(&output),
        });
    }
    Ok(())
}

fn head_oid(workdir: &Path) -> Result<String, GitSyncError> {
    let output = run_git(workdir, &["rev-parse", "HEAD"], "rev-parse")?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn push_origin(
    workdir: &Path,
    refspec: &str,
    config: &ChartHashConfig,
    proxy: Option<&str>,
    retry: &RetryConfig,
) -> Result<(), GitSyncError> {
    let remote_url_output = Command::new("git")
        .arg("-C")
        .arg(workdir)
        .args(["remote", "get-url", "origin"])
        .output()
        .map_err(|e| GitSyncError::GitCommand {
            action: "remote get-url".to_string(),
            message: format!("failed to spawn git: {e}"),
        })?;
    if !remote_url_output.status.success() {
        let stderr = String::from_utf8_lossy(&remote_url_output.stderr);
        if stderr.contains("No such remote") || stderr.contains("not a remote") {
            return Err(GitSyncError::MissingOrigin);
        }
        return Err(GitSyncError::GitCommand {
            action: "remote get-url".to_string(),
            message: command_failure_message(&remote_url_output),
        });
    }
    let remote_url = String::from_utf8_lossy(&remote_url_output.stdout)
        .trim()
        .to_string();

    let push_target = match (config.username.as_deref(), config.password.as_deref()) {
        (Some(user), Some(pass)) if !pass.is_empty() => {
            inject_credentials(&remote_url, user, pass)?
        }
        _ => remote_url,
    };

    retry_sync(
        retry,
        "git push",
        |_| {
            let mut command = Command::new("git");
            command.arg("-C").arg(workdir);
            if let Some(proxy_url) = proxy.filter(|p| !p.is_empty()) {
                command.arg("-c").arg(format!("http.proxy={proxy_url}"));
            }
            command
                .args(["push", &push_target, refspec])
                .env("GIT_TERMINAL_PROMPT", "0");
            let output = command.output().map_err(|e| GitSyncError::GitCommand {
                action: "push".to_string(),
                message: format!("failed to spawn git: {e}"),
            })?;
            if !output.status.success() {
                return Err(GitSyncError::GitCommand {
                    action: "push".to_string(),
                    message: command_failure_message(&output),
                });
            }
            Ok(())
        },
        is_retryable_git_error,
    )
}

fn inject_credentials(url: &str, username: &str, password: &str) -> Result<String, GitSyncError> {
    let (scheme, rest) = if let Some(rest) = url.strip_prefix("https://") {
        ("https://", rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        ("http://", rest)
    } else {
        return Err(GitSyncError::GitCommand {
            action: "remote get-url".to_string(),
            message: format!("cannot inject credentials into non-HTTP(S) remote URL: {url}"),
        });
    };
    let rest = match rest.find('@') {
        Some(at) if !rest[..at].contains('/') => &rest[at + 1..],
        _ => rest,
    };
    Ok(format!(
        "{scheme}{}:{}@{rest}",
        pct_encode(username),
        pct_encode(password)
    ))
}

fn pct_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

fn is_retryable_git_error(err: &GitSyncError) -> bool {
    match err {
        GitSyncError::GitCommand { action, message } if action == "push" => {
            const TRANSIENT: &[&str] = &[
                "Could not resolve host",
                "Connection refused",
                "Connection reset",
                "Connection timed out",
                "Operation timed out",
                "Failed to connect",
                "could not read from remote",
                "unable to access",
                "SSL_ERROR",
                "TLS connection",
                "early EOF",
                "RPC failed",
                "HTTP/2 stream",
                "transfer closed",
            ];
            TRANSIENT.iter().any(|needle| message.contains(needle))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    use tempfile::tempdir;

    use crate::core::config::{ChartHashConfig, RetryConfig};

    use super::{chart_hash_output_path, collect_chart_hashes, sync_chart_hashes};

    fn run(args: &[&str], cwd: &Path) {
        let output = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .expect("failed to run git");
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn create_repo_with_remote() -> (tempfile::TempDir, std::path::PathBuf, String) {
        let temp = tempdir().unwrap();
        let remote_dir = temp.path().join("remote.git");
        let repo_dir = temp.path().join("repo");
        fs::create_dir_all(&repo_dir).unwrap();

        run(
            &["init", "--bare", remote_dir.to_str().unwrap()],
            temp.path(),
        );
        run(
            &[
                "init",
                "--initial-branch=master",
                repo_dir.to_str().unwrap(),
            ],
            temp.path(),
        );
        run(&["config", "user.name", "tester"], &repo_dir);
        run(&["config", "user.email", "tester@example.com"], &repo_dir);
        run(&["config", "commit.gpgsign", "false"], &repo_dir);

        fs::write(repo_dir.join("README.md"), "init").unwrap();
        run(&["add", "README.md"], &repo_dir);
        run(&["commit", "-m", "init"], &repo_dir);
        run(
            &["remote", "add", "origin", remote_dir.to_str().unwrap()],
            &repo_dir,
        );
        run(&["push", "origin", "master"], &repo_dir);

        (temp, repo_dir, remote_dir.to_string_lossy().to_string())
    }

    #[test]
    fn collect_chart_hashes_filters_music_score_entries() {
        let mut record = BTreeMap::new();
        record.insert("music/music_score_001".to_string(), "a".to_string());
        record.insert("music/other".to_string(), "b".to_string());

        let result = collect_chart_hashes(&record);
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("music/music_score_001"));
    }

    #[test]
    fn dry_run_returns_planned_output_path() {
        let temp = tempdir().unwrap();
        let config = ChartHashConfig {
            enabled: true,
            repository_dir: Some(temp.path().to_string_lossy().into_owned()),
            ..ChartHashConfig::default()
        };
        let mut record = BTreeMap::new();
        record.insert("music/music_score_001".to_string(), "hash".to_string());

        let result = sync_chart_hashes(&config, "jp", &record, None, &RetryConfig::default(), true)
            .unwrap()
            .unwrap();
        assert_eq!(
            result.output_file,
            chart_hash_output_path(temp.path(), "jp")
        );
        assert!(!result.pushed);
    }

    #[test]
    fn sync_chart_hashes_commits_and_pushes_via_cli() {
        let (_temp, repo_dir, remote_dir) = create_repo_with_remote();

        let config = ChartHashConfig {
            enabled: true,
            repository_dir: Some(repo_dir.to_string_lossy().into_owned()),
            username: Some("tester".to_string()),
            email: Some("tester@example.com".to_string()),
            ..ChartHashConfig::default()
        };
        let mut record = BTreeMap::new();
        record.insert("music/music_score_001".to_string(), "hash".to_string());

        let result =
            sync_chart_hashes(&config, "jp", &record, None, &RetryConfig::default(), false)
                .unwrap()
                .unwrap();
        assert!(result.pushed);
        let oid = result.commit_oid.expect("commit_oid");

        let remote_head = Command::new("git")
            .args([
                "--git-dir",
                remote_dir.as_str(),
                "rev-parse",
                "refs/heads/master",
            ])
            .output()
            .expect("rev-parse");
        assert!(remote_head.status.success());
        assert_eq!(String::from_utf8_lossy(&remote_head.stdout).trim(), oid);
        assert!(repo_dir.join("jp_chart_hashes.json").exists());
    }
}
