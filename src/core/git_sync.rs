use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use git2::{Cred, IndexAddOption, PushOptions, RemoteCallbacks, Repository, Signature};
use serde::Serialize;

use crate::core::config::{ChartHashConfig, RetryConfig};
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

    let repo = Repository::open(&repository_dir)?;
    let relative_output = if let Ok(relative) = output_file.strip_prefix(&repository_dir) {
        relative.to_path_buf()
    } else if let Some(workdir) = repo.workdir() {
        output_file
            .strip_prefix(workdir)
            .map_err(|_| GitSyncError::MissingWorkdir)?
            .to_path_buf()
    } else {
        return Err(GitSyncError::MissingWorkdir);
    };

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

    let status = repo.status_file(&relative_output)?;
    let branch = repo
        .head()?
        .shorthand()
        .map(str::to_string)
        .ok_or(GitSyncError::MissingBranch)?;

    if status.is_empty() {
        return Ok(Some(ChartHashSyncResult {
            output_file,
            commit_oid: None,
            branch: Some(branch),
            pushed: false,
        }));
    }

    let mut index = repo.index()?;
    index.add_all([relative_output.as_path()], IndexAddOption::DEFAULT, None)?;
    index.write()?;

    let tree_id = index.write_tree()?;
    let tree = repo.find_tree(tree_id)?;
    let head_commit = repo.head().ok().and_then(|head| head.peel_to_commit().ok());

    let signature = Signature::now(
        config
            .username
            .as_deref()
            .unwrap_or("Haruki Automation Git Bot"),
        config.email.as_deref().unwrap_or("no-reply@seiunx.com"),
    )?;
    let message = format!("Updated {region_name} server chart hashes");

    let oid = if let Some(parent) = head_commit.as_ref() {
        repo.commit(
            Some("HEAD"),
            &signature,
            &signature,
            &message,
            &tree,
            &[parent],
        )?
    } else {
        repo.commit(Some("HEAD"), &signature, &signature, &message, &tree, &[])?
    };

    let refspec = format!("refs/heads/{branch}:refs/heads/{branch}");
    push_origin(&repo, &refspec, config, proxy, retry)?;

    Ok(Some(ChartHashSyncResult {
        output_file,
        commit_oid: Some(oid.to_string()),
        branch: Some(branch),
        pushed: true,
    }))
}

fn push_origin(
    repo: &Repository,
    refspec: &str,
    config: &ChartHashConfig,
    proxy: Option<&str>,
    retry: &RetryConfig,
) -> Result<(), GitSyncError> {
    retry_sync(
        retry,
        "git push",
        |_| {
            let mut callbacks = RemoteCallbacks::new();
            if let (Some(username), Some(password)) = (&config.username, &config.password) {
                callbacks.credentials({
                    let username = username.clone();
                    let password = password.clone();
                    move |_url, _username_from_url, _allowed_types| {
                        Cred::userpass_plaintext(&username, &password)
                    }
                });
            }

            let mut push_options = PushOptions::new();
            push_options.remote_callbacks(callbacks);
            if let Some(proxy_url) = proxy {
                let mut proxy_options = git2::ProxyOptions::new();
                proxy_options.url(proxy_url);
                push_options.proxy_options(proxy_options);
            }

            let mut remote = repo.find_remote("origin").map_err(|err| {
                if err.code() == git2::ErrorCode::NotFound {
                    GitSyncError::MissingOrigin
                } else {
                    GitSyncError::Git(err)
                }
            })?;
            remote.push(&[refspec], Some(&mut push_options))?;
            Ok(())
        },
        is_retryable_git_error,
    )
}

fn is_retryable_git_error(err: &GitSyncError) -> bool {
    match err {
        GitSyncError::Git(source) => matches!(
            source.class(),
            git2::ErrorClass::Net
                | git2::ErrorClass::Ssl
                | git2::ErrorClass::Ssh
                | git2::ErrorClass::Http
                | git2::ErrorClass::Os
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use git2::{Repository, Signature};
    use tempfile::tempdir;

    use crate::core::config::{ChartHashConfig, RetryConfig};

    use super::{chart_hash_output_path, collect_chart_hashes, sync_chart_hashes};

    fn create_repo_with_remote() -> (tempfile::TempDir, Repository, std::path::PathBuf, String) {
        let temp = tempdir().unwrap();
        let remote_dir = temp.path().join("remote.git");
        let repo_dir = temp.path().join("repo");

        let _bare = Repository::init_bare(&remote_dir).unwrap();
        let repo = Repository::init(&repo_dir).unwrap();
        fs::write(repo_dir.join("README.md"), "init").unwrap();

        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new("README.md")).unwrap();
        index.write().unwrap();
        let tree_id = index.write_tree().unwrap();
        let sig = Signature::now("tester", "tester@example.com").unwrap();
        let _oid = {
            let tree = repo.find_tree(tree_id).unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, "init", &tree, &[])
                .unwrap()
        };
        repo.remote("origin", remote_dir.to_str().unwrap()).unwrap();
        {
            let mut remote = repo.find_remote("origin").unwrap();
            remote
                .push(&["refs/heads/master:refs/heads/master"], None)
                .unwrap();
        }

        (
            temp,
            repo,
            repo_dir,
            remote_dir.to_string_lossy().to_string(),
        )
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
    fn sync_chart_hashes_commits_and_pushes_with_git2() {
        let (_temp, _repo, repo_dir, remote_dir) = create_repo_with_remote();

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
        assert!(result.commit_oid.is_some());

        let remote_repo = Repository::open_bare(remote_dir).unwrap();
        let head = remote_repo.find_reference("refs/heads/master").unwrap();
        assert_eq!(
            head.target().unwrap().to_string(),
            result.commit_oid.unwrap()
        );
        assert!(repo_dir.join("jp_chart_hashes.json").exists());
    }
}
