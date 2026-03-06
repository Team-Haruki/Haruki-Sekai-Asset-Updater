package git

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"haruki-sekai-asset/utils"
	harukiLogger "haruki-sekai-asset/utils/logger"

	gogit "github.com/go-git/go-git/v6"
	gogitconfig "github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing/object"
)

func initRepoWithCommit(t *testing.T) (*gogit.Repository, string) {
	t.Helper()
	dir := t.TempDir()
	repo, err := gogit.PlainInit(dir, false)
	if err != nil {
		t.Fatalf("PlainInit failed: %v", err)
	}

	path := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}

	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("Worktree failed: %v", err)
	}
	if _, err := wt.Add("file.txt"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if _, err := wt.Commit("init", &gogit.CommitOptions{
		Author: &object.Signature{
			Name:  "tester",
			Email: "tester@example.com",
			When:  time.Now(),
		},
	}); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	return repo, dir
}

func TestCheckUnpushedCommits_WhenRemoteBranchMissing(t *testing.T) {
	repo, _ := initRepoWithCommit(t)
	if _, err := repo.CreateRemote(&gogitconfig.RemoteConfig{
		Name: "origin",
		URLs: []string{"https://example.com/repo.git"},
	}); err != nil {
		t.Fatalf("CreateRemote failed: %v", err)
	}

	logger := harukiLogger.NewLogger("test", "DEBUG", io.Discard)
	hasUnpushed, err := checkUnpushedCommits(repo, logger)
	if err != nil {
		t.Fatalf("checkUnpushedCommits failed: %v", err)
	}
	if !hasUnpushed {
		t.Fatalf("expected unpushed commits when remote branch ref is missing")
	}
}

func TestUpdateAndRestoreRemoteURL(t *testing.T) {
	repo, _ := initRepoWithCommit(t)
	if _, err := repo.CreateRemote(&gogitconfig.RemoteConfig{
		Name: "origin",
		URLs: []string{"https://example.com/repo.git"},
	}); err != nil {
		t.Fatalf("CreateRemote failed: %v", err)
	}

	g := NewHarukiGitUpdater("user", "email@example.com", "pass", "")
	logger := harukiLogger.NewLogger("test", "DEBUG", io.Discard)
	origURL, err := g.updateRemoteURL(repo, logger)
	if err != nil {
		t.Fatalf("updateRemoteURL failed: %v", err)
	}
	if origURL != "https://example.com/repo.git" {
		t.Fatalf("unexpected original url: %s", origURL)
	}

	remote, err := repo.Remote("origin")
	if err != nil {
		t.Fatalf("Remote lookup failed: %v", err)
	}
	currentURL := remote.Config().URLs[0]
	if !strings.Contains(currentURL, "user:pass@") {
		t.Fatalf("expected credentials in url, got %s", currentURL)
	}

	restoreRemoteURL(repo, origURL)
	remote, err = repo.Remote("origin")
	if err != nil {
		t.Fatalf("Remote lookup after restore failed: %v", err)
	}
	if remote.Config().URLs[0] != origURL {
		t.Fatalf("expected restored url %s, got %s", origURL, remote.Config().URLs[0])
	}
}

func TestSetupProxyTransport(t *testing.T) {
	logger := harukiLogger.NewLogger("test", "DEBUG", io.Discard)

	g := NewHarukiGitUpdater("u", "e", "p", "://bad")
	if _, err := g.setupProxyTransport(logger); err == nil {
		t.Fatalf("expected invalid proxy url to fail")
	}

	_ = os.Setenv("HTTP_PROXY", "http://old-http:1")
	_ = os.Setenv("HTTPS_PROXY", "http://old-https:2")
	_ = os.Setenv("NO_PROXY", "old")
	t.Cleanup(func() {
		_ = os.Unsetenv("HTTP_PROXY")
		_ = os.Unsetenv("HTTPS_PROXY")
		_ = os.Unsetenv("NO_PROXY")
	})

	g = NewHarukiGitUpdater("u", "e", "p", "http://127.0.0.1:7890")
	cleanup, err := g.setupProxyTransport(logger)
	if err != nil {
		t.Fatalf("setupProxyTransport failed: %v", err)
	}
	if os.Getenv("HTTP_PROXY") != "http://127.0.0.1:7890" {
		t.Fatalf("HTTP_PROXY not set")
	}
	if os.Getenv("HTTPS_PROXY") != "http://127.0.0.1:7890" {
		t.Fatalf("HTTPS_PROXY not set")
	}
	if os.Getenv("NO_PROXY") != "localhost,127.0.0.1,::1" {
		t.Fatalf("NO_PROXY not set as expected")
	}

	cleanup()

	if os.Getenv("HTTP_PROXY") != "http://old-http:1" {
		t.Fatalf("HTTP_PROXY not restored")
	}
	if os.Getenv("HTTPS_PROXY") != "http://old-https:2" {
		t.Fatalf("HTTPS_PROXY not restored")
	}
	if os.Getenv("NO_PROXY") != "old" {
		t.Fatalf("NO_PROXY not restored")
	}
}

func TestCommitChanges(t *testing.T) {
	repo, dir := initRepoWithCommit(t)
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("Worktree failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("updated"), 0o644); err != nil {
		t.Fatalf("write updated file failed: %v", err)
	}

	g := NewHarukiGitUpdater("bot", "bot@example.com", "", "")
	logger := harukiLogger.NewLogger("test", "DEBUG", io.Discard)
	if _, err := g.commitChanges(wt, utils.HarukiSekaiServerRegionJP, logger); err != nil {
		t.Fatalf("commitChanges failed: %v", err)
	}
}
