package updater

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"haruki-sekai-asset/utils"
	harukiLogger "haruki-sekai-asset/utils/logger"
)

func newLogicTestUpdater(t *testing.T) *HarukiSekaiAssetUpdater {
	t.Helper()
	recordFile := filepath.Join(t.TempDir(), "downloaded_assets.json")
	return &HarukiSekaiAssetUpdater{
		server: utils.HarukiSekaiServerRegionJP,
		serverConfig: utils.HarukiSekaiAssetUpdaterConfig{
			DownloadedAssetRecordFile: recordFile,
		},
		logger:           harukiLogger.NewLogger("test", "DEBUG", io.Discard),
		downloadedAssets: make(map[string]string),
		batchSaveSize:    2,
	}
}

func TestShouldSkipBundle(t *testing.T) {
	u := newLogicTestUpdater(t)
	u.serverConfig.SkipRegexes = []string{`^skip/`, `[`}

	if !u.shouldSkipBundle("skip/asset_a") {
		t.Fatalf("expected bundle to be skipped")
	}
	if u.shouldSkipBundle("keep/asset_b") {
		t.Fatalf("expected bundle to be kept")
	}
}

func TestShouldDownloadBundle(t *testing.T) {
	u := newLogicTestUpdater(t)
	u.serverConfig.StartAppRegexes = []string{`^start/`}
	u.serverConfig.OndemandRegexes = []string{`^ond/`}

	if !u.shouldDownloadBundle("start/a", HarukiSekaiAssetCategoryStartApp) {
		t.Fatalf("expected start app bundle to be downloadable")
	}
	if u.shouldDownloadBundle("start/a", HarukiSekaiAssetCategoryOnDemand) {
		t.Fatalf("expected category mismatch to be false")
	}
	if !u.shouldDownloadBundle("ond/a", HarukiSekaiAssetCategoryOnDemand) {
		t.Fatalf("expected on-demand bundle to be downloadable")
	}
	if u.shouldDownloadBundle("other/a", HarukiSekaiAssetCategoryOnDemand) {
		t.Fatalf("expected unmatched on-demand bundle to be false")
	}
}

func TestBuildDownloadList(t *testing.T) {
	u := newLogicTestUpdater(t)
	u.serverConfig.SkipRegexes = []string{`^skip/`}
	u.serverConfig.StartAppRegexes = []string{`^start/`}
	u.serverConfig.OndemandRegexes = []string{`^ond/`}

	info := &HarukiSekaiAssetBundleInfo{
		Bundles: map[string]HarukiSekaiAssetBundleDetail{
			"skip/a": {
				Hash:     "h1",
				Category: HarukiSekaiAssetCategoryStartApp,
			},
			"start/a": {
				Hash:     "h2",
				Category: HarukiSekaiAssetCategoryStartApp,
			},
			"ond/a": {
				Hash:     "h3",
				Category: HarukiSekaiAssetCategoryOnDemand,
			},
			"start/same": {
				Hash:     "same",
				Category: HarukiSekaiAssetCategoryStartApp,
			},
		},
	}
	downloaded := map[string]string{
		"start/same": "same",
	}

	list := u.buildDownloadList(info, downloaded)
	if len(list) != 2 {
		t.Fatalf("expected 2 download tasks, got %d", len(list))
	}

	startTask, ok := list["start/a"]
	if !ok {
		t.Fatalf("expected start/a task to exist")
	}
	if startTask.bundleHash != "h2" {
		t.Fatalf("expected hash h2, got %s", startTask.bundleHash)
	}

	if _, ok := list["ond/a"]; !ok {
		t.Fatalf("expected ond/a task to exist")
	}
}

func TestGetDownloadPath(t *testing.T) {
	u := newLogicTestUpdater(t)
	dp := "folder/sub"

	u.server = utils.HarukiSekaiServerRegionTW
	got := u.getDownloadPath("bundle", HarukiSekaiAssetBundleDetail{
		DownloadPath: &dp,
	})
	if got != "folder/sub/bundle" {
		t.Fatalf("unexpected tw download path: %s", got)
	}

	u.server = utils.HarukiSekaiServerRegionTW
	got = u.getDownloadPath("bundle", HarukiSekaiAssetBundleDetail{})
	if got != "bundle" {
		t.Fatalf("unexpected fallback path: %s", got)
	}

	u.server = utils.HarukiSekaiServerRegionJP
	got = u.getDownloadPath("bundle", HarukiSekaiAssetBundleDetail{DownloadPath: &dp})
	if got != "bundle" {
		t.Fatalf("unexpected jp download path: %s", got)
	}
}

func TestSortDownloadsByPriority(t *testing.T) {
	u := newLogicTestUpdater(t)
	priorities := []string{`^a/`, `^b/`}
	u.serverConfig.DownloadPriorityList = &priorities

	input := map[string]downloadTask{
		"p1": {bundlePath: "b/z", bundleHash: "1"},
		"p2": {bundlePath: "a/z", bundleHash: "2"},
		"p3": {bundlePath: "c/a", bundleHash: "3"},
		"p4": {bundlePath: "a/a", bundleHash: "4"},
	}

	sorted := u.sortDownloadsByPriority(input)
	if len(sorted) != 4 {
		t.Fatalf("expected 4 tasks, got %d", len(sorted))
	}

	gotPaths := []string{
		sorted[0].task.bundlePath,
		sorted[1].task.bundlePath,
		sorted[2].task.bundlePath,
		sorted[3].task.bundlePath,
	}
	wantPaths := []string{"a/a", "a/z", "b/z", "c/a"}
	if !reflect.DeepEqual(gotPaths, wantPaths) {
		t.Fatalf("unexpected order: got %v, want %v", gotPaths, wantPaths)
	}
}

func TestLoadAndSaveDownloadedAssets(t *testing.T) {
	u := newLogicTestUpdater(t)
	want := map[string]string{
		"a": "1",
		"b": "2",
	}

	if err := u.saveDownloadedAssets(want); err != nil {
		t.Fatalf("saveDownloadedAssets failed: %v", err)
	}
	got, err := u.loadDownloadedAssets()
	if err != nil {
		t.Fatalf("loadDownloadedAssets failed: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loaded data mismatch: got %v, want %v", got, want)
	}
}

func TestLoadDownloadedAssets_MissingAndInvalidFile(t *testing.T) {
	u := newLogicTestUpdater(t)

	got, err := u.loadDownloadedAssets()
	if err != nil {
		t.Fatalf("expected no error for missing file, got %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty map for missing file, got %v", got)
	}

	if err := u.saveDownloadedAssets(map[string]string{"ok": "1"}); err != nil {
		t.Fatalf("seed save failed: %v", err)
	}
	if err := os.WriteFile(u.serverConfig.DownloadedAssetRecordFile, []byte("{"), 0o644); err != nil {
		t.Fatalf("write invalid json failed: %v", err)
	}
	if _, err := u.loadDownloadedAssets(); err == nil {
		t.Fatalf("expected error for invalid json")
	}
}

func TestAddPendingResult_TriggersFlushByBatchSize(t *testing.T) {
	u := newLogicTestUpdater(t)

	u.addPendingResult(downloadResult{bundlePath: "a", bundleHash: "1"})
	if len(u.pendingSaveResults) != 1 {
		t.Fatalf("expected pending size 1, got %d", len(u.pendingSaveResults))
	}

	u.addPendingResult(downloadResult{bundlePath: "b", bundleHash: "2"})
	if len(u.pendingSaveResults) != 0 {
		t.Fatalf("expected pending results to be flushed")
	}
	if u.downloadedAssets["a"] != "1" || u.downloadedAssets["b"] != "2" {
		t.Fatalf("downloadedAssets not updated after flush: %v", u.downloadedAssets)
	}

	got, err := u.loadDownloadedAssets()
	if err != nil {
		t.Fatalf("loadDownloadedAssets failed: %v", err)
	}
	if got["a"] != "1" || got["b"] != "2" {
		t.Fatalf("record file mismatch: %v", got)
	}

	u.flushPendingResults()
}
