package updater

import (
	"context"
	"errors"
	"fmt"
	"haruki-sekai-asset/config"
	"haruki-sekai-asset/utils"
	harukiLogger "haruki-sekai-asset/utils/logger"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/dlclark/regexp2"
	"github.com/go-resty/resty/v2"
)

type HarukiSekaiAssetUpdater struct {
	ctx                context.Context
	server             utils.HarukiSekaiServerRegion
	serverConfig       utils.HarukiSekaiAssetUpdaterConfig
	cpAssetProfiles    *map[string]string
	assetSaveDir       string
	assetVersion       *string
	assetHash          *string
	proxy              *string
	sem                chan struct{}
	cryptor            SekaiCryptor
	client             *resty.Client
	logger             *harukiLogger.Logger
	downloadedAssets   map[string]string
	pendingSaveResults []downloadResult
	batchSaveSize      int
	saveMutex          sync.Mutex
}

func NewHarukiSekaiAssetUpdater(
	ctx context.Context,
	server utils.HarukiSekaiServerRegion,
	serverConfig utils.HarukiSekaiAssetUpdaterConfig,
	cpAssetProfiles *map[string]string,
	assetSaveDir string,
	assetVersion *string,
	assetHash *string,
	proxy *string,
	sem int,
) *HarukiSekaiAssetUpdater {
	client := resty.New()
	client.
		SetRetryCount(0).
		SetTransport(&http.Transport{
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     30 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
			DisableKeepAlives:   false,
		}).
		SetHeader("Accept", "*/*").
		SetHeader("User-Agent", "ProductName/134 CFNetwork/1408.0.4 Darwin/22.5.0").
		SetHeader("Connection", "keep-alive").
		SetHeader("Accept-Encoding", "gzip, deflate, br").
		SetHeader("Accept-Language", "zh-CN,zh-Hans;q=0.9").
		SetHeader("X-Unity-Version", serverConfig.UnityVersion)
	if proxy != nil && *proxy != "" {
		client.SetProxy(*proxy)
	}
	cryptor, err := NewSekaiCryptorFromHex(serverConfig.AESKeyHex, serverConfig.AESIVHex)
	if err != nil {
		return nil
	}
	semChan := make(chan struct{}, sem)
	return &HarukiSekaiAssetUpdater{
		ctx:                ctx,
		server:             server,
		serverConfig:       serverConfig,
		cpAssetProfiles:    cpAssetProfiles,
		assetSaveDir:       assetSaveDir,
		assetVersion:       assetVersion,
		assetHash:          assetHash,
		proxy:              proxy,
		sem:                semChan,
		client:             client,
		cryptor:            *cryptor,
		logger:             harukiLogger.NewLogger(fmt.Sprintf("HarukiSekaiAssetUpdater%s", strings.ToUpper(string(server))), "INFO", nil),
		downloadedAssets:   make(map[string]string),
		pendingSaveResults: make([]downloadResult, 0),
		batchSaveSize:      50,
	}
}

func (u *HarukiSekaiAssetUpdater) parseCookies() error {
	if u.server == utils.HarukiSekaiServerRegionJP {
		var lastErr error
		for attempt := 0; attempt < 4; attempt++ {
			resp, err := u.client.R().
				SetContext(u.ctx).
				Post("https://issue.sekai.colorfulpalette.org/api/signature")
			if err != nil {
				lastErr = err
				time.Sleep(1 * time.Second)
				continue
			}
			if resp.StatusCode() == 200 {
				cookie := resp.Header().Get("Set-Cookie")
				u.client.SetHeader("Cookie", cookie)
				return nil
			} else {
				lastErr = errors.New("failed to fetch cookies")
				time.Sleep(1 * time.Second)
			}
		}
		return lastErr
	}
	return nil
}

func (u *HarukiSekaiAssetUpdater) loadDownloadedAssets() (map[string]string, error) {
	var downloadedAssetsMap map[string]string
	data, err := os.ReadFile(u.serverConfig.DownloadedAssetRecordFile)
	if err != nil {
		if os.IsNotExist(err) {
			downloadedAssetsMap = make(map[string]string)
			return downloadedAssetsMap, nil
		}
		return nil, err
	}
	if err = sonic.Unmarshal(data, &downloadedAssetsMap); err != nil {
		return nil, err
	}
	return downloadedAssetsMap, nil
}

func (u *HarukiSekaiAssetUpdater) saveDownloadedAssets(downloadedAssetsMap map[string]string) error {
	data, err := sonic.Marshal(downloadedAssetsMap)
	if err != nil {
		return err
	}
	if err = os.WriteFile(u.serverConfig.DownloadedAssetRecordFile, data, 0o644); err != nil {
		return err
	}
	return nil
}

func (u *HarukiSekaiAssetUpdater) request(url string) (*resty.Response, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		resp, err := u.client.R().
			SetContext(u.ctx).
			Get(url)
		if err != nil {
			lastErr = err
			time.Sleep(time.Second)
			continue
		}
		if resp.StatusCode() >= 500 {
			lastErr = errors.New("server error")
			time.Sleep(time.Second)
		} else {
			return resp, nil
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("request failed after retries")
}

func (u *HarukiSekaiAssetUpdater) getAssetBundleInfo(assetVersion *string, assetHash *string) *HarukiSekaiAssetBundleInfo {
	var assetURL *string
	if u.server == utils.HarukiSekaiServerRegionJP || u.server == utils.HarukiSekaiServerRegionEN {
		profileMap := *u.cpAssetProfiles
		url := strings.ReplaceAll(u.serverConfig.AssetInfoURLTemplate, "{env}", u.serverConfig.CPAssetProfile)
		url = strings.ReplaceAll(url, "{hash}", profileMap[u.serverConfig.CPAssetProfile])
		url = strings.ReplaceAll(url, "{asset_version}", *assetVersion)
		url = strings.ReplaceAll(url, "{asset_hash}", *assetHash)
		assetURL = &url
	} else {
		var nuverseAssetVersion *string
		assetVersionURL := strings.ReplaceAll(u.serverConfig.NuverseAssetVersionURL, "{app_version}", u.serverConfig.NuverseOverrideAppVersion)
		resp, err := u.request(assetVersionURL)
		if err != nil {
			return nil
		}
		if resp.StatusCode() == 200 {
			respVersion := resp.String()
			nuverseAssetVersion = &respVersion
		} else if resp.StatusCode() >= 500 {
			return nil
		}
		if nuverseAssetVersion == nil {
			return nil
		} else {
			url := strings.ReplaceAll(u.serverConfig.AssetInfoURLTemplate, "{app_version}", u.serverConfig.NuverseOverrideAppVersion)
			url = strings.ReplaceAll(url, "{asset_version}", *nuverseAssetVersion)
			assetURL = &url
		}
	}
	if assetURL == nil {
		return nil
	}
	resp, err := u.request(*assetURL + utils.GetTimeArg())
	if err != nil {
		return nil
	} else if resp.StatusCode() == 200 {
		var assetBundleInfo HarukiSekaiAssetBundleInfo
		if err := u.cryptor.UnpackInto(resp.Body(), &assetBundleInfo); err != nil {
			return nil
		}
		return &assetBundleInfo
	} else {
		return nil
	}
}

func (u *HarukiSekaiAssetUpdater) downloadAndExportAsset(downloadPath string, bundleName string, bundleHash string, category HarukiSekaiAssetCategory) (*string, *string, error) {

	assetURL := strings.ReplaceAll(u.serverConfig.AssetURLTemplate, "{bundle_path}", downloadPath)
	if u.server == utils.HarukiSekaiServerRegionJP || u.server == utils.HarukiSekaiServerRegionEN {
		assetURL = strings.ReplaceAll(assetURL, "{asset_version}", *u.assetVersion)
		assetURL = strings.ReplaceAll(assetURL, "{asset_hash}", *u.assetHash)
		assetURL = strings.ReplaceAll(assetURL, "{env}", u.serverConfig.CPAssetProfile)
		assetURL = strings.ReplaceAll(assetURL, "{hash}", (*u.cpAssetProfiles)[u.serverConfig.CPAssetProfile])
	} else {
		assetURL = strings.ReplaceAll(assetURL, "{app_version}", u.serverConfig.NuverseOverrideAppVersion)
	}

	assetURL = assetURL + utils.GetTimeArg()
	resp, err := u.request(assetURL)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode() == 200 {
		body := Deobfuscate(resp.Body())
		tempFilePath := filepath.Join(os.TempDir(), string(u.server), bundleName)
		tempDir := filepath.Dir(tempFilePath)
		if err := os.MkdirAll(tempDir, 0o755); err != nil {
			return nil, nil, err
		}

		if err := os.WriteFile(tempFilePath, body, 0o644); err != nil {
			return nil, nil, err
		}
		defer func() {
			if err := os.Remove(tempFilePath); err != nil {
			}
		}()

		if err = ExtractUnityAssetBundle(config.Cfg.Tools.AssetStudioCLIPath, tempFilePath, bundleName, u.assetSaveDir,
			category, u.serverConfig, config.Cfg.Tools.FFMPEGPath, config.Cfg.Tools.CwebpPath); err != nil {
			return nil, nil, err
		}
		return &bundleName, &bundleHash, nil
	}
	return nil, nil, errors.New("failed to download asset bundle")
}

func (u *HarukiSekaiAssetUpdater) Run() {
	downloadedAssets, err := u.loadDownloadedAssets()
	if err != nil {
		u.logger.Errorf("failed to load cached assets: %v", err)
		return
	}
	u.downloadedAssets = downloadedAssets
	if u.serverConfig.RequiredCookies {
		if err := u.parseCookies(); err != nil {
			u.logger.Errorf("failed to parse cookies: %v", err)
			return
		}
	}
	assetBundleInfo := u.getAssetBundleInfo(u.assetVersion, u.assetHash)
	if assetBundleInfo == nil {
		u.logger.Errorf("failed to get asset bundle info")
		return
	}
	toDownloadList := u.buildDownloadList(assetBundleInfo, downloadedAssets)
	if len(toDownloadList) == 0 {
		u.logger.Infof("no new assets to download")
		return
	}
	u.logger.Infof("found %d new assets to download", len(toDownloadList))
	startTime := time.Now()
	successResults, failedResults := u.downloadAssetsConcurrently(toDownloadList)
	consumedTime := time.Since(startTime)
	u.logger.Infof("Flushing remaining pending results...")
	u.flushPendingResults()
	u.logger.Infof("all downloads completed, total successful: %d, failed: %d", len(successResults), len(failedResults))
	u.logger.Infof("total time taken: %s", consumedTime.String())
}

func (u *HarukiSekaiAssetUpdater) buildDownloadList(
	assetBundleInfo *HarukiSekaiAssetBundleInfo,
	downloadedAssets map[string]string,
) map[string]downloadTask {
	toDownloadList := make(map[string]downloadTask)

	for bundleName, bundleInfo := range assetBundleInfo.Bundles {
		if u.shouldSkipBundle(bundleName) {
			continue
		}
		if !u.shouldDownloadBundle(bundleName, bundleInfo.Category) {
			continue
		}
		if existingHash, exists := downloadedAssets[bundleName]; exists && existingHash == bundleInfo.Hash {
			continue
		}
		downloadPath := u.getDownloadPath(bundleName, bundleInfo)
		toDownloadList[downloadPath] = downloadTask{
			bundlePath: bundleName,
			bundleHash: bundleInfo.Hash,
			category:   bundleInfo.Category,
		}
	}

	return toDownloadList
}

func (u *HarukiSekaiAssetUpdater) shouldSkipBundle(bundleName string) bool {
	if len(u.serverConfig.SkipPrefixes) == 0 {
		return false
	}

	for _, pattern := range u.serverConfig.SkipPrefixes {
		re, err := regexp2.Compile(pattern, 0)
		if err != nil {
			u.logger.Warnf("invalid skip prefix pattern '%s': %v", pattern, err)
			continue
		}
		matched, err := re.MatchString(bundleName)
		if err != nil {
			u.logger.Warnf("error matching pattern '%s': %v", pattern, err)
			continue
		}
		if matched {
			return true
		}
	}

	return false
}

func (u *HarukiSekaiAssetUpdater) shouldDownloadBundle(bundleName string, category HarukiSekaiAssetCategory) bool {
	switch category {
	case HarukiSekaiAssetCategoryStartApp:
		if len(u.serverConfig.StartAppPrefixes) == 0 {
			return false
		}
		for _, pattern := range u.serverConfig.StartAppPrefixes {
			re, err := regexp2.Compile(pattern, 0)
			if err != nil {
				u.logger.Warnf("invalid StartApp pattern '%s': %v", pattern, err)
				continue
			}
			matched, err := re.MatchString(bundleName)
			if err != nil {
				u.logger.Warnf("error matching pattern '%s': %v", pattern, err)
				continue
			}
			if matched {
				return true
			}
		}
		return false
	case HarukiSekaiAssetCategoryOnDemand:
		if len(u.serverConfig.OndemandPrefixes) == 0 {
			return false
		}
		for _, pattern := range u.serverConfig.OndemandPrefixes {
			re, err := regexp2.Compile(pattern, 0)
			if err != nil {
				u.logger.Warnf("invalid OnDemand pattern '%s': %v", pattern, err)
				continue
			}
			matched, err := re.MatchString(bundleName)
			if err != nil {
				u.logger.Warnf("error matching pattern '%s': %v", pattern, err)
				continue
			}
			if matched {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func (u *HarukiSekaiAssetUpdater) getDownloadPath(bundleName string, bundleInfo HarukiSekaiAssetBundleDetail) string {
	if u.server == utils.HarukiSekaiServerRegionJP || u.server == utils.HarukiSekaiServerRegionEN {
		return bundleName
	}
	if bundleInfo.DownloadPath != nil {
		return fmt.Sprintf("%s/%s", *bundleInfo.DownloadPath, bundleName)
	}
	return bundleName
}

func (u *HarukiSekaiAssetUpdater) sortDownloadsByPriority(toDownloadList map[string]downloadTask) []prioritizedDownloadTask {
	sortedTasks := make([]prioritizedDownloadTask, 0, len(toDownloadList))

	for downloadPath, task := range toDownloadList {
		priority := u.getDownloadPriority(task.bundlePath)
		sortedTasks = append(sortedTasks, prioritizedDownloadTask{
			downloadPath: downloadPath,
			task:         task,
			priority:     priority,
		})
	}

	for i := 0; i < len(sortedTasks); i++ {
		for j := i + 1; j < len(sortedTasks); j++ {
			if sortedTasks[j].priority < sortedTasks[i].priority {
				sortedTasks[i], sortedTasks[j] = sortedTasks[j], sortedTasks[i]
			} else if sortedTasks[j].priority == sortedTasks[i].priority {
				if sortedTasks[j].task.bundlePath < sortedTasks[i].task.bundlePath {
					sortedTasks[i], sortedTasks[j] = sortedTasks[j], sortedTasks[i]
				}
			}
		}
	}

	return sortedTasks
}

func (u *HarukiSekaiAssetUpdater) getDownloadPriority(bundleName string) int {
	if u.serverConfig.DownloadPriorityList == nil || len(*u.serverConfig.DownloadPriorityList) == 0 {
		return 9999999
	}

	for idx, pattern := range *u.serverConfig.DownloadPriorityList {
		re, err := regexp2.Compile(pattern, 0)
		if err != nil {
			u.logger.Warnf("invalid priority pattern '%s': %v", pattern, err)
			continue
		}
		matched, err := re.MatchString(bundleName)
		if err != nil {
			u.logger.Warnf("error matching pattern '%s': %v", pattern, err)
			continue
		}
		if matched {
			return idx
		}
	}

	return 9999999
}

func (u *HarukiSekaiAssetUpdater) downloadAssetsConcurrently(toDownloadList map[string]downloadTask) ([]downloadResult, []downloadResult) {
	sortedTasks := u.sortDownloadsByPriority(toDownloadList)
	totalTasks := len(sortedTasks)
	resultsChan := make(chan downloadResult, totalTasks)
	taskChan := make(chan prioritizedDownloadTask, totalTasks)
	workerCount := cap(u.sem)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range taskChan {
				bundleName, bundleHash, err := u.downloadAndExportAsset(
					task.downloadPath,
					task.task.bundlePath,
					task.task.bundleHash,
					task.task.category,
				)

				result := downloadResult{err: err}
				if err == nil && bundleName != nil && bundleHash != nil {
					result.bundlePath = *bundleName
					result.bundleHash = *bundleHash
				} else {
					result.bundlePath = task.task.bundlePath
					result.bundleHash = task.task.bundleHash
				}
				resultsChan <- result
			}
		}(i)
	}
	go func() {
		for _, task := range sortedTasks {
			taskChan <- task
		}
		close(taskChan)
	}()
	go func() {
		wg.Wait()
		close(resultsChan)
	}()
	successResults := make([]downloadResult, 0)
	failedResults := make([]downloadResult, 0)
	for result := range resultsChan {
		if result.err != nil {
			failedResults = append(failedResults, result)
			u.logger.Errorf("failed to download asset %s: %v", result.bundlePath, result.err)
		} else {
			successResults = append(successResults, result)
			u.logger.Infof("successfully downloaded: %s", result.bundlePath)
			u.addPendingResult(result)
		}
	}
	u.logger.Infof("download completed: %d succeeded, %d failed", len(successResults), len(failedResults))
	return successResults, failedResults
}

func (u *HarukiSekaiAssetUpdater) updateDownloadedAssetsRecord(
	downloadedAssets map[string]string,
	successResults []downloadResult,
) {
	for _, result := range successResults {
		downloadedAssets[result.bundlePath] = result.bundleHash
	}
	if err := u.saveDownloadedAssets(downloadedAssets); err != nil {
		u.logger.Errorf("failed to save downloaded assets record: %v", err)
	} else {
		u.logger.Infof("downloaded assets record updated successfully (%d assets)", len(successResults))
	}
}

func (u *HarukiSekaiAssetUpdater) addPendingResult(result downloadResult) {
	u.saveMutex.Lock()
	defer u.saveMutex.Unlock()
	u.pendingSaveResults = append(u.pendingSaveResults, result)
	currentCount := len(u.pendingSaveResults)
	if currentCount >= u.batchSaveSize {
		u.logger.Infof("Batch save threshold reached (%d assets), triggering save...", currentCount)
		u.flushPendingResultsUnsafe()
	}
}

func (u *HarukiSekaiAssetUpdater) flushPendingResults() {
	u.saveMutex.Lock()
	defer u.saveMutex.Unlock()
	u.flushPendingResultsUnsafe()
}

func (u *HarukiSekaiAssetUpdater) flushPendingResultsUnsafe() {
	pendingCount := len(u.pendingSaveResults)
	if pendingCount == 0 {
		return
	}
	u.logger.Infof("Starting to flush %d pending results to file...", pendingCount)
	for _, result := range u.pendingSaveResults {
		u.downloadedAssets[result.bundlePath] = result.bundleHash
	}
	if err := u.saveDownloadedAssets(u.downloadedAssets); err != nil {
		u.logger.Errorf("failed to batch save downloaded assets: %v", err)
	} else {
		u.logger.Infof("✓ Successfully batch saved %d assets to record file: %s", pendingCount, u.serverConfig.DownloadedAssetRecordFile)
	}
	u.pendingSaveResults = make([]downloadResult, 0)
}

func (u *HarukiSekaiAssetUpdater) Close() {
	u.client = nil
}
