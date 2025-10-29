package updater

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/bytedance/sonic"
	"github.com/go-resty/resty/v2"
)

type HarukiSekaiAssetUpdater struct {
	ctx                  context.Context
	server               HarukiSekaiServerRegion
	serverConfig         HarukiSekaiAssetUpdaterConfig
	assetSaveDir         string
	startAppPrefixes     []string
	ondemandPrefixes     []string
	downloadPriorityList *[]string
	assetVersion         *string
	assetHash            *string
	proxy                *string
	sem                  int
	client               *resty.Client
}

func NewHarukiSekaiAssetUpdater(
	ctx context.Context,
	server HarukiSekaiServerRegion,
	serverConfig HarukiSekaiAssetUpdaterConfig,
	assetSaveDir string,
	startAppPrefixes []string,
	ondemandPrefixes []string,
	downloadPriorityList *[]string,
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
	return &HarukiSekaiAssetUpdater{
		ctx:                  ctx,
		server:               server,
		serverConfig:         serverConfig,
		assetSaveDir:         assetSaveDir,
		startAppPrefixes:     startAppPrefixes,
		ondemandPrefixes:     ondemandPrefixes,
		downloadPriorityList: downloadPriorityList,
		assetVersion:         assetVersion,
		assetHash:            assetHash,
		proxy:                proxy,
		sem:                  sem,
		client:               client,
	}
}

func (u *HarukiSekaiAssetUpdater) parseCookies() error {
	if u.server == HarukiSekaiServerRegionJP {
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

func (u *HarukiSekaiAssetUpdater) getAssetBundleInfo() *HarukiSekaiAssetBundleInfo {
	return nil
}

func (u *HarukiSekaiAssetUpdater) downloadAndExportAsset() (*string, *string, error) {
	return nil, nil, nil
}

func (u *HarukiSekaiAssetUpdater) Run() {
	// Implementation of the update process goes here
}

func (u *HarukiSekaiAssetUpdater) Close() {
	u.client = nil
}
