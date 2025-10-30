package updater

import (
	"fmt"
	"haruki-sekai-asset/utils"
)

type SekaiApiHttpStatus int

const (
	SekaiApiHttpStatusOk               SekaiApiHttpStatus = 200
	SekaiApiHttpStatusClientError      SekaiApiHttpStatus = 400
	SekaiApiHttpStatusSessionError     SekaiApiHttpStatus = 403
	SekaiApiHttpStatusNotFound         SekaiApiHttpStatus = 404
	SekaiApiHttpStatusConflict         SekaiApiHttpStatus = 409
	SekaiApiHttpStatusGameUpgrade      SekaiApiHttpStatus = 426
	SekaiApiHttpStatusServerError      SekaiApiHttpStatus = 500
	SekaiApiHttpStatusUnderMaintenance SekaiApiHttpStatus = 503
)

func ParseSekaiApiHttpStatus(code int) (SekaiApiHttpStatus, error) {
	switch SekaiApiHttpStatus(code) {
	case SekaiApiHttpStatusOk,
		SekaiApiHttpStatusClientError,
		SekaiApiHttpStatusSessionError,
		SekaiApiHttpStatusNotFound,
		SekaiApiHttpStatusConflict,
		SekaiApiHttpStatusGameUpgrade,
		SekaiApiHttpStatusServerError,
		SekaiApiHttpStatusUnderMaintenance:
		return SekaiApiHttpStatus(code), nil
	default:
		return 0, fmt.Errorf("invalid http status code: %d", code)
	}
}

type HarukiSekaiAssetCategory string

const (
	HarukiSekaiAssetCategoryStartApp HarukiSekaiAssetCategory = "StartApp"
	HarukiSekaiAssetCategoryOnDemand HarukiSekaiAssetCategory = "OnDemand"
)

type HarukiSekaiAssetTargetOS string

const (
	HarukiSekaiAssetTargetOSiOS     HarukiSekaiAssetTargetOS = "ios"
	HarukiSekaiAssetTargetOSAndroid HarukiSekaiAssetTargetOS = "android"
)

type HarukiSekaiAssetUpdaterPayload struct {
	Server       utils.HarukiSekaiServerRegion `json:"server"`
	AssetVersion string                        `json:"assetVersion,omitempty"`
	AssetHash    string                        `json:"assetHash,omitempty"`
}

type HarukiSekaiAssetBundleDetail struct {
	BundleName         string                   `msgpack:"bundleName"`
	CacheFileName      string                   `msgpack:"cacheFileName"`
	CacheDirectoryName string                   `msgpack:"cacheDirectoryName"`
	Hash               string                   `msgpack:"hash"`
	Category           HarukiSekaiAssetCategory `msgpack:"category"`
	Crc                int64                    `msgpack:"crc"`
	FileSize           int64                    `msgpack:"fileSize"`
	Dependencies       []string                 `msgpack:"dependencies"`
	Paths              []string                 `msgpack:"paths,omitempty"`
	IsBuiltin          bool                     `msgpack:"isBuiltin"`
	IsRelocate         *bool                    `msgpack:"isRelocate,omitempty"`
	Md5Hash            *string                  `msgpack:"md5Hash,omitempty"`
	DownloadPath       *string                  `msgpack:"downloadPath,omitempty"`
}

type HarukiSekaiAssetBundleInfo struct {
	Version *string                                 `msgpack:"version,omitempty"`
	OS      *HarukiSekaiAssetTargetOS               `msgpack:"os,omitempty"`
	Bundles map[string]HarukiSekaiAssetBundleDetail `msgpack:"bundles"`
}

type downloadTask struct {
	bundlePath string
	bundleHash string
	category   HarukiSekaiAssetCategory
}

type downloadResult struct {
	bundlePath string
	bundleHash string
	err        error
}

type prioritizedDownloadTask struct {
	downloadPath string
	task         downloadTask
	priority     int
}
