package updater

import "fmt"

type HarukiSekaiServerRegion string

const (
	HarukiSekaiServerRegionJP HarukiSekaiServerRegion = "jp"
	HarukiSekaiServerRegionEN HarukiSekaiServerRegion = "en"
	HarukiSekaiServerRegionTW HarukiSekaiServerRegion = "tw"
	HarukiSekaiServerRegionKR HarukiSekaiServerRegion = "kr"
	HarukiSekaiServerRegionCN HarukiSekaiServerRegion = "cn"
)

func ParseSekaiServerRegion(s string) (HarukiSekaiServerRegion, error) {
	switch HarukiSekaiServerRegion(s) {
	case HarukiSekaiServerRegionJP,
		HarukiSekaiServerRegionEN,
		HarukiSekaiServerRegionTW,
		HarukiSekaiServerRegionKR,
		HarukiSekaiServerRegionCN:
		return HarukiSekaiServerRegion(s), nil
	default:
		return "", fmt.Errorf("invalid server region: %s", s)
	}
}

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

type HarukiSekaiAssetUpdaterConfig struct {
	Enabled                   bool   `yaml:"enabled"`
	ExportByCategory          bool   `yaml:"export_by_category,omitempty"`
	AssetInfoURLTemplate      string `yaml:"asset_info_url_template"`
	CPAssetProfile            string `yaml:"cp_asset_profile,omitempty"`
	NuverseAssetVersionURL    string `yaml:"nuverse_asset_version_url,omitempty"`
	NuverseOverrideAppVersion string `yaml:"nuverse_override_app_version,omitempty"`
	AssetURLTemplate          string `yaml:"asset_url_template"`
	RequiredCookies           bool   `yaml:"required_cookies,omitempty"`
	AESKeyHex                 string `yaml:"aes_key_hex,omitempty"`
	AESIVHex                  string `yaml:"aes_iv_hex,omitempty"`
	UnityVersion              string `yaml:"unity_version,omitempty"`
	AssetSaveDir              string `yaml:"asset_save_dir,omitempty"`
	DownloadedAssetRecordFile string `yaml:"downloaded_asset_record_file,omitempty"`
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
	Server       HarukiSekaiServerRegion `json:"server"`
	AssetVersion string                  `json:"assetVersion,omitempty"`
	AssetHash    string                  `json:"assetHash,omitempty"`
}

type HarukiSekaiAssetBundleDetail struct {
	BundleName         string   `msgpack:"bundleName"`
	CacheFileName      string   `msgpack:"cacheFileName"`
	CacheDirectoryName string   `msgpack:"cacheDirectoryName"`
	Hash               string   `msgpack:"hash"`
	Category           string   `msgpack:"category"`
	Crc                int64    `msgpack:"crc"`
	FileSize           int64    `msgpack:"fileSize"`
	Dependencies       []string `msgpack:"dependencies"`
	Paths              []string `msgpack:"paths,omitempty"`
	IsBuiltin          bool     `msgpack:"isBuiltin"`
	IsRelocate         *bool    `msgpack:"isRelocate,omitempty"`
	Md5Hash            *string  `msgpack:"md5Hash,omitempty"`
	DownloadPath       *string  `msgpack:"downloadPath,omitempty"`
}

type HarukiSekaiAssetBundleInfo struct {
	Version *string                                 `msgpack:"version,omitempty"`
	OS      *HarukiSekaiAssetTargetOS               `msgpack:"os,omitempty"`
	Bundles map[string]HarukiSekaiAssetBundleDetail `msgpack:"bundles"`
}
