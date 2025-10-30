package utils

import "fmt"

type HarukiSekaiAssetUpdaterConfig struct {
	Enabled                   bool      `yaml:"enabled"`
	ExportByCategory          bool      `yaml:"export_by_category,omitempty"`
	AssetInfoURLTemplate      string    `yaml:"asset_info_url_template"`
	CPAssetProfile            string    `yaml:"cp_asset_profile,omitempty"`
	NuverseAssetVersionURL    string    `yaml:"nuverse_asset_version_url,omitempty"`
	NuverseOverrideAppVersion string    `yaml:"nuverse_override_app_version,omitempty"`
	AssetURLTemplate          string    `yaml:"asset_url_template"`
	RequiredCookies           bool      `yaml:"required_cookies,omitempty"`
	AESKeyHex                 string    `yaml:"aes_key_hex,omitempty"`
	AESIVHex                  string    `yaml:"aes_iv_hex,omitempty"`
	UnityVersion              string    `yaml:"unity_version,omitempty"`
	AssetSaveDir              string    `yaml:"asset_save_dir,omitempty"`
	DownloadedAssetRecordFile string    `yaml:"downloaded_asset_record_file,omitempty"`
	StartAppPrefixes          []string  `yaml:"start_app_prefixes,omitempty"`
	OndemandPrefixes          []string  `yaml:"ondemand_prefixes,omitempty"`
	SkipPrefixes              []string  `yaml:"skip_prefixes,omitempty"`
	DownloadPriorityList      *[]string `yaml:"download_priority_list,omitempty"`
	ExportUSMFiles            bool      `yaml:"export_usm_files,omitempty"`
	DecodeUSMFiles            bool      `yaml:"decode_usm_files,omitempty"`
	ExportACBFiles            bool      `yaml:"export_acb_files,omitempty"`
	DecodeACBFiles            bool      `yaml:"decode_acb_files,omitempty"`
	DecodeHCAFiles            bool      `yaml:"decode_hca_files,omitempty"`
	ConvertPhotoToWebp        bool      `yaml:"convert_photo_to_webp,omitempty"`
	RemovePNG                 bool      `yaml:"remove_png,omitempty"`
	ConvertM2VToMP4           bool      `yaml:"convert_video_to_mp4,omitempty"`
	RemoveM2V                 bool      `yaml:"remove_m2v,omitempty"`
	ConvertWavToMP3           bool      `yaml:"convert_audio_to_mp3,omitempty"`
	ConvertWavToFLAC          bool      `yaml:"convert_wav_to_flac,omitempty"`
	RemoveWav                 bool      `yaml:"remove_wav,omitempty"`
	UploadToCloud             bool      `yaml:"upload_to_cloud,omitempty"`
	RemoveLocalAfterUpload    bool      `yaml:"remove_local_after_upload,omitempty"`
}

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
