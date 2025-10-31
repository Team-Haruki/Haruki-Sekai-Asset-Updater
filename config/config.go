package config

import (
	"haruki-sekai-asset/utils"
	harukiLogger "haruki-sekai-asset/utils/logger"
	"os"

	"gopkg.in/yaml.v3"
)

type BackendConfig struct {
	Host                     string `yaml:"host"`
	Port                     int    `yaml:"port"`
	SSL                      bool   `yaml:"ssl"`
	SSLCert                  string `yaml:"ssl_cert"`
	SSLKey                   string `yaml:"ssl_key"`
	LogLevel                 string `yaml:"log_level"`
	MainLogFile              string `yaml:"main_log_file"`
	AccessLog                string `yaml:"access_log"`
	AccessLogPath            string `yaml:"access_log_path"`
	EnableAuthorization      bool   `yaml:"enable_authorization,omitempty"`
	AcceptUserAgentPrefix    string `yaml:"accept_user_agent_prefix,omitempty"`
	AcceptAuthorizationToken string `yaml:"accept_authorization_token,omitempty"`
}

type ToolConfig struct {
	FFMPEGPath         string `yaml:"ffmpeg_path,omitempty"`
	AssetStudioCLIPath string `yaml:"asset_studio_cli_path,omitempty"`
	CwebpPath          string `yaml:"cwebp_path,omitempty"`
}

type RemoteStorageConfig struct {
	Type    string   `yaml:"type"`
	Base    string   `yaml:"base"`
	Program string   `yaml:"program"`
	Args    []string `yaml:"args"`
}

type Config struct {
	Proxy               string                                                                `yaml:"proxy,omitempty"`
	ConcurrentDownloads int                                                                   `yaml:"concurrent_downloads,omitempty"`
	ConcurrentUploads   int                                                                   `yaml:"concurrent_uploads,omitempty"`
	Backend             BackendConfig                                                         `yaml:"backend,omitempty"`
	Tools               ToolConfig                                                            `yaml:"tool,omitempty"`
	Profiles            map[utils.HarukiSekaiServerRegion]map[string]string                   `yaml:"profiles,omitempty"`
	Servers             map[utils.HarukiSekaiServerRegion]utils.HarukiSekaiAssetUpdaterConfig `yaml:"servers"`
	RemoteStorages      []RemoteStorageConfig                                                 `yaml:"remote_storages,omitempty"`
}

var Version = "v4.0.1-dev"
var Cfg Config

func init() {
	logger := harukiLogger.NewLogger("ConfigLoader", "DEBUG", nil)
	f, err := os.Open("haruki-asset-configs.yaml")
	if err != nil {
		logger.Errorf("Failed to open config file: %v", err)
		os.Exit(1)
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	decoder := yaml.NewDecoder(f)
	if err := decoder.Decode(&Cfg); err != nil {
		logger.Errorf("Failed to parse config: %v", err)
		os.Exit(1)
	}
}
