package main

import (
	"fmt"
	"io"
	"os"

	"haruki-sekai-asset/api"
	"haruki-sekai-asset/config"
	harukiLogger "haruki-sekai-asset/utils/logger"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/logger"
)

func main() {
	var logFile *os.File
	var loggerWriter io.Writer = os.Stdout
	if config.Cfg.Backend.MainLogFile != "" {
		var err error
		logFile, err = os.OpenFile(config.Cfg.Backend.MainLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			mainLogger := harukiLogger.NewLogger("Main", config.Cfg.Backend.LogLevel, os.Stdout)
			mainLogger.Errorf("failed to open main log file: %v", err)
			os.Exit(1)
		}
		loggerWriter = io.MultiWriter(os.Stdout, logFile)
		defer func(logFile *os.File) {
			_ = logFile.Close()
		}(logFile)
	}
	mainLogger := harukiLogger.NewLogger("Main", config.Cfg.Backend.LogLevel, loggerWriter)
	mainLogger.Infof("========================= Haruki Sekai Asset Updater %s =========================", config.Version)
	mainLogger.Infof("Powered By Haruki Dev Team")

	app := fiber.New(fiber.Config{BodyLimit: 30 * 1024 * 1024})

	if config.Cfg.Backend.AccessLog != "" {
		logCfg := logger.Config{Format: config.Cfg.Backend.AccessLog}
		if config.Cfg.Backend.AccessLogPath != "" {
			accessLogFile, err := os.OpenFile(config.Cfg.Backend.AccessLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				mainLogger.Errorf("failed to open access log file: %v", err)
				os.Exit(1)
			}
			defer func(accessLogFile *os.File) {
				_ = accessLogFile.Close()
			}(accessLogFile)
			logCfg.Stream = accessLogFile
		}
		app.Use(logger.New(logCfg))
	}

	api.RegisterRoutes(app)

	addr := fmt.Sprintf("%s:%d", config.Cfg.Backend.Host, config.Cfg.Backend.Port)
	var listenServerType = "HTTP"
	listenCfg := fiber.ListenConfig{
		DisableStartupMessage: true,
	}
	if config.Cfg.Backend.SSL {
		listenServerType = "HTTPS"
		mainLogger.Infof("SSL enabled, using certificate: %s", config.Cfg.Backend.SSLCert)
		listenCfg.CertFile = config.Cfg.Backend.SSLCert
		listenCfg.CertKeyFile = config.Cfg.Backend.SSLKey
	} else {
		mainLogger.Infof("SSL disabled, starting HTTP server")
	}
	err := app.Listen(addr, listenCfg)
	if err != nil {
		mainLogger.Errorf("failed to start server: %v", err)
		os.Exit(1)
	}
	mainLogger.Infof("Started listen %s server on %s...", listenServerType, addr)
}
