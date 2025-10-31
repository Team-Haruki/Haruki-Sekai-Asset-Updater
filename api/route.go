package api

import (
	"context"
	"haruki-sekai-asset/config"
	"haruki-sekai-asset/updater"
	"haruki-sekai-asset/utils"
	"strings"

	"github.com/gofiber/fiber/v2"
)

// runUpdater starts the asset updater in a goroutine
func runUpdater(server utils.HarukiSekaiServerRegion, payload updater.HarukiSekaiAssetUpdaterPayload) {
	go func() {
		serverConfig := config.Cfg.Servers[server]
		cpAssetProfiles := config.Cfg.Profiles[server]

		var proxy *string
		if config.Cfg.Proxy != "" {
			proxy = &config.Cfg.Proxy
		}

		sem := config.Cfg.Concurrents.ConcurrentDownload
		if sem <= 0 {
			sem = 4
		}

		assetUpdater := updater.NewHarukiSekaiAssetUpdater(
			context.Background(),
			server,
			serverConfig,
			&cpAssetProfiles,
			serverConfig.AssetSaveDir,
			&payload.AssetVersion,
			&payload.AssetHash,
			proxy,
			sem,
		)

		if assetUpdater != nil {
			assetUpdater.Run()
		}
	}()
}

// RegisterRoutes registers all API routes
func RegisterRoutes(app *fiber.App) {
	app.Post("/update_asset", updateAssetHandler)
}

// updateAssetHandler handles asset update requests
func updateAssetHandler(c *fiber.Ctx) error {
	// 1. Check if authorization is enabled
	if config.Cfg.Backend.EnableAuthorization {
		// 2. Check User-Agent if configured
		if config.Cfg.Backend.AcceptUserAgentPrefix != "" {
			userAgent := c.Get("User-Agent")
			if !strings.HasPrefix(userAgent, config.Cfg.Backend.AcceptUserAgentPrefix) {
				return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
					"message": "Invalid User-Agent",
				})
			}
		}

		// Check Authorization token if configured
		if config.Cfg.Backend.AcceptAuthorizationToken != "" {
			authHeader := c.Get("Authorization")
			expectedAuth := "Bearer " + config.Cfg.Backend.AcceptAuthorizationToken
			if authHeader != expectedAuth {
				return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
					"message": "Invalid authorization token",
				})
			}
		}
	}

	// Parse request payload
	var payload updater.HarukiSekaiAssetUpdaterPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"message": "Invalid request payload",
			"error":   err.Error(),
		})
	}

	// 3. Check if the server is enabled in configuration
	serverConfig, exists := config.Cfg.Servers[payload.Server]
	if !exists {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"message": "Server region not found in configuration",
		})
	}

	if !serverConfig.Enabled {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"message": "Asset updater for this region is not enabled",
			"server":  payload.Server,
		})
	}

	// 4. Start the updater in a goroutine
	runUpdater(payload.Server, payload)

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"message": "Asset updater started running",
		"server":  payload.Server,
	})
}
