package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"haruki-sekai-asset/config"
	"haruki-sekai-asset/utils"

	"github.com/gofiber/fiber/v3"
)

func setupTestApp() *fiber.App {
	app := fiber.New()
	RegisterRoutes(app)
	return app
}

func doJSONRequest(t *testing.T, app *fiber.App, method, path string, body []byte, headers map[string]string) (int, map[string]any) {
	t.Helper()

	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test failed: %v", err)
	}
	defer resp.Body.Close()

	var payload map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&payload)
	return resp.StatusCode, payload
}

func withConfig(t *testing.T, cfg config.Config) {
	t.Helper()
	orig := config.Cfg
	config.Cfg = cfg
	t.Cleanup(func() {
		config.Cfg = orig
	})
}

func TestUpdateAssetHandler_UnauthorizedByUserAgent(t *testing.T) {
	withConfig(t, config.Config{
		Backend: config.BackendConfig{
			EnableAuthorization:   true,
			AcceptUserAgentPrefix: "HarukiClient/",
		},
	})

	app := setupTestApp()
	status, resp := doJSONRequest(
		t,
		app,
		http.MethodPost,
		"/update_asset",
		[]byte(`{"server":"jp","assetVersion":"1","assetHash":"h"}`),
		map[string]string{"User-Agent": "OtherClient/1.0"},
	)

	if status != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, status)
	}
	if got := resp["message"]; got != "Invalid User-Agent" {
		t.Fatalf("unexpected message: %v", got)
	}
}

func TestUpdateAssetHandler_UnauthorizedByToken(t *testing.T) {
	withConfig(t, config.Config{
		Backend: config.BackendConfig{
			EnableAuthorization:      true,
			AcceptAuthorizationToken: "secret-token",
		},
	})

	app := setupTestApp()
	status, resp := doJSONRequest(
		t,
		app,
		http.MethodPost,
		"/update_asset",
		[]byte(`{"server":"jp","assetVersion":"1","assetHash":"h"}`),
		map[string]string{"Authorization": "Bearer wrong"},
	)

	if status != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, status)
	}
	if got := resp["message"]; got != "Invalid authorization token" {
		t.Fatalf("unexpected message: %v", got)
	}
}

func TestUpdateAssetHandler_InvalidPayload(t *testing.T) {
	withConfig(t, config.Config{})
	app := setupTestApp()

	status, resp := doJSONRequest(
		t,
		app,
		http.MethodPost,
		"/update_asset",
		[]byte(`{"server":"jp"`),
		nil,
	)

	if status != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, status)
	}
	if got := resp["message"]; got != "Invalid request payload" {
		t.Fatalf("unexpected message: %v", got)
	}
}

func TestUpdateAssetHandler_ServerNotFound(t *testing.T) {
	withConfig(t, config.Config{
		Servers: map[utils.HarukiSekaiServerRegion]utils.HarukiSekaiAssetUpdaterConfig{},
	})

	app := setupTestApp()
	status, resp := doJSONRequest(
		t,
		app,
		http.MethodPost,
		"/update_asset",
		[]byte(`{"server":"jp","assetVersion":"1","assetHash":"h"}`),
		nil,
	)

	if status != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, status)
	}
	if got := resp["message"]; got != "Server region not found in configuration" {
		t.Fatalf("unexpected message: %v", got)
	}
}

func TestUpdateAssetHandler_ServerDisabled(t *testing.T) {
	withConfig(t, config.Config{
		Servers: map[utils.HarukiSekaiServerRegion]utils.HarukiSekaiAssetUpdaterConfig{
			utils.HarukiSekaiServerRegionJP: {
				Enabled: false,
			},
		},
	})

	app := setupTestApp()
	status, resp := doJSONRequest(
		t,
		app,
		http.MethodPost,
		"/update_asset",
		[]byte(`{"server":"jp","assetVersion":"1","assetHash":"h"}`),
		nil,
	)

	if status != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, status)
	}
	if got := resp["message"]; got != "Asset updater for this region is not enabled" {
		t.Fatalf("unexpected message: %v", got)
	}
}

func TestUpdateAssetHandler_Success(t *testing.T) {
	withConfig(t, config.Config{
		Concurrents: utils.ConcurrentConfig{
			ConcurrentDownload: 1,
		},
		Servers: map[utils.HarukiSekaiServerRegion]utils.HarukiSekaiAssetUpdaterConfig{
			utils.HarukiSekaiServerRegionJP: {
				Enabled:   true,
				AESKeyHex: "zz", // force constructor to return nil quickly
				AESIVHex:  "00",
			},
		},
		Profiles: map[utils.HarukiSekaiServerRegion]map[string]string{
			utils.HarukiSekaiServerRegionJP: {},
		},
	})

	app := setupTestApp()
	status, resp := doJSONRequest(
		t,
		app,
		http.MethodPost,
		"/update_asset",
		[]byte(`{"server":"jp","assetVersion":"1","assetHash":"h"}`),
		nil,
	)

	if status != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, status)
	}
	if got := resp["message"]; got != "Asset updater started running" {
		t.Fatalf("unexpected message: %v", got)
	}
}
