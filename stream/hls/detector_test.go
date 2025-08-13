package hls

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"github.com/stretchr/testify/assert"
)

func TestNewDetector(t *testing.T) {
	detector := NewDetector()
	assert.NotNil(t, detector)
	assert.NotNil(t, detector.config)
	assert.NotNil(t, detector.client)
	assert.Equal(t, DefaultConfig().Detection, detector.config)
}

func TestNewDetectorWithConfig(t *testing.T) {
	config := &DetectionConfig{
		URLPatterns:     []string{`custom\.m3u8$`},
		ContentTypes:    []string{"custom/type"},
		RequiredHeaders: []string{"X-Custom"},
		TimeoutSeconds:  10,
	}

	detector := NewDetectorWithConfig(config)
	assert.NotNil(t, detector)
	assert.Equal(t, config, detector.config)
	assert.Equal(t, 10*time.Second, detector.client.Timeout)
}

func TestNewDetectorWithNilConfig(t *testing.T) {
	detector := NewDetectorWithConfig(nil)
	assert.NotNil(t, detector)
	assert.Equal(t, DefaultConfig().Detection, detector.config)
}

func TestDetectFromURL(t *testing.T) {
	detector := NewDetector()

	t.Run("valid HLS URLs", func(t *testing.T) {
		validURLs := []string{
			"https://example.com/playlist.m3u8",
			"https://example.com/master.m3u8",
			"https://example.com/index.m3u8",
			"https://example.com/stream/playlist.m3u8",
			"https://example.com/path/to/file.m3u8",
		}

		for _, url := range validURLs {
			result := detector.DetectFromURL(url)
			assert.Equal(t, common.StreamTypeHLS, result, "URL should be detected as HLS: %s", url)
		}
	})

	t.Run("invalid URLs", func(t *testing.T) {
		invalidURLs := []string{
			"https://example.com/file.mp3",
			"https://example.com/file.mp4",
			"https://example.com/playlist.txt",
			"not-a-url",
			"",
		}

		for _, url := range invalidURLs {
			result := detector.DetectFromURL(url)
			assert.Equal(t, common.StreamTypeUnsupported, result, "URL should not be detected as HLS: %s", url)
		}
	})

	t.Run("malformed URL", func(t *testing.T) {
		result := detector.DetectFromURL("://invalid-url")
		assert.Equal(t, common.StreamTypeUnsupported, result)
	})

	t.Run("custom patterns", func(t *testing.T) {
		config := &DetectionConfig{
			URLPatterns: []string{`/custom/.*\.stream$`},
		}
		detector := NewDetectorWithConfig(config)

		result := detector.DetectFromURL("https://example.com/custom/test.stream")
		assert.Equal(t, common.StreamTypeHLS, result)

		result = detector.DetectFromURL("https://example.com/playlist.m3u8")
		assert.Equal(t, common.StreamTypeUnsupported, result)
	})
}

func TestDetectFromHeaders(t *testing.T) {
	detector := NewDetector()

	t.Run("valid HLS content type", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		result := detector.DetectFromHeaders(ctx, server.URL, nil)
		assert.Equal(t, common.StreamTypeHLS, result)
	})

	t.Run("alternative HLS content type", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-mpegurl")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		result := detector.DetectFromHeaders(ctx, server.URL, nil)
		assert.Equal(t, common.StreamTypeHLS, result)
	})

	t.Run("invalid content type", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		result := detector.DetectFromHeaders(ctx, server.URL, nil)
		assert.Equal(t, common.StreamTypeUnsupported, result)
	})

	t.Run("required headers present", func(t *testing.T) {
		config := &DetectionConfig{
			ContentTypes:    []string{"application/vnd.apple.mpegurl"},
			RequiredHeaders: []string{"X-Stream-Type"},
		}
		detector := NewDetectorWithConfig(config)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.Header().Set("X-Stream-Type", "live")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		httpConfig := &HTTPConfig{
			UserAgent:    "Test/1.0",
			AcceptHeader: "*/*",
		}

		ctx := context.Background()
		result := detector.DetectFromHeaders(ctx, server.URL, httpConfig)
		assert.Equal(t, common.StreamTypeHLS, result)
	})

	t.Run("required headers missing", func(t *testing.T) {
		config := &DetectionConfig{
			ContentTypes:    []string{"application/vnd.apple.mpegurl"},
			RequiredHeaders: []string{"X-Stream-Type"},
		}
		detector := NewDetectorWithConfig(config)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			// Missing X-Stream-Type header
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		result := detector.DetectFromHeaders(ctx, server.URL, nil)
		assert.Equal(t, common.StreamTypeUnsupported, result)
	})

	t.Run("server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		ctx := context.Background()
		result := detector.DetectFromHeaders(ctx, server.URL, nil)
		assert.Equal(t, common.StreamTypeUnsupported, result)
	})

	t.Run("with custom HTTP config", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "CustomAgent/1.0", r.Header.Get("User-Agent"))
			assert.Equal(t, "application/vnd.apple.mpegurl", r.Header.Get("Accept"))

			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		httpConfig := &HTTPConfig{
			UserAgent:    "CustomAgent/1.0",
			AcceptHeader: "application/vnd.apple.mpegurl",
		}

		ctx := context.Background()
		result := detector.DetectFromHeaders(ctx, server.URL, httpConfig)
		assert.Equal(t, common.StreamTypeHLS, result)
	})
}

func TestDetectFromM3U8Content(t *testing.T) {
	detector := NewDetector()

	t.Run("valid M3U8 content", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		ctx := context.Background()
		playlist, err := detector.DetectFromM3U8Content(ctx, server.URL, nil, nil)

		assert.NoError(t, err)
		assert.NotNil(t, playlist)
		assert.True(t, playlist.IsValid)
		assert.False(t, playlist.IsMaster)
		assert.Len(t, playlist.Segments, 3)
	})

	t.Run("master playlist", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MasterPlaylist))
		}))
		defer server.Close()

		ctx := context.Background()
		playlist, err := detector.DetectFromM3U8Content(ctx, server.URL, nil, nil)

		assert.NoError(t, err)
		assert.NotNil(t, playlist)
		assert.True(t, playlist.IsValid)
		assert.True(t, playlist.IsMaster)
		assert.Len(t, playlist.Variants, 3)
	})

	t.Run("invalid M3U8 content", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("invalid content"))
		}))
		defer server.Close()

		ctx := context.Background()
		playlist, err := detector.DetectFromM3U8Content(ctx, server.URL, nil, nil)

		assert.Error(t, err)
		assert.Nil(t, playlist)
	})

	t.Run("HTTP error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		ctx := context.Background()
		playlist, err := detector.DetectFromM3U8Content(ctx, server.URL, nil, nil)

		assert.Error(t, err)
		assert.Nil(t, playlist)
		assert.Contains(t, err.Error(), "404")
	})

	t.Run("with custom HTTP config", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "CustomAgent/1.0", r.Header.Get("User-Agent"))

			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		httpConfig := &HTTPConfig{
			UserAgent:    "CustomAgent/1.0",
			AcceptHeader: "application/vnd.apple.mpegurl",
			BufferSize:   8192,
		}

		ctx := context.Background()
		playlist, err := detector.DetectFromM3U8Content(ctx, server.URL, httpConfig, nil)

		assert.NoError(t, err)
		assert.NotNil(t, playlist)
		assert.True(t, playlist.IsValid)
	})

	t.Run("timeout handling", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		config := &DetectionConfig{
			TimeoutSeconds: 1, // Short timeout but should be enough
		}
		detector := NewDetectorWithConfig(config)

		ctx := context.Background()
		playlist, err := detector.DetectFromM3U8Content(ctx, server.URL, nil, nil)

		assert.NoError(t, err)
		assert.NotNil(t, playlist)
	})
}

func TestIsValidHLSContent(t *testing.T) {
	detector := NewDetector()

	t.Run("valid content", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		ctx := context.Background()
		isValid := detector.IsValidHLSContent(ctx, server.URL, nil, nil)
		assert.True(t, isValid)
	})

	t.Run("invalid content", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("not an m3u8 file"))
		}))
		defer server.Close()

		ctx := context.Background()
		isValid := detector.IsValidHLSContent(ctx, server.URL, nil, nil)
		assert.False(t, isValid)
	})

	t.Run("empty playlist", func(t *testing.T) {
		emptyPlaylist := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-ENDLIST"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(emptyPlaylist))
		}))
		defer server.Close()

		ctx := context.Background()
		isValid := detector.IsValidHLSContent(ctx, server.URL, nil, nil)
		assert.False(t, isValid) // Empty playlist should be invalid
	})
}

func TestDetectType(t *testing.T) {
	client := &http.Client{Timeout: 5 * time.Second}

	t.Run("detect by URL pattern", func(t *testing.T) {
		ctx := context.Background()
		streamType, err := DetectType(ctx, client, "https://example.com/playlist.m3u8")

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeHLS, streamType)
	})

	t.Run("detect by headers", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		streamType, err := DetectType(ctx, client, server.URL)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeHLS, streamType)
	})

	t.Run("unsupported stream", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		streamType, err := DetectType(ctx, client, server.URL)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeUnsupported, streamType)
	})
}

func TestProbeStream(t *testing.T) {
	client := &http.Client{Timeout: 5 * time.Second}

	t.Run("successful probe", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.Header().Set("icy-name", "Test Station")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		ctx := context.Background()
		metadata, err := ProbeStream(ctx, client, server.URL)

		assert.NoError(t, err)
		assert.NotNil(t, metadata)
		assert.Equal(t, common.StreamTypeHLS, metadata.Type)
		assert.Equal(t, server.URL, metadata.URL)
		assert.Equal(t, "hls", metadata.Format)
	})

	t.Run("probe failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		ctx := context.Background()
		metadata, err := ProbeStream(ctx, client, server.URL)

		assert.Error(t, err)
		assert.Nil(t, metadata)
	})
}

func TestDetectTypeWithConfig(t *testing.T) {
	config := DefaultConfig()

	t.Run("successful detection", func(t *testing.T) {
		ctx := context.Background()
		streamType, err := DetectTypeWithConfig(ctx, TestCDNStreamURL, config)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeHLS, streamType)
	})

	t.Run("with nil config", func(t *testing.T) {
		ctx := context.Background()
		streamType, err := DetectTypeWithConfig(ctx, TestSRCStreamURL, nil)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeHLS, streamType)
	})

	t.Run("unsupported type", func(t *testing.T) {
		ctx := context.Background()
		streamType, err := DetectTypeWithConfig(ctx, "https://example.com/file.mp3", config)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeUnsupported, streamType)
	})
}

func TestProbeStreamWithConfig(t *testing.T) {
	config := DefaultConfig()

	t.Run("with valid playlist", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		ctx := context.Background()
		metadata, err := ProbeStreamWithConfig(ctx, server.URL, config)

		assert.NoError(t, err)
		assert.NotNil(t, metadata)
		assert.Equal(t, common.StreamTypeHLS, metadata.Type)
		assert.Equal(t, "aac", metadata.Codec)      // From default config
		assert.Equal(t, 44100, metadata.SampleRate) // From default config
		assert.Equal(t, 2, metadata.Channels)       // From default config
	})

	t.Run("with nil config", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		ctx := context.Background()
		metadata, err := ProbeStreamWithConfig(ctx, server.URL, nil)

		assert.NoError(t, err)
		assert.NotNil(t, metadata)
	})

	t.Run("fallback metadata", func(t *testing.T) {
		// Test case where playlist parsing fails but we still return basic metadata
		config := DefaultConfig()
		config.MetadataExtractor.DefaultValues["bitrate"] = 128

		ctx := context.Background()
		metadata, err := ProbeStreamWithConfig(ctx, "https://nonexistent.example.com/playlist.m3u8", config)

		// Should return an error since the URL doesn't exist
		assert.Error(t, err)
		assert.Nil(t, metadata)
	})
}

func TestConfigurableDetection(t *testing.T) {
	config := DefaultConfig()

	t.Run("successful detection and parsing", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		ctx := context.Background()
		streamType, playlist, err := ConfigurableDetection(ctx, server.URL, config)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeHLS, streamType)
		assert.NotNil(t, playlist)
		assert.True(t, playlist.IsValid)
	})

	t.Run("URL detection but parse failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		// Modify URL to look like HLS
		hlsURL := server.URL + "/playlist.m3u8"

		ctx := context.Background()
		streamType, playlist, err := ConfigurableDetection(ctx, hlsURL, config)

		assert.Error(t, err)
		assert.Equal(t, common.StreamTypeUnsupported, streamType)
		assert.Nil(t, playlist)
	})

	t.Run("header detection", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "HEAD" {
				w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		ctx := context.Background()
		streamType, playlist, err := ConfigurableDetection(ctx, server.URL, config)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeHLS, streamType)
		assert.NotNil(t, playlist)
	})

	t.Run("with nil config", func(t *testing.T) {
		ctx := context.Background()
		streamType, playlist, err := ConfigurableDetection(ctx, "https://example.com/playlist.m3u8", nil)

		// Should use default config
		assert.Error(t, err)
		assert.Equal(t, common.StreamTypeUnsupported, streamType)
		assert.Nil(t, playlist) // Will be nil due to network error
	})
}

func TestBackwardCompatibilityFunctions(t *testing.T) {
	t.Run("DetectFromURL backward compatibility", func(t *testing.T) {
		streamType := DetectFromURL("https://example.com/playlist.m3u8")
		assert.Equal(t, common.StreamTypeHLS, streamType)
	})

	t.Run("DetectFromHeaders backward compatibility", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := &http.Client{Timeout: 5 * time.Second}
		ctx := context.Background()
		streamType := DetectFromHeaders(ctx, client, server.URL)
		assert.Equal(t, common.StreamTypeHLS, streamType)
	})

	t.Run("DetectFromM3U8Content backward compatibility", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		client := &http.Client{Timeout: 5 * time.Second}
		ctx := context.Background()
		playlist, err := DetectFromM3U8Content(ctx, client, server.URL)

		assert.NoError(t, err)
		assert.NotNil(t, playlist)
		assert.True(t, playlist.IsValid)
	})

	t.Run("IsValidHLSContent backward compatibility", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		client := &http.Client{Timeout: 5 * time.Second}
		ctx := context.Background()
		isValid := IsValidHLSContent(ctx, client, server.URL)
		assert.True(t, isValid)
	})
}

func TestHTTPConfigGetHTTPHeaders(t *testing.T) {
	httpConfig := &HTTPConfig{
		UserAgent:    "TestAgent/1.0",
		AcceptHeader: "application/test",
		CustomHeaders: map[string]string{
			"X-Custom-1": "value1",
			"X-Custom-2": "value2",
		},
	}

	headers := httpConfig.GetHTTPHeaders()

	assert.Equal(t, "TestAgent/1.0", headers["User-Agent"])
	assert.Equal(t, "application/test", headers["Accept"])
	assert.Equal(t, "value1", headers["X-Custom-1"])
	assert.Equal(t, "value2", headers["X-Custom-2"])
	assert.Len(t, headers, 4)
}

func TestDetectorWithRealURLs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping real URL tests in short mode")
	}

	detector := NewDetector()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := logging.NewDefaultLogger().WithFields(logging.Fields{
		"test": "TestDetectorWithRealURLs",
	})

	t.Run("test SRC stream URL", func(t *testing.T) {
		streamType := detector.DetectFromURL(TestSRCStreamURL)
		assert.Equal(t, common.StreamTypeHLS, streamType)

		// Try to fetch actual content (might fail due to network)
		playlist, err := detector.DetectFromM3U8Content(ctx, TestSRCStreamURL, nil, nil)
		if err != nil {
			logger.Warn("Failed to fetch SRC stream (expected in some environments)", logging.Fields{
				"url":   TestSRCStreamURL,
				"error": err.Error(),
			})
		} else {
			assert.NotNil(t, playlist)
			logger.Info("Successfully fetched SRC stream", logging.Fields{
				"url":           TestSRCStreamURL,
				"is_valid":      playlist.IsValid,
				"is_master":     playlist.IsMaster,
				"segment_count": len(playlist.Segments),
				"variant_count": len(playlist.Variants),
			})
		}
	})

	t.Run("test CDN stream URL", func(t *testing.T) {
		streamType := detector.DetectFromURL(TestCDNStreamURL)
		assert.Equal(t, common.StreamTypeHLS, streamType)

		// Try to fetch actual content (might fail due to network)
		playlist, err := detector.DetectFromM3U8Content(ctx, TestCDNStreamURL, nil, nil)
		if err != nil {
			logger.Warn("Failed to fetch CDN stream (expected in some environments)", logging.Fields{
				"url":   TestCDNStreamURL,
				"error": err.Error(),
			})
		} else {
			assert.NotNil(t, playlist)
			logger.Info("Successfully fetched CDN stream", logging.Fields{
				"url":           TestCDNStreamURL,
				"is_valid":      playlist.IsValid,
				"is_master":     playlist.IsMaster,
				"segment_count": len(playlist.Segments),
				"variant_count": len(playlist.Variants),
			})
		}
	})
}
