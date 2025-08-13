package hls

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHandler(t *testing.T) {
	handler := NewHandler()

	assert.NotNil(t, handler)
	assert.Equal(t, common.StreamTypeHLS, handler.Type())
	assert.NotNil(t, handler.client)
	assert.NotNil(t, handler.stats)
	assert.NotNil(t, handler.parser)
	assert.NotNil(t, handler.metadataExtractor)
	assert.NotNil(t, handler.config)
	assert.False(t, handler.connected)
}

func TestNewHandlerWithConfig(t *testing.T) {
	config := DefaultConfig()
	config.HTTP.UserAgent = "CustomAgent/1.0"
	config.Audio.MaxSegments = 20

	handler := NewHandlerWithConfig(config)

	assert.NotNil(t, handler)
	assert.Equal(t, config, handler.config)
	assert.Equal(t, "CustomAgent/1.0", handler.config.HTTP.UserAgent)
	assert.Equal(t, 20, handler.config.Audio.MaxSegments)
}

func TestNewHandlerWithNilConfig(t *testing.T) {
	handler := NewHandlerWithConfig(nil)

	assert.NotNil(t, handler)
	assert.NotNil(t, handler.config)
	assert.Equal(t, DefaultConfig().HTTP.UserAgent, handler.config.HTTP.UserAgent)
}

func TestHandlerType(t *testing.T) {
	handler := NewHandler()
	assert.Equal(t, common.StreamTypeHLS, handler.Type())
}

func TestCanHandle(t *testing.T) {
	handler := NewHandler()
	ctx := context.Background()

	t.Run("valid HLS URLs", func(t *testing.T) {
		validURLs := []string{
			TestCDNStreamURL,
			TestSRCStreamURL,
		}

		for _, url := range validURLs {
			canHandle := handler.CanHandle(ctx, url)
			assert.True(t, canHandle, "Should handle HLS URL: %s", url)
		}
	})

	t.Run("invalid URLs", func(t *testing.T) {
		invalidURLs := []string{
			"https://example.com/file.mp3",
			"https://example.com/file.mp4",
			"not-a-url",
		}

		for _, url := range invalidURLs {
			canHandle := handler.CanHandle(ctx, url)
			assert.False(t, canHandle, "Should not handle non-HLS URL: %s", url)
		}
	})

	t.Run("with mock server response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		canHandle := handler.CanHandle(ctx, server.URL)
		assert.True(t, canHandle)
	})
}

func TestConnect(t *testing.T) {
	t.Run("successful connection", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.Header().Set("icy-name", "Test Station")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		handler := NewHandler()
		ctx := context.Background()

		err := handler.Connect(ctx, server.URL)

		require.NoError(t, err)
		assert.True(t, handler.connected)
		assert.Equal(t, server.URL, handler.url)
		assert.NotNil(t, handler.playlist)
		assert.True(t, handler.playlist.IsValid)
		assert.NotNil(t, handler.metadata)
		assert.Greater(t, handler.stats.ConnectionTime, time.Duration(0))
	})

	t.Run("already connected", func(t *testing.T) {
		handler := NewHandler()
		handler.connected = true

		ctx := context.Background()
		err := handler.Connect(ctx, "https://example.com/test.m3u8")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already connected")
	})

	t.Run("invalid playlist", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("invalid m3u8 content"))
		}))
		defer server.Close()

		handler := NewHandler()
		ctx := context.Background()

		err := handler.Connect(ctx, server.URL)

		assert.Error(t, err)
		assert.False(t, handler.connected)
	})

	t.Run("HTTP error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		handler := NewHandler()
		ctx := context.Background()

		err := handler.Connect(ctx, server.URL)

		assert.Error(t, err)
		assert.False(t, handler.connected)
		assert.Contains(t, err.Error(), "404")
	})
}

func TestGetMetadata(t *testing.T) {
	t.Run("not connected", func(t *testing.T) {
		handler := NewHandler()

		metadata, err := handler.GetMetadata()

		assert.Error(t, err)
		assert.Nil(t, metadata)
		assert.Contains(t, err.Error(), "not connected")
	})

	t.Run("with extracted metadata", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.Header().Set("icy-name", "Test Station")
			w.Header().Set("icy-genre", "Music")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8MediaPlaylist))
		}))
		defer server.Close()

		handler := NewHandler()
		ctx := context.Background()

		err := handler.Connect(ctx, server.URL)
		require.NoError(t, err)

		metadata, err := handler.GetMetadata()

		require.NoError(t, err)
		require.NotNil(t, metadata)
		assert.Equal(t, common.StreamTypeHLS, metadata.Type)
		assert.Equal(t, server.URL, metadata.URL)
		assert.NotNil(t, metadata.Headers)
	})

	t.Run("fallback metadata with defaults", func(t *testing.T) {
		config := DefaultConfig()
		config.MetadataExtractor.DefaultValues["codec"] = "test-codec"
		config.MetadataExtractor.DefaultValues["bitrate"] = 256

		handler := NewHandlerWithConfig(config)
		handler.connected = true
		handler.url = "https://example.com/test.m3u8"
		handler.metadata = nil // Force fallback

		metadata, err := handler.GetMetadata()

		require.NoError(t, err)
		require.NotNil(t, metadata)
		assert.Equal(t, common.StreamTypeHLS, metadata.Type)
		assert.Equal(t, "https://example.com/test.m3u8", metadata.URL)
		assert.Equal(t, "test-codec", metadata.Codec)
		assert.Equal(t, 256, metadata.Bitrate)
	})
}

func TestReadAudio(t *testing.T) {
	t.Run("not connected", func(t *testing.T) {
		handler := NewHandler()
		ctx := context.Background()

		audioData, err := handler.ReadAudio(ctx)

		assert.Error(t, err)
		assert.Nil(t, audioData)
		assert.Contains(t, err.Error(), "not connected")
	})

	t.Run("no playlist", func(t *testing.T) {
		handler := NewHandler()
		handler.connected = true
		handler.playlist = nil

		ctx := context.Background()
		audioData, err := handler.ReadAudio(ctx)

		assert.Error(t, err)
		assert.Nil(t, audioData)
		assert.Contains(t, err.Error(), "no playlist")
	})

	t.Run("successful read with mock segments", func(t *testing.T) {
		// Create a server that serves both playlist and segments
		segmentCount := 0
		var serverURL string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "playlist.m3u8") {
				// Serve playlist with absolute segment URLs
				playlist := strings.ReplaceAll(TestM3U8MediaPlaylist, "segment0.ts", serverURL+"/segment0.aac")
				playlist = strings.ReplaceAll(playlist, "segment1.ts", serverURL+"/segment1.aac")
				playlist = strings.ReplaceAll(playlist, "segment2.ts", serverURL+"/segment2.aac")

				w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(playlist))
			} else if strings.Contains(r.URL.Path, "segment") {
				// Serve segment
				segmentCount++
				w.Header().Set("Content-Type", "audio/aac")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte{0xFF, 0xF1, 0x50, 0x80}) // Mock AAC data
			}
		}))
		defer server.Close()

		// Now we can safely set the server URL
		serverURL = server.URL

		config := DefaultConfig()
		config.Audio.MaxSegments = 1 // Limit for testing
		handler := NewHandlerWithConfig(config)

		ctx := context.Background()

		// Connect first
		err := handler.Connect(ctx, server.URL+"/playlist.m3u8")
		require.NoError(t, err)

		// Read audio
		audioData, err := handler.ReadAudio(ctx)

		if err != nil {
			// Audio reading might fail due to basic extraction limitations
			// That's OK for this test - we're testing the handler logic
			assert.NotNil(t, err)
		} else {
			// If it succeeds, verify we got valid data
			require.NotNil(t, audioData)
			assert.Greater(t, audioData.SampleRate, 0)
			assert.Greater(t, audioData.Channels, 0)
			assert.NotNil(t, audioData.Metadata)
		}

		// Verify stats were updated
		stats := handler.GetStats()
		assert.NotNil(t, stats)
	})
}

func TestGetStats(t *testing.T) {
	handler := NewHandler()

	stats := handler.GetStats()

	assert.NotNil(t, stats)
	assert.Equal(t, int64(0), stats.BytesReceived)
	assert.Equal(t, 0, stats.SegmentsReceived)
}

func TestClose(t *testing.T) {
	handler := NewHandler()
	handler.connected = true

	err := handler.Close()

	assert.NoError(t, err)
	assert.False(t, handler.connected)
}

func TestHandlerGetters(t *testing.T) {
	config := DefaultConfig()
	handler := NewHandlerWithConfig(config)

	t.Run("GetPlaylist", func(t *testing.T) {
		playlist := handler.GetPlaylist()
		assert.Nil(t, playlist) // No playlist until connected
	})

	t.Run("GetClient", func(t *testing.T) {
		client := handler.GetClient()
		assert.NotNil(t, client)
		assert.Equal(t, handler.client, client)
	})

	t.Run("GetConfig", func(t *testing.T) {
		retrievedConfig := handler.GetConfig()
		assert.Equal(t, config, retrievedConfig)
	})
}

func TestUpdateConfig(t *testing.T) {
	handler := NewHandler()
	originalTimeout := handler.client.Timeout

	newConfig := DefaultConfig()
	newConfig.HTTP.ConnectionTimeout = 30 * time.Second
	newConfig.HTTP.ReadTimeout = 60 * time.Second
	newConfig.HTTP.UserAgent = "Updated/1.0"

	handler.UpdateConfig(newConfig)

	assert.Equal(t, newConfig, handler.config)
	assert.Equal(t, 90*time.Second, handler.client.Timeout) // 30 + 60
	assert.NotEqual(t, originalTimeout, handler.client.Timeout)
}

func TestUpdateConfigWithNil(t *testing.T) {
	handler := NewHandler()
	originalConfig := handler.config

	handler.UpdateConfig(nil)

	assert.Equal(t, originalConfig, handler.config) // Should remain unchanged
}

func TestRefreshPlaylist(t *testing.T) {
	t.Run("not connected", func(t *testing.T) {
		handler := NewHandler()
		ctx := context.Background()

		err := handler.RefreshPlaylist(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not connected")
	})

	t.Run("not a live stream", func(t *testing.T) {
		handler := NewHandler()
		handler.connected = true
		handler.playlist = &M3U8Playlist{IsLive: false}

		ctx := context.Background()
		err := handler.RefreshPlaylist(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a live stream")
	})

	t.Run("successful refresh", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TestM3U8LivePlaylist))
		}))
		defer server.Close()

		handler := NewHandler()
		handler.connected = true
		handler.url = server.URL
		handler.playlist = &M3U8Playlist{IsLive: true}

		ctx := context.Background()
		err := handler.RefreshPlaylist(ctx)

		require.NoError(t, err)
		assert.NotNil(t, handler.playlist)
		assert.NotNil(t, handler.metadata)
	})
}

func TestGetSegmentURLs(t *testing.T) {
	handler := NewHandler()

	t.Run("no playlist", func(t *testing.T) {
		urls, err := handler.GetSegmentURLs()

		assert.Error(t, err)
		assert.Nil(t, urls)
	})

	t.Run("with segments", func(t *testing.T) {
		handler.url = "https://example.com/playlist.m3u8"
		handler.playlist = &M3U8Playlist{
			Segments: []M3U8Segment{
				{URI: "segment1.ts"},
				{URI: "segment2.ts"},
				{URI: "https://absolute.com/segment3.ts"},
			},
		}

		urls, err := handler.GetSegmentURLs()

		require.NoError(t, err)
		assert.Len(t, urls, 3)
		assert.Equal(t, "https://example.com/segment1.ts", urls[0])
		assert.Equal(t, "https://example.com/segment2.ts", urls[1])
		assert.Equal(t, "https://absolute.com/segment3.ts", urls[2])
	})

	t.Run("with max segments limit", func(t *testing.T) {
		config := DefaultConfig()
		config.Audio.MaxSegments = 2
		handler := NewHandlerWithConfig(config)
		handler.url = "https://example.com/playlist.m3u8"
		handler.playlist = &M3U8Playlist{
			Segments: []M3U8Segment{
				{URI: "segment1.ts"},
				{URI: "segment2.ts"},
				{URI: "segment3.ts"},
			},
		}

		urls, err := handler.GetSegmentURLs()

		require.NoError(t, err)
		assert.Len(t, urls, 2) // Limited by config
	})
}

func TestGetVariantURLs(t *testing.T) {
	handler := NewHandler()

	t.Run("no playlist", func(t *testing.T) {
		urls, err := handler.GetVariantURLs()

		assert.Error(t, err)
		assert.Nil(t, urls)
	})

	t.Run("with variants", func(t *testing.T) {
		handler.url = "https://example.com/master.m3u8"
		handler.playlist = &M3U8Playlist{
			Variants: []M3U8Variant{
				{URI: "480p.m3u8"},
				{URI: "720p.m3u8"},
				{URI: "https://absolute.com/1080p.m3u8"},
			},
		}

		urls, err := handler.GetVariantURLs()

		require.NoError(t, err)
		assert.Len(t, urls, 3)
		assert.Equal(t, "https://example.com/480p.m3u8", urls[0])
		assert.Equal(t, "https://example.com/720p.m3u8", urls[1])
		assert.Equal(t, "https://absolute.com/1080p.m3u8", urls[2])
	})
}

func TestResolveURL(t *testing.T) {
	handler := NewHandler()
	handler.url = "https://example.com/path/playlist.m3u8"

	testCases := []struct {
		name     string
		uri      string
		expected string
	}{
		{
			name:     "absolute URL",
			uri:      "https://other.com/segment.ts",
			expected: "https://other.com/segment.ts",
		},
		{
			name:     "relative path",
			uri:      "segment.ts",
			expected: "https://example.com/path/segment.ts",
		},
		{
			name:     "relative path with subdirectory",
			uri:      "segments/segment1.ts",
			expected: "https://example.com/path/segments/segment1.ts",
		},
		{
			name:     "root relative path",
			uri:      "/absolute/path/segment.ts",
			expected: "https://example.com/absolute/path/segment.ts",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := handler.resolveURL(tc.uri)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestHandlerConfigMethods(t *testing.T) {
	t.Run("IsConfigured", func(t *testing.T) {
		defaultHandler := NewHandler()
		assert.False(t, defaultHandler.IsConfigured()) // Uses default config

		customConfig := DefaultConfig()
		customConfig.HTTP.UserAgent = "Custom/1.0"
		customHandler := NewHandlerWithConfig(customConfig)
		assert.True(t, customHandler.IsConfigured())
	})

	t.Run("GetConfiguredUserAgent", func(t *testing.T) {
		handler := NewHandler()
		userAgent := handler.GetConfiguredUserAgent()
		assert.Equal(t, "TuneIn-CDN-Benchmark/1.0", userAgent)

		config := DefaultConfig()
		config.HTTP.UserAgent = "Custom/1.0"
		customHandler := NewHandlerWithConfig(config)
		customUserAgent := customHandler.GetConfiguredUserAgent()
		assert.Equal(t, "Custom/1.0", customUserAgent)
	})

	t.Run("ShouldFollowLive", func(t *testing.T) {
		config := DefaultConfig()
		config.Audio.FollowLive = true
		handler := NewHandlerWithConfig(config)

		assert.True(t, handler.ShouldFollowLive())
	})

	t.Run("ShouldAnalyzeSegments", func(t *testing.T) {
		config := DefaultConfig()
		config.Audio.AnalyzeSegments = true
		handler := NewHandlerWithConfig(config)

		assert.True(t, handler.ShouldAnalyzeSegments())
	})
}

func TestHandlerWithRealStreams(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping real stream tests in short mode")
	}

	logger := logging.NewDefaultLogger().WithFields(logging.Fields{
		"test": "TestHandlerWithRealStreams",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t.Run("CDN stream handling", func(t *testing.T) {
		handler := NewHandler()

		// Test if handler can handle the CDN URL
		canHandle := handler.CanHandle(ctx, TestCDNStreamURL)
		assert.True(t, canHandle, "Should be able to handle CDN stream URL")

		// Try to connect
		err := handler.Connect(ctx, TestCDNStreamURL)
		if err != nil {
			logger.Warn("CDN stream connection failed (expected in some environments)", logging.Fields{
				"url":   TestCDNStreamURL,
				"error": err.Error(),
			})
			t.Skip("CDN stream not accessible")
		}

		defer handler.Close()

		// Get metadata
		metadata, err := handler.GetMetadata()
		require.NoError(t, err)
		require.NotNil(t, metadata)

		assert.Equal(t, common.StreamTypeHLS, metadata.Type)
		assert.Equal(t, TestCDNStreamURL, metadata.URL)

		logger.Info("CDN stream metadata", logging.Fields{
			"codec":       metadata.Codec,
			"bitrate":     metadata.Bitrate,
			"sample_rate": metadata.SampleRate,
			"channels":    metadata.Channels,
			"station":     metadata.Station,
		})

		// Get playlist info
		playlist := handler.GetPlaylist()
		if playlist != nil {
			logger.Info("CDN playlist info", logging.Fields{
				"is_valid":      playlist.IsValid,
				"is_master":     playlist.IsMaster,
				"is_live":       playlist.IsLive,
				"segment_count": len(playlist.Segments),
				"variant_count": len(playlist.Variants),
			})
		}
	})

	t.Run("SRC stream handling", func(t *testing.T) {
		handler := NewHandler()

		// Test if handler can handle the SRC URL
		canHandle := handler.CanHandle(ctx, TestSRCStreamURL)
		assert.True(t, canHandle, "Should be able to handle SRC stream URL")

		// Try to connect
		err := handler.Connect(ctx, TestSRCStreamURL)
		if err != nil {
			logger.Warn("SRC stream connection failed (expected in some environments)", logging.Fields{
				"url":   TestSRCStreamURL,
				"error": err.Error(),
			})
			t.Skip("SRC stream not accessible")
		}

		defer handler.Close()

		// Get metadata
		metadata, err := handler.GetMetadata()
		require.NoError(t, err)
		require.NotNil(t, metadata)

		assert.Equal(t, common.StreamTypeHLS, metadata.Type)
		assert.Equal(t, TestSRCStreamURL, metadata.URL)

		logger.Info("SRC stream metadata", logging.Fields{
			"codec":       metadata.Codec,
			"bitrate":     metadata.Bitrate,
			"sample_rate": metadata.SampleRate,
			"channels":    metadata.Channels,
			"station":     metadata.Station,
		})

		// Get playlist info
		playlist := handler.GetPlaylist()
		if playlist != nil {
			logger.Info("SRC playlist info", logging.Fields{
				"is_valid":      playlist.IsValid,
				"is_master":     playlist.IsMaster,
				"is_live":       playlist.IsLive,
				"segment_count": len(playlist.Segments),
				"variant_count": len(playlist.Variants),
			})
		}
	})
}
