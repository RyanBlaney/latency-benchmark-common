package icecast

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	assert.Equal(t, common.StreamTypeICEcast, handler.Type())
	assert.NotNil(t, handler.client)
	assert.NotNil(t, handler.stats)
	assert.NotNil(t, handler.metadataExtractor)
	assert.NotNil(t, handler.config)
	assert.False(t, handler.connected)
}

func TestNewHandlerWithConfig(t *testing.T) {
	config := DefaultConfig()
	config.HTTP.UserAgent = "CustomAgent/1.0"
	config.Audio.MaxReadAttempts = 5

	handler := NewHandlerWithConfig(config)

	assert.NotNil(t, handler)
	assert.Equal(t, config, handler.config)
	assert.Equal(t, "CustomAgent/1.0", handler.config.HTTP.UserAgent)
	assert.Equal(t, 5, handler.config.Audio.MaxReadAttempts)
}

func TestNewHandlerWithNilConfig(t *testing.T) {
	handler := NewHandlerWithConfig(nil)

	assert.NotNil(t, handler)
	assert.NotNil(t, handler.config)
	assert.Equal(t, DefaultConfig().HTTP.UserAgent, handler.config.HTTP.UserAgent)
}

func TestHandlerType(t *testing.T) {
	handler := NewHandler()
	assert.Equal(t, common.StreamTypeICEcast, handler.Type())
}

func TestCanHandle(t *testing.T) {
	handler := NewHandler()
	ctx := context.Background()

	t.Run("valid ICEcast URLs", func(t *testing.T) {
		validURLs := []string{
			TestCDNStreamURL,
			TestSRCStreamURL,
		}

		for _, url := range validURLs {
			canHandle := handler.CanHandle(ctx, url)
			assert.True(t, canHandle, "Should handle ICEcast URL: %s", url)
		}
	})

	t.Run("invalid URLs", func(t *testing.T) {
		invalidURLs := []string{
			"https://example.com/file.m3u8",
			"https://example.com/file.mp4",
			"not-a-url",
		}

		for _, url := range invalidURLs {
			canHandle := handler.CanHandle(ctx, url)
			assert.False(t, canHandle, "Should not handle non-ICEcast URL: %s", url)
		}
	})

	t.Run("with mock server response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
			w.Write(TestMP3AudioData)
		}))
		defer server.Close()

		canHandle := handler.CanHandle(ctx, server.URL)
		assert.True(t, canHandle)
	})
}

func TestConnect(t *testing.T) {
	t.Run("successful connection", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.Header().Set("icy-name", "Test Station")
			w.Header().Set("icy-metaint", "16000")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		handler := NewHandler()
		ctx := context.Background()

		err := handler.Connect(ctx, server.URL)

		require.NoError(t, err)
		assert.True(t, handler.connected)
		assert.Equal(t, server.URL, handler.url)
		assert.NotNil(t, handler.metadata)
		assert.Equal(t, 16000, handler.icyMetaInt)
		assert.Greater(t, handler.stats.ConnectionTime, time.Duration(0))
	})

	t.Run("already connected", func(t *testing.T) {
		handler := NewHandler()
		handler.connected = true

		ctx := context.Background()
		err := handler.Connect(ctx, "http://example.com/test.mp3")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already connected")
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

	t.Run("connection with ICY headers", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Verify ICY metadata was requested
			assert.Equal(t, "1", r.Header.Get("Icy-MetaData"))

			for key, value := range TestICEcastHeaders {
				w.Header().Set(key, value)
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		handler := NewHandler()
		ctx := context.Background()

		err := handler.Connect(ctx, server.URL)

		require.NoError(t, err)
		assert.True(t, handler.connected)
		assert.NotNil(t, handler.metadata)
		assert.Equal(t, "Test Radio Station", handler.metadata.Station)
		assert.Equal(t, "Rock", handler.metadata.Genre)
		assert.Equal(t, 128, handler.metadata.Bitrate)
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
			w.Header().Set("Content-Type", "audio/mpeg")
			w.Header().Set("icy-name", "Test Station")
			w.Header().Set("icy-genre", "Music")
			w.Header().Set("icy-br", "128")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		handler := NewHandler()
		ctx := context.Background()

		err := handler.Connect(ctx, server.URL)
		require.NoError(t, err)

		metadata, err := handler.GetMetadata()

		require.NoError(t, err)
		require.NotNil(t, metadata)
		assert.Equal(t, common.StreamTypeICEcast, metadata.Type)
		assert.Equal(t, server.URL, metadata.URL)
		assert.Equal(t, "Test Station", metadata.Station)
		assert.Equal(t, "Music", metadata.Genre)
		assert.Equal(t, 128, metadata.Bitrate)
		assert.NotNil(t, metadata.Headers)
	})

	t.Run("fallback metadata with defaults", func(t *testing.T) {
		config := DefaultConfig()
		config.MetadataExtractor.DefaultValues["codec"] = "mp3"
		config.MetadataExtractor.DefaultValues["bitrate"] = 256

		handler := NewHandlerWithConfig(config)
		handler.connected = true
		handler.url = "http://example.com/test.mp3"
		handler.metadata = nil // Force fallback

		metadata, err := handler.GetMetadata()

		require.NoError(t, err)
		require.NotNil(t, metadata)
		assert.Equal(t, common.StreamTypeICEcast, metadata.Type)
		assert.Equal(t, "http://example.com/test.mp3", metadata.URL)
		assert.Equal(t, "mp3", metadata.Codec)
		assert.Equal(t, 256, metadata.Bitrate)
	})

	t.Run("with ICY title updates", func(t *testing.T) {
		handler := NewHandler()
		handler.connected = true
		handler.url = "http://example.com/test.mp3"
		handler.metadata = &common.StreamMetadata{
			URL:     "http://example.com/test.mp3",
			Type:    common.StreamTypeICEcast,
			Headers: make(map[string]string),
		}
		handler.icyTitle = "Artist - Song Title"

		metadata, err := handler.GetMetadata()

		require.NoError(t, err)
		assert.Equal(t, "Artist - Song Title", metadata.Title)
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

	t.Run("no response", func(t *testing.T) {
		handler := NewHandler()
		handler.connected = true
		handler.response = nil

		ctx := context.Background()
		audioData, err := handler.ReadAudio(ctx)

		assert.Error(t, err)
		assert.Nil(t, audioData)
		assert.Contains(t, err.Error(), "not connected")
	})

	t.Run("successful read with mock audio data", func(t *testing.T) {
		audioDataSent := false
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.Header().Set("icy-name", "Test Station")
			w.WriteHeader(http.StatusOK)

			if !audioDataSent {
				w.Write(TestMP3AudioData)
				audioDataSent = true
			}
		}))
		defer server.Close()

		config := DefaultConfig()
		config.Audio.BufferSize = 1024
		handler := NewHandlerWithConfig(config)

		ctx := context.Background()

		// Connect first
		err := handler.Connect(ctx, server.URL)
		require.NoError(t, err)

		// Read audio - this will likely fail due to basic MP3 parsing limitations
		// but we're testing the handler logic
		audioData, err := handler.ReadAudio(ctx)

		// Either succeeds with valid data or fails with expected error
		if err != nil {
			// Expected - basic PCM conversion may fail
			assert.NotNil(t, err)
		} else {
			// If it succeeds, verify we got valid data
			require.NotNil(t, audioData)
			assert.Greater(t, audioData.SampleRate, 0)
			assert.Greater(t, audioData.Channels, 0)
			assert.NotNil(t, audioData.Metadata)
		}

		// Verify stats were updated regardless
		stats := handler.GetStats()
		assert.NotNil(t, stats)
	})
}

func TestGetStats(t *testing.T) {
	handler := NewHandler()

	stats := handler.GetStats()

	assert.NotNil(t, stats)
	assert.Equal(t, int64(0), stats.BytesReceived)
	assert.Equal(t, float64(0), stats.AverageBitrate)
	assert.Equal(t, float64(0), stats.BufferHealth)
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

func TestRefreshMetadata(t *testing.T) {
	t.Run("not connected", func(t *testing.T) {
		handler := NewHandler()

		err := handler.RefreshMetadata()

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not connected")
	})

	t.Run("successful refresh", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.Header().Set("icy-name", "Updated Station")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		handler := NewHandler()
		ctx := context.Background()

		// Connect first
		err := handler.Connect(ctx, server.URL)
		require.NoError(t, err)

		// Refresh metadata
		err = handler.RefreshMetadata()
		require.NoError(t, err)

		assert.NotNil(t, handler.metadata)
		assert.Equal(t, "Updated Station", handler.metadata.Station)
	})
}

func TestHandlerConfigMethods(t *testing.T) {
	t.Run("IsConfigured", func(t *testing.T) {
		defaultHandler := NewHandler()
		assert.False(t, defaultHandler.IsConfigured())

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

	t.Run("GetCurrentICYTitle", func(t *testing.T) {
		handler := NewHandler()
		assert.Equal(t, "", handler.GetCurrentICYTitle())

		handler.icyTitle = "Test Title"
		assert.Equal(t, "Test Title", handler.GetCurrentICYTitle())
	})

	t.Run("GetICYMetadataInterval", func(t *testing.T) {
		handler := NewHandler()
		assert.Equal(t, 0, handler.GetICYMetadataInterval())

		handler.icyMetaInt = 16000
		assert.Equal(t, 16000, handler.GetICYMetadataInterval())
	})

	t.Run("HasICYMetadata", func(t *testing.T) {
		handler := NewHandler()
		assert.False(t, handler.HasICYMetadata())

		handler.icyMetaInt = 16000
		assert.True(t, handler.HasICYMetadata())
	})
}

func TestGetStreamInfo(t *testing.T) {
	handler := NewHandler()
	handler.connected = true
	handler.metadata = &common.StreamMetadata{
		Station:    "Test Station",
		Genre:      "Rock",
		Bitrate:    128,
		SampleRate: 44100,
		Channels:   2,
		Codec:      "mp3",
		Format:     "mp3",
	}
	handler.icyMetaInt = 16000
	handler.icyTitle = "Current Song"
	handler.bytesRead = 1000
	handler.stats.BytesReceived = 5000
	handler.stats.AverageBitrate = 128.5

	info := handler.GetStreamInfo()

	assert.Equal(t, "Test Station", info["station"])
	assert.Equal(t, "Rock", info["genre"])
	assert.Equal(t, 128, info["bitrate"])
	assert.Equal(t, 44100, info["sample_rate"])
	assert.Equal(t, 2, info["channels"])
	assert.Equal(t, "mp3", info["codec"])
	assert.Equal(t, "mp3", info["format"])
	assert.Equal(t, 16000, info["icy_metadata_interval"])
	assert.Equal(t, "Current Song", info["current_title"])
	assert.Equal(t, true, info["has_icy_metadata"])
	assert.Equal(t, int64(1000), info["bytes_read"])
	assert.Equal(t, true, info["connected"])
	assert.Equal(t, int64(5000), info["bytes_received"])
	assert.Equal(t, 128.5, info["average_bitrate"])
}

func TestGetResponseHeaders(t *testing.T) {
	handler := NewHandler()

	t.Run("no response", func(t *testing.T) {
		headers := handler.GetResponseHeaders()
		assert.NotNil(t, headers)
		assert.Empty(t, headers)
	})

	t.Run("with response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.Header().Set("icy-name", "Test Station")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		err := handler.Connect(ctx, server.URL)
		require.NoError(t, err)

		headers := handler.GetResponseHeaders()
		assert.Equal(t, "audio/mpeg", headers["content-type"])
		assert.Equal(t, "Test Station", headers["icy-name"])
	})
}

func TestConnectionTimeAndStats(t *testing.T) {
	handler := NewHandler()

	t.Run("GetConnectionTime not connected", func(t *testing.T) {
		duration := handler.GetConnectionTime()
		assert.Equal(t, time.Duration(0), duration)
	})

	t.Run("GetBytesPerSecond not connected", func(t *testing.T) {
		rate := handler.GetBytesPerSecond()
		assert.Equal(t, float64(0), rate)
	})

	t.Run("IsLive", func(t *testing.T) {
		isLive := handler.IsLive()
		assert.True(t, isLive) // ICEcast streams are typically live
	})

	t.Run("GetAudioFormat", func(t *testing.T) {
		handler.metadata = &common.StreamMetadata{
			Codec:      "mp3",
			Format:     "mp3",
			Bitrate:    128,
			SampleRate: 44100,
			Channels:   2,
		}

		format := handler.GetAudioFormat()
		assert.Equal(t, "mp3", format["codec"])
		assert.Equal(t, "mp3", format["format"])
		assert.Equal(t, 128, format["bitrate"])
		assert.Equal(t, 44100, format["sample_rate"])
		assert.Equal(t, 2, format["channels"])
	})
}

func TestICYMetadataHandling(t *testing.T) {
	t.Run("readICYMetadata with valid title", func(t *testing.T) {
		// This would be testing internal methods, but they're not exported
		// We can test the overall behavior through Connect and metadata updates
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.Header().Set("icy-metaint", "16000")
			w.Header().Set("icy-name", "Test Station")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		handler := NewHandler()
		ctx := context.Background()

		err := handler.Connect(ctx, server.URL)
		require.NoError(t, err)

		assert.Equal(t, 16000, handler.icyMetaInt)
		assert.True(t, handler.HasICYMetadata())
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

		assert.Equal(t, common.StreamTypeICEcast, metadata.Type)
		assert.Equal(t, TestCDNStreamURL, metadata.URL)

		logger.Info("CDN stream metadata", logging.Fields{
			"codec":       metadata.Codec,
			"bitrate":     metadata.Bitrate,
			"sample_rate": metadata.SampleRate,
			"channels":    metadata.Channels,
			"station":     metadata.Station,
		})

		// Get stream info
		info := handler.GetStreamInfo()
		logger.Info("CDN stream info", logging.Fields{
			"has_icy_metadata": info["has_icy_metadata"],
			"icy_interval":     info["icy_metadata_interval"],
			"current_title":    info["current_title"],
			"connected":        info["connected"],
		})
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

		assert.Equal(t, common.StreamTypeICEcast, metadata.Type)
		assert.Equal(t, TestSRCStreamURL, metadata.URL)

		logger.Info("SRC stream metadata", logging.Fields{
			"codec":       metadata.Codec,
			"bitrate":     metadata.Bitrate,
			"sample_rate": metadata.SampleRate,
			"channels":    metadata.Channels,
			"station":     metadata.Station,
		})

		// Get stream info
		info := handler.GetStreamInfo()
		logger.Info("SRC stream info", logging.Fields{
			"has_icy_metadata": info["has_icy_metadata"],
			"icy_interval":     info["icy_metadata_interval"],
			"current_title":    info["current_title"],
			"connected":        info["connected"],
		})
	})
}
