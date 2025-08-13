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
		URLPatterns:     []string{"custom\\.mp3$"},
		ContentTypes:    []string{"custom/type"},
		RequiredHeaders: []string{"X-Custom"},
		TimeoutSeconds:  10,
		CommonPorts:     []string{"9000"},
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

	t.Run("valid ICEcast URLs", func(t *testing.T) {
		url := TestCDNStreamURL
		result := detector.DetectFromURL(url)
		assert.Equal(t, common.StreamTypeICEcast, result, "URL should be detected as ICEcast: %s", url)
	})

	t.Run("invalid URLs", func(t *testing.T) {
		for _, url := range TestInvalidICEcastURLs {
			result := detector.DetectFromURL(url)
			assert.Equal(t, common.StreamTypeUnsupported, result, "URL should not be detected as ICEcast: %s", url)
		}
	})

	t.Run("malformed URL", func(t *testing.T) {
		result := detector.DetectFromURL("://invalid-url")
		assert.Equal(t, common.StreamTypeUnsupported, result)
	})

	t.Run("custom patterns", func(t *testing.T) {
		config := &DetectionConfig{
			URLPatterns: []string{"/custom/.*\\.stream$"},
			CommonPorts: []string{}, // Disable port detection
		}
		detector := NewDetectorWithConfig(config)

		result := detector.DetectFromURL("https://example.com/custom/test.stream")
		assert.Equal(t, common.StreamTypeICEcast, result)

		result = detector.DetectFromURL("https://example.com/stream.mp3")
		assert.Equal(t, common.StreamTypeUnsupported, result)
	})
}

func TestDetectFromHeaders(t *testing.T) {
	detector := NewDetector()

	t.Run("valid ICEcast content type", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		result := detector.DetectFromHeaders(ctx, server.URL, nil)
		assert.Equal(t, common.StreamTypeICEcast, result)
	})

	t.Run("alternative MP3 content type", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mp3")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		result := detector.DetectFromHeaders(ctx, server.URL, nil)
		assert.Equal(t, common.StreamTypeICEcast, result)
	})

	t.Run("invalid content type", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		result := detector.DetectFromHeaders(ctx, server.URL, nil)
		assert.Equal(t, common.StreamTypeUnsupported, result)
	})

	t.Run("required headers present", func(t *testing.T) {
		config := &DetectionConfig{
			ContentTypes:    []string{"audio/mpeg"},
			RequiredHeaders: []string{"X-Stream-Type"},
		}
		detector := NewDetectorWithConfig(config)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.Header().Set("X-Stream-Type", "live")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		httpConfig := &HTTPConfig{
			UserAgent:    "Test/1.0",
			AcceptHeader: "audio/*",
		}

		ctx := context.Background()
		result := detector.DetectFromHeaders(ctx, server.URL, httpConfig)
		assert.Equal(t, common.StreamTypeICEcast, result)
	})

	t.Run("required headers missing", func(t *testing.T) {
		config := &DetectionConfig{
			ContentTypes:    []string{"audio/mpeg"},
			RequiredHeaders: []string{"X-Stream-Type"},
		}
		detector := NewDetectorWithConfig(config)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
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
			assert.Equal(t, "audio/mpeg", r.Header.Get("Accept"))

			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		httpConfig := &HTTPConfig{
			UserAgent:    "CustomAgent/1.0",
			AcceptHeader: "audio/mpeg",
		}

		ctx := context.Background()
		result := detector.DetectFromHeaders(ctx, server.URL, httpConfig)
		assert.Equal(t, common.StreamTypeICEcast, result)
	})
}

func TestIsValidICEcastContent(t *testing.T) {
	detector := NewDetector()

	t.Run("valid content", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
			w.Write(TestMP3AudioData)
		}))
		defer server.Close()

		ctx := context.Background()
		isValid := detector.IsValidICEcastContent(ctx, server.URL, nil)
		assert.True(t, isValid)
	})

	t.Run("invalid content", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("not an mp3 file"))
		}))
		defer server.Close()

		ctx := context.Background()
		isValid := detector.IsValidICEcastContent(ctx, server.URL, nil)
		assert.False(t, isValid)
	})

	t.Run("empty content", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
			// No content written
		}))
		defer server.Close()

		ctx := context.Background()
		isValid := detector.IsValidICEcastContent(ctx, server.URL, nil)
		assert.False(t, isValid) // Empty content should be invalid
	})
}

func TestDetectType(t *testing.T) {
	client := &http.Client{Timeout: 5 * time.Second}

	t.Run("detect by URL pattern", func(t *testing.T) {
		ctx := context.Background()
		streamType, err := DetectType(ctx, client, "http://stream.example.com:8000/stream.mp3")

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeICEcast, streamType)
	})

	t.Run("detect by headers", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		streamType, err := DetectType(ctx, client, server.URL)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeICEcast, streamType)
	})

	t.Run("unsupported stream", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
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
			w.Header().Set("Content-Type", "audio/mpeg")
			w.Header().Set("icy-name", "Test Station")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		metadata, err := ProbeStream(ctx, client, server.URL)

		assert.NoError(t, err)
		assert.NotNil(t, metadata)
		assert.Equal(t, common.StreamTypeICEcast, metadata.Type)
		assert.Equal(t, server.URL, metadata.URL)
		assert.Equal(t, "mp3", metadata.Format)
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
		assert.Equal(t, common.StreamTypeICEcast, streamType)
	})

	t.Run("with nil config", func(t *testing.T) {
		ctx := context.Background()
		streamType, err := DetectTypeWithConfig(ctx, TestSRCStreamURL, nil)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeICEcast, streamType)
	})

	t.Run("unsupported type", func(t *testing.T) {
		ctx := context.Background()
		streamType, err := DetectTypeWithConfig(ctx, "https://example.com/file.m3u8", config)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeUnsupported, streamType)
	})
}

func TestProbeStreamWithConfig(t *testing.T) {
	config := DefaultConfig()

	t.Run("with valid stream", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		metadata, err := ProbeStreamWithConfig(ctx, server.URL, config)

		assert.NoError(t, err)
		assert.NotNil(t, metadata)
		assert.Equal(t, common.StreamTypeICEcast, metadata.Type)
		assert.Equal(t, "mp3", metadata.Codec)      // From default config
		assert.Equal(t, 44100, metadata.SampleRate) // From default config
		assert.Equal(t, 2, metadata.Channels)       // From default config
	})

	t.Run("with nil config", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		metadata, err := ProbeStreamWithConfig(ctx, server.URL, nil)

		assert.NoError(t, err)
		assert.NotNil(t, metadata)
	})

	t.Run("fallback metadata", func(t *testing.T) {
		config := DefaultConfig()
		config.MetadataExtractor.DefaultValues["bitrate"] = 128

		ctx := context.Background()
		metadata, err := ProbeStreamWithConfig(ctx, "https://nonexistent.example.com/stream.mp3", config)

		// Should return an error since the URL doesn't exist
		assert.Error(t, err)
		assert.Nil(t, metadata)
	})
}

func TestConfigurableDetection(t *testing.T) {
	config := DefaultConfig()

	t.Run("successful detection", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
			w.Write(TestMP3AudioData)
		}))
		defer server.Close()

		ctx := context.Background()
		streamType, err := ConfigurableDetection(ctx, server.URL, config)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeICEcast, streamType)
	})

	t.Run("URL detection but connection failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		// Modify URL to look like ICEcast
		icecastURL := server.URL + "/stream.mp3"

		ctx := context.Background()
		streamType, err := ConfigurableDetection(ctx, icecastURL, config)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeICEcast, streamType)
	})

	t.Run("header detection", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "HEAD" {
				w.Header().Set("Content-Type", "audio/mpeg")
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
			w.Write(TestMP3AudioData)
		}))
		defer server.Close()

		ctx := context.Background()
		streamType, err := ConfigurableDetection(ctx, server.URL, config)

		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeICEcast, streamType)
	})

	t.Run("with nil config", func(t *testing.T) {
		ctx := context.Background()
		streamType, err := ConfigurableDetection(ctx, "http://stream.example.com:8000/test.mp3", nil)

		// Should use default config
		assert.NoError(t, err)
		assert.Equal(t, common.StreamTypeICEcast, streamType)
	})
}

func TestBackwardCompatibilityFunctions(t *testing.T) {
	t.Run("DetectFromURL backward compatibility", func(t *testing.T) {
		streamType := DetectFromURL("http://stream.example.com:8000/stream.mp3")
		assert.Equal(t, common.StreamTypeICEcast, streamType)
	})

	t.Run("DetectFromHeaders backward compatibility", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := &http.Client{Timeout: 5 * time.Second}
		ctx := context.Background()
		streamType := DetectFromHeaders(ctx, client, server.URL)
		assert.Equal(t, common.StreamTypeICEcast, streamType)
	})

	t.Run("IsValidICEcastContent backward compatibility", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "audio/mpeg")
			w.WriteHeader(http.StatusOK)
			w.Write(TestMP3AudioData)
		}))
		defer server.Close()

		client := &http.Client{Timeout: 5 * time.Second}
		ctx := context.Background()
		isValid := IsValidICEcastContent(ctx, client, server.URL)
		assert.True(t, isValid)
	})
}

func TestHTTPConfigGetHTTPHeaders(t *testing.T) {
	httpConfig := &HTTPConfig{
		UserAgent:    "TestAgent/1.0",
		AcceptHeader: "audio/mpeg",
		CustomHeaders: map[string]string{
			"X-Custom-1": "value1",
			"X-Custom-2": "value2",
		},
		RequestICYMeta: true,
	}

	headers := httpConfig.GetHTTPHeaders()

	assert.Equal(t, "TestAgent/1.0", headers["User-Agent"])
	assert.Equal(t, "audio/mpeg", headers["Accept"])
	assert.Equal(t, "value1", headers["X-Custom-1"])
	assert.Equal(t, "value2", headers["X-Custom-2"])
	assert.Equal(t, "1", headers["Icy-MetaData"])
	assert.Len(t, headers, 5)
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
		assert.Equal(t, common.StreamTypeUnsupported, streamType)

		// Try to probe actual stream (might fail due to network)
		metadata, err := ProbeStream(ctx, detector.client, TestSRCStreamURL)
		if err != nil {
			logger.Warn("Failed to probe SRC stream (expected in some environments)", logging.Fields{
				"url":   TestSRCStreamURL,
				"error": err.Error(),
			})
		} else {
			assert.NotNil(t, metadata)
			logger.Info("Successfully probed SRC stream", logging.Fields{
				"url":         TestSRCStreamURL,
				"station":     metadata.Station,
				"genre":       metadata.Genre,
				"bitrate":     metadata.Bitrate,
				"codec":       metadata.Codec,
				"sample_rate": metadata.SampleRate,
				"channels":    metadata.Channels,
			})
		}
	})

	t.Run("test CDN stream URL", func(t *testing.T) {
		streamType := detector.DetectFromURL(TestCDNStreamURL)
		assert.Equal(t, common.StreamTypeICEcast, streamType)

		// Try to probe actual stream (might fail due to network)
		metadata, err := ProbeStream(ctx, detector.client, TestCDNStreamURL)
		if err != nil {
			logger.Warn("Failed to probe CDN stream (expected in some environments)", logging.Fields{
				"url":   TestCDNStreamURL,
				"error": err.Error(),
			})
		} else {
			assert.NotNil(t, metadata)
			logger.Info("Successfully probed CDN stream", logging.Fields{
				"url":         TestCDNStreamURL,
				"station":     metadata.Station,
				"genre":       metadata.Genre,
				"bitrate":     metadata.Bitrate,
				"codec":       metadata.Codec,
				"sample_rate": metadata.SampleRate,
				"channels":    metadata.Channels,
			})
		}
	})
}
