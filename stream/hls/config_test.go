package hls

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.NotNil(t, config)
	assert.NotNil(t, config.Parser)
	assert.NotNil(t, config.MetadataExtractor)
	assert.NotNil(t, config.Detection)
	assert.NotNil(t, config.HTTP)
	assert.NotNil(t, config.Audio)

	// Test parser defaults
	assert.False(t, config.Parser.StrictMode)
	assert.Equal(t, 10, config.Parser.MaxSegmentAnalysis)
	assert.NotNil(t, config.Parser.CustomTagHandlers)
	assert.False(t, config.Parser.IgnoreUnknownTags)
	assert.False(t, config.Parser.ValidateURIs)

	// Test metadata extractor defaults
	assert.True(t, config.MetadataExtractor.EnableURLPatterns)
	assert.True(t, config.MetadataExtractor.EnableHeaderMappings)
	assert.True(t, config.MetadataExtractor.EnableSegmentAnalysis)
	assert.NotNil(t, config.MetadataExtractor.DefaultValues)
	assert.Equal(t, "aac", config.MetadataExtractor.DefaultValues["codec"])
	assert.Equal(t, 2, config.MetadataExtractor.DefaultValues["channels"])
	assert.Equal(t, 44100, config.MetadataExtractor.DefaultValues["sample_rate"])

	// Test detection defaults
	assert.Len(t, config.Detection.URLPatterns, 4)
	assert.Contains(t, config.Detection.URLPatterns, `\.m3u8$`)
	assert.Len(t, config.Detection.ContentTypes, 3)
	assert.Contains(t, config.Detection.ContentTypes, "application/vnd.apple.mpegurl")
	assert.Equal(t, 5, config.Detection.TimeoutSeconds)

	// Test HTTP defaults
	assert.Equal(t, "TuneIn-CDN-Benchmark/1.0", config.HTTP.UserAgent)
	assert.Equal(t, "application/vnd.apple.mpegurl,application/x-mpegurl,text/plain", config.HTTP.AcceptHeader)
	assert.Equal(t, 5*time.Second, config.HTTP.ConnectionTimeout)
	assert.Equal(t, 15*time.Second, config.HTTP.ReadTimeout)
	assert.Equal(t, 5, config.HTTP.MaxRedirects)
	assert.Equal(t, 16384, config.HTTP.BufferSize)

	// Test audio defaults
	assert.Equal(t, 30*time.Second, config.Audio.SampleDuration)
	assert.Equal(t, 2*time.Second, config.Audio.BufferDuration)
	assert.Equal(t, 10, config.Audio.MaxSegments)
	assert.False(t, config.Audio.FollowLive)
	assert.False(t, config.Audio.AnalyzeSegments)
}

func TestConfigFromAppConfig(t *testing.T) {
	appConfig := map[string]any{
		"stream": map[string]any{
			"user_agent":         "CustomAgent/1.0",
			"connection_timeout": 10 * time.Second,
			"read_timeout":       30 * time.Second,
			"max_redirects":      10,
			"buffer_size":        32768,
			"headers": map[string]string{
				"X-Custom-Header": "value",
			},
		},
		"audio": map[string]any{
			"buffer_duration": 5 * time.Second,
			"sample_rate":     48000,
			"channels":        1,
		},
		"hls": map[string]any{
			"parser": map[string]any{
				"strict_mode":          true,
				"max_segment_analysis": 20,
				"ignore_unknown_tags":  true,
				"validate_uris":        true,
			},
			"detection": map[string]any{
				"timeout_seconds": 10,
				"url_patterns":    []string{`custom\.m3u8$`},
				"content_types":   []string{"custom/type"},
			},
			"http": map[string]any{
				"user_agent":    "OverrideAgent/1.0",
				"accept_header": "custom/accept",
				"custom_headers": map[string]string{
					"X-HLS-Header": "hls-value",
				},
			},
			"audio": map[string]any{
				"sample_duration":  60 * time.Second,
				"max_segments":     20,
				"follow_live":      true,
				"analyze_segments": true,
			},
		},
	}

	config := ConfigFromAppConfig(appConfig)

	// Test stream config overrides
	assert.Equal(t, 10*time.Second, config.HTTP.ConnectionTimeout)
	assert.Equal(t, 30*time.Second, config.HTTP.ReadTimeout)
	assert.Equal(t, 10, config.HTTP.MaxRedirects)
	assert.Equal(t, 32768, config.HTTP.BufferSize)

	// Test audio config overrides
	assert.Equal(t, 5*time.Second, config.Audio.BufferDuration)
	assert.Equal(t, 48000, config.MetadataExtractor.DefaultValues["sample_rate"])
	assert.Equal(t, 1, config.MetadataExtractor.DefaultValues["channels"])

	// Test HLS-specific overrides
	assert.True(t, config.Parser.StrictMode)
	assert.Equal(t, 20, config.Parser.MaxSegmentAnalysis)
	assert.True(t, config.Parser.IgnoreUnknownTags)
	assert.True(t, config.Parser.ValidateURIs)

	assert.Equal(t, 10, config.Detection.TimeoutSeconds)
	assert.Equal(t, []string{`custom\.m3u8$`}, config.Detection.URLPatterns)
	assert.Equal(t, []string{"custom/type"}, config.Detection.ContentTypes)

	// HTTP should be overridden by HLS-specific config
	assert.Equal(t, "OverrideAgent/1.0", config.HTTP.UserAgent)
	assert.Equal(t, "custom/accept", config.HTTP.AcceptHeader)
	assert.Equal(t, "hls-value", config.HTTP.CustomHeaders["X-HLS-Header"])

	assert.Equal(t, 60*time.Second, config.Audio.SampleDuration)
	assert.Equal(t, 20, config.Audio.MaxSegments)
	assert.True(t, config.Audio.FollowLive)
	assert.True(t, config.Audio.AnalyzeSegments)
}

func TestConfigFromMap(t *testing.T) {
	configMap := map[string]any{
		"hls": map[string]any{
			"parser": map[string]any{
				"strict_mode": true,
			},
		},
	}

	config := ConfigFromMap(configMap)
	assert.NotNil(t, config)
	assert.True(t, config.Parser.StrictMode)
}

func TestConfigValidation(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		config := DefaultConfig()
		err := config.Validate()
		assert.NoError(t, err)
	})

	t.Run("invalid connection timeout", func(t *testing.T) {
		config := DefaultConfig()
		config.HTTP.ConnectionTimeout = -1 * time.Second
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection timeout must be positive")
	})

	t.Run("invalid read timeout", func(t *testing.T) {
		config := DefaultConfig()
		config.HTTP.ReadTimeout = 0
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "read timeout must be positive")
	})

	t.Run("invalid max redirects", func(t *testing.T) {
		config := DefaultConfig()
		config.HTTP.MaxRedirects = -1
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max redirects cannot be negative")
	})

	t.Run("invalid buffer size", func(t *testing.T) {
		config := DefaultConfig()
		config.HTTP.BufferSize = 0
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "buffer size must be positive")
	})

	t.Run("invalid sample duration", func(t *testing.T) {
		config := DefaultConfig()
		config.Audio.SampleDuration = 0
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sample duration must be positive")
	})
}

func TestGetHTTPHeaders(t *testing.T) {
	config := DefaultConfig()
	config.HTTP.CustomHeaders["X-Test"] = "test-value"

	headers := config.GetHTTPHeaders()

	assert.Equal(t, config.HTTP.UserAgent, headers["User-Agent"])
	assert.Equal(t, config.HTTP.AcceptHeader, headers["Accept"])
	assert.Equal(t, "test-value", headers["X-Test"])
}

func TestConfigurableMetadataExtractor(t *testing.T) {
	config := &MetadataExtractorConfig{
		EnableURLPatterns:     true,
		EnableHeaderMappings:  true,
		EnableSegmentAnalysis: true,
		CustomPatterns: []CustomURLPattern{
			{
				Pattern: `/test/(\d+)`,
				Fields: map[string]string{
					"bitrate": "1",
				},
				Priority:    100,
				Description: "Test pattern",
			},
		},
		CustomHeaderMappings: []CustomHeaderMapping{
			{
				HeaderKey:   "x-test-bitrate",
				MetadataKey: "bitrate",
				Transform:   "int",
				Description: "Test header mapping",
			},
		},
		DefaultValues: map[string]any{
			"codec":       "mp3",
			"sample_rate": 48000,
			"channels":    1,
		},
	}

	extractor := NewConfigurableMetadataExtractor(config)
	assert.NotNil(t, extractor)
	assert.NotNil(t, extractor.MetadataExtractor)
	assert.Equal(t, config, extractor.config)
}

func TestConfigurableMetadataExtractorWithNilConfig(t *testing.T) {
	extractor := NewConfigurableMetadataExtractor(nil)
	assert.NotNil(t, extractor)
	assert.NotNil(t, extractor.config)
	assert.Equal(t, DefaultConfig().MetadataExtractor, extractor.config)
}

func TestCustomPatternExtractor(t *testing.T) {
	config := &MetadataExtractorConfig{
		CustomPatterns: []CustomURLPattern{
			{
				Pattern: `/bitrate/(\d+)`,
				Fields: map[string]string{
					"bitrate": "1",
				},
				Priority:    100,
				Description: "Bitrate pattern",
			},
		},
		DefaultValues: make(map[string]any),
	}

	extractor := NewConfigurableMetadataExtractor(config)

	// Create test metadata
	metadata := &common.StreamMetadata{
		Headers: make(map[string]string),
	}

	// Test the custom pattern extractor
	matches := []string{"full_match", "128"}
	extractor.createCustomPatternExtractor(config.CustomPatterns[0].Fields)(matches, metadata)

	assert.Equal(t, 128, metadata.Bitrate)
}

func TestCustomTransformer(t *testing.T) {
	config := &MetadataExtractorConfig{}
	extractor := NewConfigurableMetadataExtractor(config)

	t.Run("int transform", func(t *testing.T) {
		transformer := extractor.createCustomTransformer("int")
		result := transformer("123")
		assert.Equal(t, 123, result)

		result = transformer("invalid")
		assert.Nil(t, result)
	})

	t.Run("float transform", func(t *testing.T) {
		transformer := extractor.createCustomTransformer("float")
		result := transformer("123.45")
		assert.Equal(t, 123.45, result)

		result = transformer("invalid")
		assert.Nil(t, result)
	})

	t.Run("bool transform", func(t *testing.T) {
		transformer := extractor.createCustomTransformer("bool")
		result := transformer("true")
		assert.Equal(t, true, result)

		result = transformer("invalid")
		assert.Nil(t, result)
	})

	t.Run("string transforms", func(t *testing.T) {
		lower := extractor.createCustomTransformer("lower")
		assert.Equal(t, "test", lower("TEST"))

		upper := extractor.createCustomTransformer("upper")
		assert.Equal(t, "TEST", upper("test"))

		title := extractor.createCustomTransformer("title")
		assert.Equal(t, "Test String", title("test string"))

		none := extractor.createCustomTransformer("unknown")
		assert.Equal(t, "unchanged", none("unchanged"))
	})
}

func TestSetMetadataField(t *testing.T) {
	config := &MetadataExtractorConfig{}
	extractor := NewConfigurableMetadataExtractor(config)

	metadata := &common.StreamMetadata{
		Headers: make(map[string]string),
	}

	// Test known fields
	extractor.setMetadataField(metadata, "bitrate", "128")
	assert.Equal(t, 128, metadata.Bitrate)

	extractor.setMetadataField(metadata, "sample_rate", "44100")
	assert.Equal(t, 44100, metadata.SampleRate)

	extractor.setMetadataField(metadata, "channels", "2")
	assert.Equal(t, 2, metadata.Channels)

	extractor.setMetadataField(metadata, "codec", "aac")
	assert.Equal(t, "aac", metadata.Codec)

	extractor.setMetadataField(metadata, "station", "Test Station")
	assert.Equal(t, "Test Station", metadata.Station)

	// Test unknown field goes to headers
	extractor.setMetadataField(metadata, "custom_field", "custom_value")
	assert.Equal(t, "custom_value", metadata.Headers["custom_field"])
}

func TestApplyDefaults(t *testing.T) {
	config := &MetadataExtractorConfig{
		DefaultValues: map[string]any{
			"codec":       "aac",
			"channels":    2,
			"sample_rate": 44100,
			"bitrate":     128,
		},
	}

	extractor := NewConfigurableMetadataExtractor(config)

	t.Run("apply defaults to empty metadata", func(t *testing.T) {
		metadata := &common.StreamMetadata{}
		extractor.applyDefaults(metadata)

		assert.Equal(t, "aac", metadata.Codec)
		assert.Equal(t, 2, metadata.Channels)
		assert.Equal(t, 44100, metadata.SampleRate)
		assert.Equal(t, 128, metadata.Bitrate)
	})

	t.Run("don't override existing values", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Codec:      "mp3",
			Channels:   1,
			SampleRate: 48000,
			Bitrate:    256,
		}
		extractor.applyDefaults(metadata)

		assert.Equal(t, "mp3", metadata.Codec)
		assert.Equal(t, 1, metadata.Channels)
		assert.Equal(t, 48000, metadata.SampleRate)
		assert.Equal(t, 256, metadata.Bitrate)
	})

	t.Run("handle float64 defaults", func(t *testing.T) {
		config := &MetadataExtractorConfig{
			DefaultValues: map[string]any{
				"channels":    float64(2),
				"sample_rate": float64(44100),
				"bitrate":     float64(128),
			},
		}

		extractor := NewConfigurableMetadataExtractor(config)
		metadata := &common.StreamMetadata{}
		extractor.applyDefaults(metadata)

		assert.Equal(t, 2, metadata.Channels)
		assert.Equal(t, 44100, metadata.SampleRate)
		assert.Equal(t, 128, metadata.Bitrate)
	})
}

func TestConfigSerialization(t *testing.T) {
	config := DefaultConfig()

	// Test JSON marshaling (should work for most fields)
	data, err := json.Marshal(config)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Test JSON unmarshaling
	var newConfig Config
	err = json.Unmarshal(data, &newConfig)
	require.NoError(t, err)

	// Compare some key fields
	assert.Equal(t, config.Parser.StrictMode, newConfig.Parser.StrictMode)
	assert.Equal(t, config.HTTP.UserAgent, newConfig.HTTP.UserAgent)
	assert.Equal(t, config.Audio.MaxSegments, newConfig.Audio.MaxSegments)

	// Note: AudioDecoder is marked as non-serializable so it won't be included
	assert.Nil(t, newConfig.AudioDecoder)
}

func TestCustomTagHandlers(t *testing.T) {
	config := &ParserConfig{
		CustomTagHandlers: map[string]string{
			"#EXT-X-CUSTOM":    "custom_handler",
			"#EXT-X-MYCOMPANY": "company_handler",
		},
	}

	parser := NewConfigurableParser(config)
	assert.NotNil(t, parser)
	assert.Equal(t, config, parser.config)
}
