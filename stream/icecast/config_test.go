package icecast

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.NotNil(t, config)
	assert.NotNil(t, config.MetadataExtractor)
	assert.NotNil(t, config.Detection)
	assert.NotNil(t, config.HTTP)
	assert.NotNil(t, config.Audio)

	// Test metadata extractor defaults
	assert.True(t, config.MetadataExtractor.EnableHeaderMappings)
	assert.True(t, config.MetadataExtractor.EnableICYMetadata)
	assert.NotNil(t, config.MetadataExtractor.CustomHeaderMappings)
	assert.NotNil(t, config.MetadataExtractor.DefaultValues)
	assert.Equal(t, "mp3", config.MetadataExtractor.DefaultValues["codec"])
	assert.Equal(t, 2, config.MetadataExtractor.DefaultValues["channels"])
	assert.Equal(t, 44100, config.MetadataExtractor.DefaultValues["sample_rate"])
	assert.Equal(t, "mp3", config.MetadataExtractor.DefaultValues["format"])
	assert.Equal(t, 5*time.Second, config.MetadataExtractor.ICYMetadataTimeout)

	// Test detection defaults
	assert.Len(t, config.Detection.URLPatterns, 7)
	assert.Contains(t, config.Detection.URLPatterns, `\.mp3$`)
	assert.Contains(t, config.Detection.URLPatterns, `/stream$`)
	assert.Contains(t, config.Detection.URLPatterns, `/listen$`)
	assert.Len(t, config.Detection.ContentTypes, 5)
	assert.Contains(t, config.Detection.ContentTypes, "audio/mpeg")
	assert.Contains(t, config.Detection.ContentTypes, "audio/mp3")
	assert.Equal(t, 5, config.Detection.TimeoutSeconds)
	assert.Len(t, config.Detection.CommonPorts, 4)
	assert.Contains(t, config.Detection.CommonPorts, "8000")
	assert.Contains(t, config.Detection.CommonPorts, "8080")

	// Test HTTP defaults
	assert.Equal(t, "TuneIn-CDN-Benchmark/1.0", config.HTTP.UserAgent)
	assert.Equal(t, "audio/*,*/*", config.HTTP.AcceptHeader)
	assert.Equal(t, 5*time.Second, config.HTTP.ConnectionTimeout)
	assert.Equal(t, 15*time.Second, config.HTTP.ReadTimeout)
	assert.Equal(t, 5, config.HTTP.MaxRedirects)
	assert.NotNil(t, config.HTTP.CustomHeaders)
	assert.True(t, config.HTTP.RequestICYMeta)

	// Test audio defaults
	assert.Equal(t, 4096, config.Audio.BufferSize)
	assert.Equal(t, 30*time.Second, config.Audio.SampleDuration)
	assert.Equal(t, 3, config.Audio.MaxReadAttempts)
	assert.Equal(t, 10*time.Second, config.Audio.ReadTimeout)
	assert.True(t, config.Audio.HandleICYMeta)
	assert.Equal(t, 0, config.Audio.MetadataInterval) // Will be determined from stream
}

func TestConfigFromAppConfig(t *testing.T) {
	appConfig := map[string]any{
		"stream": map[string]any{
			"user_agent":         "CustomAgent/1.0",
			"connection_timeout": 10 * time.Second,
			"read_timeout":       30 * time.Second,
			"max_redirects":      10,
			"headers": map[string]string{
				"X-Custom-Header": "value",
			},
		},
		"audio": map[string]any{
			"buffer_duration": 5 * time.Second,
			"sample_rate":     48000,
			"channels":        1,
		},
		"icecast": map[string]any{
			"metadata_extractor": map[string]any{
				"enable_header_mappings": false,
				"enable_icy_metadata":    false,
				"icy_metadata_timeout":   10 * time.Second,
				"default_values": map[string]any{
					"codec":       "mp3",
					"sample_rate": 48000,
				},
			},
			"detection": map[string]any{
				"timeout_seconds":  10,
				"url_patterns":     []string{`custom\.mp3$`},
				"content_types":    []string{"audio/custom"},
				"common_ports":     []string{"9000"},
				"required_headers": []string{"X-Stream-Type"},
			},
			"http": map[string]any{
				"user_agent":       "OverrideAgent/1.0",
				"accept_header":    "audio/mpeg",
				"request_icy_meta": false,
				"custom_headers": map[string]string{
					"X-ICEcast-Header": "icecast-value",
				},
			},
			"audio": map[string]any{
				"buffer_size":       8192,
				"sample_duration":   60 * time.Second,
				"max_read_attempts": 5,
				"handle_icy_meta":   false,
				"metadata_interval": 8000,
				"read_timeout":      20 * time.Second,
			},
		},
	}

	config := ConfigFromAppConfig(appConfig)

	// Test stream config overrides
	assert.Equal(t, 10*time.Second, config.HTTP.ConnectionTimeout)
	assert.Equal(t, 30*time.Second, config.HTTP.ReadTimeout)
	assert.Equal(t, 10, config.HTTP.MaxRedirects)

	// Test audio config overrides (buffer_duration maps to sample_duration)
	assert.Equal(t, 48000, config.MetadataExtractor.DefaultValues["sample_rate"])
	assert.Equal(t, 1, config.MetadataExtractor.DefaultValues["channels"])

	// Test ICEcast-specific overrides
	assert.False(t, config.MetadataExtractor.EnableHeaderMappings)
	assert.False(t, config.MetadataExtractor.EnableICYMetadata)
	assert.Equal(t, 10*time.Second, config.MetadataExtractor.ICYMetadataTimeout)

	assert.Equal(t, 10, config.Detection.TimeoutSeconds)
	assert.Equal(t, []string{`custom\.mp3$`}, config.Detection.URLPatterns)
	assert.Equal(t, []string{"audio/custom"}, config.Detection.ContentTypes)
	assert.Equal(t, []string{"9000"}, config.Detection.CommonPorts)
	assert.Equal(t, []string{"X-Stream-Type"}, config.Detection.RequiredHeaders)

	// HTTP should be overridden by ICEcast-specific config
	assert.Equal(t, "OverrideAgent/1.0", config.HTTP.UserAgent)
	assert.Equal(t, "audio/mpeg", config.HTTP.AcceptHeader)
	assert.False(t, config.HTTP.RequestICYMeta)
	assert.Equal(t, "icecast-value", config.HTTP.CustomHeaders["X-ICEcast-Header"])

	assert.Equal(t, 8192, config.Audio.BufferSize)
	assert.Equal(t, 60*time.Second, config.Audio.SampleDuration)
	assert.Equal(t, 5, config.Audio.MaxReadAttempts)
	assert.False(t, config.Audio.HandleICYMeta)
	assert.Equal(t, 8000, config.Audio.MetadataInterval)
	assert.Equal(t, 20*time.Second, config.Audio.ReadTimeout)
}

func TestConfigFromMap(t *testing.T) {
	configMap := map[string]any{
		"icecast": map[string]any{
			"http": map[string]any{
				"user_agent": "TestAgent/1.0",
			},
		},
	}

	config := ConfigFromMap(configMap)
	assert.NotNil(t, config)
	assert.Equal(t, "TestAgent/1.0", config.HTTP.UserAgent)
}

func TestConfigValidation(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		config := DefaultConfig()
		err := config.Validate()
		assert.NoError(t, err)
	})

	t.Run("invalid HTTP connection timeout", func(t *testing.T) {
		config := DefaultConfig()
		config.HTTP.ConnectionTimeout = -1 * time.Second
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "HTTP connection timeout must be positive")
	})

	t.Run("invalid HTTP read timeout", func(t *testing.T) {
		config := DefaultConfig()
		config.HTTP.ReadTimeout = 0
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "HTTP read timeout must be positive")
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
		config.Audio.BufferSize = 0
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

	t.Run("invalid max read attempts", func(t *testing.T) {
		config := DefaultConfig()
		config.Audio.MaxReadAttempts = 0
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max read attempts must be positive")
	})

	t.Run("invalid ICY metadata timeout", func(t *testing.T) {
		config := DefaultConfig()
		config.MetadataExtractor.ICYMetadataTimeout = 0
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ICY metadata timeout must be positive")
	})

	t.Run("invalid detection timeout", func(t *testing.T) {
		config := DefaultConfig()
		config.Detection.TimeoutSeconds = 0
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "detection timeout must be positive")
	})

	t.Run("empty URL patterns", func(t *testing.T) {
		config := DefaultConfig()
		config.Detection.URLPatterns = []string{}
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least one URL pattern must be configured")
	})

	t.Run("empty content types", func(t *testing.T) {
		config := DefaultConfig()
		config.Detection.ContentTypes = []string{}
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least one content type must be configured")
	})
}

func TestGetHTTPHeaders(t *testing.T) {
	t.Run("HTTP config headers", func(t *testing.T) {
		httpConfig := &HTTPConfig{
			UserAgent:    "TestAgent/1.0",
			AcceptHeader: "audio/mpeg",
			CustomHeaders: map[string]string{
				"X-Test": "test-value",
			},
			RequestICYMeta: true,
		}

		headers := httpConfig.GetHTTPHeaders()

		assert.Equal(t, "TestAgent/1.0", headers["User-Agent"])
		assert.Equal(t, "audio/mpeg", headers["Accept"])
		assert.Equal(t, "test-value", headers["X-Test"])
		assert.Equal(t, "1", headers["Icy-MetaData"])
	})

	t.Run("Config headers", func(t *testing.T) {
		config := DefaultConfig()
		config.HTTP.CustomHeaders["X-Custom"] = "custom-value"

		headers := config.GetHTTPHeaders()

		assert.Equal(t, config.HTTP.UserAgent, headers["User-Agent"])
		assert.Equal(t, config.HTTP.AcceptHeader, headers["Accept"])
		assert.Equal(t, "custom-value", headers["X-Custom"])
		assert.Equal(t, "1", headers["Icy-MetaData"]) // Default is true
	})

	t.Run("without ICY metadata request", func(t *testing.T) {
		config := DefaultConfig()
		config.HTTP.RequestICYMeta = false

		headers := config.GetHTTPHeaders()

		_, hasICY := headers["Icy-MetaData"]
		assert.False(t, hasICY)
	})
}

func TestConfigClone(t *testing.T) {
	original := DefaultConfig()
	original.HTTP.UserAgent = "Original/1.0"
	original.HTTP.CustomHeaders["X-Original"] = "original-value"
	original.MetadataExtractor.DefaultValues["custom"] = "original-custom"
	original.Detection.URLPatterns = append(original.Detection.URLPatterns, "custom-pattern")

	clone := original.Clone()

	// Test that clone is different object
	assert.NotSame(t, original, clone)

	// Test that values are copied
	assert.Equal(t, original.HTTP.UserAgent, clone.HTTP.UserAgent)
	assert.Equal(t, original.HTTP.CustomHeaders["X-Original"], clone.HTTP.CustomHeaders["X-Original"])
	assert.Equal(t, original.MetadataExtractor.DefaultValues["custom"], clone.MetadataExtractor.DefaultValues["custom"])

	// Test that modifying clone doesn't affect original
	clone.HTTP.UserAgent = "Clone/1.0"
	clone.HTTP.CustomHeaders["X-Clone"] = "clone-value"
	clone.MetadataExtractor.DefaultValues["clone"] = "clone-custom"

	assert.Equal(t, "Original/1.0", original.HTTP.UserAgent)
	assert.Equal(t, "Clone/1.0", clone.HTTP.UserAgent)
	assert.Equal(t, "", original.HTTP.CustomHeaders["X-Clone"])
	assert.Equal(t, "clone-value", clone.HTTP.CustomHeaders["X-Clone"])
	assert.Nil(t, original.MetadataExtractor.DefaultValues["clone"])
	assert.Equal(t, "clone-custom", clone.MetadataExtractor.DefaultValues["clone"])
}

func TestConfigMerge(t *testing.T) {
	base := DefaultConfig()
	base.HTTP.UserAgent = "Base/1.0"
	base.HTTP.CustomHeaders["X-Base"] = "base-value"

	other := DefaultConfig()
	other.HTTP.UserAgent = "Other/1.0"
	other.HTTP.AcceptHeader = "audio/mpeg"
	other.HTTP.CustomHeaders["X-Other"] = "other-value"
	other.Audio.BufferSize = 8192

	base.Merge(other)

	// Test that other config takes precedence
	assert.Equal(t, "Other/1.0", base.HTTP.UserAgent)
	assert.Equal(t, "audio/mpeg", base.HTTP.AcceptHeader)
	assert.Equal(t, 8192, base.Audio.BufferSize)

	// Test that custom headers are merged
	assert.Equal(t, "other-value", base.HTTP.CustomHeaders["X-Other"])
	// Note: Base headers may be overwritten in merge
}

func TestConfigMergeWithNil(t *testing.T) {
	config := DefaultConfig()
	originalUserAgent := config.HTTP.UserAgent

	config.Merge(nil)

	assert.Equal(t, originalUserAgent, config.HTTP.UserAgent) // Should remain unchanged
}

func TestConfigSerialization(t *testing.T) {
	config := DefaultConfig()

	// Test JSON marshaling
	data, err := json.Marshal(config)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Test JSON unmarshaling
	var newConfig Config
	err = json.Unmarshal(data, &newConfig)
	require.NoError(t, err)

	// Compare some key fields
	assert.Equal(t, config.HTTP.UserAgent, newConfig.HTTP.UserAgent)
	assert.Equal(t, config.Audio.BufferSize, newConfig.Audio.BufferSize)
	assert.Equal(t, config.Detection.TimeoutSeconds, newConfig.Detection.TimeoutSeconds)
	assert.Equal(t, config.MetadataExtractor.EnableICYMetadata, newConfig.MetadataExtractor.EnableICYMetadata)
}

func TestCustomHeaderMappings(t *testing.T) {
	config := &MetadataExtractorConfig{
		CustomHeaderMappings: []CustomHeaderMapping{
			{
				HeaderKey:   "x-custom-station",
				MetadataKey: "station",
				Transform:   "title",
				Description: "Custom station header",
			},
			{
				HeaderKey:   "x-custom-bitrate",
				MetadataKey: "bitrate",
				Transform:   "int",
				Description: "Custom bitrate header",
			},
		},
	}

	extractor := NewConfigurableMetadataExtractor(config)
	assert.NotNil(t, extractor)
	assert.Equal(t, config, extractor.config)
	assert.Len(t, config.CustomHeaderMappings, 2)
}

func TestApplyICEcastSpecificConfig(t *testing.T) {
	config := DefaultConfig()
	icecastConfig := map[string]any{
		"metadata_extractor": map[string]any{
			"enable_icy_metadata": false,
			"default_values": map[string]any{
				"codec":    "mp3",
				"bitrate":  128,
				"channels": 1,
			},
		},
		"detection": map[string]any{
			"url_patterns":    []string{`\.custom$`},
			"content_types":   []string{"audio/custom"},
			"common_ports":    []string{"9999"},
			"timeout_seconds": 15,
		},
		"http": map[string]any{
			"user_agent":       "Custom/1.0",
			"accept_header":    "audio/custom",
			"request_icy_meta": false,
		},
		"audio": map[string]any{
			"buffer_size":       16384,
			"sample_duration":   45 * time.Second,
			"max_read_attempts": 10,
			"handle_icy_meta":   false,
		},
	}

	applyICEcastSpecificConfig(config, icecastConfig)

	// Test metadata extractor config
	assert.False(t, config.MetadataExtractor.EnableICYMetadata)
	assert.Equal(t, "mp3", config.MetadataExtractor.DefaultValues["codec"])
	assert.Equal(t, 128, config.MetadataExtractor.DefaultValues["bitrate"])
	assert.Equal(t, 1, config.MetadataExtractor.DefaultValues["channels"])

	// Test detection config
	assert.Equal(t, []string{`\.custom$`}, config.Detection.URLPatterns)
	assert.Equal(t, []string{"audio/custom"}, config.Detection.ContentTypes)
	assert.Equal(t, []string{"9999"}, config.Detection.CommonPorts)
	assert.Equal(t, 15, config.Detection.TimeoutSeconds)

	// Test HTTP config
	assert.Equal(t, "Custom/1.0", config.HTTP.UserAgent)
	assert.Equal(t, "audio/custom", config.HTTP.AcceptHeader)
	assert.False(t, config.HTTP.RequestICYMeta)

	// Test audio config
	assert.Equal(t, 16384, config.Audio.BufferSize)
	assert.Equal(t, 45*time.Second, config.Audio.SampleDuration)
	assert.Equal(t, 10, config.Audio.MaxReadAttempts)
	assert.False(t, config.Audio.HandleICYMeta)
}

func TestBufferDurationMapping(t *testing.T) {
	// Test that buffer_duration maps to sample_duration in audio config
	appConfig := map[string]any{
		"audio": map[string]any{
			"buffer_duration": 15 * time.Second,
		},
		"icecast": map[string]any{
			"audio": map[string]any{
				"buffer_duration": 25 * time.Second, // This should take precedence
			},
		},
	}

	config := ConfigFromAppConfig(appConfig)

	// ICEcast-specific config should override general audio config
	assert.Equal(t, 25*time.Second, config.Audio.SampleDuration)
}
