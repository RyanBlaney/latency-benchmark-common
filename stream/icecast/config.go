package icecast

import (
	"maps"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/audio/transcode"
	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// Config holds configuration for ICEcast processing
type Config struct {
	MetadataExtractor *MetadataExtractorConfig `json:"metadata_extractor"`
	Detection         *DetectionConfig         `json:"detection"`
	HTTP              *HTTPConfig              `json:"http"`
	Audio             *AudioConfig             `json:"audio"`
	AudioDecoder      common.AudioDecoder      `json:"-"` // nonserializable
}

// MetadataExtractorConfig holds configuration for metadata extraction
type MetadataExtractorConfig struct {
	EnableHeaderMappings bool                  `json:"enable_header_mappings"`
	EnableICYMetadata    bool                  `json:"enable_icy_metadata"`
	CustomHeaderMappings []CustomHeaderMapping `json:"custom_header_mappings"`
	DefaultValues        map[string]any        `json:"default_values"`
	ICYMetadataTimeout   time.Duration         `json:"icy_metadata_timeout"`
}

// DetectionConfig holds configuration for stream detection
type DetectionConfig struct {
	URLPatterns     []string `json:"url_patterns"`
	ContentTypes    []string `json:"content_types"`
	RequiredHeaders []string `json:"required_headers"`
	TimeoutSeconds  int      `json:"timeout_seconds"`
	CommonPorts     []string `json:"common_ports"`
}

// CustomHeaderMapping defines a custom header mapping for configuration
type CustomHeaderMapping struct {
	HeaderKey   string `json:"header_key"`
	MetadataKey string `json:"metadata_key"`
	Transform   string `json:"transform"`
	Description string `json:"description"`
}

// HTTPConfig holds HTTP-related configuration for ICEcast
type HTTPConfig struct {
	UserAgent         string            `json:"user_agent"`
	AcceptHeader      string            `json:"accept_header"`
	ConnectionTimeout time.Duration     `json:"connection_timeout"`
	ReadTimeout       time.Duration     `json:"read_timeout"`
	MaxRedirects      int               `json:"max_redirects"`
	CustomHeaders     map[string]string `json:"custom_headers"`
	RequestICYMeta    bool              `json:"request_icy_meta"`
}

// GetHTTPHeaders returns configured HTTP headers for ICEcast requests
func (httpConfig *HTTPConfig) GetHTTPHeaders() map[string]string {
	headers := make(map[string]string)

	// Set standard headers
	headers["User-Agent"] = httpConfig.UserAgent
	headers["Accept"] = httpConfig.AcceptHeader

	// Add ICEcast-specific header if requested
	if httpConfig.RequestICYMeta {
		headers["Icy-MetaData"] = "1"
	}

	// Add custom headers
	maps.Copy(headers, httpConfig.CustomHeaders)

	return headers
}

// AudioConfig holds audio-specific configuration for ICEcast
type AudioConfig struct {
	BufferSize       int           `json:"buffer_size"`
	SampleDuration   time.Duration `json:"sample_duration"`
	MaxReadAttempts  int           `json:"max_read_attempts"`
	ReadTimeout      time.Duration `json:"read_timeout"`
	HandleICYMeta    bool          `json:"handle_icy_meta"`
	MetadataInterval int           `json:"metadata_interval"`
}

// DefaultConfig returns the default ICEcast configuration
func DefaultConfig() *Config {
	return &Config{
		MetadataExtractor: &MetadataExtractorConfig{
			EnableHeaderMappings: true,
			EnableICYMetadata:    true,
			CustomHeaderMappings: []CustomHeaderMapping{},
			DefaultValues: map[string]any{
				"codec":       "mp3",
				"channels":    2,
				"sample_rate": 44100,
				"format":      "mp3",
			},
			ICYMetadataTimeout: 5 * time.Second,
		},
		Detection: &DetectionConfig{
			URLPatterns: []string{
				`\.mp3$`,
				`\.aac$`,
				`\.ogg$`,
				`/stream$`,
				`/listen$`,
				`/audio$`,
				`/radio$`,
			},
			ContentTypes: []string{
				"audio/mpeg",
				"audio/mp3",
				"audio/aac",
				"audio/ogg",
				"application/ogg",
			},
			RequiredHeaders: []string{},
			TimeoutSeconds:  15,
			CommonPorts:     []string{"8000", "8080", "8443", "9000"},
		},
		HTTP: &HTTPConfig{
			UserAgent:         "TuneIn-CDN-Benchmark/1.0",
			AcceptHeader:      "audio/*,*/*",
			ConnectionTimeout: 15 * time.Second,
			ReadTimeout:       300 * time.Second,
			MaxRedirects:      5,
			CustomHeaders:     make(map[string]string),
			RequestICYMeta:    true,
		},
		Audio: &AudioConfig{
			BufferSize:       4096,
			SampleDuration:   90 * time.Second,
			MaxReadAttempts:  3,
			ReadTimeout:      10 * time.Second,
			HandleICYMeta:    true,
			MetadataInterval: 0, // Will be determined from stream
		},
		AudioDecoder: transcode.NewDecoder(transcode.DefaultDecoderConfig()),
	}
}

// ConfigFromAppConfig creates an ICEcast config from application config
// This allows ICEcast library to remain a standalone library while integrating with the main app
func ConfigFromAppConfig(appConfig any) *Config {
	config := DefaultConfig()

	if appCfg, ok := appConfig.(map[string]any); ok {
		if streamCfg, exists := appCfg["stream"].(map[string]any); exists {
			if userAgent, ok := streamCfg["user_agent"].(string); ok && userAgent != "" {
				config.HTTP.UserAgent = userAgent
			}
			if headers, ok := streamCfg["headers"].(map[string]string); ok {
				config.HTTP.CustomHeaders = headers
			}
			if connTimeout, ok := streamCfg["connection_timeout"].(time.Duration); ok {
				config.HTTP.ConnectionTimeout = connTimeout
			}
			if readTimeout, ok := streamCfg["read_timeout"].(time.Duration); ok {
				config.HTTP.ReadTimeout = readTimeout
			}
			if maxRedirects, ok := streamCfg["max_redirects"].(int); ok {
				config.HTTP.MaxRedirects = maxRedirects
			}
		}

		// Apply audio config if available
		if audioCfg, exists := appCfg["audio"].(map[string]any); exists {
			if bufferDuration, ok := audioCfg["buffer_duration"].(time.Duration); ok {
				config.Audio.SampleDuration = bufferDuration
			}
			if sampleRate, ok := audioCfg["sample_rate"].(int); ok {
				config.MetadataExtractor.DefaultValues["sample_rate"] = sampleRate
			}
			if channels, ok := audioCfg["channels"].(int); ok {
				config.MetadataExtractor.DefaultValues["channels"] = channels
			}
		}

		// Apply ICEcast-specific config if available
		if icecastCfg, exists := appCfg["icecast"].(map[string]any); exists {
			applyICEcastSpecificConfig(config, icecastCfg)
		}
	}

	return config
}

// ConfigFromMap creates an ICEcast config from a map (useful for testing and flexibility)
func ConfigFromMap(configMap map[string]any) *Config {
	return ConfigFromAppConfig(configMap)
}

// applyICEcastSpecificConfig applies ICEcast-specific configuration overrides
func applyICEcastSpecificConfig(config *Config, icecastCfg map[string]any) {
	// Apply metadata extractor config
	if metaCfg, exists := icecastCfg["metadata_extractor"].(map[string]any); exists {
		if enableHeaders, ok := metaCfg["enable_header_mappings"].(bool); ok {
			config.MetadataExtractor.EnableHeaderMappings = enableHeaders
		}
		if enableICY, ok := metaCfg["enable_icy_metadata"].(bool); ok {
			config.MetadataExtractor.EnableICYMetadata = enableICY
		}
		if timeout, ok := metaCfg["icy_metadata_timeout"].(time.Duration); ok {
			config.MetadataExtractor.ICYMetadataTimeout = timeout
		}
		if defaultValues, ok := metaCfg["default_values"].(map[string]any); ok {
			if config.MetadataExtractor.DefaultValues == nil {
				config.MetadataExtractor.DefaultValues = make(map[string]any)
			}
			maps.Copy(config.MetadataExtractor.DefaultValues, defaultValues)
		}
	}

	// Apply detection config
	if detectionCfg, exists := icecastCfg["detection"].(map[string]any); exists {
		if patterns, ok := detectionCfg["url_patterns"].([]string); ok {
			config.Detection.URLPatterns = patterns
		}
		if contentTypes, ok := detectionCfg["content_types"].([]string); ok {
			config.Detection.ContentTypes = contentTypes
		}
		if timeout, ok := detectionCfg["timeout_seconds"].(int); ok {
			config.Detection.TimeoutSeconds = timeout
		}
		if ports, ok := detectionCfg["common_ports"].([]string); ok {
			config.Detection.CommonPorts = ports
		}
		if requiredHeaders, ok := detectionCfg["required_headers"].([]string); ok {
			config.Detection.RequiredHeaders = requiredHeaders
		}
	}

	// Apply HTTP config
	if httpCfg, exists := icecastCfg["http"].(map[string]any); exists {
		if userAgent, ok := httpCfg["user_agent"].(string); ok {
			config.HTTP.UserAgent = userAgent
		}
		if acceptHeader, ok := httpCfg["accept_header"].(string); ok {
			config.HTTP.AcceptHeader = acceptHeader
		}
		if headers, ok := httpCfg["custom_headers"].(map[string]string); ok {
			config.HTTP.CustomHeaders = headers
		}
		if requestICY, ok := httpCfg["request_icy_meta"].(bool); ok {
			config.HTTP.RequestICYMeta = requestICY
		}
		if connTimeout, ok := httpCfg["connection_timeout"].(time.Duration); ok {
			config.HTTP.ConnectionTimeout = connTimeout
		}
		if readTimeout, ok := httpCfg["read_timeout"].(time.Duration); ok {
			config.HTTP.ReadTimeout = readTimeout
		}
		if maxRedirects, ok := httpCfg["max_redirects"].(int); ok {
			config.HTTP.MaxRedirects = maxRedirects
		}
	}

	// Apply audio config
	if audioCfg, exists := icecastCfg["audio"].(map[string]any); exists {
		if bufferSize, ok := audioCfg["buffer_size"].(int); ok {
			config.Audio.BufferSize = bufferSize
		}
		if duration, ok := audioCfg["sample_duration"].(time.Duration); ok {
			config.Audio.SampleDuration = duration
		}
		if bufferDuration, ok := audioCfg["buffer_duration"].(time.Duration); ok {
			config.Audio.SampleDuration = bufferDuration
		}
		if maxAttempts, ok := audioCfg["max_read_attempts"].(int); ok {
			config.Audio.MaxReadAttempts = maxAttempts
		}
		if handleICY, ok := audioCfg["handle_icy_meta"].(bool); ok {
			config.Audio.HandleICYMeta = handleICY
		}
		if interval, ok := audioCfg["metadata_interval"].(int); ok {
			config.Audio.MetadataInterval = interval
		}
		if readTimeout, ok := audioCfg["read_timeout"].(time.Duration); ok {
			config.Audio.ReadTimeout = readTimeout
		}
	}
}

// GetHTTPHeaders returns all HTTP headers that should be set for requests
func (c *Config) GetHTTPHeaders() map[string]string {
	headers := make(map[string]string)

	// Set standard headers
	headers["User-Agent"] = c.HTTP.UserAgent
	headers["Accept"] = c.HTTP.AcceptHeader

	// Request ICY metadata if enabled
	if c.HTTP.RequestICYMeta {
		headers["Icy-MetaData"] = "1"
	}

	// Add custom headers
	maps.Copy(headers, c.HTTP.CustomHeaders)

	return headers
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.HTTP.ConnectionTimeout <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "HTTP connection timeout must be positive", nil,
			logging.Fields{"timeout": c.HTTP.ConnectionTimeout})
	}

	if c.HTTP.ReadTimeout <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "HTTP read timeout must be positive", nil,
			logging.Fields{"timeout": c.HTTP.ReadTimeout})
	}

	if c.HTTP.MaxRedirects < 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "max redirects cannot be negative", nil,
			logging.Fields{"max_redirects": c.HTTP.MaxRedirects})
	}

	if c.Audio.BufferSize <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "buffer size must be positive", nil,
			logging.Fields{"buffer_size": c.Audio.BufferSize})
	}

	if c.Audio.SampleDuration <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "sample duration must be positive", nil,
			logging.Fields{"sample_duration": c.Audio.SampleDuration})
	}

	if c.Audio.MaxReadAttempts <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "max read attempts must be positive", nil,
			logging.Fields{"max_read_attempts": c.Audio.MaxReadAttempts})
	}

	if c.MetadataExtractor.ICYMetadataTimeout <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "ICY metadata timeout must be positive", nil,
			logging.Fields{"icy_timeout": c.MetadataExtractor.ICYMetadataTimeout})
	}

	if c.Detection.TimeoutSeconds <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "detection timeout must be positive", nil,
			logging.Fields{"timeout_seconds": c.Detection.TimeoutSeconds})
	}

	if len(c.Detection.URLPatterns) == 0 {
		return common.NewStreamError(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "at least one URL pattern must be configured", nil)
	}

	if len(c.Detection.ContentTypes) == 0 {
		return common.NewStreamError(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "at least one content type must be configured", nil)
	}

	return nil
}

// Clone creates a deep copy of the configuration
func (c *Config) Clone() *Config {
	clone := &Config{
		MetadataExtractor: &MetadataExtractorConfig{
			EnableHeaderMappings: c.MetadataExtractor.EnableHeaderMappings,
			EnableICYMetadata:    c.MetadataExtractor.EnableICYMetadata,
			ICYMetadataTimeout:   c.MetadataExtractor.ICYMetadataTimeout,
			DefaultValues:        make(map[string]any),
			CustomHeaderMappings: make([]CustomHeaderMapping, len(c.MetadataExtractor.CustomHeaderMappings)),
		},
		Detection: &DetectionConfig{
			URLPatterns:     make([]string, len(c.Detection.URLPatterns)),
			ContentTypes:    make([]string, len(c.Detection.ContentTypes)),
			RequiredHeaders: make([]string, len(c.Detection.RequiredHeaders)),
			CommonPorts:     make([]string, len(c.Detection.CommonPorts)),
			TimeoutSeconds:  c.Detection.TimeoutSeconds,
		},
		HTTP: &HTTPConfig{
			UserAgent:         c.HTTP.UserAgent,
			AcceptHeader:      c.HTTP.AcceptHeader,
			ConnectionTimeout: c.HTTP.ConnectionTimeout,
			ReadTimeout:       c.HTTP.ReadTimeout,
			MaxRedirects:      c.HTTP.MaxRedirects,
			RequestICYMeta:    c.HTTP.RequestICYMeta,
			CustomHeaders:     make(map[string]string),
		},
		Audio: &AudioConfig{
			BufferSize:       c.Audio.BufferSize,
			SampleDuration:   c.Audio.SampleDuration,
			MaxReadAttempts:  c.Audio.MaxReadAttempts,
			ReadTimeout:      c.Audio.ReadTimeout,
			HandleICYMeta:    c.Audio.HandleICYMeta,
			MetadataInterval: c.Audio.MetadataInterval,
		},
	}

	// Deep copy maps and slices
	maps.Copy(clone.MetadataExtractor.DefaultValues, c.MetadataExtractor.DefaultValues)
	copy(clone.MetadataExtractor.CustomHeaderMappings, c.MetadataExtractor.CustomHeaderMappings)
	copy(clone.Detection.URLPatterns, c.Detection.URLPatterns)
	copy(clone.Detection.ContentTypes, c.Detection.ContentTypes)
	copy(clone.Detection.RequiredHeaders, c.Detection.RequiredHeaders)
	copy(clone.Detection.CommonPorts, c.Detection.CommonPorts)
	maps.Copy(clone.HTTP.CustomHeaders, c.HTTP.CustomHeaders)

	return clone
}

// Merge merges another configuration into this one, with the other config taking precedence
func (c *Config) Merge(other *Config) {
	if other == nil {
		return
	}

	// Merge metadata extractor config
	if other.MetadataExtractor != nil {
		if c.MetadataExtractor == nil {
			c.MetadataExtractor = &MetadataExtractorConfig{}
		}
		c.MetadataExtractor.EnableHeaderMappings = other.MetadataExtractor.EnableHeaderMappings
		c.MetadataExtractor.EnableICYMetadata = other.MetadataExtractor.EnableICYMetadata
		c.MetadataExtractor.ICYMetadataTimeout = other.MetadataExtractor.ICYMetadataTimeout

		if other.MetadataExtractor.DefaultValues != nil {
			if c.MetadataExtractor.DefaultValues == nil {
				c.MetadataExtractor.DefaultValues = make(map[string]any)
			}
			maps.Copy(c.MetadataExtractor.DefaultValues, other.MetadataExtractor.DefaultValues)
		}

		if len(other.MetadataExtractor.CustomHeaderMappings) > 0 {
			c.MetadataExtractor.CustomHeaderMappings = make([]CustomHeaderMapping, len(other.MetadataExtractor.CustomHeaderMappings))
			copy(c.MetadataExtractor.CustomHeaderMappings, other.MetadataExtractor.CustomHeaderMappings)
		}
	}

	// Merge detection config
	if other.Detection != nil {
		if c.Detection == nil {
			c.Detection = &DetectionConfig{}
		}
		c.Detection.TimeoutSeconds = other.Detection.TimeoutSeconds
		if len(other.Detection.URLPatterns) > 0 {
			c.Detection.URLPatterns = make([]string, len(other.Detection.URLPatterns))
			copy(c.Detection.URLPatterns, other.Detection.URLPatterns)
		}
		if len(other.Detection.ContentTypes) > 0 {
			c.Detection.ContentTypes = make([]string, len(other.Detection.ContentTypes))
			copy(c.Detection.ContentTypes, other.Detection.ContentTypes)
		}
		if len(other.Detection.RequiredHeaders) > 0 {
			c.Detection.RequiredHeaders = make([]string, len(other.Detection.RequiredHeaders))
			copy(c.Detection.RequiredHeaders, other.Detection.RequiredHeaders)
		}
		if len(other.Detection.CommonPorts) > 0 {
			c.Detection.CommonPorts = make([]string, len(other.Detection.CommonPorts))
			copy(c.Detection.CommonPorts, other.Detection.CommonPorts)
		}
	}

	// Merge HTTP config
	if other.HTTP != nil {
		if c.HTTP == nil {
			c.HTTP = &HTTPConfig{}
		}
		c.HTTP.UserAgent = other.HTTP.UserAgent
		c.HTTP.AcceptHeader = other.HTTP.AcceptHeader
		c.HTTP.ConnectionTimeout = other.HTTP.ConnectionTimeout
		c.HTTP.ReadTimeout = other.HTTP.ReadTimeout
		c.HTTP.MaxRedirects = other.HTTP.MaxRedirects
		c.HTTP.RequestICYMeta = other.HTTP.RequestICYMeta

		if other.HTTP.CustomHeaders != nil {
			if c.HTTP.CustomHeaders == nil {
				c.HTTP.CustomHeaders = make(map[string]string)
			}
			maps.Copy(c.HTTP.CustomHeaders, other.HTTP.CustomHeaders)
		}
	}

	// Merge audio config
	if other.Audio != nil {
		if c.Audio == nil {
			c.Audio = &AudioConfig{}
		}
		c.Audio.BufferSize = other.Audio.BufferSize
		c.Audio.SampleDuration = other.Audio.SampleDuration
		c.Audio.MaxReadAttempts = other.Audio.MaxReadAttempts
		c.Audio.ReadTimeout = other.Audio.ReadTimeout
		c.Audio.HandleICYMeta = other.Audio.HandleICYMeta
		c.Audio.MetadataInterval = other.Audio.MetadataInterval
	}
}
