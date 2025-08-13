package hls

import (
	"maps"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/audio/transcode"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// Config holds configuration for HLS processing
type Config struct {
	Parser            *ParserConfig            `json:"parser"`
	MetadataExtractor *MetadataExtractorConfig `json:"metadata_extractor"`
	Detection         *DetectionConfig         `json:"detection"`
	HTTP              *HTTPConfig              `json:"http"`
	Audio             *AudioConfig             `json:"audio"`
	AudioDecoder      common.AudioDecoder      `json:"-"` // External package
}

// ParserConfig holds configuration for M3U8 parsing
type ParserConfig struct {
	StrictMode         bool              `json:"strict_mode"`
	MaxSegmentAnalysis int               `json:"max_segment_analysis"`
	CustomTagHandlers  map[string]string `json:"custom_tag_handlers"`
	IgnoreUnknownTags  bool              `json:"ignore_unknown_tags"`
	ValidateURIs       bool              `json:"validate_uris"`
}

// MetadataExtractorConfig holds configuration for metadata extraction
type MetadataExtractorConfig struct {
	EnableURLPatterns     bool                  `json:"enable_url_patterns"`
	EnableHeaderMappings  bool                  `json:"enable_header_mappings"`
	EnableSegmentAnalysis bool                  `json:"enable_segment_analysis"`
	CustomPatterns        []CustomURLPattern    `json:"custom_patterns"`
	CustomHeaderMappings  []CustomHeaderMapping `json:"custom_header_mappings"`
	DefaultValues         map[string]any        `json:"default_values"`
}

// DetectionConfig holds configuration for stream detection
type DetectionConfig struct {
	URLPatterns     []string `json:"url_patterns"`
	ContentTypes    []string `json:"content_types"`
	RequiredHeaders []string `json:"required_headers"`
	TimeoutSeconds  int      `json:"timeout_seconds"`
}

// CustomURLPattern defines a custom URL pattern for configuration
type CustomURLPattern struct {
	Pattern     string            `json:"pattern"`
	Fields      map[string]string `json:"fields"`
	Priority    int               `json:"priority"`
	Description string            `json:"description"`
}

// CustomHeaderMapping defines a custom header mapping for configuration
type CustomHeaderMapping struct {
	HeaderKey   string `json:"header_key"`
	MetadataKey string `json:"metadata_key"`
	Transform   string `json:"transform"`
	Description string `json:"description"`
}

// HTTPConfig holds HTTP-related configuration for HLS
type HTTPConfig struct {
	UserAgent         string            `json:"user_agent"`
	AcceptHeader      string            `json:"accept_header"`
	ConnectionTimeout time.Duration     `json:"connection_timeout"`
	ReadTimeout       time.Duration     `json:"read_timeout"`
	MaxRedirects      int               `json:"max_redirects"`
	CustomHeaders     map[string]string `json:"custom_headers"`
	BufferSize        int               `json:"buffer_size"`
}

// AudioConfig holds audio-specific configuration for HLS
type AudioConfig struct {
	SampleDuration  time.Duration `json:"sample_duration"`
	BufferDuration  time.Duration `json:"buffer_duration"`
	MaxSegments     int           `json:"max_segments"`
	FollowLive      bool          `json:"follow_live"`
	AnalyzeSegments bool          `json:"analyze_segments"`
}

// DefaultConfig returns the default HLS configuration
func DefaultConfig() *Config {
	return &Config{
		Parser: &ParserConfig{
			StrictMode:         false,
			MaxSegmentAnalysis: 10,
			CustomTagHandlers:  make(map[string]string),
			IgnoreUnknownTags:  false,
			ValidateURIs:       false,
		},
		MetadataExtractor: &MetadataExtractorConfig{
			EnableURLPatterns:     true,
			EnableHeaderMappings:  true,
			EnableSegmentAnalysis: true,
			CustomPatterns:        []CustomURLPattern{},
			CustomHeaderMappings:  []CustomHeaderMapping{},
			DefaultValues: map[string]any{
				"codec":       "aac",
				"channels":    2,
				"sample_rate": 44100,
			},
		},
		Detection: &DetectionConfig{
			URLPatterns: []string{
				`\.m3u8$`,
				`/playlist\.m3u8`,
				`/master\.m3u8`,
				`/index\.m3u8`,
			},
			ContentTypes: []string{
				"application/vnd.apple.mpegurl",
				"application/x-mpegurl",
				"vnd.apple.mpegurl",
			},
			RequiredHeaders: []string{},
			TimeoutSeconds:  5,
		},
		HTTP: &HTTPConfig{
			UserAgent:         "TuneIn-CDN-Benchmark/1.0",
			AcceptHeader:      "application/vnd.apple.mpegurl,application/x-mpegurl,text/plain",
			ConnectionTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			MaxRedirects:      5,
			CustomHeaders:     make(map[string]string),
			BufferSize:        16384,
		},
		Audio: &AudioConfig{
			SampleDuration:  90 * time.Second,
			BufferDuration:  2 * time.Second,
			MaxSegments:     10,
			FollowLive:      false,
			AnalyzeSegments: false,
		},
		AudioDecoder: transcode.NewDecoder(transcode.DefaultDecoderConfig()),
	}
}

// ConfigFromAppConfig creates an HLS config from application config
// This allows HLS library to remain a standalone library while integrating with the main app
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
			if bufferSize, ok := streamCfg["buffer_size"].(int); ok {
				config.HTTP.BufferSize = bufferSize
			}
		}

		// Apply audio config if available
		if audioCfg, exists := appCfg["audio"].(map[string]any); exists {
			if bufferDuration, ok := audioCfg["buffer_duration"].(time.Duration); ok {
				config.Audio.BufferDuration = bufferDuration
			}
			if sampleRate, ok := audioCfg["sample_rate"].(int); ok {
				config.MetadataExtractor.DefaultValues["sample_rate"] = sampleRate
			}
			if channels, ok := audioCfg["channels"].(int); ok {
				config.MetadataExtractor.DefaultValues["channels"] = channels
			}
		}

		// Apply HLS-specific config if available
		if hlsCfg, exists := appCfg["hls"].(map[string]any); exists {
			applyHLSSpecificConfig(config, hlsCfg)
		}
	}

	return config
}

// ConfigFromMap creates an HLS config from a map (useful for testing and flexibility)
func ConfigFromMap(configMap map[string]any) *Config {
	return ConfigFromAppConfig(configMap)
}

// applyHLSSpecificConfig applies HLS-specific configuration overrides
func applyHLSSpecificConfig(config *Config, hlsCfg map[string]any) {
	// Apply parser config
	if parserCfg, exists := hlsCfg["parser"].(map[string]any); exists {
		if strictMode, ok := parserCfg["strict_mode"].(bool); ok {
			config.Parser.StrictMode = strictMode
		}
		if maxSegments, ok := parserCfg["max_segment_analysis"].(int); ok {
			config.Parser.MaxSegmentAnalysis = maxSegments
		}
		if ignoreUnknown, ok := parserCfg["ignore_unknown_tags"].(bool); ok {
			config.Parser.IgnoreUnknownTags = ignoreUnknown
		}
		if validateURIs, ok := parserCfg["validate_uris"].(bool); ok {
			config.Parser.ValidateURIs = validateURIs
		}
	}

	// Apply detection config
	if detectionCfg, exists := hlsCfg["detection"].(map[string]any); exists {
		if patterns, ok := detectionCfg["url_patterns"].([]string); ok {
			config.Detection.URLPatterns = patterns
		}
		if contentTypes, ok := detectionCfg["content_types"].([]string); ok {
			config.Detection.ContentTypes = contentTypes
		}
		if timeout, ok := detectionCfg["timeout_seconds"].(int); ok {
			config.Detection.TimeoutSeconds = timeout
		}
	}

	// Apply HTTP config
	if httpCfg, exists := hlsCfg["http"].(map[string]any); exists {
		if userAgent, ok := httpCfg["user_agent"].(string); ok {
			config.HTTP.UserAgent = userAgent
		}
		if acceptHeader, ok := httpCfg["accept_header"].(string); ok {
			config.HTTP.AcceptHeader = acceptHeader
		}
		if headers, ok := httpCfg["custom_headers"].(map[string]string); ok {
			config.HTTP.CustomHeaders = headers
		}
	}

	// Apply audio config
	if audioCfg, exists := hlsCfg["audio"].(map[string]any); exists {
		if duration, ok := audioCfg["sample_duration"].(time.Duration); ok {
			config.Audio.SampleDuration = duration
		}
		if maxSegments, ok := audioCfg["max_segments"].(int); ok {
			config.Audio.MaxSegments = maxSegments
		}
		if followLive, ok := audioCfg["follow_live"].(bool); ok {
			config.Audio.FollowLive = followLive
		}
		if analyzeSegments, ok := audioCfg["analyze_segments"].(bool); ok {
			config.Audio.AnalyzeSegments = analyzeSegments
		}
	}
}

// ConfigurableMetadataExtractor is a metadata extractor that can be configured
type ConfigurableMetadataExtractor struct {
	*MetadataExtractor
	config *MetadataExtractorConfig
}

// NewConfigurableMetadataExtractor creates a configurable metadata extractor
func NewConfigurableMetadataExtractor(config *MetadataExtractorConfig) *ConfigurableMetadataExtractor {
	if config == nil {
		config = DefaultConfig().MetadataExtractor
	}

	extractor := &ConfigurableMetadataExtractor{
		MetadataExtractor: NewMetadataExtractor(),
		config:            config,
	}

	// Apply custom configurations
	extractor.applyConfig()

	return extractor
}

// applyConfig applies the configuration to the metadata extractor
func (cme *ConfigurableMetadataExtractor) applyConfig() {
	// Add custom URL patterns
	for _, customPattern := range cme.config.CustomPatterns {
		if regex, err := regexp.Compile(customPattern.Pattern); err == nil {
			pattern := URLPattern{
				Pattern:     regex,
				Priority:    customPattern.Priority,
				Description: customPattern.Description,
				Extractor:   cme.createCustomPatternExtractor(customPattern.Fields),
			}
			cme.AddURLPattern(pattern)
		}
	}

	// Add custom header mappings
	for _, customMapping := range cme.config.CustomHeaderMappings {
		mapping := HeaderMapping{
			HeaderKey:   customMapping.HeaderKey,
			MetadataKey: customMapping.MetadataKey,
			Transformer: cme.createCustomTransformer(customMapping.Transform),
		}
		cme.AddHeaderMapping(mapping)
	}
}

// createCustomPatternExtractor creates an extractor function for custom patterns
func (cme *ConfigurableMetadataExtractor) createCustomPatternExtractor(fields map[string]string) func([]string, *common.StreamMetadata) {
	return func(matches []string, metadata *common.StreamMetadata) {
		for field, pattern := range fields {
			if groupIndex, err := strconv.Atoi(pattern); err == nil && groupIndex < len(matches) {
				value := matches[groupIndex]
				cme.setMetadataField(metadata, field, value)
			}
		}
	}
}

// createCustomTransformer creates a transformer function based on configuration
func (cme *ConfigurableMetadataExtractor) createCustomTransformer(transform string) func(string) any {
	return func(value string) any {
		switch transform {
		case "int":
			if i, err := strconv.Atoi(value); err == nil {
				return i
			}
		case "float":
			if f, err := strconv.ParseFloat(value, 64); err == nil {
				return f
			}
		case "bool":
			if b, err := strconv.ParseBool(value); err == nil {
				return b
			}
		case "lower":
			return strings.ToLower(value)
		case "upper":
			return strings.ToUpper(value)
		case "title":
			return titleCaser.String(value)
		default:
			return value
		}
		return nil
	}
}

// setMetadataField sets a field in the metadata based on field name
func (cme *ConfigurableMetadataExtractor) setMetadataField(metadata *common.StreamMetadata, field, value string) {
	switch field {
	case "bitrate":
		if i, err := strconv.Atoi(value); err == nil {
			metadata.Bitrate = i
		}
	case "sample_rate":
		if i, err := strconv.Atoi(value); err == nil {
			metadata.SampleRate = i
		}
	case "channels":
		if i, err := strconv.Atoi(value); err == nil {
			metadata.Channels = i
		}
	case "codec":
		metadata.Codec = value
	case "format":
		metadata.Format = value
	case "station":
		metadata.Station = value
	case "genre":
		metadata.Genre = value
	case "title":
		metadata.Title = value
	case "artist":
		metadata.Artist = value
	default:
		// Store in headers for unknown fields
		if metadata.Headers == nil {
			metadata.Headers = make(map[string]string)
		}
		metadata.Headers[field] = value
	}
}

// ExtractMetadata extracts metadata with configuration overrides
func (cme *ConfigurableMetadataExtractor) ExtractMetadata(playlist *M3U8Playlist, streamURL string) *common.StreamMetadata {
	metadata := cme.MetadataExtractor.ExtractMetadata(playlist, streamURL)

	// Apply default values for missing fields
	cme.applyDefaults(metadata)

	return metadata
}

// applyDefaults applies default values from configuration
func (cme *ConfigurableMetadataExtractor) applyDefaults(metadata *common.StreamMetadata) {
	for field, defaultValue := range cme.config.DefaultValues {
		switch field {
		case "codec":
			if metadata.Codec == "" {
				if codec, ok := defaultValue.(string); ok {
					metadata.Codec = codec
				}
			}
		case "channels":
			if metadata.Channels == 0 {
				if channels, ok := defaultValue.(int); ok {
					metadata.Channels = channels
				} else if channelsFloat, ok := defaultValue.(float64); ok {
					metadata.Channels = int(channelsFloat)
				}
			}
		case "sample_rate":
			if metadata.SampleRate == 0 {
				if rate, ok := defaultValue.(int); ok {
					metadata.SampleRate = rate
				} else if rateFloat, ok := defaultValue.(float64); ok {
					metadata.SampleRate = int(rateFloat)
				}
			}
		case "bitrate":
			if metadata.Bitrate == 0 {
				if bitrate, ok := defaultValue.(int); ok {
					metadata.Bitrate = bitrate
				} else if bitrateFloat, ok := defaultValue.(float64); ok {
					metadata.Bitrate = int(bitrateFloat)
				}
			}
		}
	}
}

// ConfigurableParser is a parser that can be configured
type ConfigurableParser struct {
	*Parser
	config *ParserConfig
}

// ParseM3U8Content parses with configuration options
func (cp *ConfigurableParser) ParseM3U8Content(reader any) (*M3U8Playlist, error) {
	// Convert reader to io.Reader if needed
	// This would be expanded based on actual implementation needs

	// For now, delegate to the base parser
	// In a real implementation, you'd apply config options here

	// TODO: apply config options
	return cp.Parser.ParseM3U8Content(reader.(interface {
		Read([]byte) (int, error)
	}))
}

// GetHTTPHeaders returns all HTTP headers that should be set for requests
func (c *Config) GetHTTPHeaders() map[string]string {
	headers := make(map[string]string)

	// Set standard headers
	headers["User-Agent"] = c.HTTP.UserAgent
	headers["Accept"] = c.HTTP.AcceptHeader

	// Add custom headers
	maps.Copy(headers, c.HTTP.CustomHeaders)

	return headers
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.HTTP.ConnectionTimeout <= 0 {
		return common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "HTTP connection timeout must be positive", nil)
	}
	if c.HTTP.ReadTimeout <= 0 {
		return common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "HTTP read timeout must be positive", nil)
	}
	if c.HTTP.MaxRedirects < 0 {
		return common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "max redirects cannot be negative", nil)
	}
	if c.HTTP.BufferSize <= 0 {
		return common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "buffer size must be positive", nil)
	}
	if c.Audio.SampleDuration <= 0 {
		return common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "sample duration must be positive", nil)
	}
	return nil
}
