package icecast

import (
	"maps"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var titleCaser = cases.Title(language.English)

// MetadataExtractor handles extraction of metadata from ICEcast streams
type MetadataExtractor struct {
	headerMappings []HeaderMapping
}

// HeaderMapping defines how to extract metadata from HTTP headers
type HeaderMapping struct {
	HeaderKey   string
	MetadataKey string
	Transformer func(value string) any
}

// NewMetadataExtractor creates a new metadata extractor with default mappings
func NewMetadataExtractor() *MetadataExtractor {
	extractor := &MetadataExtractor{
		headerMappings: make([]HeaderMapping, 0, 16), // Pre-allocate for known mappings
	}

	// Register default header mappings
	extractor.registerDefaultHeaderMappings()

	return extractor
}

// ExtractMetadata extracts comprehensive metadata from ICEcast headers and URL
func (me *MetadataExtractor) ExtractMetadata(headers http.Header, streamURL string) *common.StreamMetadata {
	metadata := &common.StreamMetadata{
		URL:       streamURL,
		Type:      common.StreamTypeICEcast,
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
	}

	// Extract from HTTP headers using mappings
	me.extractFromHeaders(headers, metadata)

	// Set intelligent defaults for missing fields
	me.setDefaults(metadata)

	return metadata
}

// registerDefaultHeaderMappings registers common ICEcast header to metadata mappings
func (me *MetadataExtractor) registerDefaultHeaderMappings() {
	mappings := []HeaderMapping{
		{
			HeaderKey:   "icy-name",
			MetadataKey: "station",
			Transformer: me.stringTrimmer,
		},
		{
			HeaderKey:   "icy-genre",
			MetadataKey: "genre",
			Transformer: me.stringTrimmer,
		},
		{
			HeaderKey:   "icy-description",
			MetadataKey: "title",
			Transformer: me.stringTrimmer,
		},
		{
			HeaderKey:   "icy-url",
			MetadataKey: "icy-url",
			Transformer: me.stringTrimmer,
		},
		{
			HeaderKey:   "icy-br",
			MetadataKey: "bitrate",
			Transformer: me.integerParser,
		},
		{
			HeaderKey:   "icy-sr",
			MetadataKey: "sample_rate",
			Transformer: me.integerParser,
		},
		{
			HeaderKey:   "icy-channels",
			MetadataKey: "channels",
			Transformer: me.integerParser,
		},
		{
			HeaderKey:   "content-type",
			MetadataKey: "content_type",
			Transformer: me.contentTypeNormalizer,
		},
		{
			HeaderKey:   "server",
			MetadataKey: "server",
			Transformer: me.stringTrimmer,
		},
		{
			HeaderKey:   "icy-metaint",
			MetadataKey: "icy_metaint",
			Transformer: me.integerParser,
		},
		{
			HeaderKey:   "icy-pub",
			MetadataKey: "icy_public",
			Transformer: me.booleanParser,
		},
		{
			HeaderKey:   "icy-notice1",
			MetadataKey: "icy_notice1",
			Transformer: me.stringTrimmer,
		},
		{
			HeaderKey:   "icy-notice2",
			MetadataKey: "icy_notice2",
			Transformer: me.stringTrimmer,
		},
		{
			HeaderKey:   "icy-version",
			MetadataKey: "icy_version",
			Transformer: me.stringTrimmer,
		},
		// Additional useful headers
		{
			HeaderKey:   "icy-audio-info",
			MetadataKey: "icy_audio_info",
			Transformer: me.stringTrimmer,
		},
		{
			HeaderKey:   "x-audiocast-name",
			MetadataKey: "station", // Alternative header for station name
			Transformer: me.stringTrimmer,
		},
	}

	for _, mapping := range mappings {
		me.AddHeaderMapping(mapping)
	}
}

// Pre-defined transformer functions for efficiency
func (me *MetadataExtractor) stringTrimmer(value string) any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func (me *MetadataExtractor) integerParser(value string) any {
	if parsed, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
		return parsed
	}
	return nil
}

func (me *MetadataExtractor) booleanParser(value string) any {
	return strings.TrimSpace(value) == "1"
}

func (me *MetadataExtractor) contentTypeNormalizer(value string) any {
	return strings.ToLower(strings.TrimSpace(value))
}

// extractFromHeaders extracts metadata from HTTP headers using registered mappings
func (me *MetadataExtractor) extractFromHeaders(headers http.Header, metadata *common.StreamMetadata) {
	// Store all headers in lowercase for reference using maps.Copy
	headerMap := make(map[string]string)
	for key, values := range headers {
		if len(values) > 0 {
			headerMap[strings.ToLower(key)] = values[0]
		}
	}
	maps.Copy(metadata.Headers, headerMap)

	// Apply header mappings efficiently
	for _, mapping := range me.headerMappings {
		if value := headers.Get(mapping.HeaderKey); value != "" {
			if transformed := mapping.Transformer(value); transformed != nil {
				me.setMetadataField(metadata, mapping.MetadataKey, transformed)
			}
		}
	}

	// Determine codec from content type
	me.extractCodecFromContentType(metadata)
}

// extractCodecFromContentType determines codec and format from content-type header
func (me *MetadataExtractor) extractCodecFromContentType(metadata *common.StreamMetadata) {
	contentType := metadata.ContentType
	if contentType == "" {
		contentType = metadata.Headers["content-type"]
	}

	if contentType != "" {
		contentType = strings.ToLower(strings.TrimSpace(contentType))

		// Remove parameters (e.g., "audio/mpeg; charset=utf-8" -> "audio/mpeg")
		if idx := strings.Index(contentType, ";"); idx != -1 {
			contentType = contentType[:idx]
		}
		contentType = strings.TrimSpace(contentType)

		switch {
		case strings.Contains(contentType, "mpeg") || contentType == "audio/mp3":
			metadata.Codec = "mp3"
			metadata.Format = "mp3"
		case strings.Contains(contentType, "aac") || contentType == "audio/aac":
			metadata.Codec = "aac"
			metadata.Format = "aac"
		case strings.Contains(contentType, "ogg") || contentType == "application/ogg":
			metadata.Codec = "ogg"
			metadata.Format = "ogg"
		case strings.Contains(contentType, "flac"):
			metadata.Codec = "flac"
			metadata.Format = "flac"
		case contentType == "audio/wav" || contentType == "audio/wave":
			metadata.Codec = "pcm"
			metadata.Format = "wav"
		case strings.Contains(contentType, "opus"):
			metadata.Codec = "opus"
			metadata.Format = "opus"
		case strings.Contains(contentType, "vorbis"):
			metadata.Codec = "vorbis"
			metadata.Format = "ogg"
		default:
			// Try to guess from common patterns
			if strings.HasPrefix(contentType, "audio/") {
				// Extract format from audio/ prefix
				format := strings.TrimPrefix(contentType, "audio/")
				metadata.Format = format
				// Common codec mappings
				switch format {
				case "mpeg", "mp3":
					metadata.Codec = "mp3"
				case "aac", "mp4":
					metadata.Codec = "aac"
				case "ogg", "vorbis":
					metadata.Codec = "ogg"
				case "webm":
					metadata.Codec = "opus"
				default:
					metadata.Codec = format
				}
			}
		}
	}
}

// setMetadataField sets a field in the metadata based on field name
func (me *MetadataExtractor) setMetadataField(metadata *common.StreamMetadata, field string, value any) {
	switch field {
	case "station":
		if str, ok := value.(string); ok && metadata.Station == "" { // Don't overwrite existing
			metadata.Station = str
		}
	case "genre":
		if str, ok := value.(string); ok {
			metadata.Genre = str
		}
	case "title":
		if str, ok := value.(string); ok && metadata.Title == "" { // Don't overwrite existing
			metadata.Title = str
		}
	case "artist":
		if str, ok := value.(string); ok {
			metadata.Artist = str
		}
	case "bitrate":
		if i, ok := value.(int); ok {
			metadata.Bitrate = i
		}
	case "sample_rate":
		if i, ok := value.(int); ok {
			metadata.SampleRate = i
		}
	case "channels":
		if i, ok := value.(int); ok {
			metadata.Channels = i
		}
	case "codec":
		if str, ok := value.(string); ok {
			metadata.Codec = str
		}
	case "format":
		if str, ok := value.(string); ok {
			metadata.Format = str
		}
	case "content_type":
		if str, ok := value.(string); ok {
			metadata.ContentType = str
		}
	default:
		// Store in headers for custom/unknown fields
		if str, ok := value.(string); ok {
			metadata.Headers[field] = str
		} else if i, ok := value.(int); ok {
			metadata.Headers[field] = strconv.Itoa(i)
		} else if b, ok := value.(bool); ok {
			metadata.Headers[field] = strconv.FormatBool(b)
		}
	}
}

// setDefaults sets intelligent defaults for missing metadata fields
func (me *MetadataExtractor) setDefaults(metadata *common.StreamMetadata) {
	// Set default codec if not determined
	if metadata.Codec == "" {
		metadata.Codec = "mp3" // Most common for ICEcast
		metadata.Format = "mp3"
	}

	// Set default channels if not specified
	if metadata.Channels == 0 {
		metadata.Channels = 2 // Stereo default
	}

	// Set default sample rate if not specified
	if metadata.SampleRate == 0 {
		metadata.SampleRate = 44100 // CD quality default
	}

	// Ensure format matches codec if not set
	if metadata.Format == "" && metadata.Codec != "" {
		metadata.Format = metadata.Codec
	}
}

// AddHeaderMapping adds a new header mapping for metadata extraction
func (me *MetadataExtractor) AddHeaderMapping(mapping HeaderMapping) {
	me.headerMappings = append(me.headerMappings, mapping)
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

// createCustomTransformer creates a transformer function based on configuration
func (cme *ConfigurableMetadataExtractor) createCustomTransformer(transform string) func(string) any {
	return func(value string) any {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil
		}

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
			return titleCaser.String(strings.ToLower(value))
		case "trim":
			return value // Already trimmed above
		default:
			return value
		}
		return nil
	}
}

// ExtractMetadata extracts metadata with configuration overrides
func (cme *ConfigurableMetadataExtractor) ExtractMetadata(headers http.Header, streamURL string) *common.StreamMetadata {
	metadata := cme.MetadataExtractor.ExtractMetadata(headers, streamURL)

	// Apply default values for missing fields
	cme.applyDefaults(metadata)

	return metadata
}

// applyDefaults applies default values from configuration
func (cme *ConfigurableMetadataExtractor) applyDefaults(metadata *common.StreamMetadata) {
	if cme.config.DefaultValues == nil {
		return
	}

	for field, defaultValue := range cme.config.DefaultValues {
		switch field {
		case "codec":
			if metadata.Codec == "" {
				if codec, ok := defaultValue.(string); ok {
					metadata.Codec = codec
					if metadata.Format == "" {
						metadata.Format = codec
					}
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
		case "format":
			if metadata.Format == "" {
				if format, ok := defaultValue.(string); ok {
					metadata.Format = format
				}
			}
		case "station":
			if metadata.Station == "" {
				if station, ok := defaultValue.(string); ok {
					metadata.Station = station
				}
			}
		}
	}
}

// ParseICYTitle parses ICY stream title metadata (format: "Artist - Title")
func ParseICYTitle(icyTitle string) (artist, title string) {
	icyTitle = strings.TrimSpace(icyTitle)
	if icyTitle == "" {
		return "", ""
	}

	// Common patterns: "Artist - Title", "Artist: Title", "Artist | Title"
	separators := []string{" - ", " – ", " — ", ": ", " | ", " / ", " :: "}

	for _, sep := range separators {
		if strings.Contains(icyTitle, sep) {
			parts := strings.SplitN(icyTitle, sep, 2)
			if len(parts) == 2 {
				artist := strings.TrimSpace(parts[0])
				title := strings.TrimSpace(parts[1])
				if artist != "" && title != "" {
					return artist, title
				}
			}
		}
	}

	// No separator found, return as title only
	return "", icyTitle
}

// UpdateWithICYMetadata updates metadata with ICY stream title information
func (me *MetadataExtractor) UpdateWithICYMetadata(metadata *common.StreamMetadata, icyTitle string) {
	if icyTitle == "" {
		return
	}

	artist, title := ParseICYTitle(icyTitle)

	// Update metadata fields
	if artist != "" {
		metadata.Artist = artist
	}
	if title != "" {
		metadata.Title = title
	}

	// Store raw ICY title in headers using maps.Copy for efficiency
	icyHeaders := map[string]string{
		"icy_current_title": icyTitle,
	}
	if artist != "" {
		icyHeaders["icy_current_artist"] = artist
	}
	if title != "" {
		icyHeaders["icy_current_song"] = title
	}

	maps.Copy(metadata.Headers, icyHeaders)
	metadata.Timestamp = time.Now()
}

// ExtractAudioInfo attempts to extract audio info from icy-audio-info header
func (me *MetadataExtractor) ExtractAudioInfo(metadata *common.StreamMetadata) {
	if audioInfo, exists := metadata.Headers["icy-audio-info"]; exists && audioInfo != "" {
		// Parse icy-audio-info header (format: "ice-samplerate=44100;ice-bitrate=128;ice-channels=2")
		parts := strings.Split(audioInfo, ";")
		for _, part := range parts {
			if kv := strings.SplitN(part, "=", 2); len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])

				switch key {
				case "ice-samplerate":
					if sr, err := strconv.Atoi(value); err == nil && metadata.SampleRate == 0 {
						metadata.SampleRate = sr
					}
				case "ice-bitrate":
					if br, err := strconv.Atoi(value); err == nil && metadata.Bitrate == 0 {
						metadata.Bitrate = br
					}
				case "ice-channels":
					if ch, err := strconv.Atoi(value); err == nil && metadata.Channels == 0 {
						metadata.Channels = ch
					}
				}
			}
		}
	}
}

// GetSupportedHeaders returns a list of all supported ICEcast headers
func (me *MetadataExtractor) GetSupportedHeaders() []string {
	headers := make([]string, 0, len(me.headerMappings))
	for _, mapping := range me.headerMappings {
		headers = append(headers, mapping.HeaderKey)
	}
	return headers
}

// GetHeaderMapping returns the mapping for a specific header key
func (me *MetadataExtractor) GetHeaderMapping(headerKey string) (HeaderMapping, bool) {
	for _, mapping := range me.headerMappings {
		if strings.EqualFold(mapping.HeaderKey, headerKey) {
			return mapping, true
		}
	}
	return HeaderMapping{}, false
}

// RemoveHeaderMapping removes a header mapping by header key
func (me *MetadataExtractor) RemoveHeaderMapping(headerKey string) bool {
	for i, mapping := range me.headerMappings {
		if strings.EqualFold(mapping.HeaderKey, headerKey) {
			// Remove the mapping efficiently
			me.headerMappings = append(me.headerMappings[:i], me.headerMappings[i+1:]...)
			return true
		}
	}
	return false
}

// ValidateMetadata performs basic validation on extracted metadata
func (me *MetadataExtractor) ValidateMetadata(metadata *common.StreamMetadata) []string {
	var issues []string

	if metadata.SampleRate > 0 && (metadata.SampleRate < 8000 || metadata.SampleRate > 192000) {
		issues = append(issues, "unusual sample rate")
	}

	if metadata.Channels > 0 && metadata.Channels > 8 {
		issues = append(issues, "unusual channel count")
	}

	if metadata.Bitrate > 0 && (metadata.Bitrate < 8 || metadata.Bitrate > 2000) {
		issues = append(issues, "unusual bitrate")
	}

	if metadata.Codec == "" {
		issues = append(issues, "missing codec information")
	}

	return issues
}
