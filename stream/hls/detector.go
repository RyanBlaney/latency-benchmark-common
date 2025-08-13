package hls

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// Detector handles HLS stream detection with configurable options
type Detector struct {
	config *DetectionConfig
	client *http.Client
}

// NewDetector creates a new HLS detector with default configuration
func NewDetector() *Detector {
	return NewDetectorWithConfig(nil)
}

// NewDetectorWithConfig creates a new HLS detector with custom configuration
func NewDetectorWithConfig(config *DetectionConfig) *Detector {
	if config == nil {
		config = DefaultConfig().Detection
	}

	client := &http.Client{
		Timeout: time.Duration(config.TimeoutSeconds) * time.Second,
	}

	return &Detector{
		config: config,
		client: client,
	}
}

// DetectType determines if the stream is HLS using URL patterns and headers
func DetectType(ctx context.Context, client *http.Client, streamURL string) (common.StreamType, error) {
	detector := NewDetector()
	config := DefaultConfig()

	// First try URL-based detection (fastest)
	if detector.DetectFromURL(streamURL) == common.StreamTypeHLS {
		return common.StreamTypeHLS, nil
	}

	// Fall back to header-based detection
	if detector.DetectFromHeaders(ctx, streamURL, config.HTTP) == common.StreamTypeHLS {
		return common.StreamTypeHLS, nil
	}

	// Content validation as last resort (most expensive)
	if detector.IsValidHLSContent(ctx, streamURL, config.HTTP, config.Parser) {
		return common.StreamTypeHLS, nil
	}

	// Not HLS
	return common.StreamTypeUnsupported, nil
}

// ProbeStream performs a lightweight probe to gather HLS stream metadata
func ProbeStream(ctx context.Context, client *http.Client, streamURL string) (*common.StreamMetadata, error) {
	// Use the existing DetectFromM3U8Content function which already extracts metadata
	playlist, err := DetectFromM3U8Content(ctx, client, streamURL)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeConnection, "failed to probe HLS stream", err)
	}

	// Return the metadata that was extracted during parsing
	if playlist.Metadata != nil {
		if playlist.Metadata.Format == "" {
			playlist.Metadata.Format = "hls"
		}
		return playlist.Metadata, nil
	}

	// Fallback: create basic metadata if parsing didn't extract it
	return &common.StreamMetadata{
		URL:       streamURL,
		Type:      common.StreamTypeHLS,
		Format:    "hls",
		Headers:   playlist.Headers,
		Timestamp: time.Now(),
	}, nil
}

// DetectTypeWithConfig determines if the stream is HLS using full configuration
func DetectTypeWithConfig(ctx context.Context, streamURL string, config *Config) (common.StreamType, error) {
	if config == nil {
		config = DefaultConfig()
	}

	detector := NewDetectorWithConfig(config.Detection)

	// Create a context with configured timeout
	detectCtx, cancel := context.WithTimeout(ctx, time.Duration(config.Detection.TimeoutSeconds)*time.Second)
	defer cancel()

	// Step 1: URL pattern detection (fastest, no network calls)
	if streamType := detector.DetectFromURL(streamURL); streamType == common.StreamTypeHLS {
		// Verify with a lightweight HEAD request if configured to do so
		if len(config.Detection.RequiredHeaders) > 0 || len(config.Detection.ContentTypes) > 0 {
			if detector.DetectFromHeaders(detectCtx, streamURL, config.HTTP) == common.StreamTypeHLS {
				return common.StreamTypeHLS, nil
			}
		} else {
			// Trust URL pattern if no header validation required
			return common.StreamTypeHLS, nil
		}
	}

	// Step 2: Header-based detection
	if streamType := detector.DetectFromHeaders(detectCtx, streamURL, config.HTTP); streamType == common.StreamTypeHLS {
		return common.StreamTypeHLS, nil
	}

	// Step 3: Content validation as last resort (most expensive)
	if detector.IsValidHLSContent(detectCtx, streamURL, config.HTTP, config.Parser) {
		return common.StreamTypeHLS, nil
	}

	// Not HLS or detection failed
	return common.StreamTypeUnsupported, nil
}

// ProbeStreamWithConfig performs a probe with full configuration control
func ProbeStreamWithConfig(ctx context.Context, streamURL string, config *Config) (*common.StreamMetadata, error) {
	if config == nil {
		config = DefaultConfig()
	}

	detector := NewDetectorWithConfig(config.Detection)

	// Try to get the playlist with full configuration
	playlist, err := detector.DetectFromM3U8Content(ctx, streamURL, config.HTTP, config.Parser)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeConnection, "failed to probe HLS stream", err)
	}

	// If we have metadata from parsing, return it
	if playlist != nil && playlist.Metadata != nil {
		return playlist.Metadata, nil
	}

	// Extract metadata using configured extractor if playlist exists but no metadata
	if playlist != nil {
		metadataExtractor := NewConfigurableMetadataExtractor(config.MetadataExtractor)
		metadata := metadataExtractor.ExtractMetadata(playlist, streamURL)
		return metadata, nil
	}

	// Final fallback: create basic metadata with configured defaults
	metadata := &common.StreamMetadata{
		URL:       streamURL,
		Type:      common.StreamTypeHLS,
		Format:    "hls",
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
	}

	// Apply configured default values
	if config.MetadataExtractor.DefaultValues != nil {
		if codec, ok := config.MetadataExtractor.DefaultValues["codec"].(string); ok {
			metadata.Codec = codec
		}
		if sampleRate, ok := config.MetadataExtractor.DefaultValues["sample_rate"].(int); ok {
			metadata.SampleRate = sampleRate
		} else if sampleRateFloat, ok := config.MetadataExtractor.DefaultValues["sample_rate"].(float64); ok {
			metadata.SampleRate = int(sampleRateFloat)
		}
		if channels, ok := config.MetadataExtractor.DefaultValues["channels"].(int); ok {
			metadata.Channels = channels
		} else if channelsFloat, ok := config.MetadataExtractor.DefaultValues["channels"].(float64); ok {
			metadata.Channels = int(channelsFloat)
		}
		if bitrate, ok := config.MetadataExtractor.DefaultValues["bitrate"].(int); ok {
			metadata.Bitrate = bitrate
		} else if bitrateFloat, ok := config.MetadataExtractor.DefaultValues["bitrate"].(float64); ok {
			metadata.Bitrate = int(bitrateFloat)
		}
	}

	return metadata, nil
}

// DetectFromURL matches the URL with configured HLS patterns
func (d *Detector) DetectFromURL(streamURL string) common.StreamType {
	u, err := url.Parse(streamURL)
	if err != nil {
		logging.Debug("Error parsing URL", logging.Fields{"url": streamURL, "error": err.Error()})
		return common.StreamTypeUnsupported
	}

	path := strings.ToLower(u.Path)
	query := strings.ToLower(u.RawQuery)

	// Check against configured URL patterns
	for _, pattern := range d.config.URLPatterns {
		if matched, err := regexp.MatchString(pattern, path); err == nil && matched {
			return common.StreamTypeHLS
		}
		if matched, err := regexp.MatchString(pattern, query); err == nil && matched {
			return common.StreamTypeHLS
		}
	}

	return common.StreamTypeUnsupported
}

// DetectFromHeaders matches the HTTP headers with configured HLS patterns
func (d *Detector) DetectFromHeaders(ctx context.Context, streamURL string, httpConfig *HTTPConfig) common.StreamType {
	// Use provided client or create one with timeout
	client := d.client
	if httpConfig != nil {
		client = &http.Client{
			Timeout: httpConfig.ConnectionTimeout + httpConfig.ReadTimeout,
		}
	}

	req, err := http.NewRequestWithContext(ctx, "HEAD", streamURL, nil)
	if err != nil {
		logging.Debug("Error creating request for HTTP headers", logging.Fields{"url": streamURL, "error": err.Error()})
		return common.StreamTypeUnsupported
	}

	// Set headers from configuration
	if httpConfig != nil {
		headers := httpConfig.GetHTTPHeaders()
		for key, value := range headers {
			req.Header.Set(key, value)
		}
	} else {
		// Fallback to default headers
		req.Header.Set("User-Agent", "TuneIn-CDN-Benchmark/1.0")
		req.Header.Set("Accept", "*/*")
	}

	resp, err := client.Do(req)
	if err != nil {
		logging.Debug("Error getting response from client", logging.Fields{"url": streamURL, "error": err.Error()})
		return common.StreamTypeUnsupported
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logging.Debug("HTTP error response", logging.Fields{"url": streamURL, "status": resp.StatusCode})
		return common.StreamTypeUnsupported
	}

	// Check required headers if configured
	for _, requiredHeader := range d.config.RequiredHeaders {
		if resp.Header.Get(requiredHeader) == "" {
			return common.StreamTypeUnsupported
		}
	}

	// Check content type against configured patterns
	contentType := strings.ToLower(resp.Header.Get("Content-Type"))
	for _, pattern := range d.config.ContentTypes {
		if strings.Contains(contentType, strings.ToLower(pattern)) {
			return common.StreamTypeHLS
		}
	}

	return common.StreamTypeUnsupported
}

// DetectFromM3U8Content attempts to validate and parse M3U8 content with configuration
func (d *Detector) DetectFromM3U8Content(ctx context.Context, streamURL string, httpConfig *HTTPConfig, parserConfig *ParserConfig) (*M3U8Playlist, error) {
	// Use configured timeout
	detectCtx, cancel := context.WithTimeout(ctx, time.Duration(d.config.TimeoutSeconds)*time.Second)
	defer cancel()

	// Use provided client or create one with timeout
	client := d.client
	if httpConfig != nil {
		client = &http.Client{
			Timeout: httpConfig.ConnectionTimeout + httpConfig.ReadTimeout,
		}
	}

	req, err := http.NewRequestWithContext(detectCtx, "GET", streamURL, nil)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeConnection, "failed to create request", err)
	}

	// Set headers from configuration
	if httpConfig != nil {
		headers := httpConfig.GetHTTPHeaders()
		for key, value := range headers {
			req.Header.Set(key, value)
		}
	} else {
		// Fallback to default headers
		req.Header.Set("User-Agent", "TuneIn-CDN-Benchmark/1.0")
		req.Header.Set("Accept", "application/vnd.apple.mpegurl,application/x-mpegurl,text/plain")
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeConnection, "failed to fetch playlist", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, common.NewStreamErrorWithFields(common.StreamTypeHLS, streamURL,
			common.ErrCodeConnection, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, resp.Status), nil,
			logging.Fields{
				"status_code": resp.StatusCode,
				"status_text": resp.Status,
			})
	}

	// Store response headers
	headers := make(map[string]string)
	for key, values := range resp.Header {
		if len(values) > 0 {
			headers[strings.ToLower(key)] = values[0]
		}
	}

	// Parse the M3U8 content using configured parser
	var parser *Parser
	if parserConfig != nil {
		parser = NewConfigurableParser(parserConfig).Parser
	} else {
		parser = NewParser()
	}

	var reader io.Reader = resp.Body
	if httpConfig != nil && httpConfig.BufferSize > 0 {
		reader = bufio.NewReaderSize(resp.Body, httpConfig.BufferSize)
	}

	playlist, err := parser.ParseM3U8Content(reader)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeConnection, "failed to parse M3U8", err)
	}

	// Merge HTTP headers with playlist headers
	if playlist.Headers == nil {
		playlist.Headers = make(map[string]string)
	}
	maps.Copy(playlist.Headers, headers)

	// Extract metadata using configured extractor
	metadataExtractor := NewMetadataExtractor()
	playlist.Metadata = metadataExtractor.ExtractMetadata(playlist, streamURL)

	return playlist, nil
}

// IsValidHLSContent checks if the content appears to be valid HLS with configuration
func (d *Detector) IsValidHLSContent(ctx context.Context, streamURL string, httpConfig *HTTPConfig, parserConfig *ParserConfig) bool {
	playlist, err := d.DetectFromM3U8Content(ctx, streamURL, httpConfig, parserConfig)
	if err != nil {
		return false
	}

	return playlist.IsValid && (len(playlist.Segments) > 0 || len(playlist.Variants) > 0)
}

// GetHTTPHeaders returns configured HTTP headers for this detector
func (httpConfig *HTTPConfig) GetHTTPHeaders() map[string]string {
	headers := make(map[string]string)

	// Set standard headers
	headers["User-Agent"] = httpConfig.UserAgent
	headers["Accept"] = httpConfig.AcceptHeader

	// Add custom headers
	maps.Copy(headers, httpConfig.CustomHeaders)

	return headers
}

// ConfigurableDetection provides a complete detection suite with configuration
func ConfigurableDetection(ctx context.Context, streamURL string, config *Config) (common.StreamType, *M3U8Playlist, error) {
	if config == nil {
		config = DefaultConfig()
	}

	detector := NewDetectorWithConfig(config.Detection)

	// Step 1: URL pattern detection
	if streamType := detector.DetectFromURL(streamURL); streamType == common.StreamTypeHLS {
		// Step 2: Try to parse content for verification
		playlist, err := detector.DetectFromM3U8Content(ctx, streamURL, config.HTTP, config.Parser)
		if err == nil && playlist.IsValid {
			return common.StreamTypeHLS, playlist, nil
		}
	}

	// Step 3: Header detection as fallback
	if streamType := detector.DetectFromHeaders(ctx, streamURL, config.HTTP); streamType == common.StreamTypeHLS {
		// Try to parse content
		playlist, err := detector.DetectFromM3U8Content(ctx, streamURL, config.HTTP, config.Parser)
		if err == nil {
			return common.StreamTypeHLS, playlist, nil
		}
		// Return HLS type even if parsing failed

		return common.StreamTypeHLS, nil, common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeConnection, "failed to parse M3U8", err)
	}

	// Step 4: Content detection as last resort
	playlist, err := detector.DetectFromM3U8Content(ctx, streamURL, config.HTTP, config.Parser)
	if err == nil && playlist.IsValid {
		return common.StreamTypeHLS, playlist, nil
	}

	return common.StreamTypeUnsupported, nil, common.NewStreamError(common.StreamTypeHLS, streamURL,
		common.ErrCodeConnection, "stream type not supported or valid", err)
}

// DetectFromURL matches the URL with default HLS patterns (backward compatibility)
func DetectFromURL(streamURL string) common.StreamType {
	detector := NewDetector()
	return detector.DetectFromURL(streamURL)
}

// DetectFromHeaders matches the HTTP headers with default HLS patterns (backward compatibility)
func DetectFromHeaders(ctx context.Context, client *http.Client, streamURL string) common.StreamType {
	detector := NewDetector()

	// Create HTTP config from client
	httpConfig := &HTTPConfig{
		UserAgent:    "TuneIn-CDN-Benchmark/1.0",
		AcceptHeader: "*/*",
		BufferSize:   16384,
	}
	if client != nil && client.Timeout > 0 {
		httpConfig.ConnectionTimeout = client.Timeout / 2
		httpConfig.ReadTimeout = client.Timeout / 2
	}

	return detector.DetectFromHeaders(ctx, streamURL, httpConfig)
}

// DetectFromM3U8Content attempts to validate and parse M3U8 content (backward compatibility)
func DetectFromM3U8Content(ctx context.Context, client *http.Client, streamURL string) (*M3U8Playlist, error) {
	detector := NewDetector()

	// Create HTTP config from client
	httpConfig := &HTTPConfig{
		UserAgent:     "TuneIn-CDN-Benchmark/1.0",
		AcceptHeader:  "application/vnd.apple.mpegurl,application/x-mpegurl,text/plain",
		BufferSize:    16384,
		MaxRedirects:  5,
		CustomHeaders: make(map[string]string),
	}
	if client != nil && client.Timeout > 0 {
		httpConfig.ConnectionTimeout = client.Timeout / 2
		httpConfig.ReadTimeout = client.Timeout / 2
	}

	return detector.DetectFromM3U8Content(ctx, streamURL, httpConfig, nil)
}

// IsValidHLSContent checks if the content appears to be valid HLS (backward compatibility)
func IsValidHLSContent(ctx context.Context, client *http.Client, streamURL string) bool {
	detector := NewDetector()

	// Create HTTP config from client
	httpConfig := &HTTPConfig{
		UserAgent:     "TuneIn-CDN-Benchmark/1.0",
		AcceptHeader:  "application/vnd.apple.mpegurl,application/x-mpegurl,text/plain",
		BufferSize:    16384,
		MaxRedirects:  5,
		CustomHeaders: make(map[string]string),
	}
	if client != nil && client.Timeout > 0 {
		httpConfig.ConnectionTimeout = client.Timeout / 2
		httpConfig.ReadTimeout = client.Timeout / 2
	}

	return detector.IsValidHLSContent(ctx, streamURL, httpConfig, nil)
}
