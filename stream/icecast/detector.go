package icecast

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/audio/transcode"
	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// Detector handles ICEcast stream detection with configurable options
type Detector struct {
	config *DetectionConfig
	client *http.Client
}

// NewDetector creates a new ICEcast detector with default configuration
func NewDetector() *Detector {
	return NewDetectorWithConfig(nil)
}

// NewDetectorWithConfig creates a new ICEcast detector with custom configuration
func NewDetectorWithConfig(config *DetectionConfig) *Detector {
	if config == nil {
		config = DefaultConfig().Detection
	}

	client := &http.Client{
		Timeout: time.Duration(config.TimeoutSeconds) * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:       10,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: false,
		},
	}

	return &Detector{
		config: config,
		client: client,
	}
}

// DetectType determines if the stream is ICEcast using URL patterns and headers
func DetectType(ctx context.Context, client *http.Client, streamURL string) (common.StreamType, error) {
	detector := NewDetector()
	config := DefaultConfig()

	// First try URL-based detection (fastest)
	if detector.DetectFromURL(streamURL) == common.StreamTypeICEcast {
		return common.StreamTypeICEcast, nil
	}

	// Fall back to header-based detection
	if detector.DetectFromHeaders(ctx, streamURL, config.HTTP) == common.StreamTypeICEcast {
		return common.StreamTypeICEcast, nil
	}

	if detector.IsValidICEcastContent(ctx, streamURL, config.HTTP) {
		return common.StreamTypeICEcast, nil
	}

	// Not ICEcast
	return common.StreamTypeUnsupported, nil
}

// DetectTypeWithConfig determines if the stream is ICEcast using full configuration
func DetectTypeWithConfig(ctx context.Context, streamURL string, config *Config) (common.StreamType, error) {
	if config == nil {
		config = DefaultConfig()
	}

	detector := NewDetectorWithConfig(config.Detection)

	// Create a context with configured timeout
	detectCtx, cancel := context.WithTimeout(ctx, time.Duration(config.Detection.TimeoutSeconds)*time.Second)
	defer cancel()

	// Step 1: URL pattern detection (fastest, no network calls)
	if streamType := detector.DetectFromURL(streamURL); streamType == common.StreamTypeICEcast {
		// Verify with a lightweight HEAD request if configured to do so
		if len(config.Detection.RequiredHeaders) > 0 || len(config.Detection.ContentTypes) > 0 {
			if detector.DetectFromHeaders(detectCtx, streamURL, config.HTTP) == common.StreamTypeICEcast {
				return common.StreamTypeICEcast, nil
			}
		} else {
			// Trust URL pattern if no header validation required
			return common.StreamTypeICEcast, nil
		}
	}

	// Step 2: Header-based detection
	if streamType := detector.DetectFromHeaders(detectCtx, streamURL, config.HTTP); streamType == common.StreamTypeICEcast {
		return common.StreamTypeICEcast, nil
	}

	// Step 3: Content validation as last resort (check if audio stream is actually streaming)
	if detector.IsValidICEcastContent(detectCtx, streamURL, config.HTTP) {
		return common.StreamTypeICEcast, nil
	}

	// Not ICEcast or detection failed
	return common.StreamTypeUnsupported, nil
}

// ProbeStream performs a lightweight probe to gather ICEcast stream metadata
func ProbeStream(ctx context.Context, client *http.Client, streamURL string) (*common.StreamMetadata, error) {
	// Perform HEAD request to get metadata from headers
	req, err := http.NewRequestWithContext(ctx, "HEAD", streamURL, nil)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeConnection, "failed to create request", err)
	}

	// Set headers for ICEcast compatibility
	req.Header.Set("User-Agent", "TuneIn-CDN-Benchmark/1.0")
	req.Header.Set("Accept", "audio/*")
	req.Header.Set("Icy-MetaData", "1") // Request ICY metadata

	resp, err := client.Do(req)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeConnection, "failed to probe ICEcast stream", err)
	}
	defer resp.Body.Close()

	// Check if the response status indicates success
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeConnection, fmt.Sprintf("HTTP error: %d %s", resp.StatusCode, resp.Status), nil)
	}

	// Create metadata from response headers
	metadata := &common.StreamMetadata{
		URL:       streamURL,
		Type:      common.StreamTypeICEcast,
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
	}

	// Extract ICEcast-specific metadata from headers
	extractMetadataFromHeaders(resp.Header, metadata)

	return metadata, nil
}

// ProbeStreamWithConfig performs a probe with full configuration control
// ProbeStreamWithConfig probes an ICEcast stream to detect audio properties
// Priority order: FFprobe > HTTP headers > config defaults
func ProbeStreamWithConfig(ctx context.Context, streamURL string, config *Config) (*common.StreamMetadata, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "icecast_prober",
		"function":  "ProbeStreamWithConfig",
		"url":       streamURL,
	})

	// Step 1: Try FFprobe first (highest priority)
	logger.Debug("Attempting FFprobe detection")
	metadata, err := probeWithFFprobe(ctx, streamURL, config, logger)

	if err == nil && metadata != nil && isValidProbeResult(metadata) {
		logger.Debug("FFprobe detection successful", logging.Fields{
			"sample_rate": metadata.SampleRate,
			"channels":    metadata.Channels,
			"bitrate":     metadata.Bitrate,
			"codec":       metadata.Codec,
		})
		metadata.Headers["probe_method"] = "ffprobe"
		return metadata, nil
	}

	if err != nil {
		logger.Warn("FFprobe detection failed", logging.Fields{
			"error": err.Error(),
		})
	}

	// Step 2: Try HTTP headers (medium priority)
	logger.Debug("Attempting HTTP header detection")
	metadata, err = probeWithHeaders(ctx, streamURL, config, logger)

	if err == nil && metadata != nil && isValidProbeResult(metadata) {
		logger.Debug("HTTP header detection successful", logging.Fields{
			"sample_rate": metadata.SampleRate,
			"channels":    metadata.Channels,
			"bitrate":     metadata.Bitrate,
			"codec":       metadata.Codec,
		})
		metadata.Headers["probe_method"] = "headers"
		return metadata, nil
	}

	// Step 3: Fall back to config defaults (lowest priority)
	logger.Debug("Using config defaults as fallback")
	metadata = createMetadataFromConfig(streamURL, config)
	metadata.Headers["probe_method"] = "config"

	logger.Warn("Probe methods failed, using config defaults", logging.Fields{
		"sample_rate": metadata.SampleRate,
		"channels":    metadata.Channels,
		"bitrate":     metadata.Bitrate,
		"codec":       metadata.Codec,
	})

	return metadata, nil
}

// probeWithFFprobe uses FFmpeg/FFprobe to detect stream properties
func probeWithFFprobe(ctx context.Context, streamURL string, config *Config, logger logging.Logger) (*common.StreamMetadata, error) {
	// Create a short timeout context for probing
	probeTimeout := 15 * time.Second
	if config != nil && config.Detection != nil && config.Detection.TimeoutSeconds > 0 {
		probeTimeout = time.Duration(config.Detection.TimeoutSeconds) * time.Second
	}

	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	// Create decoder for probing
	decoderConfig := transcode.DefaultDecoderConfig()
	decoderConfig.Timeout = probeTimeout

	decoder := transcode.NewDecoder(decoderConfig)
	defer decoder.Close()

	logger.Debug("Starting FFprobe stream analysis", logging.Fields{
		"timeout_seconds": probeTimeout.Seconds(),
	})

	// Use FFprobe to analyze the stream
	probeResult, err := decoder.ProbeURL(probeCtx, streamURL)
	if err != nil {
		return nil, fmt.Errorf("FFprobe failed: %w", err)
	}

	if probeResult == nil {
		return nil, fmt.Errorf("FFprobe returned no results")
	}

	// Convert probe result to StreamMetadata
	metadata := &common.StreamMetadata{
		URL:        streamURL,
		Type:       common.StreamTypeICEcast,
		SampleRate: probeResult.SampleRate,
		Channels:   probeResult.Channels,
		Bitrate:    probeResult.Bitrate,
		Codec:      probeResult.Codec,
		Format:     probeResult.Format,
		Headers:    make(map[string]string),
		Timestamp:  time.Now(),
	}

	logger.Debug("FFprobe completed successfully", logging.Fields{
		"detected_sample_rate": metadata.SampleRate,
		"detected_channels":    metadata.Channels,
		"detected_bitrate":     metadata.Bitrate,
		"detected_codec":       metadata.Codec,
		"detected_format":      metadata.Format,
	})

	return metadata, nil
}

// probeWithHeaders attempts to extract stream info from HTTP headers
func probeWithHeaders(ctx context.Context, streamURL string, config *Config, logger logging.Logger) (*common.StreamMetadata, error) {
	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			ResponseHeaderTimeout: 5 * time.Second,
		},
	}

	req, err := http.NewRequestWithContext(ctx, "HEAD", streamURL, nil)
	if err != nil {
		// Try GET if HEAD fails
		req, err = http.NewRequestWithContext(ctx, "GET", streamURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
	}

	// Set headers
	if config != nil && config.HTTP != nil {
		headers := config.GetHTTPHeaders()
		for key, value := range headers {
			req.Header.Set(key, value)
		}
	} else {
		req.Header.Set("User-Agent", "TuneIn-CDN-Benchmark/1.0")
		req.Header.Set("Accept", "audio/*,*/*")
		req.Header.Set("Icy-MetaData", "1")
	}

	logger.Debug("Sending HTTP request for header analysis")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("HTTP request returned status %d", resp.StatusCode)
	}

	// Extract metadata from headers
	metadata := &common.StreamMetadata{
		URL:         streamURL,
		Type:        common.StreamTypeICEcast,
		ContentType: resp.Header.Get("Content-Type"),
		Headers:     make(map[string]string),
		Timestamp:   time.Now(),
	}

	// Copy all headers
	for key, values := range resp.Header {
		if len(values) > 0 {
			metadata.Headers[strings.ToLower(key)] = values[0]
		}
	}

	// Extract ICEcast-specific headers
	extractICEcastHeaders(metadata, resp.Header, logger)

	logger.Debug("Header analysis completed", logging.Fields{
		"content_type":     metadata.ContentType,
		"detected_bitrate": metadata.Bitrate,
		"icy_name":         metadata.Headers["icy-name"],
		"icy_br":           metadata.Headers["icy-br"],
	})

	return metadata, nil
}

// extractICEcastHeaders extracts audio properties from ICEcast headers
func extractICEcastHeaders(metadata *common.StreamMetadata, headers http.Header, logger logging.Logger) {
	// Extract bitrate from icy-br header
	if icyBr := headers.Get("icy-br"); icyBr != "" {
		if bitrate, err := strconv.Atoi(icyBr); err == nil {
			metadata.Bitrate = bitrate
			logger.Debug("Extracted bitrate from icy-br header", logging.Fields{
				"bitrate": bitrate,
			})
		}
	}

	// Extract station name
	if icyName := headers.Get("icy-name"); icyName != "" {
		metadata.Station = icyName
	}

	// Extract genre
	if icyGenre := headers.Get("icy-genre"); icyGenre != "" {
		metadata.Genre = icyGenre
	}

	// Try to infer codec from content-type
	contentType := strings.ToLower(headers.Get("Content-Type"))
	switch {
	case strings.Contains(contentType, "mpeg") || strings.Contains(contentType, "mp3"):
		metadata.Codec = "mp3"
		metadata.Format = "mp3"
		// Default values for MP3
		if metadata.SampleRate == 0 {
			metadata.SampleRate = 44100
		}
		if metadata.Channels == 0 {
			metadata.Channels = 2
		}
	case strings.Contains(contentType, "aac"):
		metadata.Codec = "aac"
		metadata.Format = "aac"
		if metadata.SampleRate == 0 {
			metadata.SampleRate = 44100
		}
		if metadata.Channels == 0 {
			metadata.Channels = 2
		}
	case strings.Contains(contentType, "ogg"):
		metadata.Codec = "vorbis"
		metadata.Format = "ogg"
		if metadata.SampleRate == 0 {
			metadata.SampleRate = 44100
		}
		if metadata.Channels == 0 {
			metadata.Channels = 2
		}
	}
}

// createMetadataFromConfig creates metadata using config defaults
func createMetadataFromConfig(streamURL string, config *Config) *common.StreamMetadata {
	metadata := &common.StreamMetadata{
		URL:       streamURL,
		Type:      common.StreamTypeICEcast,
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
	}

	if config != nil && config.MetadataExtractor != nil && config.MetadataExtractor.DefaultValues != nil {
		defaults := config.MetadataExtractor.DefaultValues

		if codec, ok := defaults["codec"].(string); ok {
			metadata.Codec = codec
		}
		if format, ok := defaults["format"].(string); ok {
			metadata.Format = format
		}
		if sampleRate, ok := defaults["sample_rate"].(int); ok {
			metadata.SampleRate = sampleRate
		} else if sampleRateFloat, ok := defaults["sample_rate"].(float64); ok {
			metadata.SampleRate = int(sampleRateFloat)
		}
		if channels, ok := defaults["channels"].(int); ok {
			metadata.Channels = channels
		} else if channelsFloat, ok := defaults["channels"].(float64); ok {
			metadata.Channels = int(channelsFloat)
		}
		if bitrate, ok := defaults["bitrate"].(int); ok {
			metadata.Bitrate = bitrate
		} else if bitrateFloat, ok := defaults["bitrate"].(float64); ok {
			metadata.Bitrate = int(bitrateFloat)
		}
	}

	// Set reasonable defaults if nothing was configured
	if metadata.Codec == "" {
		metadata.Codec = "mp3"
	}
	if metadata.Format == "" {
		metadata.Format = "mp3"
	}
	if metadata.SampleRate == 0 {
		metadata.SampleRate = 44100
	}
	if metadata.Channels == 0 {
		metadata.Channels = 2
	}
	if metadata.Bitrate == 0 {
		metadata.Bitrate = 128
	}
	if metadata.ContentType == "" {
		metadata.ContentType = "audio/mpeg"
	}

	return metadata
}

// isValidProbeResult checks if probe result contains valid audio properties
func isValidProbeResult(metadata *common.StreamMetadata) bool {
	if metadata == nil {
		return false
	}

	// Check for valid sample rate
	if metadata.SampleRate <= 0 || metadata.SampleRate > 192000 {
		return false
	}

	// Check for valid channels
	if metadata.Channels <= 0 || metadata.Channels > 8 {
		return false
	}

	// Check for valid codec
	if metadata.Codec == "" {
		return false
	}

	return true
}

func (h *Handler) ReadAudioWithDuration(ctx context.Context, duration time.Duration) (*common.AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component":       "icecast_handler",
		"function":        "ReadAudioWithDuration",
		"target_duration": duration.Seconds(),
		"approach":        "ffmpeg_direct",
	})

	if !h.connected {
		return nil, common.NewStreamError(common.StreamTypeICEcast, h.url,
			common.ErrCodeConnection, "not connected", nil)
	}

	logger.Debug("Starting FFmpeg-based duration audio reading", logging.Fields{
		"target_duration": duration.Seconds(),
		"url":             h.url,
	})

	// STEP 1: Use enhanced probing with priority order (FFprobe > Headers > Config)
	detectedSampleRate := 44100 // fallback default
	detectedChannels := 1       // fallback default

	logger.Debug("Starting enhanced stream probing with priority: FFprobe > Headers > Config")

	// Use the enhanced ProbeStreamWithConfig function
	probedMetadata, err := ProbeStreamWithConfig(ctx, h.url, h.config)
	if err == nil && probedMetadata != nil && isValidProbeResult(probedMetadata) {
		// FFprobe/probed results take absolute priority over config
		detectedSampleRate = probedMetadata.SampleRate
		detectedChannels = probedMetadata.Channels

		logger.Debug("Successfully probed stream format", logging.Fields{
			"approach":             "ffmpeg_direct",
			"detected_sample_rate": detectedSampleRate,
			"detected_channels":    detectedChannels,
			"detected_codec":       probedMetadata.Codec,
			"detected_bitrate":     probedMetadata.Bitrate,
			"probe_source":         probedMetadata.Headers["probe_method"],
		})

		// Update handler metadata with probed values (FFprobe results override config)
		if h.metadata != nil {
			h.metadata.SampleRate = detectedSampleRate
			h.metadata.Channels = detectedChannels
			h.metadata.Codec = probedMetadata.Codec
			h.metadata.Bitrate = probedMetadata.Bitrate
			h.metadata.Format = probedMetadata.Format
		}
	} else {
		logger.Warn("Failed to probe stream, using config defaults", logging.Fields{
			"approach":        "ffmpeg_direct",
			"probe_error":     err,
			"target_duration": duration.Seconds(),
		})

		// Final fallback to config values if probing completely failed
		if h.config != nil && h.config.MetadataExtractor != nil && h.config.MetadataExtractor.DefaultValues != nil {
			if sampleRate, ok := h.config.MetadataExtractor.DefaultValues["sample_rate"].(int); ok && sampleRate > 0 {
				detectedSampleRate = sampleRate
			}
			if channels, ok := h.config.MetadataExtractor.DefaultValues["channels"].(int); ok && channels > 0 {
				detectedChannels = channels
			}
		}
	}

	// TODO: @@@ Key fix
	detectedChannels = 1
	h.metadata.Channels = 1

	// STEP 2: Create optimized downloader with probed properties (FFprobe results take priority)
	downloaderConfig := DefaultDownloadConfig()
	downloaderConfig.TargetDuration = duration
	downloaderConfig.OutputSampleRate = detectedSampleRate // Use FFprobe detected rate!
	downloaderConfig.OutputChannels = detectedChannels     // Use FFprobe detected channels!

	logger.Debug("Using detected audio properties for downloader", logging.Fields{
		"approach":        "ffmpeg_direct",
		"sample_rate":     downloaderConfig.OutputSampleRate,
		"channels":        downloaderConfig.OutputChannels,
		"target_duration": duration.Seconds(),
	})

	downloader := &AudioDownloader{
		client: &http.Client{
			Timeout: 0, // FFmpeg handles timeouts
		},
		icecastConfig: h.config,
		downloadStats: &DownloadStats{
			AudioMetrics:  &AudioMetrics{},
			LiveStartTime: &time.Time{},
		},
		config: downloaderConfig,
	}
	*downloader.downloadStats.LiveStartTime = time.Now()

	// Use direct FFmpeg approach with ICEcast stream type
	audioData, err := downloader.DownloadAudioSampleDirect(ctx, h.url, duration)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeICEcast, h.url,
			common.ErrCodeDecoding, "failed to download audio using FFmpeg direct approach", err)
	}

	// Ensure the returned AudioData uses the probed sample rate and channels
	audioData.SampleRate = detectedSampleRate
	audioData.Channels = detectedChannels

	// Update handler statistics
	h.stats.BytesReceived += int64(len(audioData.PCM) * 8) // float64 = 8 bytes per sample

	// Calculate average bitrate
	elapsed := time.Since(h.startTime)
	if elapsed > 0 {
		h.stats.AverageBitrate = float64(h.stats.BytesReceived*8) / elapsed.Seconds() / 1000
	}

	// Update metadata with probed values and current timestamp
	if audioData.Metadata != nil {
		audioData.Metadata.SampleRate = detectedSampleRate
		audioData.Metadata.Channels = detectedChannels
		audioData.Metadata.Timestamp = time.Now()
		audioData.Metadata.Type = common.StreamTypeICEcast

		// Record the probing method used
		if probedMetadata != nil && probedMetadata.Headers != nil {
			if probeMethod, exists := probedMetadata.Headers["probe_method"]; exists {
				if audioData.Metadata.Headers == nil {
					audioData.Metadata.Headers = make(map[string]string)
				}
				audioData.Metadata.Headers["probe_method"] = probeMethod
			}
		}

		// Merge additional metadata from handler if available
		if h.metadata != nil {
			if audioData.Metadata.Station == "" && h.metadata.Station != "" {
				audioData.Metadata.Station = h.metadata.Station
			}
			if audioData.Metadata.Genre == "" && h.metadata.Genre != "" {
				audioData.Metadata.Genre = h.metadata.Genre
			}
			if audioData.Metadata.Title == "" && h.metadata.Title != "" {
				audioData.Metadata.Title = h.metadata.Title
			}
		}
	}

	logger.Debug("ICEcast FFmpeg-based audio extraction completed successfully", logging.Fields{
		"approach":         "ffmpeg_direct",
		"actual_duration":  audioData.Duration.Seconds(),
		"target_duration":  duration.Seconds(),
		"samples":          len(audioData.PCM),
		"sample_rate":      audioData.SampleRate,
		"channels":         audioData.Channels,
		"efficiency_ratio": audioData.Duration.Seconds() / duration.Seconds(),
		"reconnect_used":   audioData.Metadata != nil && audioData.Metadata.Headers["reconnect_enabled"] == "true",
	})

	return audioData, nil
}

// DetectFromURL matches the URL with configured ICEcast patterns
func (d *Detector) DetectFromURL(streamURL string) common.StreamType {
	u, err := url.Parse(streamURL)
	if err != nil {
		logging.Debug("Error parsing URL", logging.Fields{"url": streamURL, "error": err.Error()})
		return common.StreamTypeUnsupported
	}

	// ICEcast streams must use HTTP or HTTPS
	if u.Scheme != "http" && u.Scheme != "https" {
		return common.StreamTypeUnsupported
	}

	path := strings.ToLower(u.Path)
	query := strings.ToLower(u.RawQuery)
	port := u.Port()

	// Check port-based detection first
	if slices.Contains(d.config.CommonPorts, port) {
		return common.StreamTypeICEcast
	}

	// Check against configured URL patterns
	for _, pattern := range d.config.URLPatterns {
		if matched, err := regexp.MatchString(pattern, path); err == nil && matched {
			return common.StreamTypeICEcast
		}
		// Also check query string for edge cases
		if matched, err := regexp.MatchString(pattern, query); err == nil && matched {
			return common.StreamTypeICEcast
		}
	}

	return common.StreamTypeUnsupported
}

// DetectFromHeaders matches the HTTP headers with configured ICEcast patterns
func (d *Detector) DetectFromHeaders(ctx context.Context, streamURL string, httpConfig *HTTPConfig) common.StreamType {
	// Use provided client or create one with timeout
	client := d.client
	if httpConfig != nil {
		client = &http.Client{
			Timeout: httpConfig.ConnectionTimeout + httpConfig.ReadTimeout,
			Transport: &http.Transport{
				MaxIdleConns:       10,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: false,
			},
		}
	}
	req, err := http.NewRequestWithContext(ctx, "HEAD", streamURL, nil)
	if err != nil {
		logging.Error(err, "Error creating request for HTTP headers", logging.Fields{
			"url":   streamURL,
			"error": err.Error(),
		})
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
		req.Header.Set("Accept", "audio/*")
		req.Header.Set("Icy-MetaData", "1")
	}
	resp, err := client.Do(req)
	if err != nil {
		logging.Error(err, "Error getting response from client", logging.Fields{
			"url":   streamURL,
			"error": err.Error(),
		})
		return common.StreamTypeUnsupported
	}
	defer resp.Body.Close()

	// Check for ICEcast-specific headers FIRST, regardless of status code
	server := resp.Header.Get("Server")
	serverLower := strings.ToLower(server)
	logging.Debug("Checking for ICEcast", logging.Fields{
		"server_lower":     serverLower,
		"contains_icecast": strings.Contains(serverLower, "icecast"),
	})

	if strings.Contains(serverLower, "icecast") ||
		resp.Header.Get("icy-name") != "" ||
		resp.Header.Get("icy-description") != "" ||
		resp.Header.Get("icy-genre") != "" ||
		resp.Header.Get("icy-br") != "" ||
		resp.Header.Get("icy-metaint") != "" {
		logging.Debug("Detected ICEcast stream", logging.Fields{
			"url": streamURL,
		})
		return common.StreamTypeICEcast
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
			return common.StreamTypeICEcast
		}
	}

	logging.Debug("No ICEcast detection criteria met", logging.Fields{
		"url": streamURL,
	})
	return common.StreamTypeUnsupported
}

// IsValidICEcastContent checks if the content appears to be valid ICEcast audio stream
func (d *Detector) IsValidICEcastContent(ctx context.Context, streamURL string, httpConfig *HTTPConfig) bool {
	// Use configured timeout for content validation
	detectCtx, cancel := context.WithTimeout(ctx, time.Duration(d.config.TimeoutSeconds)*time.Second)
	defer cancel()

	// Use provided client or create one with timeout
	client := d.client
	if httpConfig != nil {
		client = &http.Client{
			Timeout: httpConfig.ConnectionTimeout + httpConfig.ReadTimeout,
			Transport: &http.Transport{
				MaxIdleConns:       10,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: false,
			},
		}
	}

	req, err := http.NewRequestWithContext(detectCtx, "GET", streamURL, nil)
	if err != nil {
		return false
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
		req.Header.Set("Accept", "audio/*")
		req.Header.Set("Icy-MetaData", "1")
	}

	// Don't use Range header - ICEcast streams don't support it
	// req.Header.Set("Range", "bytes=0-1023")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	// Accept 200 status (ICEcast streams return 200, not 206)
	if resp.StatusCode != http.StatusOK {
		return false
	}

	// Read a small amount of data to verify it's actually streaming
	buffer := make([]byte, 1024)
	n, err := resp.Body.Read(buffer)
	if err != nil && n == 0 {
		return false
	}

	if n > 0 {
		return isValidMP3Data(buffer[:n])
	}

	return false
}

func isValidMP3Data(data []byte) bool {
	if len(data) < 4 {
		return false
	}

	// Look for MP3 frame sync
	// MP3 frames start with 11 bits set (0xFFE)
	for i := 0; i < len(data)-1; i++ {
		if data[i] == 0xFF && (data[i+1]&0xE0) == 0xE0 {
			// Found potential MP3 frame header
			if i+3 < len(data) {
				// Additional validation of the MP3 frame header
				if isValidMP3FrameHeader(data[i : i+4]) {
					return true
				}
			}
		}
	}

	return false
}

func isValidMP3FrameHeader(header []byte) bool {
	if len(header) < 4 {
		return false
	}

	// First 11 bits should be 1 (frame sync)
	if header[0] != 0xFF || (header[1]&0xE0) != 0xE0 {
		return false
	}

	// MPEG version (bits 19-20)
	version := (header[1] >> 3) & 0x03
	if version == 0x01 { // Reserved version
		return false
	}

	// Layer (bits 17-18)
	layer := (header[1] >> 1) & 0x03
	if layer == 0x00 { // Reserved layer
		return false
	}

	// Bitrate (bits 12-15)
	bitrate := (header[2] >> 4) & 0x0F
	if bitrate == 0x00 || bitrate == 0x0F { // Free or reserved bitrate
		return false
	}

	// Sample rate (bits 10-11)
	sampleRate := (header[2] >> 2) & 0x03
	if sampleRate == 0x03 { // Reserved sample rate
		return false
	}

	return true
}

// extractMetadataFromHeaders extracts ICEcast metadata from HTTP headers
func extractMetadataFromHeaders(headers http.Header, metadata *common.StreamMetadata) {
	// Store all headers in lowercase for reference using maps.Copy
	headerMap := make(map[string]string)
	for key, values := range headers {
		if len(values) > 0 {
			headerMap[strings.ToLower(key)] = values[0]
		}
	}
	maps.Copy(metadata.Headers, headerMap)

	// Extract ICEcast-specific headers
	if name := headers.Get("icy-name"); name != "" {
		metadata.Station = name
		metadata.Headers["icy-name"] = name
	}
	if genre := headers.Get("icy-genre"); genre != "" {
		metadata.Genre = genre
		metadata.Headers["icy-genre"] = genre
	}
	if desc := headers.Get("icy-description"); desc != "" {
		metadata.Title = desc
		metadata.Headers["icy-description"] = desc
	}
	if url := headers.Get("icy-url"); url != "" {
		metadata.Headers["icy-url"] = url
	}

	// Extract technical metadata
	if bitrate := headers.Get("icy-br"); bitrate != "" {
		if br, err := strconv.Atoi(bitrate); err == nil {
			metadata.Bitrate = br
		}
		metadata.Headers["icy-br"] = bitrate
	}
	if sampleRate := headers.Get("icy-sr"); sampleRate != "" {
		if sr, err := strconv.Atoi(sampleRate); err == nil {
			metadata.SampleRate = sr
		}
		metadata.Headers["icy-sr"] = sampleRate
	}
	if channels := headers.Get("icy-channels"); channels != "" {
		if ch, err := strconv.Atoi(channels); err == nil {
			metadata.Channels = ch
		}
		metadata.Headers["icy-channels"] = channels
	}

	// Determine format/codec from content type
	contentType := headers.Get("Content-Type")
	metadata.ContentType = contentType

	switch {
	case strings.Contains(strings.ToLower(contentType), "mpeg"):
		metadata.Codec = "mp3"
		metadata.Format = "mp3"
	case strings.Contains(strings.ToLower(contentType), "aac"):
		metadata.Codec = "aac"
		metadata.Format = "aac"
	case strings.Contains(strings.ToLower(contentType), "ogg"):
		metadata.Codec = "ogg"
		metadata.Format = "ogg"
	default:
		metadata.Format = "unknown"
	}

	// Store all relevant ICEcast headers efficiently
	relevantHeaders := map[string]string{
		"content-type":    headers.Get("content-type"),
		"server":          headers.Get("server"),
		"icy-name":        headers.Get("icy-name"),
		"icy-genre":       headers.Get("icy-genre"),
		"icy-description": headers.Get("icy-description"),
		"icy-url":         headers.Get("icy-url"),
		"icy-br":          headers.Get("icy-br"),
		"icy-sr":          headers.Get("icy-sr"),
		"icy-channels":    headers.Get("icy-channels"),
		"icy-pub":         headers.Get("icy-pub"),
		"icy-notice1":     headers.Get("icy-notice1"),
		"icy-notice2":     headers.Get("icy-notice2"),
		"icy-metaint":     headers.Get("icy-metaint"),
		"icy-version":     headers.Get("icy-version"),
	}

	// Only add non-empty headers using maps.Copy
	filteredHeaders := make(map[string]string)
	for key, value := range relevantHeaders {
		if value != "" {
			filteredHeaders[key] = value
		}
	}
	maps.Copy(metadata.Headers, filteredHeaders)
}

// applyDefaultValues applies default values from configuration
func applyDefaultValues(metadata *common.StreamMetadata, defaults map[string]any) {
	for field, defaultValue := range defaults {
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
		case "format":
			if metadata.Format == "" || metadata.Format == "unknown" {
				if format, ok := defaultValue.(string); ok {
					metadata.Format = format
				}
			}
		}
	}
}

// ConfigurableDetection provides a complete detection suite with configuration
func ConfigurableDetection(ctx context.Context, streamURL string, config *Config) (common.StreamType, error) {
	if config == nil {
		config = DefaultConfig()
	}

	detector := NewDetectorWithConfig(config.Detection)

	// Step 1: URL pattern detection
	if streamType := detector.DetectFromURL(streamURL); streamType == common.StreamTypeICEcast {
		return common.StreamTypeICEcast, nil
	}

	// Step 2: Header detection
	if streamType := detector.DetectFromHeaders(ctx, streamURL, config.HTTP); streamType == common.StreamTypeICEcast {
		return common.StreamTypeICEcast, nil
	}

	// Step 3: Content validation as last resort
	if detector.IsValidICEcastContent(ctx, streamURL, config.HTTP) {
		return common.StreamTypeICEcast, nil
	}

	return common.StreamTypeUnsupported, common.NewStreamError(common.StreamTypeICEcast, streamURL,
		common.ErrCodeUnsupported, "stream type not supported or invalid", nil)
}

// Package-level convenience functions that use default configuration
// These maintain backward compatibility with existing code

// DetectFromURL matches the URL with common ICEcast patterns (backward compatibility)
func DetectFromURL(streamURL string) common.StreamType {
	detector := NewDetector()
	return detector.DetectFromURL(streamURL)
}

// DetectFromHeaders matches the HTTP headers with common ICEcast patterns (backward compatibility)
func DetectFromHeaders(ctx context.Context, client *http.Client, streamURL string) common.StreamType {
	detector := NewDetector()

	// Create a temporary HTTP config from the client timeout
	httpConfig := &HTTPConfig{
		UserAgent:      "TuneIn-CDN-Benchmark/1.0",
		AcceptHeader:   "audio/*",
		RequestICYMeta: true,
		CustomHeaders:  make(map[string]string),
	}

	if client != nil && client.Timeout > 0 {
		httpConfig.ConnectionTimeout = client.Timeout / 2
		httpConfig.ReadTimeout = client.Timeout / 2
	}

	return detector.DetectFromHeaders(ctx, streamURL, httpConfig)
}

// IsValidICEcastContent checks if the content appears to be valid ICEcast (backward compatibility)
func IsValidICEcastContent(ctx context.Context, client *http.Client, streamURL string) bool {
	detector := NewDetector()

	// Create HTTP config from client
	httpConfig := &HTTPConfig{
		UserAgent:      "TuneIn-CDN-Benchmark/1.0",
		AcceptHeader:   "audio/*",
		RequestICYMeta: true,
		CustomHeaders:  make(map[string]string),
	}
	if client != nil && client.Timeout > 0 {
		httpConfig.ConnectionTimeout = client.Timeout / 2
		httpConfig.ReadTimeout = client.Timeout / 2
	}

	return detector.IsValidICEcastContent(ctx, streamURL, httpConfig)
}
