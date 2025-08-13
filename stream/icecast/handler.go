package icecast

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// Handler implements StreamHandler for ICEcast streams with full configuration support
type Handler struct {
	client            *http.Client
	url               string
	metadata          *common.StreamMetadata
	stats             *common.StreamStats
	connected         bool
	response          *http.Response
	metadataExtractor *MetadataExtractor
	config            *Config
	icyMetaInt        int    // ICY metadata interval
	icyTitle          string // Current ICY title
	bytesRead         int64  // Bytes read since last metadata
	startTime         time.Time
}

// NewHandler creates a new ICEcast stream handler with default configuration
func NewHandler() *Handler {
	return NewHandlerWithConfig(nil)
}

// NewHandlerWithConfig creates a new ICEcast stream handler with custom configuration
func NewHandlerWithConfig(config *Config) *Handler {
	if config == nil {
		config = DefaultConfig()
	}

	// Create HTTP client with configured timeouts
	client := &http.Client{
		Timeout: config.HTTP.ConnectionTimeout + config.HTTP.ReadTimeout,
		Transport: &http.Transport{
			MaxIdleConns:       10,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: false,
		},
	}

	return &Handler{
		client:            client,
		stats:             &common.StreamStats{},
		metadataExtractor: NewConfigurableMetadataExtractor(config.MetadataExtractor).MetadataExtractor,
		config:            config,
	}
}

// Type returns the stream type for this handler
func (h *Handler) Type() common.StreamType {
	return common.StreamTypeICEcast
}

// CanHandle determines if this handler can process the given URL
func (h *Handler) CanHandle(ctx context.Context, url string) bool {
	// Use configurable detection for better accuracy
	streamType, err := ConfigurableDetection(ctx, url, h.config)
	if err == nil && streamType == common.StreamTypeICEcast {
		return true
	}

	// Fallback to individual detection methods for backward compatibility
	detector := NewDetectorWithConfig(h.config.Detection)

	if st := detector.DetectFromURL(url); st == common.StreamTypeICEcast {
		return true
	}

	if st := detector.DetectFromHeaders(ctx, url, h.config.HTTP); st == common.StreamTypeICEcast {
		return true
	}

	// Content validation as final check
	return detector.IsValidICEcastContent(ctx, url, h.config.HTTP)
}

// Connect establishes connection to the ICEcast stream
func (h *Handler) Connect(ctx context.Context, url string) error {
	if h.connected {
		return common.NewStreamError(common.StreamTypeICEcast, url,
			common.ErrCodeConnection, "already connected", nil)
	}

	h.url = url
	h.startTime = time.Now()

	logger := logging.WithFields(logging.Fields{
		"component": "icecast_handler",
		"function":  "Connect",
		"url":       url,
	})

	// Create context with configured timeout
	connectCtx, cancel := context.WithTimeout(ctx, h.config.HTTP.ConnectionTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(connectCtx, "GET", url, nil)
	if err != nil {
		return common.NewStreamError(common.StreamTypeICEcast, url,
			common.ErrCodeConnection, "failed to create request", err)
	}

	// Start with minimal headers - some ICEcast streams don't like extra headers
	req.Header.Set("User-Agent", h.config.HTTP.UserAgent)
	req.Header.Set("Accept", "*/*")

	logger.Debug("Attempting connection with minimal headers")

	resp, err := h.client.Do(req)
	if err != nil {
		// If the minimal approach fails, log and return error
		logger.Error(err, "Connection failed with minimal headers")
		return common.NewStreamError(common.StreamTypeICEcast, url,
			common.ErrCodeConnection, "connection failed", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()

		// For some ICEcast servers, try with ICY headers if basic request failed
		if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusMethodNotAllowed {
			logger.Debug("Basic request failed, trying with ICY headers", logging.Fields{
				"status_code": resp.StatusCode,
			})

			// Try again with ICY metadata header
			req2, err := http.NewRequestWithContext(connectCtx, "GET", url, nil)
			if err != nil {
				return common.NewStreamError(common.StreamTypeICEcast, url,
					common.ErrCodeConnection, "failed to create retry request", err)
			}

			req2.Header.Set("User-Agent", h.config.HTTP.UserAgent)
			req2.Header.Set("Accept", "*/*")
			req2.Header.Set("Icy-MetaData", "1")

			resp2, err := h.client.Do(req2)
			if err != nil {
				return common.NewStreamError(common.StreamTypeICEcast, url,
					common.ErrCodeConnection, "retry connection failed", err)
			}

			if resp2.StatusCode != http.StatusOK {
				resp2.Body.Close()
				return common.NewStreamErrorWithFields(common.StreamTypeICEcast, url,
					common.ErrCodeConnection, fmt.Sprintf("HTTP %d: %s", resp2.StatusCode, resp2.Status), nil,
					logging.Fields{
						"status_code": resp2.StatusCode,
						"status_text": resp2.Status,
					})
			}

			resp = resp2
		} else {
			return common.NewStreamErrorWithFields(common.StreamTypeICEcast, url,
				common.ErrCodeConnection, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, resp.Status), nil,
				logging.Fields{
					"status_code": resp.StatusCode,
					"status_text": resp.Status,
				})
		}
	}

	h.response = resp

	logger.Debug("ICEcast connection successful", logging.Fields{
		"status_code":  resp.StatusCode,
		"content_type": resp.Header.Get("Content-Type"),
	})

	// Parse ICY metadata interval if present (might not be present for all streams)
	if metaInt := resp.Header.Get("icy-metaint"); metaInt != "" {
		if interval, err := strconv.Atoi(metaInt); err == nil {
			h.icyMetaInt = interval
			logger.Debug("ICY metadata interval detected", logging.Fields{
				"interval": interval,
			})
		}
	} else {
		logger.Debug("No ICY metadata interval found - stream may not support metadata")
	}

	// Store response headers (may be minimal or empty for some streams)
	headers := make(map[string]string)
	for key, values := range resp.Header {
		if len(values) > 0 {
			headers[strings.ToLower(key)] = values[0]
		}
	}

	// Extract metadata using the configured extractor (handle case where headers are minimal)
	h.metadata = h.metadataExtractor.ExtractMetadata(resp.Header, url)

	// If metadata extraction failed due to missing headers, create basic metadata
	if h.metadata == nil {
		logger.Debug("Creating basic metadata due to minimal headers")
		h.metadata = &common.StreamMetadata{
			URL:         url,
			Type:        common.StreamTypeICEcast,
			ContentType: resp.Header.Get("Content-Type"),
			Headers:     headers,
			Timestamp:   time.Now(),
		}

		// Apply defaults for streams without proper headers
		h.applyDefaultMetadata()
	}

	// Merge HTTP headers with metadata headers
	if h.metadata.Headers == nil {
		h.metadata.Headers = make(map[string]string)
	}
	maps.Copy(h.metadata.Headers, headers)

	h.stats.ConnectionTime = time.Since(h.startTime)
	h.stats.FirstByteTime = h.stats.ConnectionTime
	h.connected = true

	logger.Debug("ICEcast handler connected successfully", logging.Fields{
		"has_icy_metadata": h.icyMetaInt > 0,
		"connection_time":  h.stats.ConnectionTime.Milliseconds(),
		"headers_count":    len(headers),
	})

	return nil
}

// GetMetadata retrieves stream metadata
func (h *Handler) GetMetadata() (*common.StreamMetadata, error) {
	if !h.connected {
		return nil, common.NewStreamError(common.StreamTypeICEcast, h.url,
			common.ErrCodeConnection, "not connected", nil)
	}

	// Return the metadata extracted during connection or updated during streaming
	if h.metadata == nil {
		// Fallback metadata if extraction failed, using configured defaults
		h.metadata = &common.StreamMetadata{
			URL:       h.url,
			Type:      common.StreamTypeICEcast,
			Headers:   make(map[string]string),
			Timestamp: time.Now(),
		}

		// Apply default values from configuration
		h.applyDefaultMetadata()
	}

	// Update with current ICY title if available
	if h.icyTitle != "" {
		h.metadata.Title = h.icyTitle
		h.metadata.Timestamp = time.Now()
	}

	return h.metadata, nil
}

// applyDefaultMetadata applies default values from configuration
func (h *Handler) applyDefaultMetadata() {
	if defaults := h.config.MetadataExtractor.DefaultValues; defaults != nil {
		if codec, ok := defaults["codec"].(string); ok {
			h.metadata.Codec = codec
		}
		if sampleRate, ok := defaults["sample_rate"].(int); ok {
			h.metadata.SampleRate = sampleRate
		} else if sampleRateFloat, ok := defaults["sample_rate"].(float64); ok {
			h.metadata.SampleRate = int(sampleRateFloat)
		}
		if channels, ok := defaults["channels"].(int); ok {
			h.metadata.Channels = channels
		} else if channelsFloat, ok := defaults["channels"].(float64); ok {
			h.metadata.Channels = int(channelsFloat)
		}
		if bitrate, ok := defaults["bitrate"].(int); ok {
			h.metadata.Bitrate = bitrate
		} else if bitrateFloat, ok := defaults["bitrate"].(float64); ok {
			h.metadata.Bitrate = int(bitrateFloat)
		}
		if format, ok := defaults["format"].(string); ok {
			h.metadata.Format = format
		}
	}
}

// ReadAudio provides basic audio reading (falls back to FFmpeg approach)
func (h *Handler) ReadAudio(ctx context.Context) (*common.AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "icecast_handler",
		"function":  "ReadAudio",
	})

	if !h.connected {
		return nil, common.NewStreamError(common.StreamTypeICEcast, h.url,
			common.ErrCodeConnection, "not connected", nil)
	}

	// Use a default duration for basic audio reading
	defaultDuration := 10 * time.Second
	if h.config != nil && h.config.Audio != nil && h.config.Audio.SampleDuration > 0 {
		defaultDuration = h.config.Audio.SampleDuration
	}

	logger.Debug("Using FFmpeg approach for basic audio reading", logging.Fields{
		"default_duration": defaultDuration.Seconds(),
	})

	return h.ReadAudioWithDuration(ctx, defaultDuration)
}

// readAudioWithFreshConnection creates a completely new HTTP connection just for reading audio
func (h *Handler) readAudioWithFreshConnection() (*common.AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "icecast_handler",
		"function":  "readAudioWithFreshConnection",
		"url":       h.url,
	})

	// Create a completely isolated HTTP client for this audio request
	audioClient := &http.Client{
		Timeout: h.config.HTTP.ConnectionTimeout + h.config.HTTP.ReadTimeout,
		Transport: &http.Transport{
			MaxIdleConns:       1,
			IdleConnTimeout:    10 * time.Second,
			DisableCompression: false,
			DisableKeepAlives:  true, // Important: disable keep-alives for one-shot requests
		},
	}

	// Create a background context with a reasonable timeout for audio reading
	// This is NOT derived from the original context to avoid cancellation inheritance
	audioTimeout := 300 * time.Second // Fixed timeout just for audio reading
	audioCtx, cancel := context.WithTimeout(context.Background(), audioTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(audioCtx, "GET", h.url, nil)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeICEcast, h.url,
			common.ErrCodeConnection, "failed to create audio request", err)
	}

	// Use minimal headers
	req.Header.Set("User-Agent", h.config.HTTP.UserAgent)
	req.Header.Set("Accept", "*/*")

	logger.Debug("Making fresh HTTP request for audio data")

	resp, err := audioClient.Do(req)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeICEcast, h.url,
			common.ErrCodeConnection, "failed to connect for audio", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, common.NewStreamError(common.StreamTypeICEcast, h.url,
			common.ErrCodeConnection, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, resp.Status), nil)
	}

	logger.Debug("Fresh audio connection successful", logging.Fields{
		"status_code":  resp.StatusCode,
		"content_type": resp.Header.Get("Content-Type"),
	})

	// Read audio data using a simple, direct approach
	return h.readAudioDataFromResponse(resp, logger)
}

// readAudioFromExistingConnection reads from the existing persistent connection
func (h *Handler) readAudioFromExistingConnection() (*common.AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "icecast_handler",
		"function":  "readAudioFromExistingConnection",
	})

	// Test if the existing connection is still alive
	testBuf := make([]byte, 1)
	_, testErr := h.response.Body.Read(testBuf)
	if testErr != nil {
		logger.Debug("Existing connection failed, creating fresh connection", logging.Fields{
			"test_error": testErr.Error(),
		})
		return h.readAudioWithFreshConnection()
	}

	// Connection is alive, put the test byte back and continue
	multiReader := io.MultiReader(bytes.NewReader(testBuf), h.response.Body)
	h.response.Body = io.NopCloser(multiReader)

	return h.readAudioDataFromResponse(h.response, logger)
}

// readAudioDataFromResponse reads audio data from an HTTP response
func (h *Handler) readAudioDataFromResponse(resp *http.Response, logger logging.Logger) (*common.AudioData, error) {
	bufferSize := h.config.Audio.BufferSize
	if bufferSize <= 0 {
		bufferSize = 8192
	}

	// Read a reasonable amount of audio data quickly
	targetBytes := bufferSize * 3
	maxAttempts := 5
	readTimeout := 10 * time.Second // Short timeout for individual reads

	var allAudioBytes []byte
	attempts := 0
	startTime := time.Now()

	logger.Debug("Starting to read audio data", logging.Fields{
		"buffer_size":  bufferSize,
		"target_bytes": targetBytes,
		"max_attempts": maxAttempts,
		"read_timeout": readTimeout.Seconds(),
	})

	for attempts < maxAttempts && len(allAudioBytes) < targetBytes {
		attempts++

		// Set a read deadline to prevent hanging
		if conn, ok := resp.Body.(interface{ SetReadDeadline(time.Time) error }); ok {
			conn.SetReadDeadline(time.Now().Add(readTimeout))
		}

		buffer := make([]byte, bufferSize)
		n, err := resp.Body.Read(buffer)

		logger.Debug("Read attempt completed", logging.Fields{
			"attempt":     attempts,
			"bytes_read":  n,
			"error":       err,
			"total_bytes": len(allAudioBytes),
		})

		if err != nil {
			if err == io.EOF {
				logger.Debug("Reached end of stream")
				break
			}

			// For any other error, if we have some data, use it
			if len(allAudioBytes) > 0 {
				logger.Debug("Got error but have some data, proceeding", logging.Fields{
					"error":      err.Error(),
					"bytes_have": len(allAudioBytes),
				})
				break
			}

			return nil, common.NewStreamError(common.StreamTypeICEcast, h.url,
				common.ErrCodeDecoding, fmt.Sprintf("failed to read audio data: %v", err), err)
		}

		if n > 0 {
			allAudioBytes = append(allAudioBytes, buffer[:n]...)
			logger.Debug("Successfully read audio chunk", logging.Fields{
				"chunk_size":  n,
				"total_bytes": len(allAudioBytes),
				"attempt":     attempts,
			})
		} else {
			logger.Debug("Zero bytes read, stopping")
			break
		}

		// Small delay to avoid busy waiting
		time.Sleep(10 * time.Millisecond)
	}

	if len(allAudioBytes) == 0 {
		return nil, common.NewStreamError(common.StreamTypeICEcast, h.url,
			common.ErrCodeDecoding, "no audio data received after all attempts", nil)
	}

	logger.Debug("Audio reading completed", logging.Fields{
		"total_bytes": len(allAudioBytes),
		"attempts":    attempts,
		"read_time":   time.Since(startTime).Seconds(),
	})

	// Update statistics
	h.stats.BytesReceived += int64(len(allAudioBytes))

	// Calculate average bitrate
	elapsed := time.Since(h.startTime)
	if elapsed > 0 {
		h.stats.AverageBitrate = float64(h.stats.BytesReceived*8) / elapsed.Seconds() / 1000
	}

	// Convert to PCM
	pcmSamples, err := h.convertToPCM(allAudioBytes)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeICEcast, h.url,
			common.ErrCodeDecoding, "failed to convert audio to PCM", err)
	}

	// Get metadata
	metadata, err := h.GetMetadata()
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeICEcast, h.url,
			common.ErrCodeMetadata, "failed to get metadata", err)
	}

	// Create metadata copy
	metadataCopy := *metadata
	metadataCopy.Timestamp = time.Now()

	// Calculate duration
	sampleRate := metadataCopy.SampleRate
	if sampleRate <= 0 {
		sampleRate = 44100
	}

	audioData := &common.AudioData{
		PCM:        pcmSamples,
		SampleRate: sampleRate,
		Channels:   metadataCopy.Channels,
		Duration:   time.Duration(len(pcmSamples)) * time.Second / time.Duration(sampleRate),
		Timestamp:  time.Now(),
		Metadata:   &metadataCopy,
	}

	logger.Debug("Audio processing completed successfully", logging.Fields{
		"samples":          len(pcmSamples),
		"duration_seconds": audioData.Duration.Seconds(),
		"sample_rate":      audioData.SampleRate,
		"channels":         audioData.Channels,
	})

	return audioData, nil
}

// convertToPCM converts raw audio bytes to PCM samples
func (h *Handler) convertToPCM(audioBytes []byte) ([]float64, error) {
	// TODO: decode ICEcast
	// This is a placeholder implementation
	// In a real implementation, you would use LibAV or similar to decode the audio
	// For now, we'll simulate PCM conversion based on the assumed format

	if h.metadata == nil || h.metadata.Codec == "" {
		return nil, common.NewStreamError(common.StreamTypeICEcast, h.url,
			common.ErrCodeDecoding, "no codec information available for PCM conversion", nil)
	}

	// For demonstration, assume 16-bit samples and convert to normalized float64
	// Real implementation would use proper audio decoding libraries
	channels := h.metadata.Channels
	if channels <= 0 {
		channels = 2 // Default to stereo
	}

	sampleCount := len(audioBytes) / 2 // Assuming 16-bit samples
	if channels > 1 {
		sampleCount /= channels
	}

	pcmSamples := make([]float64, sampleCount)
	for i := 0; i < sampleCount && i*2+1 < len(audioBytes); i++ {
		// Convert little-endian 16-bit to normalized float64
		sample := int16(audioBytes[i*2]) | int16(audioBytes[i*2+1])<<8
		pcmSamples[i] = float64(sample) / 32768.0
	}

	return pcmSamples, nil
}

// GetStats returns current streaming statistics
func (h *Handler) GetStats() *common.StreamStats {
	// Update buffer health based on connection state and configuration
	if h.connected && h.response != nil {
		h.stats.BufferHealth = 1.0 // Simplified - real implementation would check buffer levels
	} else {
		h.stats.BufferHealth = 0.0
	}

	return h.stats
}

// Close closes the stream connection
func (h *Handler) Close() error {
	h.connected = false
	if h.response != nil {
		err := h.response.Body.Close()
		h.response = nil
		return err
	}
	return nil
}

// GetClient returns the HTTP Client
func (h *Handler) GetClient() *http.Client {
	return h.client
}

// GetConfig returns the current configuration
func (h *Handler) GetConfig() *Config {
	return h.config
}

// UpdateConfig updates the handler configuration
func (h *Handler) UpdateConfig(config *Config) {
	if config == nil {
		return
	}

	h.config = config

	// Update HTTP client timeout if changed
	totalTimeout := config.HTTP.ConnectionTimeout + config.HTTP.ReadTimeout
	h.client.Timeout = totalTimeout

	// Recreate metadata extractor with new config
	h.metadataExtractor = NewConfigurableMetadataExtractor(config.MetadataExtractor).MetadataExtractor
}

// IsConfigured returns true if the handler has a non-default configuration
func (h *Handler) IsConfigured() bool {
	if h.config == nil {
		return false
	}
	defaultConfig := DefaultConfig()
	return !reflect.DeepEqual(h.config, defaultConfig)
}

// GetConfiguredUserAgent returns the configured user agent
func (h *Handler) GetConfiguredUserAgent() string {
	if h.config != nil && h.config.HTTP != nil {
		return h.config.HTTP.UserAgent
	}
	return "TuneIn-CDN-Benchmark/1.0"
}

// GetCurrentICYTitle returns the current ICY title from metadata
func (h *Handler) GetCurrentICYTitle() string {
	return h.icyTitle
}

// GetICYMetadataInterval returns the ICY metadata interval
func (h *Handler) GetICYMetadataInterval() int {
	return h.icyMetaInt
}

// HasICYMetadata returns true if the stream supports ICY metadata
func (h *Handler) HasICYMetadata() bool {
	return h.icyMetaInt > 0
}

// RefreshMetadata manually refreshes metadata from headers (useful for live streams)
func (h *Handler) RefreshMetadata() error {
	if !h.connected || h.response == nil {
		return common.NewStreamError(common.StreamTypeICEcast, h.url,
			common.ErrCodeConnection, "not connected", nil)
	}

	// Re-extract metadata from current response headers
	h.metadata = h.metadataExtractor.ExtractMetadata(h.response.Header, h.url)

	return nil
}

// GetStreamInfo returns detailed information about the current stream
func (h *Handler) GetStreamInfo() map[string]any {
	info := make(map[string]any)

	if h.metadata != nil {
		info["station"] = h.metadata.Station
		info["genre"] = h.metadata.Genre
		info["bitrate"] = h.metadata.Bitrate
		info["sample_rate"] = h.metadata.SampleRate
		info["channels"] = h.metadata.Channels
		info["codec"] = h.metadata.Codec
		info["format"] = h.metadata.Format
	}

	info["icy_metadata_interval"] = h.icyMetaInt
	info["current_title"] = h.icyTitle
	info["has_icy_metadata"] = h.HasICYMetadata()
	info["bytes_read"] = h.bytesRead
	info["connected"] = h.connected

	if h.stats != nil {
		info["bytes_received"] = h.stats.BytesReceived
		info["average_bitrate"] = h.stats.AverageBitrate
		info["connection_time"] = h.stats.ConnectionTime
		info["buffer_health"] = h.stats.BufferHealth
	}

	return info
}

// GetResponseHeaders returns the HTTP response headers from the stream connection
func (h *Handler) GetResponseHeaders() map[string]string {
	if h.response == nil {
		return make(map[string]string)
	}

	headers := make(map[string]string)
	for key, values := range h.response.Header {
		if len(values) > 0 {
			headers[strings.ToLower(key)] = values[0]
		}
	}

	return headers
}

// GetConnectionTime returns the time elapsed since connection
func (h *Handler) GetConnectionTime() time.Duration {
	if !h.connected {
		return 0
	}
	return time.Since(h.startTime)
}

// GetBytesPerSecond returns the current bytes per second rate
func (h *Handler) GetBytesPerSecond() float64 {
	if !h.connected || h.stats.BytesReceived == 0 {
		return 0
	}

	elapsed := time.Since(h.startTime)
	if elapsed <= 0 {
		return 0
	}

	return float64(h.stats.BytesReceived) / elapsed.Seconds()
}

// IsLive returns whether this appears to be a live stream (always true for ICEcast)
func (h *Handler) IsLive() bool {
	return true // ICEcast streams are typically live
}

// GetAudioFormat returns the detected audio format information
func (h *Handler) GetAudioFormat() map[string]any {
	if h.metadata == nil {
		return make(map[string]any)
	}

	return map[string]any{
		"codec":       h.metadata.Codec,
		"format":      h.metadata.Format,
		"bitrate":     h.metadata.Bitrate,
		"sample_rate": h.metadata.SampleRate,
		"channels":    h.metadata.Channels,
	}
}
