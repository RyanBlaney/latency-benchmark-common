package hls

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// Validator implements StreamValidator interface for HLS streams
type Validator struct {
	client *http.Client
	config *Config
	logger logging.Logger
}

// NewValidator creates a new HLS validator with default configuration
func NewValidator() *Validator {
	return NewValidatorWithConfig(nil)
}

// NewValidatorWithConfig creates a new HLS validator with custom configuration
func NewValidatorWithConfig(config *Config) *Validator {
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

	return &Validator{
		client: client,
		config: config,
		logger: logging.GetGlobalLogger(),
	}
}

// SetLogger sets a custom logger
// Useful if you have different loggers for different components
func (v *Validator) SetLogger(logger logging.Logger) {
	v.logger = logger
}

// ValidateURL validates if URL is accessible and valid for HLS
func (v *Validator) ValidateURL(ctx context.Context, streamURL string) error {
	// Parse URL
	parsedURL, err := url.Parse(streamURL)
	if err != nil {
		return common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeInvalidFormat, "invalid URL format", err)
	}

	// Check scheme
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeUnsupported, "unsupported URL scheme", nil)
	}

	// Check if URL looks like HLS using configured patterns
	detector := NewDetectorWithConfig(v.config.Detection)
	if detector.DetectFromURL(streamURL) != common.StreamTypeHLS {
		return common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeInvalidFormat, "URL does not appear to be HLS", nil)
	}

	// Perform HEAD request to check accessibility with configured timeout
	ctx, cancel := context.WithTimeout(ctx, v.config.HTTP.ConnectionTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "HEAD", streamURL, nil)
	if err != nil {
		return common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeConnection, "failed to create request", err)
	}

	// Set headers from configuration
	headers := v.config.GetHTTPHeaders()
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := v.client.Do(req)
	if err != nil {
		return common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeConnection, "connection failed", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode >= 400 {
		return common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeConnection, fmt.Sprintf("HTTP %d", resp.StatusCode), nil)
	}

	// Validate content type for HLS using configured content types
	contentType := resp.Header.Get("Content-Type")
	if contentType != "" && !v.isValidHLSContentType(contentType) {
		return common.NewStreamError(common.StreamTypeHLS, streamURL,
			common.ErrCodeInvalidFormat, fmt.Sprintf("invalid content type: %s", contentType), nil)
	}

	// Check required headers if configured
	for _, requiredHeader := range v.config.Detection.RequiredHeaders {
		if resp.Header.Get(requiredHeader) == "" {
			return common.NewStreamError(common.StreamTypeHLS, streamURL,
				common.ErrCodeInvalidFormat, fmt.Sprintf("missing required header: %s", requiredHeader), nil)
		}
	}

	return nil
}

// ValidateStream validates HLS stream content and format
func (v *Validator) ValidateStream(ctx context.Context, handler common.StreamHandler) error {
	// Ensure this is an HLS handler
	if handler.Type() != common.StreamTypeHLS {
		return common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeUnsupported, "handler is not for HLS streams", nil)
	}

	// Get metadata
	metadata, err := handler.GetMetadata()
	if err != nil {
		return common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeMetadata, "failed to get metadata", err)
	}

	// Validate essential HLS metadata
	if err := v.validateMetadata(metadata); err != nil {
		return err
	}

	// If this is an HLS handler, get the playlist for validation
	if hlsHandler, ok := handler.(*Handler); ok {
		playlist := hlsHandler.GetPlaylist()
		if playlist == nil {
			return common.NewStreamError(common.StreamTypeHLS, metadata.URL,
				common.ErrCodeInvalidFormat, "no playlist available", nil)
		}

		// Validate playlist structure
		if err := v.validatePlaylist(playlist, metadata.URL); err != nil {
			return err
		}

		// Validate playlist content
		if err := v.validatePlaylistContent(playlist, metadata.URL); err != nil {
			return err
		}
	}

	return nil
}

// ValidateAudio validates audio data quality for HLS
func (v *Validator) ValidateAudio(data *common.AudioData) error {
	if data == nil {
		return common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "audio data is nil", nil)
	}

	if len(data.PCM) == 0 {
		return common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "empty PCM data", nil)
	}

	if data.SampleRate <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "invalid sample rate", nil,
			logging.Fields{"sample_rate": data.SampleRate})
	}

	if data.Channels <= 0 || data.Channels > 8 {
		return common.NewStreamErrorWithFields(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "invalid channel count", nil,
			logging.Fields{"channels": data.Channels})
	}

	if data.Duration <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "invalid duration", nil,
			logging.Fields{"duration": data.Duration.String()})
	}

	if data.Metadata != nil && data.Metadata.Type == common.StreamTypeHLS {
		// Check if sample rate matches expected values for HLS using configured defaults
		validSampleRates := []int{22050, 44100, 48000}

		// Add configured default sample rate if it's not in the standard list
		if defaultSampleRate, ok := v.config.MetadataExtractor.DefaultValues["sample_rate"]; ok {
			if rate, ok := defaultSampleRate.(int); ok && !slices.Contains(validSampleRates, rate) {
				validSampleRates = append(validSampleRates, rate)
			}
		}

		validRate := slices.Contains(validSampleRates, data.SampleRate)
		if !validRate {
			return common.NewStreamErrorWithFields(common.StreamTypeHLS, "",
				common.ErrCodeInvalidFormat, "unusual sample rate for HLS", nil,
				logging.Fields{
					"sample_rate": data.SampleRate,
					"valid_rates": validSampleRates,
				})
		}

		// Check for reasonable audio levels (not all silence)
		if v.isAllSilence(data.PCM) {
			return common.NewStreamError(common.StreamTypeHLS, "",
				common.ErrCodeInvalidFormat, "audio data contains only silence", nil)
		}

		// Validate against configured minimum bitrate if available
		if data.Metadata.Bitrate > 0 {
			if minBitrate, ok := v.config.MetadataExtractor.DefaultValues["min_bitrate"]; ok {
				if minRate, ok := minBitrate.(int); ok && data.Metadata.Bitrate < minRate {
					return common.NewStreamErrorWithFields(common.StreamTypeHLS, "",
						common.ErrCodeInvalidFormat, "bitrate below minimum threshold", nil,
						logging.Fields{
							"actual_bitrate":  data.Metadata.Bitrate,
							"minimum_bitrate": minRate,
						})
				}
			}
		}
	}

	return nil
}

// validateMetadata validates HLS metadata using configuration
func (v *Validator) validateMetadata(metadata *common.StreamMetadata) error {
	if metadata.Type != common.StreamTypeHLS {
		return common.NewStreamError(common.StreamTypeHLS, metadata.URL,
			common.ErrCodeInvalidFormat, "stream type is not HLS", nil)
	}

	if metadata.Codec == "" {
		// Check if we have a default codec configured
		if defaultCodec, ok := v.config.MetadataExtractor.DefaultValues["codec"]; ok {
			if codec, ok := defaultCodec.(string); ok && codec != "" {
				return nil // Allow empty codec if we have a default
			}
		}
		return common.NewStreamError(common.StreamTypeHLS, metadata.URL,
			common.ErrCodeInvalidFormat, "codec not detected", nil)
	}

	// Validate HLS-appropriate codecs (can be configured in the future)
	validCodecs := []string{"aac", "mp3", "ac3", "he-aac", "opus"}

	// Add configured default codec if it's not in the standard list
	if defaultCodec, ok := v.config.MetadataExtractor.DefaultValues["codec"]; ok {
		if codec, ok := defaultCodec.(string); ok && !slices.Contains(validCodecs, strings.ToLower(codec)) {
			validCodecs = append(validCodecs, strings.ToLower(codec))
		}
	}

	validCodec := false
	for _, codec := range validCodecs {
		if strings.EqualFold(metadata.Codec, codec) {
			validCodec = true
			break
		}
	}
	if !validCodec {
		return common.NewStreamError(common.StreamTypeHLS, metadata.URL,
			common.ErrCodeInvalidFormat, fmt.Sprintf("unusual codec for HLS: %s", metadata.Codec), nil)
	}

	return nil
}

// validatePlaylist validates M3U8 playlist structure using configuration
func (v *Validator) validatePlaylist(playlist *M3U8Playlist, url string) error {
	if !playlist.IsValid {
		return common.NewStreamError(common.StreamTypeHLS, url,
			common.ErrCodeInvalidFormat, "invalid M3U8 playlist", nil)
	}

	// Check playlist has content
	if len(playlist.Segments) == 0 && len(playlist.Variants) == 0 {
		return common.NewStreamError(common.StreamTypeHLS, url,
			common.ErrCodeInvalidFormat, "empty playlist - no segments or variants", nil)
	}

	// Validate playlist version
	if playlist.Version > 0 && (playlist.Version < 1 || playlist.Version > 10) {
		return common.NewStreamError(common.StreamTypeHLS, url,
			common.ErrCodeInvalidFormat, fmt.Sprintf("invalid playlist version: %d", playlist.Version), nil)
	}

	// For media playlists, validate target duration
	if !playlist.IsMaster && playlist.TargetDuration <= 0 {
		return common.NewStreamError(common.StreamTypeHLS, url,
			common.ErrCodeInvalidFormat, "media playlist missing target duration", nil)
	}

	// Validate segment count against configured maximum
	if v.config.Parser.MaxSegmentAnalysis > 0 && len(playlist.Segments) > v.config.Parser.MaxSegmentAnalysis {
		if v.config.Parser.StrictMode {
			return common.NewStreamError(common.StreamTypeHLS, url,
				common.ErrCodeInvalidFormat, fmt.Sprintf("too many segments: %d > %d", len(playlist.Segments), v.config.Parser.MaxSegmentAnalysis), nil)
		}
		v.logger.Warn("Playlist exceeds configured segment limit", logging.Fields{
			"segment_count": len(playlist.Segments),
			"max_allowed":   v.config.Parser.MaxSegmentAnalysis,
			"url":           url,
		})
	}

	return nil
}

// validatePlaylistContent validates playlist content quality using configuration
func (v *Validator) validatePlaylistContent(playlist *M3U8Playlist, streamURL string) error {
	// Validate segments
	for i, segment := range playlist.Segments {
		if segment.URI == "" {
			return common.NewStreamError(common.StreamTypeHLS, streamURL,
				common.ErrCodeInvalidFormat, fmt.Sprintf("segment %d missing URI", i), nil)
		}

		if segment.Duration < 0 {
			return common.NewStreamError(common.StreamTypeHLS, streamURL,
				common.ErrCodeInvalidFormat, fmt.Sprintf("segment %d has negative duration", i), nil)
		}

		// Validate URI format if configured
		if v.config.Parser.ValidateURIs {
			if _, err := url.Parse(segment.URI); err != nil {
				return common.NewStreamError(common.StreamTypeHLS, streamURL,
					common.ErrCodeInvalidFormat, fmt.Sprintf("segment %d has invalid URI", i), err)
			}
		}

		// In strict mode, validate segment duration against target duration
		if v.config.Parser.StrictMode && playlist.TargetDuration > 0 {
			if segment.Duration > float64(playlist.TargetDuration)*1.5 {
				return common.NewStreamError(common.StreamTypeHLS, streamURL,
					common.ErrCodeInvalidFormat, fmt.Sprintf("segment %d duration exceeds target duration significantly", i), nil)
			}
		}
	}

	// Validate variants
	for i, variant := range playlist.Variants {
		if variant.URI == "" {
			return common.NewStreamError(common.StreamTypeHLS, streamURL,
				common.ErrCodeInvalidFormat, fmt.Sprintf("variant %d missing URI", i), nil)
		}

		if variant.Bandwidth <= 0 {
			return common.NewStreamError(common.StreamTypeHLS, streamURL,
				common.ErrCodeInvalidFormat, fmt.Sprintf("variant %d has invalid bandwidth", i), nil)
		}

		// Validate URI format if configured
		if v.config.Parser.ValidateURIs {
			if _, err := url.Parse(variant.URI); err != nil {
				return common.NewStreamError(common.StreamTypeHLS, streamURL,
					common.ErrCodeInvalidFormat, fmt.Sprintf("variant %d has invalid URI", i), err)
			}
		}
	}

	// Check for reasonable bandwidth progression in variants
	if len(playlist.Variants) > 1 {
		if err := v.validateBandwidthProgression(playlist.Variants, streamURL); err != nil {
			return err
		}
	}

	return nil
}

// validateBandwidthProgression checks if variant bandwidths make sense
func (v *Validator) validateBandwidthProgression(variants []M3U8Variant, url string) error {
	if len(variants) < 2 {
		return nil
	}

	// Check for duplicate bandwidths
	bandwidthMap := make(map[int]bool)
	for _, variant := range variants {
		if bandwidthMap[variant.Bandwidth] {
			return common.NewStreamError(common.StreamTypeHLS, url,
				common.ErrCodeInvalidFormat, fmt.Sprintf("duplicate bandwidth: %d", variant.Bandwidth), nil)
		}
		bandwidthMap[variant.Bandwidth] = true
	}

	// Check for reasonable progression (each step should be meaningful)
	bandwidths := make([]int, len(variants))
	for i, variant := range variants {
		bandwidths[i] = variant.Bandwidth
	}

	// Simple bubble sort
	for i := 0; i < len(bandwidths)-1; i++ {
		for j := 0; j < len(bandwidths)-i-1; j++ {
			if bandwidths[j] > bandwidths[j+1] {
				bandwidths[j], bandwidths[j+1] = bandwidths[j+1], bandwidths[j]
			}
		}
	}

	// Check progression ratios (more lenient in non-strict mode)
	minRatio := 1.1
	maxRatio := 10.0

	if !v.config.Parser.StrictMode {
		minRatio = 1.05 // More lenient
		maxRatio = 20.0 // More lenient
	}

	for i := 1; i < len(bandwidths); i++ {
		ratio := float64(bandwidths[i]) / float64(bandwidths[i-1])
		if ratio < minRatio || ratio > maxRatio {
			errMsg := "unreasonable bandwidth progression in variants"
			if v.config.Parser.StrictMode {
				return common.NewStreamError(common.StreamTypeHLS, url,
					common.ErrCodeInvalidFormat, errMsg, nil)
			} else {
				v.logger.Warn(errMsg, logging.Fields{
					"bandwidth_ratio":    ratio,
					"previous_bandwidth": bandwidths[i-1],
					"current_bandwidth":  bandwidths[i],
				})
			}
		}
	}

	return nil
}

// isValidHLSContentType checks if content type is valid for HLS using configured content types
func (v *Validator) isValidHLSContentType(contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))

	// Remove parameters
	if idx := strings.Index(contentType, ";"); idx != -1 {
		contentType = contentType[:idx]
	}

	// Use configured content types
	for _, validType := range v.config.Detection.ContentTypes {
		if strings.EqualFold(contentType, validType) {
			return true
		}
	}

	// Fallback to text/plain which is sometimes used
	return contentType == "text/plain"
}

// isAllSilence checks if audio data is all silence
func (v *Validator) isAllSilence(pcm []float64) bool {
	threshold := 0.001 // Very small threshold for silence detection

	for _, sample := range pcm {
		if sample > threshold || sample < -threshold {
			return false
		}
	}

	return true
}

// GetConfig returns the validator's configuration
func (v *Validator) GetConfig() *Config {
	return v.config
}

// UpdateConfig updates the validator's configuration
func (v *Validator) UpdateConfig(config *Config) {
	if config == nil {
		return
	}

	v.config = config

	// Update HTTP client with new timeouts
	v.client.Timeout = config.HTTP.ConnectionTimeout + config.HTTP.ReadTimeout
}

// ValidateConfiguration validates the validator's own configuration
func (v *Validator) ValidateConfiguration() error {
	if v.config == nil {
		return common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "validator configuration is nil", nil)
	}

	if v.config.HTTP.ConnectionTimeout <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "connection timeout must be positive", nil,
			logging.Fields{"connection_timeout": v.config.HTTP.ConnectionTimeout})
	}

	if v.config.HTTP.ReadTimeout <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "read timeout must be positive", nil,
			logging.Fields{"read_timeout": v.config.HTTP.ReadTimeout})
	}

	if v.config.Detection.TimeoutSeconds <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "detection timeout must be positive", nil,
			logging.Fields{"timeout_seconds": v.config.Detection.TimeoutSeconds})
	}

	if len(v.config.Detection.URLPatterns) == 0 {
		return common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "at least one URL pattern must be configured", nil)
	}

	if len(v.config.Detection.ContentTypes) == 0 {
		return common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "at least one content type must be configured", nil)
	}

	return nil
}
