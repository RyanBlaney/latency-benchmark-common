package icecast

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

// Validator implements StreamValidator interface for ICEcast streams
type Validator struct {
	client *http.Client
	config *Config
}

// NewValidator creates a new ICEcast validator with default configuration
func NewValidator() *Validator {
	return NewValidatorWithConfig(nil)
}

// NewValidatorWithConfig creates a new ICEcast validator with custom configuration
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
	}
}

// ValidateURL validates if URL is accessible and valid for ICEcast
func (v *Validator) ValidateURL(ctx context.Context, streamURL string) error {
	// Parse URL
	parsedURL, err := url.Parse(streamURL)
	if err != nil {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeInvalidFormat, "invalid URL format", err)
	}

	// Check scheme
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeUnsupported, "unsupported URL scheme", nil)
	}

	// Check if URL looks like ICEcast using configured patterns
	detector := NewDetectorWithConfig(v.config.Detection)
	if detector.DetectFromURL(streamURL) != common.StreamTypeICEcast {
		// Don't fail here - URL might still be ICEcast even if it doesn't match patterns
		// We'll do a more thorough check with headers
	}

	// Perform HEAD request to check accessibility with configured timeout
	ctx, cancel := context.WithTimeout(ctx, v.config.HTTP.ConnectionTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "HEAD", streamURL, nil)
	if err != nil {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeConnection, "failed to create request", err)
	}

	// Set headers from configuration
	headers := v.config.GetHTTPHeaders()
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := v.client.Do(req)
	if err != nil {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeConnection, "connection failed", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode >= 400 {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeConnection, fmt.Sprintf("HTTP %d", resp.StatusCode), nil)
	}

	// Validate content type for ICEcast using configured content types
	contentType := resp.Header.Get("Content-Type")
	if contentType != "" && !v.isValidICEcastContentType(contentType) {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeInvalidFormat, fmt.Sprintf("invalid content type: %s", contentType), nil)
	}

	// Check required headers if configured
	for _, requiredHeader := range v.config.Detection.RequiredHeaders {
		if resp.Header.Get(requiredHeader) == "" {
			return common.NewStreamError(common.StreamTypeICEcast, streamURL,
				common.ErrCodeInvalidFormat, fmt.Sprintf("missing required header: %s", requiredHeader), nil)
		}
	}

	// Validate that this looks like an audio stream
	if err := v.validateAudioStreamHeaders(resp.Header, streamURL); err != nil {
		return err
	}

	return nil
}

// ValidateStream validates ICEcast stream content and format
func (v *Validator) ValidateStream(ctx context.Context, handler common.StreamHandler) error {
	// Ensure this is an ICEcast handler
	if handler.Type() != common.StreamTypeICEcast {
		return common.NewStreamError(common.StreamTypeICEcast, "",
			common.ErrCodeUnsupported, "handler is not for ICEcast streams", nil)
	}

	// Get metadata
	metadata, err := handler.GetMetadata()
	if err != nil {
		return common.NewStreamError(common.StreamTypeICEcast, "",
			common.ErrCodeMetadata, "failed to get metadata", err)
	}

	// Validate essential ICEcast metadata
	if err := v.validateMetadata(metadata); err != nil {
		return err
	}

	// Validate stream is actually serving audio content
	if err := v.validateStreamContent(ctx, metadata.URL); err != nil {
		return err
	}

	return nil
}

// ValidateAudio validates audio data quality for ICEcast
func (v *Validator) ValidateAudio(data *common.AudioData) error {
	if data == nil {
		return common.NewStreamError(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "audio data is nil", nil)
	}

	if len(data.PCM) == 0 {
		return common.NewStreamError(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "empty PCM data", nil)
	}

	if data.SampleRate <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "invalid sample rate", nil,
			logging.Fields{"sample_rate": data.SampleRate})
	}

	if data.Channels <= 0 || data.Channels > 8 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "invalid channel count", nil,
			logging.Fields{"channels": data.Channels})
	}

	if data.Duration <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "invalid duration", nil,
			logging.Fields{"duration": data.Duration.String()})
	}

	if data.Metadata != nil && data.Metadata.Type == common.StreamTypeICEcast {
		// Check if sample rate matches expected values for ICEcast using configured defaults
		validSampleRates := []int{8000, 11025, 16000, 22050, 32000, 44100, 48000, 96000}

		// Add configured default sample rate if it's not in the standard list
		if defaultSampleRate, ok := v.config.MetadataExtractor.DefaultValues["sample_rate"]; ok {
			if rate, ok := defaultSampleRate.(int); ok && !slices.Contains(validSampleRates, rate) {
				validSampleRates = append(validSampleRates, rate)
			}
		}

		validRate := slices.Contains(validSampleRates, data.SampleRate)
		if !validRate {
			return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
				common.ErrCodeInvalidFormat, "unusual sample rate for ICEcast", nil,
				logging.Fields{
					"sample_rate": data.SampleRate,
					"valid_rates": validSampleRates,
				})
		}

		// Check for reasonable audio levels (not all silence)
		if v.isAllSilence(data.PCM) {
			return common.NewStreamError(common.StreamTypeICEcast, "",
				common.ErrCodeInvalidFormat, "audio data contains only silence", nil)
		}

		// Validate against configured minimum bitrate if available
		if data.Metadata.Bitrate > 0 {
			if minBitrate, ok := v.config.MetadataExtractor.DefaultValues["min_bitrate"]; ok {
				if minRate, ok := minBitrate.(int); ok && data.Metadata.Bitrate < minRate {
					return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
						common.ErrCodeInvalidFormat, "bitrate below minimum threshold", nil,
						logging.Fields{
							"actual_bitrate":  data.Metadata.Bitrate,
							"minimum_bitrate": minRate,
						})
				}
			}
		}

		// Validate codec is appropriate for ICEcast
		if data.Metadata.Codec != "" {
			if err := v.validateCodec(data.Metadata.Codec); err != nil {
				return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
					common.ErrCodeInvalidFormat, "codec validation failed", err,
					logging.Fields{"codec": data.Metadata.Codec})
			}
		}
	}

	return nil
}

// validateMetadata validates ICEcast metadata using configuration
func (v *Validator) validateMetadata(metadata *common.StreamMetadata) error {
	if metadata.Type != common.StreamTypeICEcast {
		return common.NewStreamError(common.StreamTypeICEcast, metadata.URL,
			common.ErrCodeInvalidFormat, "stream type is not ICEcast", nil)
	}

	// Validate codec if present
	if metadata.Codec != "" {
		if err := v.validateCodec(metadata.Codec); err != nil {
			return common.NewStreamError(common.StreamTypeICEcast, metadata.URL,
				common.ErrCodeInvalidFormat, fmt.Sprintf("invalid codec: %s", metadata.Codec), err)
		}
	} else {
		// Check if we have a default codec configured
		if defaultCodec, ok := v.config.MetadataExtractor.DefaultValues["codec"]; ok {
			if codec, ok := defaultCodec.(string); ok && codec != "" {
				return nil // Allow empty codec if we have a default
			}
		}
		return common.NewStreamError(common.StreamTypeICEcast, metadata.URL,
			common.ErrCodeInvalidFormat, "codec not detected", nil)
	}

	// Validate content type if present
	if metadata.ContentType != "" {
		if !v.isValidICEcastContentType(metadata.ContentType) {
			return common.NewStreamError(common.StreamTypeICEcast, metadata.URL,
				common.ErrCodeInvalidFormat, fmt.Sprintf("invalid content type: %s", metadata.ContentType), nil)
		}
	}

	// Validate technical parameters
	if metadata.SampleRate > 0 {
		if metadata.SampleRate < 8000 || metadata.SampleRate > 192000 {
			return common.NewStreamError(common.StreamTypeICEcast, metadata.URL,
				common.ErrCodeInvalidFormat, fmt.Sprintf("unusual sample rate: %d", metadata.SampleRate), nil)
		}
	}

	if metadata.Channels > 0 {
		if metadata.Channels > 8 {
			return common.NewStreamError(common.StreamTypeICEcast, metadata.URL,
				common.ErrCodeInvalidFormat, fmt.Sprintf("too many channels: %d", metadata.Channels), nil)
		}
	}

	if metadata.Bitrate > 0 {
		if metadata.Bitrate < 8 || metadata.Bitrate > 2000 {
			return common.NewStreamError(common.StreamTypeICEcast, metadata.URL,
				common.ErrCodeInvalidFormat, fmt.Sprintf("unusual bitrate: %d", metadata.Bitrate), nil)
		}
	}

	return nil
}

// validateCodec validates if the codec is appropriate for ICEcast
func (v *Validator) validateCodec(codec string) error {
	// ICEcast-appropriate codecs (can be extended via configuration)
	validCodecs := []string{"mp3", "aac", "ogg", "vorbis", "opus", "flac", "wav", "pcm"}

	// Add configured default codec if it's not in the standard list
	if defaultCodec, ok := v.config.MetadataExtractor.DefaultValues["codec"]; ok {
		if configCodec, ok := defaultCodec.(string); ok && !slices.Contains(validCodecs, strings.ToLower(configCodec)) {
			validCodecs = append(validCodecs, strings.ToLower(configCodec))
		}
	}

	codecLower := strings.ToLower(strings.TrimSpace(codec))
	if !slices.Contains(validCodecs, codecLower) {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "unsupported codec for ICEcast", nil,
			logging.Fields{
				"codec":        codec,
				"valid_codecs": validCodecs,
			})
	}

	return nil
}

// validateAudioStreamHeaders validates that headers indicate an audio stream
func (v *Validator) validateAudioStreamHeaders(headers http.Header, streamURL string) error {
	// Check for audio content type
	contentType := headers.Get("Content-Type")
	if contentType != "" {
		if !strings.HasPrefix(strings.ToLower(contentType), "audio/") &&
			!v.isValidICEcastContentType(contentType) {
			return common.NewStreamError(common.StreamTypeICEcast, streamURL,
				common.ErrCodeInvalidFormat, "content type does not indicate audio stream", nil)
		}
	}

	// Check for ICEcast-specific headers (good indicators)
	icyHeaders := []string{"icy-name", "icy-genre", "icy-br", "icy-metaint"}
	hasICYHeader := false
	for _, header := range icyHeaders {
		if headers.Get(header) != "" {
			hasICYHeader = true
			break
		}
	}

	// Check for server header indicating ICEcast
	server := strings.ToLower(headers.Get("Server"))
	isICEcastServer := strings.Contains(server, "icecast")

	// If we don't have clear ICEcast indicators, be more lenient but warn
	if !hasICYHeader && !isICEcastServer && contentType == "" {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeInvalidFormat, "no clear audio stream indicators found", nil)
	}

	return nil
}

// validateStreamContent validates that the stream is actually serving audio content
func (v *Validator) validateStreamContent(ctx context.Context, streamURL string) error {
	// Use configured timeout for content validation
	ctx, cancel := context.WithTimeout(ctx, v.config.HTTP.ReadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", streamURL, nil)
	if err != nil {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeConnection, "failed to create content validation request", err)
	}

	// Set headers from configuration
	headers := v.config.GetHTTPHeaders()
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	// Set range header to only read a small amount of data
	req.Header.Set("Range", "bytes=0-1023")

	resp, err := v.client.Do(req)
	if err != nil {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeConnection, "content validation request failed", err)
	}
	defer resp.Body.Close()

	// Accept both 200 (full content) and 206 (partial content)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeConnection, fmt.Sprintf("content validation failed: HTTP %d", resp.StatusCode), nil)
	}

	// Read a small amount of data to verify it's actually streaming
	buffer := make([]byte, 1024)
	n, err := resp.Body.Read(buffer)
	if err != nil && n == 0 {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeInvalidFormat, "no content data received", err)
	}

	// Very basic validation - ensure we got some data
	if n == 0 {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeInvalidFormat, "empty content stream", nil)
	}

	return nil
}

// isValidICEcastContentType checks if content type is valid for ICEcast using configured content types
func (v *Validator) isValidICEcastContentType(contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))

	// Remove parameters
	if idx := strings.Index(contentType, ";"); idx != -1 {
		contentType = contentType[:idx]
	}
	contentType = strings.TrimSpace(contentType)

	// Use configured content types
	for _, validType := range v.config.Detection.ContentTypes {
		if strings.EqualFold(contentType, validType) {
			return true
		}
	}

	// Additional common audio types that might not be in config
	commonAudioTypes := []string{
		"audio/mpeg", "audio/mp3", "audio/aac", "audio/ogg", "audio/vorbis",
		"audio/opus", "audio/flac", "audio/wav", "audio/wave", "application/ogg",
	}

	for _, audioType := range commonAudioTypes {
		if strings.EqualFold(contentType, audioType) {
			return true
		}
	}

	return false
}

// isAllSilence checks if audio data is all silence
func (v *Validator) isAllSilence(pcm []float64) bool {
	threshold := 0.001 // Very small threshold for silence detection
	nonSilentSamples := 0

	for _, sample := range pcm {
		if sample > threshold || sample < -threshold {
			nonSilentSamples++
			// If we find enough non-silent samples, it's not all silence
			if nonSilentSamples > len(pcm)/100 { // At least 1% non-silent
				return false
			}
		}
	}

	// Less than 1% of samples are non-silent, consider it silence
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
		return common.NewStreamError(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "validator configuration is nil", nil)
	}

	if v.config.HTTP.ConnectionTimeout <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "connection timeout must be positive", nil,
			logging.Fields{"connection_timeout": v.config.HTTP.ConnectionTimeout})
	}

	if v.config.HTTP.ReadTimeout <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "read timeout must be positive", nil,
			logging.Fields{"read_timeout": v.config.HTTP.ReadTimeout})
	}

	if v.config.Detection.TimeoutSeconds <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "detection timeout must be positive", nil,
			logging.Fields{"timeout_seconds": v.config.Detection.TimeoutSeconds})
	}

	if len(v.config.Detection.URLPatterns) == 0 {
		return common.NewStreamError(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "at least one URL pattern must be configured", nil)
	}

	if len(v.config.Detection.ContentTypes) == 0 {
		return common.NewStreamError(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "at least one content type must be configured", nil)
	}

	if v.config.Audio.BufferSize <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "audio buffer size must be positive", nil,
			logging.Fields{"buffer_size": v.config.Audio.BufferSize})
	}

	if v.config.Audio.MaxReadAttempts <= 0 {
		return common.NewStreamErrorWithFields(common.StreamTypeICEcast, "",
			common.ErrCodeInvalidFormat, "max read attempts must be positive", nil,
			logging.Fields{"max_read_attempts": v.config.Audio.MaxReadAttempts})
	}

	return nil
}

// ValidateICEcastSpecific performs ICEcast-specific validation checks
func (v *Validator) ValidateICEcastSpecific(ctx context.Context, streamURL string) error {
	detector := NewDetectorWithConfig(v.config.Detection)

	// Try different detection methods
	urlDetection := detector.DetectFromURL(streamURL)
	headerDetection := detector.DetectFromHeaders(ctx, streamURL, v.config.HTTP)

	if urlDetection != common.StreamTypeICEcast && headerDetection != common.StreamTypeICEcast {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeInvalidFormat, "stream does not appear to be ICEcast", nil)
	}

	// Validate content if possible
	if st := detector.DetectFromHeaders(ctx, streamURL, v.config.HTTP); st != common.StreamTypeICEcast {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeInvalidFormat, "invalid ICEcast content", nil)
	}

	if st := detector.DetectFromURL(streamURL); st != common.StreamTypeICEcast {
		return common.NewStreamError(common.StreamTypeICEcast, streamURL,
			common.ErrCodeInvalidFormat, "invalid ICEcast content", nil)
	}

	return nil
}
