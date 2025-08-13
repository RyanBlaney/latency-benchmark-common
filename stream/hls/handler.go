package hls

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// Handler implements StreamHandler for HLS streams
type Handler struct {
	client            *http.Client
	url               string
	metadata          *common.StreamMetadata
	stats             *common.StreamStats
	connected         bool
	playlist          *M3U8Playlist
	parser            *Parser
	downloader        *AudioDownloader
	metadataExtractor *MetadataExtractor
	config            *Config
}

// NewHandler creates a new HLS stream handler with default configuration
func NewHandler() *Handler {
	return NewHandlerWithConfig(nil)
}

// NewHandlerWithConfig creates a new HLS stream handler with custom configuration
func NewHandlerWithConfig(config *Config) *Handler {
	if config == nil {
		config = DefaultConfig()
	}
	// Create HTTP client with configured timeouts
	client := &http.Client{
		Timeout: config.HTTP.ConnectionTimeout + config.HTTP.ReadTimeout,
		Transport: &http.Transport{
			MaxIdleConns:       10,
			IdleConnTimeout:    300 * time.Second,
			DisableCompression: false,
		},
	}

	return &Handler{
		client:            client,
		stats:             &common.StreamStats{},
		parser:            NewConfigurableParser(config.Parser).Parser,
		metadataExtractor: NewConfigurableMetadataExtractor(config.MetadataExtractor).MetadataExtractor,
		config:            config,
	}
}

// Type returns the stream type for this handler
func (h *Handler) Type() common.StreamType {
	return common.StreamTypeHLS
}

// CanHandle determines if this handler can process the given URL
func (h *Handler) CanHandle(ctx context.Context, url string) bool {
	// Use configurable detection for better accuracy
	streamType, _, err := ConfigurableDetection(ctx, url, h.config)
	if err == nil && streamType == common.StreamTypeHLS {
		return true
	}

	// Fallback to individual detection methods for backward compatibility
	detector := NewDetectorWithConfig(h.config.Detection)

	if st := detector.DetectFromURL(url); st == common.StreamTypeHLS {
		return true
	}

	if st := detector.DetectFromHeaders(ctx, url, h.config.HTTP); st == common.StreamTypeHLS {
		return true
	}

	// M3U8 content parsing as final check
	return detector.IsValidHLSContent(ctx, url, h.config.HTTP, h.config.Parser)
}

// Connect establishes connection to the HLS stream
func (h *Handler) Connect(ctx context.Context, url string) error {
	if h.connected {
		return common.NewStreamError(common.StreamTypeHLS, url,
			common.ErrCodeConnection, "already connected", nil)
	}

	h.url = url
	startTime := time.Now()

	// Parse the M3U8 playlist using the configured parser and extractor
	playlist, err := h.parsePlaylist(ctx, url)
	if err != nil {
		return common.NewStreamError(common.StreamTypeHLS, url,
			common.ErrCodeConnection, "failed to parse M3U8 playlist", err)
	}

	if !playlist.IsValid {
		return common.NewStreamError(common.StreamTypeHLS, url,
			common.ErrCodeInvalidFormat, "invalid M3U8 playlist format", nil)
	}

	// Store the parsed playlist
	h.playlist = playlist

	// Extract metadata using the configured extractor
	h.metadata = h.metadataExtractor.ExtractMetadata(playlist, url)

	h.stats.ConnectionTime = time.Since(startTime)
	h.stats.FirstByteTime = h.stats.ConnectionTime
	h.connected = true

	return nil
}

// parsePlaylist parses the M3U8 playlist with current configuration
func (h *Handler) parsePlaylist(ctx context.Context, url string) (*M3U8Playlist, error) {
	// Create context with configured timeout
	ctx, cancel := context.WithTimeout(ctx, h.config.HTTP.ConnectionTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, url,
			common.ErrCodeConnection, "failed to create request", err)

	}

	// Set headers from configuration
	headers := h.config.GetHTTPHeaders()
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, url,
			common.ErrCodeConnection, "failed to fetch playlist", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, common.NewStreamErrorWithFields(common.StreamTypeHLS, url,
			common.ErrCodeConnection, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, resp.Status), nil,
			logging.Fields{
				"status_code": resp.StatusCode,
				"status_text": resp.Status,
			})

	}

	// Store response headers
	headers = make(map[string]string)
	for key, values := range resp.Header {
		if len(values) > 0 {
			headers[strings.ToLower(key)] = values[0]
		}
	}

	var reader io.Reader = resp.Body
	if h.config.HTTP.BufferSize > 0 {
		reader = bufio.NewReaderSize(resp.Body, h.config.HTTP.BufferSize)
	}

	// Parse the M3U8 content
	playlist, err := h.parser.ParseM3U8Content(reader)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, url,
			common.ErrCodeInvalidFormat, "failed to parse M3U8", err)
	}

	// Merge HTTP headers with playlist headers
	if playlist.Headers == nil {
		playlist.Headers = make(map[string]string)
	}
	maps.Copy(playlist.Headers, headers)

	return playlist, nil
}

// GetMetadata retrieves stream metadata
func (h *Handler) GetMetadata() (*common.StreamMetadata, error) {
	if !h.connected {
		return nil, common.NewStreamError(common.StreamTypeHLS, h.url,
			common.ErrCodeConnection, "not connected", nil)
	}

	// Return the metadata extracted during connection
	if h.metadata == nil {
		// Fallback metadata if parsing failed, using configured defaults
		h.metadata = &common.StreamMetadata{
			URL:       h.url,
			Type:      common.StreamTypeHLS,
			Headers:   make(map[string]string),
			Timestamp: time.Now(),
		}

		// Apply default values from configuration
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
		}
	}

	return h.metadata, nil
}

// ResolveMasterPlaylist resolves a master playlist to a media playlist
func (h *Handler) ResolveMasterPlaylist(ctx context.Context, masterPlaylist *M3U8Playlist) (*M3U8Playlist, error) {
	if !masterPlaylist.IsMaster || len(masterPlaylist.Variants) == 0 {
		return masterPlaylist, nil // Already a media playlist or no variants
	}

	logger := logging.WithFields(logging.Fields{
		"component": "hls_handler",
		"function":  "ResolveMasterPlaylist",
		"variants":  len(masterPlaylist.Variants),
	})

	// Select the best variant based on configuration
	selectedVariant := h.selectBestVariant(masterPlaylist.Variants)
	if selectedVariant == nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, h.url,
			common.ErrCodeInvalidFormat, "no suitable variant found", nil)
	}

	logger.Debug("Selected variant", logging.Fields{
		"variant_uri": selectedVariant.URI,
		"bandwidth":   selectedVariant.Bandwidth,
		"resolution":  selectedVariant.Resolution,
		"codecs":      selectedVariant.Codecs,
	})

	// Resolve the variant URL
	variantURL := h.resolveURL(selectedVariant.URI)

	// Parse the variant playlist (should be a media playlist)
	mediaPlaylist, err := h.parsePlaylist(ctx, variantURL)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, variantURL,
			common.ErrCodeConnection, "failed to parse variant playlist", err)
	}

	if mediaPlaylist.IsMaster {
		return nil, common.NewStreamError(common.StreamTypeHLS, variantURL,
			common.ErrCodeInvalidFormat, "variant playlist is still a master playlist", nil)
	}

	logger.Debug("Master playlist resolved to media playlist", logging.Fields{
		"segments": len(mediaPlaylist.Segments),
		"is_live":  mediaPlaylist.IsLive,
	})

	return mediaPlaylist, nil
}

// ReadAudioWithDuration reads audio using FFmpeg directly (bypasses HLS parsing)
func (h *Handler) ReadAudioWithDuration(ctx context.Context, duration time.Duration) (*common.AudioData, error) {
	if !h.connected {
		return nil, common.NewStreamError(common.StreamTypeHLS, h.url,
			common.ErrCodeConnection, "not connected", nil)
	}

	logger := logging.WithFields(logging.Fields{
		"component":       "hls_handler",
		"function":        "ReadAudioWithDurationDirect",
		"target_duration": duration.Seconds(),
		"method":          "ffmpeg_direct",
	})

	// Initialize downloader if not exists
	if h.downloader == nil {
		downloadConfig := DefaultDownloadConfig()
		if h.config != nil && h.config.Audio != nil {
			downloadConfig.TargetDuration = duration
			downloadConfig.OutputSampleRate = 44100
			downloadConfig.OutputChannels = 1

			if h.config.Audio.MaxSegments > 0 {
				downloadConfig.MaxSegments = h.config.Audio.MaxSegments
			}
		}
		h.downloader = NewAudioDownloader(h.client, downloadConfig, h.config)
		h.downloader.SetBaseURL(h.url)
	}

	logger.Debug("Using direct FFmpeg method for HLS download")

	// Use the direct FFmpeg method
	var audioData *common.AudioData
	var err error
	audioData, err = h.downloader.DownloadAudioSampleDirect(ctx, h.url, duration)
	if err != nil {
		logger.Warn("Direct FFmpeg method failed, falling back to standard HLS parsing", logging.Fields{
			"error": err.Error(),
		})

		// Fallback to standard HLS parsing method
		return h.ReadAudioWithDuration(ctx, duration)
	}

	// Enrich metadata from playlist/stream info if available
	h.enrichAudioMetadata(audioData)

	// Update stats
	h.stats.BytesReceived = h.downloader.GetDownloadStats().BytesDownloaded
	h.stats.SegmentsReceived = h.downloader.GetDownloadStats().SegmentsDownloaded

	logger.Debug("Direct FFmpeg download completed successfully", logging.Fields{
		"actual_duration": audioData.Duration.Seconds(),
		"samples":         len(audioData.PCM),
		"sample_rate":     audioData.SampleRate,
		"channels":        audioData.Channels,
	})

	return audioData, nil
}

// selectBestVariant selects the best variant based on configuration
func (h *Handler) selectBestVariant(variants []M3U8Variant) *M3U8Variant {
	if len(variants) == 0 {
		return nil
	}

	// If only one variant, use it
	if len(variants) == 1 {
		return &variants[0]
	}

	// Get preferred bitrate from config
	preferredBitrate := 128000 // Default 128kbps
	if h.config != nil && h.downloader != nil && h.downloader.config != nil {
		preferredBitrate = h.downloader.config.PreferredBitrate * 1000 // Convert kbps to bps
	}

	var bestVariant *M3U8Variant
	var bestScore int

	for i := range variants {
		variant := &variants[i]
		score := h.calculateVariantScore(variant, preferredBitrate)

		if bestVariant == nil || score > bestScore {
			bestVariant = variant
			bestScore = score
		}
	}

	return bestVariant
}

// calculateVariantScore calculates a score for variant selection
func (h *Handler) calculateVariantScore(variant *M3U8Variant, preferredBitrate int) int {
	score := 0

	// Prefer variants closer to preferred bitrate
	bitrateDiff := variant.Bandwidth - preferredBitrate
	if bitrateDiff < 0 {
		bitrateDiff = -bitrateDiff
	}

	// Lower difference = higher score
	score += 1000000 - bitrateDiff

	// Prefer audio-only variants (no resolution)
	if variant.Resolution == "" {
		score += 10000
	}

	// Prefer AAC codec
	if strings.Contains(strings.ToLower(variant.Codecs), "mp4a") {
		score += 1000
	}

	return score
}

// ReadAudio reads the audio stream and returns the resulting `AudioData`
func (h *Handler) ReadAudio(ctx context.Context) (*common.AudioData, error) {
	if !h.connected {
		return nil, common.NewStreamError(common.StreamTypeHLS, h.url,
			common.ErrCodeConnection, "not connected", nil)
	}

	if h.playlist == nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, h.url,
			common.ErrCodeInvalidFormat, "no playlist available", nil)
	}

	// Resolve master playlist to media playlist if needed
	/* mediaPlaylist := h.playlist
	if h.playlist.IsMaster {
		resolvedPlaylist, err := h.ResolveMasterPlaylist(ctx, h.playlist)
		if err != nil {
			return nil, common.NewStreamError(common.StreamTypeHLS, h.url,
				common.ErrCodeInvalidFormat, "failed to resolve master playlist", err)
		}
		mediaPlaylist = resolvedPlaylist
	} */

	// Initialize downloader if not exists
	if h.downloader == nil {
		// TODO: pass download config
		h.downloader = NewAudioDownloader(h.client, DefaultDownloadConfig(), h.config)
		// Set the base URL for resolving relative segment URLs
		h.downloader.SetBaseURL(h.url)
	}

	// Download audio sample using configured duration
	targetDuration := h.config.Audio.SampleDuration

	audioData, err := h.downloader.DownloadAudioSampleDirect(ctx, h.url, targetDuration)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, h.url,
			common.ErrCodeDecoding, "failed to download audio sample", err)
	}

	// Enrich metadata from playlist/stream info
	h.enrichAudioMetadata(audioData)

	// Update stats
	h.stats.SegmentsReceived = h.downloader.GetDownloadStats().SegmentsDownloaded
	h.stats.BytesReceived = h.downloader.GetDownloadStats().BytesDownloaded

	return audioData, nil
}

// enrichAudioMetadata enriches audio data with metadata from the stream
func (h *Handler) enrichAudioMetadata(audioData *common.AudioData) {
	if h.metadata == nil {
		return
	}

	// Copy the existing stream metadata to audio data
	// Create a copy to avoid modifying the original
	metadataCopy := *h.metadata
	metadataCopy.Timestamp = time.Now()

	// Set the metadata reference
	audioData.Metadata = &metadataCopy
}

// GetStats returns current streaming statistics
func (h *Handler) GetStats() *common.StreamStats {
	return h.stats
}

// Close closes the stream connection
func (h *Handler) Close() error {
	h.connected = false
	return nil
}

// GetPlaylist returns the parsed playlist
func (h *Handler) GetPlaylist() *M3U8Playlist {
	return h.playlist
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

	// Recreate parser and metadata extractor with new config
	h.parser = NewConfigurableParser(config.Parser).Parser
	h.metadataExtractor = NewConfigurableMetadataExtractor(config.MetadataExtractor).MetadataExtractor
}

// RefreshPlaylist refreshes the playlist for live streams
func (h *Handler) RefreshPlaylist(ctx context.Context) error {
	if !h.connected {
		return common.NewStreamError(common.StreamTypeHLS, h.url,
			common.ErrCodeConnection, "not connected", nil)
	}

	// Only refresh for live streams
	if h.playlist != nil && !h.playlist.IsLive {
		return common.NewStreamError(common.StreamTypeHLS, h.url,
			common.ErrCodeUnsupported, "not a live stream", nil)
	}

	newPlaylist, err := h.parsePlaylist(ctx, h.url)
	if err != nil {
		return common.NewStreamError(common.StreamTypeHLS, h.url,
			common.ErrCodeConnection, "failed to refresh playlist", err)
	}

	// Update playlist and re-extract metadata
	h.playlist = newPlaylist
	h.metadata = h.metadataExtractor.ExtractMetadata(newPlaylist, h.url)

	return nil
}

// GetSegmentURLs returns URLs for all segments in the playlist
func (h *Handler) GetSegmentURLs() ([]string, error) {
	if h.playlist == nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, h.url,
			common.ErrCodeInvalidFormat, "no playlist available", nil)
	}

	maxSegments := h.config.Audio.MaxSegments
	segments := h.playlist.Segments

	// Limit segments if configured
	if maxSegments > 0 && len(segments) > maxSegments {
		segments = segments[:maxSegments]
	}

	urls := make([]string, 0, len(segments))
	for _, segment := range segments {
		// Resolve relative URLs if needed
		segmentURL := h.resolveURL(segment.URI)
		urls = append(urls, segmentURL)
	}

	return urls, nil
}

// GetVariantURLs returns URLs for all variants in the playlist
func (h *Handler) GetVariantURLs() ([]string, error) {
	if h.playlist == nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, h.url,
			common.ErrCodeInvalidFormat, "no playlist available", nil)
	}

	urls := make([]string, 0, len(h.playlist.Variants))
	for _, variant := range h.playlist.Variants {
		// Resolve relative URLs if needed
		variantURL := h.resolveURL(variant.URI)
		urls = append(urls, variantURL)
	}

	return urls, nil
}

// resolveURL resolves relative URLs against the base playlist URL
func (h *Handler) resolveURL(uri string) string {
	// If it's already an absolute URL, return as-is
	if strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://") {
		return uri
	}

	// Parse base URL
	baseURL, err := url.Parse(h.url)
	if err != nil {
		return uri // Return original if can't parse
	}

	// Parse relative URI
	relativeURL, err := url.Parse(uri)
	if err != nil {
		return uri // Return original if can't parse
	}

	// Resolve relative to base
	resolvedURL := baseURL.ResolveReference(relativeURL)
	return resolvedURL.String()
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

// ShouldFollowLive returns true if live playlist following is enabled
func (h *Handler) ShouldFollowLive() bool {
	return h.config != nil && h.config.Audio != nil && h.config.Audio.FollowLive
}

// ShouldAnalyzeSegments returns true if segment analysis is enabled
func (h *Handler) ShouldAnalyzeSegments() bool {
	return h.config != nil && h.config.Audio != nil && h.config.Audio.AnalyzeSegments
}
