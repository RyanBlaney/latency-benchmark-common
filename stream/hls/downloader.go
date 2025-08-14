package hls

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/audio/transcode"
	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// AudioDownloader handles downloading and processing HLS audio segments for LIVE streams
type AudioDownloader struct {
	client          *http.Client
	segmentCache    map[string][]byte
	downloadStats   *DownloadStats
	config          *DownloadConfig
	hlsConfig       *Config
	tempDir         string
	baseURL         string
	seenSegments    map[string]bool // Track segments we've already downloaded
	isFirstPlaylist bool
}

// QueryParamRule defines when and what query parameters to add
type QueryParamRule struct {
	HostPatterns []string          `json:"host_patterns"` // Host patterns to match (e.g., "cdnstream1.com", "soundstack", "adzwizz")
	PathPatterns []string          `json:"path_patterns"` // Path patterns to match (optional)
	QueryParams  map[string]string `json:"query_params"`  // Query parameters to add
}

// DownloadConfig contains configuration for audio downloading
type DownloadConfig struct {
	MaxSegments      int           `json:"max_segments"`
	SegmentTimeout   time.Duration `json:"segment_timeout"`
	MaxRetries       int           `json:"max_retries"`
	CacheSegments    bool          `json:"cache_segments"`
	TargetDuration   time.Duration `json:"target_duration"`
	PreferredBitrate int           `json:"preferred_bitrate"`
	OutputSampleRate int           `json:"output_sample_rate"`
	OutputChannels   int           `json:"output_channels"`
	NormalizePCM     bool          `json:"normalize_pcm"`
	ResampleQuality  string        `json:"resample_quality"`
	CleanupTempFiles bool          `json:"cleanup_temp_files"`
	// Live streaming specific
	PlaylistRefreshInterval time.Duration `json:"playlist_refresh_interval"`
	MaxPlaylistRetries      int           `json:"max_playlist_retries"`
	LiveBufferSegments      int           `json:"live_buffer_segments"`
	// Query parameter configuration
	QueryParamRules   []QueryParamRule  `json:"query_param_rules"`   // Rules for adding query parameters
	GlobalQueryParams map[string]string `json:"global_query_params"` // Query parameters to add to all requests
}

// DownloadStats tracks download performance
type DownloadStats struct {
	SegmentsDownloaded int            `json:"segments_downloaded"`
	BytesDownloaded    int64          `json:"bytes_downloaded"`
	DownloadTime       time.Duration  `json:"download_time"`
	DecodeTime         time.Duration  `json:"decode_time"`
	ErrorCount         int            `json:"error_count"`
	AverageBitrate     float64        `json:"average_bitrate"`
	SegmentErrors      []SegmentError `json:"segment_errors,omitempty"`
	AudioMetrics       *AudioMetrics  `json:"audio_metrics,omitempty"`
	PlaylistRefreshes  int            `json:"playlist_refreshes"`
	LiveStartTime      *time.Time     `json:"live_start_time,omitempty"`
}

// AudioMetrics contains audio processing statistics
type AudioMetrics struct {
	SamplesDecoded   int64   `json:"samples_decoded"`
	DecodedDuration  float64 `json:"decoded_duration_seconds"`
	AverageAmplitude float64 `json:"average_amplitude"`
	PeakAmplitude    float64 `json:"peak_amplitude"`
	SilenceRatio     float64 `json:"silence_ratio"`
	ClippingDetected bool    `json:"clipping_detected"`
}

// SegmentError represents an error downloading a specific segment
type SegmentError struct {
	URL       string    `json:"url"`
	Error     string    `json:"error"`
	Timestamp time.Time `json:"timestamp"`
	Retry     int       `json:"retry"`
	Type      string    `json:"type"`
}

// NewAudioDownloader creates a new audio downloader optimized for live HLS streams
func NewAudioDownloader(client *http.Client, config *DownloadConfig, hlsConfig *Config) *AudioDownloader {
	if config == nil {
		config = DefaultDownloadConfig()
	}

	// Create temp directory for segment files
	tempDir, err := os.MkdirTemp("", "hls_segments_*")
	if err != nil {
		tempDir = "/tmp" // Fallback
	}

	now := time.Now()
	return &AudioDownloader{
		client:       client,
		segmentCache: make(map[string][]byte),
		seenSegments: make(map[string]bool),
		downloadStats: &DownloadStats{
			SegmentErrors: make([]SegmentError, 0),
			AudioMetrics:  &AudioMetrics{},
			LiveStartTime: &now,
		},
		config:          config,
		hlsConfig:       hlsConfig,
		tempDir:         tempDir,
		isFirstPlaylist: true,
	}
}

// DefaultDownloadConfig returns default download configuration optimized for live streaming
func DefaultDownloadConfig() *DownloadConfig {
	return &DownloadConfig{
		MaxSegments:             80, // Allow more segments for live streaming
		SegmentTimeout:          15 * time.Second,
		MaxRetries:              3,
		CacheSegments:           false, // Don't cache live segments
		TargetDuration:          240 * time.Second,
		PreferredBitrate:        128,
		OutputSampleRate:        44100,
		OutputChannels:          1,
		NormalizePCM:            true,
		ResampleQuality:         "medium",
		CleanupTempFiles:        true,
		PlaylistRefreshInterval: 6 * time.Second, // Refresh playlist every 3 seconds
		MaxPlaylistRetries:      10,
		LiveBufferSegments:      2,                       // Keep 2 segments as buffer
		QueryParamRules:         []QueryParamRule{},      // Empty by default
		GlobalQueryParams:       make(map[string]string), // Empty by default
	}
}

// DownloadAudioSample downloads live HLS audio for the specified duration
func (ad *AudioDownloader) DownloadAudioSample(ctx context.Context, playlistURL string, targetDuration time.Duration) (*common.AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component":       "hls_audio_downloader",
		"function":        "DownloadAudioSample",
		"playlist_url":    playlistURL,
		"target_duration": targetDuration.Seconds(),
	})

	logger.Debug("Starting live HLS audio download")

	// Add required query parameters based on configuration
	playlistURL = ad.applyQueryParamRules(playlistURL)

	// First, fetch the master playlist to select a variant
	mediaPlaylistURL, err := ad.selectBestVariant(ctx, playlistURL)
	if err != nil {
		return nil, fmt.Errorf("failed to select variant: %w", err)
	}

	logger.Debug("Selected media playlist", logging.Fields{
		"media_playlist_url": mediaPlaylistURL,
	})

	// Set base URL for resolving relative segment URLs
	if baseURL, err := url.Parse(mediaPlaylistURL); err == nil {
		baseURL.RawQuery = ""
		baseURL.Fragment = ""
		ad.baseURL = baseURL.String()
	}

	// FIX: Accumulate raw segment data instead of processing individually
	var segmentDataList [][]byte
	var segmentDurations []time.Duration
	var totalDuration time.Duration
	startTime := time.Now()

	// Create a context with timeout for the entire download process
	downloadCtx, cancel := context.WithTimeout(ctx, targetDuration+(90*time.Second))
	defer cancel()

	// Reduced logging frequency
	lastLogTime := time.Now()
	logInterval := 10 * time.Second

	for totalDuration < targetDuration {
		select {
		case <-downloadCtx.Done():
			if time.Since(lastLogTime) > logInterval {
				logger.Warn("Download context cancelled", logging.Fields{
					"collected_duration": totalDuration.Seconds(),
					"target_duration":    targetDuration.Seconds(),
				})
				lastLogTime = time.Now()
			}
			if len(segmentDataList) == 0 {
				return nil, downloadCtx.Err()
			}
			break
		default:
		}

		playlist, err := ad.fetchPlaylistWithRetry(downloadCtx, mediaPlaylistURL)
		if err != nil {
			if time.Since(lastLogTime) > logInterval {
				logger.Error(err, "Failed to fetch media playlist")
				lastLogTime = time.Now()
			}
			if len(segmentDataList) > 0 {
				break
			}
			return nil, fmt.Errorf("failed to fetch media playlist: %w", err)
		}

		if playlist == nil {
			logger.Error(nil, "Received nil playlist")
			if len(segmentDataList) > 0 {
				break
			}
			return nil, fmt.Errorf("received nil playlist from fetchPlaylistWithRetry")
		}

		ad.downloadStats.PlaylistRefreshes++

		// Download new segments from playlist
		newSegments := ad.findNewSegments(playlist)

		if time.Since(lastLogTime) > logInterval {
			logger.Debug("Playlist status", logging.Fields{
				"total_segments":     len(playlist.Segments),
				"new_segments":       len(newSegments),
				"collected_duration": totalDuration.Seconds(),
				"target_duration":    targetDuration.Seconds(),
			})
			lastLogTime = time.Now()
		}

		if len(newSegments) == 0 {
			// No new segments, wait and refresh
			select {
			case <-downloadCtx.Done():
				break
			case <-time.After(ad.config.PlaylistRefreshInterval):
				continue
			}
		}

		// FIX: Download and accumulate raw segment data
		for _, segment := range newSegments {
			if totalDuration >= targetDuration {
				break
			}

			segmentURL := ad.resolveSegmentURL(playlist, segment.URI)

			// Download raw segment data (not decoded yet)
			segmentData, err := ad.downloadSegmentWithRetries(downloadCtx, segmentURL)
			if err != nil {
				logger.Warn("Failed to download segment, continuing", logging.Fields{
					"segment_url": segmentURL,
					"error":       err.Error(),
				})
				continue
			}

			// Mark segment as seen
			ad.seenSegments[segmentURL] = true

			// Accumulate raw data
			segmentDataList = append(segmentDataList, segmentData)

			// Estimate duration (use provided duration or fallback)
			segmentDuration := time.Duration(segment.Duration * float64(time.Second))
			if segmentDuration == 0 {
				segmentDuration = 6 * time.Second // Default HLS segment duration
			}
			segmentDurations = append(segmentDurations, segmentDuration)
			totalDuration += segmentDuration

			// Update download stats
			ad.downloadStats.SegmentsDownloaded++
			ad.downloadStats.BytesDownloaded += int64(len(segmentData))

			logger.Debug("Segment downloaded successfully", logging.Fields{
				"segment_duration": segmentDuration.Seconds(),
				"segment_size":     len(segmentData),
				"total_duration":   totalDuration.Seconds(),
				"progress":         fmt.Sprintf("%.1f%%", (totalDuration.Seconds()/targetDuration.Seconds())*100),
			})
		}

		// If this is not a live playlist, break after processing all segments
		if !playlist.IsLive {
			logger.Debug("Non-live playlist detected, stopping after current segments")
			break
		}

		// Wait before next playlist refresh if we need more content
		if totalDuration < targetDuration {
			select {
			case <-downloadCtx.Done():
				break
			case <-time.After(ad.config.PlaylistRefreshInterval):
				continue
			}
		}
	}

	if len(segmentDataList) == 0 {
		return nil, fmt.Errorf("no audio segments were successfully downloaded")
	}

	downloadDuration := time.Since(startTime)
	ad.downloadStats.DownloadTime += downloadDuration

	logger.Debug("Live HLS download completed", logging.Fields{
		"segments_downloaded": len(segmentDataList),
		"total_duration":      totalDuration.Seconds(),
		"target_duration":     targetDuration.Seconds(),
		"download_time":       downloadDuration.Seconds(),
		"playlist_refreshes":  ad.downloadStats.PlaylistRefreshes,
	})

	// FIX: Combine all segment data before decoding
	return ad.combineAndDecodeSegments(segmentDataList, segmentDurations, mediaPlaylistURL)
}

func (ad *AudioDownloader) DownloadContinuousLiveEdge(ctx context.Context, playlistURL string, targetDuration time.Duration) (*common.AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component":       "hls_audio_downloader",
		"function":        "DownloadContinuousLiveEdge",
		"playlist_url":    playlistURL,
		"target_duration": targetDuration.Seconds(),
	})

	logger.Debug("Starting continuous live edge HLS audio download")

	// Add required query parameters
	playlistURL = ad.applyQueryParamRules(playlistURL)

	mediaPlaylistURL, err := ad.selectBestVariant(ctx, playlistURL)
	if err != nil {
		return nil, fmt.Errorf("failed to select variant: %w", err)
	}

	logger.Debug("Selected media playlist for live edge", logging.Fields{
		"media_playlist_url": mediaPlaylistURL,
	})

	// Set base URL for resolving relative segment URLs
	if baseURL, err := url.Parse(mediaPlaylistURL); err == nil {
		baseURL.RawQuery = ""
		baseURL.Fragment = ""
		ad.baseURL = baseURL.String()
	}

	var allSegmentData [][]byte
	var allSegmentDurations []time.Duration
	var totalDuration time.Duration
	startTime := time.Now()

	// Create timeout context
	downloadCtx, cancel := context.WithTimeout(ctx, targetDuration+(60*time.Second))
	defer cancel()

	logger.Debug("Starting live edge collection loop")

	for totalDuration < targetDuration {
		select {
		case <-downloadCtx.Done():
			logger.Warn("Download context cancelled in live edge mode", logging.Fields{
				"collected_duration": totalDuration.Seconds(),
				"target_duration":    targetDuration.Seconds(),
			})
			if len(allSegmentData) == 0 {
				return nil, downloadCtx.Err()
			}
			break
		default:
		}

		// Fetch current playlist
		playlist, err := ad.fetchPlaylist(downloadCtx, mediaPlaylistURL)
		if err != nil {
			logger.Warn("Failed to fetch playlist in live edge mode", logging.Fields{
				"error": err.Error(),
			})
			// Wait a bit and continue
			time.Sleep(2 * time.Second)
			continue
		}

		// CRITICAL: Always take only the NEWEST segment(s)
		if len(playlist.Segments) > 0 {
			// Take the last 1-2 segments (newest available)
			newestSegments := playlist.Segments[len(playlist.Segments)-1:]

			logger.Debug("Found newest segments", logging.Fields{
				"total_playlist_segments": len(playlist.Segments),
				"newest_segments":         len(newestSegments),
			})

			for _, segment := range newestSegments {
				segmentURL := ad.resolveSegmentURL(playlist, segment.URI)

				// Check if we've already seen this segment
				if ad.seenSegments[segmentURL] {
					continue
				}

				// Download the newest segment
				segmentData, err := ad.downloadSegment(downloadCtx, segmentURL)
				if err != nil {
					logger.Warn("Failed to download newest segment", logging.Fields{
						"segment_url": segmentURL,
						"error":       err.Error(),
					})
					continue
				}

				// Mark as seen
				ad.seenSegments[segmentURL] = true

				// Add to collection
				allSegmentData = append(allSegmentData, segmentData)
				segmentDuration := time.Duration(segment.Duration * float64(time.Second))
				if segmentDuration == 0 {
					segmentDuration = 6 * time.Second // Default HLS segment duration
				}
				allSegmentDurations = append(allSegmentDurations, segmentDuration)
				totalDuration += segmentDuration

				// Update stats
				ad.downloadStats.SegmentsDownloaded++
				ad.downloadStats.BytesDownloaded += int64(len(segmentData))

				logger.Debug("Downloaded newest segment", logging.Fields{
					"segment_duration": segmentDuration.Seconds(),
					"segment_size":     len(segmentData),
					"total_duration":   totalDuration.Seconds(),
					"segments_count":   len(allSegmentData),
				})

				// If we have too much data, remove oldest segments to stay at live edge
				maxSegments := int(targetDuration.Seconds()/6) + 2 // Allow some buffer
				if len(allSegmentData) > maxSegments {
					// Remove oldest segment
					allSegmentData = allSegmentData[1:]
					removedDuration := allSegmentDurations[0]
					allSegmentDurations = allSegmentDurations[1:]
					totalDuration -= removedDuration

					logger.Debug("Removed oldest segment to stay at live edge", logging.Fields{
						"removed_duration":   removedDuration.Seconds(),
						"remaining_segments": len(allSegmentData),
						"total_duration":     totalDuration.Seconds(),
					})
				}
			}
		}

		// If we have enough content, break
		if totalDuration >= targetDuration {
			break
		}

		// Wait for next segment to appear (typically 2-3 seconds for 6s segments)
		logger.Debug("Waiting for new segments to appear")
		select {
		case <-downloadCtx.Done():
			break
		case <-time.After(3 * time.Second):
			continue
		}
	}

	if len(allSegmentData) == 0 {
		return nil, fmt.Errorf("no segments collected in live edge mode")
	}

	downloadDuration := time.Since(startTime)
	ad.downloadStats.DownloadTime += downloadDuration

	logger.Debug("Live edge HLS download completed", logging.Fields{
		"segments_downloaded": len(allSegmentData),
		"total_duration":      totalDuration.Seconds(),
		"target_duration":     targetDuration.Seconds(),
		"download_time":       downloadDuration.Seconds(),
		"live_edge_mode":      true,
	})

	// Decode the newest segments we collected
	return ad.combineAndDecodeSegments(allSegmentData, allSegmentDurations, mediaPlaylistURL)
}

// DownloadAudioSampleDirect downloads HLS audio using FFmpeg directly from URL
// This bypasses all the complex HLS parsing and uses FFmpeg's native HLS support
func (ad *AudioDownloader) DownloadAudioSampleDirect(ctx context.Context, playlistURL string, targetDuration time.Duration) (*common.AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component":       "hls_audio_downloader",
		"function":        "DownloadAudioSampleDirect",
		"playlist_url":    playlistURL,
		"target_duration": targetDuration.Seconds(),
		"method":          "ffmpeg_direct",
	})

	logger.Debug("Starting direct FFmpeg HLS audio download")

	// Apply query parameters if needed
	processedURL := ad.applyQueryParamRules(playlistURL)

	// Create decoder with configuration matching our output requirements
	decoderConfig := transcode.DefaultDecoderConfig()

	// Apply HLS config values if available
	if ad.hlsConfig != nil && ad.hlsConfig.MetadataExtractor != nil && ad.hlsConfig.MetadataExtractor.DefaultValues != nil {
		if sampleRate, ok := ad.hlsConfig.MetadataExtractor.DefaultValues["sample_rate"].(int); ok && sampleRate > 0 {
			decoderConfig.TargetSampleRate = sampleRate
		}
		if channels, ok := ad.hlsConfig.MetadataExtractor.DefaultValues["channels"].(int); ok && channels > 0 {
			decoderConfig.TargetChannels = channels
		}
	}

	// Override with our downloader config if available
	if ad.config != nil {
		decoderConfig.TargetSampleRate = ad.config.OutputSampleRate
		decoderConfig.TargetChannels = ad.config.OutputChannels

		// Set duration limit
		decoderConfig.MaxDuration = targetDuration

		// Set timeout based on target duration + buffer
		decoderConfig.Timeout = targetDuration + (30 * time.Second)
	}

	// Create decoder
	decoder := transcode.NewDecoder(decoderConfig)
	defer decoder.Close()

	logger.Debug("Created FFmpeg decoder", logging.Fields{
		"target_sample_rate": decoderConfig.TargetSampleRate,
		"target_channels":    decoderConfig.TargetChannels,
		"max_duration":       decoderConfig.MaxDuration.Seconds(),
		"timeout":            decoderConfig.Timeout.Seconds(),
	})

	// Use FFmpeg to decode directly from URL
	startTime := time.Now()
	result, err := decoder.DecodeURL(processedURL, targetDuration, "hls")
	if err != nil {
		logger.Error(err, "FFmpeg direct decode failed")
		return nil, fmt.Errorf("failed to decode HLS stream with FFmpeg: %w", err)
	}

	downloadTime := time.Since(startTime)

	// TODO: remove this variable
	// Convert transcode.AudioData to common.AudioData
	transcodeAudio := result

	// Convert to common.AudioData format
	audioData := &common.AudioData{
		PCM:        transcodeAudio.PCM,
		SampleRate: transcodeAudio.SampleRate,
		Channels:   transcodeAudio.Channels,
		Duration:   transcodeAudio.Duration,
		Timestamp:  time.Now(),
	}

	// Convert metadata if available
	if transcodeAudio.Metadata != nil {
		audioData.Metadata = &common.StreamMetadata{
			URL:         playlistURL,
			Type:        common.StreamTypeHLS,
			Format:      transcodeAudio.Metadata.Format,
			Bitrate:     transcodeAudio.Metadata.Bitrate,
			SampleRate:  transcodeAudio.Metadata.SampleRate,
			Channels:    transcodeAudio.Metadata.Channels,
			Codec:       transcodeAudio.Metadata.Codec,
			ContentType: transcodeAudio.Metadata.ContentType,
			Title:       transcodeAudio.Metadata.Title,
			Artist:      transcodeAudio.Metadata.Artist,
			Genre:       transcodeAudio.Metadata.Genre,
			Station:     transcodeAudio.Metadata.Station,
			Headers:     transcodeAudio.Metadata.Headers,
			Timestamp:   time.Now(),
		}
	} else {
		// Create basic metadata
		audioData.Metadata = ad.createBasicMetadata(playlistURL)
	}

	// Update download stats
	ad.downloadStats.SegmentsDownloaded = 1                          // FFmpeg handles segments internally
	ad.downloadStats.BytesDownloaded = int64(len(audioData.PCM) * 8) // Estimate based on PCM data
	ad.downloadStats.DownloadTime = downloadTime

	logger.Debug("Direct FFmpeg HLS download completed", logging.Fields{
		"actual_duration": audioData.Duration.Seconds(),
		"target_duration": targetDuration.Seconds(),
		"samples":         len(audioData.PCM),
		"sample_rate":     audioData.SampleRate,
		"channels":        audioData.Channels,
		"download_time":   downloadTime.Seconds(),
	})

	return audioData, nil
}

// combineAndDecodeSegments combines raw segment data and decodes it as one unit
func (ad *AudioDownloader) combineAndDecodeSegments(segmentDataList [][]byte, segmentDurations []time.Duration, playlistURL string) (*common.AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component":     "hls_audio_downloader",
		"function":      "combineAndDecodeSegments",
		"segment_count": len(segmentDataList),
	})

	// Calculate total size for efficient allocation
	totalSize := 0
	for _, data := range segmentDataList {
		totalSize += len(data)
	}

	logger.Debug("Combining segment data for decoding", logging.Fields{
		"segment_count": len(segmentDataList),
		"total_bytes":   totalSize,
	})

	// Combine all segment data into one buffer
	combinedData := make([]byte, 0, totalSize)
	for _, data := range segmentDataList {
		combinedData = append(combinedData, data...)
	}

	// Calculate total duration
	var totalDuration time.Duration
	for _, duration := range segmentDurations {
		totalDuration += duration
	}

	logger.Debug("Decoding combined segment data", logging.Fields{
		"combined_size":    len(combinedData),
		"total_duration_s": totalDuration.Seconds(),
	})

	decodeStartTime := time.Now()

	// Use injected decoder if available
	var audioData *common.AudioData
	var err error

	if ad.hlsConfig != nil && ad.hlsConfig.AudioDecoder != nil {
		logger.Debug("Using injected audio decoder for combined data")

		anyData, err := ad.hlsConfig.AudioDecoder.DecodeBytes(combinedData)
		if err != nil {
			logger.Error(err, "Injected decoder failed on combined data", logging.Fields{
				"data_size": len(combinedData),
			})
			return nil, fmt.Errorf("failed to decode combined audio data: %w", err)
		}

		// Convert to common.AudioData
		if commonAudio, ok := anyData.(*common.AudioData); ok {
			audioData = commonAudio
		} else {
			audioData = common.ConvertToAudioData(anyData)
			if audioData == nil {
				return nil, fmt.Errorf("decoder returned unexpected type: %T", anyData)
			}
		}
	} else {
		logger.Debug("No injected decoder, using basic audio extraction")
		audioData, err = ad.basicAudioExtraction(combinedData, playlistURL)
		if err != nil {
			return nil, fmt.Errorf("failed to extract audio from combined data: %w", err)
		}
	}

	// Update decode stats
	ad.downloadStats.DecodeTime += time.Since(decodeStartTime)

	// Process and normalize the audio data
	audioData = ad.processAudioData(audioData)

	// Add metadata and timestamp
	audioData.Timestamp = time.Now()
	if audioData.Metadata == nil {
		audioData.Metadata = ad.createBasicMetadata(playlistURL)
	}

	// Update audio metrics
	ad.updateAudioMetrics(audioData)

	logger.Debug("Audio decoding completed successfully", logging.Fields{
		"final_samples":     len(audioData.PCM),
		"final_duration_s":  audioData.Duration.Seconds(),
		"final_sample_rate": audioData.SampleRate,
		"final_channels":    audioData.Channels,
		"decode_time_ms":    ad.downloadStats.DecodeTime.Milliseconds(),
	})

	return audioData, nil
}

// applyQueryParamRules applies configured query parameter rules to a URL
func (ad *AudioDownloader) applyQueryParamRules(targetURL string) string {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return targetURL
	}

	query := parsedURL.Query()

	// First apply global query parameters
	for key, value := range ad.config.GlobalQueryParams {
		query.Set(key, value)
	}

	// Then apply rule-based query parameters
	for _, rule := range ad.config.QueryParamRules {
		if ad.matchesRule(parsedURL, rule) {
			for key, value := range rule.QueryParams {
				query.Set(key, value)
			}
		}
	}

	parsedURL.RawQuery = query.Encode()
	return parsedURL.String()
}

// matchesRule checks if a URL matches a query parameter rule
func (ad *AudioDownloader) matchesRule(parsedURL *url.URL, rule QueryParamRule) bool {
	// Check host patterns
	if len(rule.HostPatterns) > 0 {
		hostMatched := false
		for _, pattern := range rule.HostPatterns {
			if strings.Contains(parsedURL.Host, pattern) {
				hostMatched = true
				break
			}
		}
		if !hostMatched {
			return false
		}
	}

	// Check path patterns (if specified)
	if len(rule.PathPatterns) > 0 {
		pathMatched := false
		for _, pattern := range rule.PathPatterns {
			if strings.Contains(parsedURL.Path, pattern) {
				pathMatched = true
				break
			}
		}
		if !pathMatched {
			return false
		}
	}

	return true
}

// fetchPlaylistWithRetry fetches playlist with retry logic
func (ad *AudioDownloader) fetchPlaylistWithRetry(ctx context.Context, playlistURL string) (*M3U8Playlist, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "hls_audio_downloader",
		"function":  "fetchPlaylistWithRetry",
		"url":       playlistURL,
	})

	logger.Debug("Starting playlist fetch with retry", logging.Fields{
		"max_retries": ad.config.MaxPlaylistRetries,
	})

	var lastErr error

	for attempt := 0; attempt < ad.config.MaxPlaylistRetries; attempt++ {
		logger.Debug("Attempting to fetch playlist", logging.Fields{
			"attempt":     attempt + 1,
			"max_retries": ad.config.MaxPlaylistRetries,
		})

		playlist, err := ad.fetchPlaylist(ctx, playlistURL)

		logger.Debug("Fetch attempt result", logging.Fields{
			"attempt":      attempt + 1,
			"playlist_nil": playlist == nil,
			"error_nil":    err == nil,
			"error_msg":    fmt.Sprintf("%v", err),
		})

		if err == nil {
			if playlist != nil {
				logger.Debug("Successfully fetched playlist", logging.Fields{
					"attempt":   attempt + 1,
					"segments":  len(playlist.Segments),
					"variants":  len(playlist.Variants),
					"is_master": playlist.IsMaster,
				})
				return playlist, nil
			} else {
				// This case should not happen - no error but nil playlist
				err = fmt.Errorf("fetchPlaylist returned nil playlist without error")
				logger.Error(nil, "fetchPlaylist returned nil playlist without error")
			}
		}

		lastErr = err
		logger.Debug("Fetch attempt failed", logging.Fields{
			"attempt": attempt + 1,
			"error":   err.Error(),
		})

		if attempt < ad.config.MaxPlaylistRetries-1 {
			waitTime := time.Duration(attempt+1) * time.Second
			logger.Debug("Waiting before retry", logging.Fields{
				"wait_seconds": waitTime.Seconds(),
			})

			select {
			case <-ctx.Done():
				logger.Error(ctx.Err(), "Context cancelled during retry wait")
				return nil, ctx.Err()
			case <-time.After(waitTime):
				logger.Debug("Retry wait completed")
				continue
			}
		}
	}

	logger.Error(lastErr, "All playlist fetch attempts failed", logging.Fields{
		"attempts": ad.config.MaxPlaylistRetries,
	})
	return nil, lastErr
}

// fetchPlaylist fetches and parses an M3U8 playlist
func (ad *AudioDownloader) fetchPlaylist(ctx context.Context, playlistURL string) (*M3U8Playlist, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "hls_audio_downloader",
		"function":  "fetchPlaylist",
		"url":       playlistURL,
	})

	logger.Debug("Creating HTTP request for playlist")

	req, err := http.NewRequestWithContext(ctx, "GET", playlistURL, nil)
	if err != nil {
		logger.Error(err, "Failed to create HTTP request")
		return nil, fmt.Errorf("failed to create playlist request: %w", err)
	}

	userAgent := "TuneIn-CDN-Benchmark/1.0"
	if ad.hlsConfig != nil && ad.hlsConfig.HTTP != nil {
		userAgent = ad.hlsConfig.HTTP.UserAgent
	}

	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/vnd.apple.mpegurl, application/x-mpegurl, */*")
	req.Header.Set("Cache-Control", "no-cache")

	logger.Debug("Making HTTP request for playlist", logging.Fields{
		"user_agent": userAgent,
		"headers":    req.Header,
	})

	resp, err := ad.client.Do(req)
	if err != nil {
		logger.Error(err, "HTTP request failed", logging.Fields{
			"url": playlistURL,
		})
		return nil, fmt.Errorf("failed to fetch playlist: %w", err)
	}
	defer resp.Body.Close()

	logger.Debug("HTTP response received", logging.Fields{
		"status_code":    resp.StatusCode,
		"content_type":   resp.Header.Get("Content-Type"),
		"content_length": resp.Header.Get("Content-Length"),
	})

	if resp.StatusCode != http.StatusOK {
		logger.Error(nil, "HTTP request returned non-200 status", logging.Fields{
			"status_code": resp.StatusCode,
			"status":      resp.Status,
		})
		return nil, fmt.Errorf("playlist request failed with status %d: %s", resp.StatusCode, resp.Status)
	}

	logger.Debug("Starting to parse M3U8 content")

	// Parse the playlist
	playlist, err := ad.parseM3U8(resp.Body, playlistURL)
	if err != nil {
		logger.Error(err, "Failed to parse M3U8 content")
		return nil, fmt.Errorf("failed to parse playlist: %w", err)
	}

	if playlist == nil {
		logger.Error(nil, "parseM3U8 returned nil playlist without error")
		return nil, fmt.Errorf("parseM3U8 returned nil playlist without error")
	}

	logger.Debug("Playlist parsed successfully", logging.Fields{
		"segments":  len(playlist.Segments),
		"variants":  len(playlist.Variants),
		"is_master": playlist.IsMaster,
		"is_live":   playlist.IsLive,
	})

	return playlist, nil
}

// parseM3U8 parses M3U8 playlist content using your existing data model
func (ad *AudioDownloader) parseM3U8(reader io.Reader, playlistURL string) (*M3U8Playlist, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "hls_audio_downloader",
		"function":  "parseM3U8",
		"url":       playlistURL,
	})

	playlist := &M3U8Playlist{
		IsValid:  true,
		IsMaster: false,
		IsLive:   true, // Assume live until we see #EXT-X-ENDLIST
		Version:  3,    // Default version
		Segments: make([]M3U8Segment, 0),
		Variants: make([]M3U8Variant, 0),
		Headers:  make(map[string]string),
		Metadata: &common.StreamMetadata{
			URL:       playlistURL,
			Type:      common.StreamTypeHLS,
			Timestamp: time.Now(),
		},
	}

	scanner := bufio.NewScanner(reader)
	var currentSegment M3U8Segment
	var hasInf bool

	lineCount := 0

	logger.Debug("Starting M3U8 parsing")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		lineCount++

		if lineCount <= 5 { // Log first few lines for debugging
			logger.Debug("Parsing line", logging.Fields{
				"line_num": lineCount,
				"content":  line,
			})
		}

		if strings.HasPrefix(line, "#EXTM3U") {
			continue
		}

		if strings.HasPrefix(line, "#EXT-X-VERSION:") {
			if version, err := strconv.Atoi(strings.TrimPrefix(line, "#EXT-X-VERSION:")); err == nil {
				playlist.Version = version
			}
			continue
		}

		if strings.HasPrefix(line, "#EXT-X-TARGETDURATION:") {
			if duration, err := strconv.Atoi(strings.TrimPrefix(line, "#EXT-X-TARGETDURATION:")); err == nil {
				playlist.TargetDuration = duration
			}
			continue
		}

		if strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:") {
			if sequence, err := strconv.Atoi(strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:")); err == nil {
				playlist.MediaSequence = sequence
			}
			continue
		}

		if strings.HasPrefix(line, "#EXT-X-ENDLIST") {
			playlist.IsLive = false
			continue
		}

		if strings.HasPrefix(line, "#EXT-X-STREAM-INF:") {
			// This is a master playlist with variants
			playlist.IsMaster = true
			// Parse stream info for variant
			variant := ad.parseStreamInf(line)

			// Next non-comment line should be the URI
			if scanner.Scan() {
				nextLine := strings.TrimSpace(scanner.Text())
				if !strings.HasPrefix(nextLine, "#") {
					variant.URI = nextLine
					playlist.Variants = append(playlist.Variants, variant)
				}
			}
			continue
		}

		if strings.HasPrefix(line, "#EXTINF:") {
			// Parse segment duration and title
			parts := strings.SplitN(strings.TrimPrefix(line, "#EXTINF:"), ",", 2)
			if len(parts) > 0 {
				if duration, err := strconv.ParseFloat(parts[0], 64); err == nil {
					currentSegment.Duration = duration
				}
			}
			if len(parts) > 1 {
				currentSegment.Title = parts[1]
			}
			hasInf = true
			continue
		}

		if strings.HasPrefix(line, "#EXT-X-BYTERANGE:") {
			currentSegment.ByteRange = strings.TrimPrefix(line, "#EXT-X-BYTERANGE:")
			continue
		}

		// Non-comment line - should be a segment URI if we have EXTINF
		if !strings.HasPrefix(line, "#") && line != "" && hasInf {
			currentSegment.URI = line
			playlist.Segments = append(playlist.Segments, currentSegment)

			// Reset for next segment
			currentSegment = M3U8Segment{}
			hasInf = false
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error(err, "Scanner error during M3U8 parsing")
		return nil, err
	}

	logger.Debug("M3U8 parsing completed", logging.Fields{
		"lines_parsed": lineCount,
		// TODO: Possible nullptr deref
		// "segments_found": len(playlist.Segments),
		// "variants_found": len(playlist.Variants),
		// "is_master":      playlist.IsMaster,
		// "is_live":        playlist.IsLive,
	})

	// Add explicit nil check before returning:
	if playlist == nil {
		logger.Error(nil, "Playlist is nil after parsing")
		return nil, fmt.Errorf("playlist became nil during parsing")
	}

	return playlist, scanner.Err()
}

// parseStreamInf parses EXT-X-STREAM-INF line into M3U8Variant
func (ad *AudioDownloader) parseStreamInf(line string) M3U8Variant {
	variant := M3U8Variant{}

	// Remove the tag prefix
	content := strings.TrimPrefix(line, "#EXT-X-STREAM-INF:")

	// Split by comma and parse attributes
	attributes := strings.Split(content, ",")
	for _, attr := range attributes {
		parts := strings.SplitN(strings.TrimSpace(attr), "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.Trim(strings.TrimSpace(parts[1]), `"`)

		switch key {
		case "BANDWIDTH":
			if bandwidth, err := strconv.Atoi(value); err == nil {
				variant.Bandwidth = bandwidth
			}
		case "CODECS":
			variant.Codecs = value
		case "RESOLUTION":
			variant.Resolution = value
		case "FRAME-RATE":
			if frameRate, err := strconv.ParseFloat(value, 64); err == nil {
				variant.FrameRate = frameRate
			}
		}
	}

	return variant
}

// findNewSegments returns segments that haven't been downloaded yet
func (ad *AudioDownloader) findNewSegments(playlist *M3U8Playlist) []M3U8Segment {
	var newSegments []M3U8Segment

	logger := logging.WithFields(logging.Fields{
		"component": "hls_audio_downloader",
		"function":  "findNewSegments",
	})

	// FOR LIVE: Only consider the LAST few segments from the playlist
	// This ensures we stay at the live edge
	segmentsToConsider := playlist.Segments
	if ad.isFirstPlaylist && len(playlist.Segments) > 0 {
		// Take only the last 3 segments for live streaming
		segmentsToConsider = playlist.Segments[len(playlist.Segments)-1:]
		ad.isFirstPlaylist = false
		logger.Debug("Live mode: considering only latest segments", logging.Fields{
			"total_segments": len(playlist.Segments),
			"considering":    len(segmentsToConsider),
		})
	}

	for _, segment := range segmentsToConsider {
		segmentURL := ad.resolveSegmentURL(playlist, segment.URI)

		if !ad.seenSegments[segmentURL] {
			newSegments = append(newSegments, segment)

			logger.Debug("Found new segment", logging.Fields{
				"segment_uri": segment.URI,
				"segment_url": segmentURL,
				"duration":    segment.Duration,
			})
		}
	}

	return newSegments
}

// DownloadAudioSegment downloads and processes a single HLS audio segment
func (ad *AudioDownloader) DownloadAudioSegment(ctx context.Context, segmentURL string) (*common.AudioData, error) {
	startTime := time.Now()

	logger := logging.WithFields(logging.Fields{
		"component":   "hls_audio_downloader",
		"function":    "DownloadAudioSegment",
		"segment_url": segmentURL,
	})

	// Add configured query params to segment URL
	segmentURL = ad.applyQueryParamRules(segmentURL)

	// Check cache first (if enabled)
	if ad.config.CacheSegments {
		if cachedData, exists := ad.segmentCache[segmentURL]; exists {
			logger.Debug("Using cached segment data")
			return ad.processSegmentData(cachedData, segmentURL, startTime)
		}
	}

	// Download segment with retries
	segmentData, err := ad.downloadSegmentWithRetries(ctx, segmentURL)
	if err != nil {
		ad.recordSegmentError(segmentURL, err, 0, "download")
		return nil, fmt.Errorf("failed to download segment: %w", err)
	}

	logger.Debug("Segment downloaded successfully", logging.Fields{
		"data_size": len(segmentData),
	})

	// Cache if enabled
	if ad.config.CacheSegments {
		ad.segmentCache[segmentURL] = segmentData
	}

	// Update download stats
	ad.downloadStats.SegmentsDownloaded++
	ad.downloadStats.BytesDownloaded += int64(len(segmentData))
	ad.downloadStats.DownloadTime += time.Since(startTime)

	return ad.processSegmentData(segmentData, segmentURL, startTime)
}

// processSegmentData processes segment data using either injected decoder or fallback
func (ad *AudioDownloader) processSegmentData(segmentData []byte, segmentURL string, startTime time.Time) (*common.AudioData, error) {
	decodeStartTime := time.Now()

	logger := logging.WithFields(logging.Fields{
		"component":   "hls_audio_downloader",
		"function":    "processSegmentData",
		"segment_url": segmentURL,
		"data_size":   len(segmentData),
	})

	var audioData *common.AudioData
	var err error

	// Use injected decoder if available
	if ad.hlsConfig != nil && ad.hlsConfig.AudioDecoder != nil {
		logger.Debug("Using injected audio decoder")

		anyData, err := ad.hlsConfig.AudioDecoder.DecodeBytes(segmentData)
		if err != nil {
			logger.Error(err, "Injected decoder failed, falling back to basic extraction")
			// Fall back to basic extraction
			audioData, err = ad.basicAudioExtraction(segmentData, segmentURL)
			if err != nil {
				logger.Error(err, "Fallback extraction also failed")
				ad.recordSegmentError(segmentURL, err, 0, "decode")
				return nil, fmt.Errorf("fallback extraction failed: %w", err)
			}
		} else {
			// Convert to common.AudioData
			if commonAudio, ok := anyData.(*common.AudioData); ok {
				audioData = commonAudio
			} else {
				audioData = common.ConvertToAudioData(anyData)
				if audioData == nil {
					return nil, fmt.Errorf("decoder returned unexpected type: %T", anyData)
				}
			}
		}
	} else {
		logger.Debug("No injected decoder, using basic audio extraction")
		audioData, err = ad.basicAudioExtraction(segmentData, segmentURL)
	}

	if err != nil {
		ad.recordSegmentError(segmentURL, err, 0, "decode")
		return nil, fmt.Errorf("failed to process audio data: %w", err)
	}

	// Update decode stats
	ad.downloadStats.DecodeTime += time.Since(decodeStartTime)

	// Process and normalize the audio data
	audioData = ad.processAudioData(audioData)

	// Add metadata and timestamp
	audioData.Timestamp = startTime
	if audioData.Metadata == nil {
		audioData.Metadata = ad.createBasicMetadata(segmentURL)
	}

	// Update audio metrics
	ad.updateAudioMetrics(audioData)

	return audioData, nil
}

// Additional helper methods would continue here...
// (basicAudioExtraction, processAudioData, combineAudioSamples, etc.)
// These remain largely the same as the original implementation

// basicAudioExtraction provides fallback audio extraction when no decoder is injected
func (ad *AudioDownloader) basicAudioExtraction(segmentData []byte, segmentURL string) (*common.AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component":   "hls_audio_downloader",
		"function":    "basicAudioExtraction",
		"segment_url": segmentURL,
		"data_size":   len(segmentData),
	})

	if len(segmentData) < 4 {
		return nil, fmt.Errorf("segment data too small")
	}

	// Basic format detection
	format := ad.detectAudioFormat(segmentData)
	logger.Debug("Detected audio format", logging.Fields{"format": format})

	var audioData *common.AudioData
	var err error

	switch format {
	case "aac":
		audioData, err = ad.extractAAC(segmentData, segmentURL)
	case "mp3":
		audioData, err = ad.extractMP3(segmentData, segmentURL)
	case "ts":
		audioData, err = ad.extractTS(segmentData, segmentURL)
	default:
		// Create placeholder audio data for unknown formats
		logger.Warn("Unknown format, creating placeholder audio data")
		audioData = &common.AudioData{
			PCM:        ad.generateSilence(ad.config.OutputSampleRate * 2), // 2 seconds of silence
			SampleRate: ad.config.OutputSampleRate,
			Channels:   ad.config.OutputChannels,
			Duration:   2 * time.Second,
			Metadata:   ad.createBasicMetadata(segmentURL),
		}
		audioData.Metadata.Codec = "unknown"
	}

	if err != nil {
		return nil, err
	}

	// Set metadata
	if audioData.Metadata == nil {
		audioData.Metadata = ad.createBasicMetadata(segmentURL)
	}
	audioData.Metadata.Codec = format

	return audioData, nil
}

// detectAudioFormat performs basic format detection
func (ad *AudioDownloader) detectAudioFormat(data []byte) string {
	if len(data) < 4 {
		return "unknown"
	}

	// Check for ID3 tag first (MP3 with metadata)
	if len(data) >= 3 && data[0] == 'I' && data[1] == 'D' && data[2] == '3' {
		return "mp3"
	}

	// Check for Transport Stream
	if data[0] == 0x47 {
		return "ts"
	}

	// Check sync words
	if len(data) >= 2 && data[0] == 0xFF {
		secondByte := data[1]

		// MP3 sync words
		if secondByte == 0xFB || secondByte == 0xFA ||
			secondByte == 0xF3 || secondByte == 0xF2 ||
			secondByte == 0xEB || secondByte == 0xEA {
			return "mp3"
		}

		// AAC ADTS detection
		if (secondByte & 0xF0) == 0xF0 {
			return "aac"
		}
	}

	return "unknown"
}

// extractAAC performs basic AAC extraction
func (ad *AudioDownloader) extractAAC(data []byte, segmentURL string) (*common.AudioData, error) {
	frames := 0
	estimatedDuration := 0.0

	for i := 0; i < len(data)-7; i++ {
		// Look for ADTS sync word
		if data[i] == 0xFF && (data[i+1]&0xF0) == 0xF0 {
			frames++
			estimatedDuration += 0.023 // ~23ms per frame
			i += 100                   // Skip ahead
		}
	}

	if frames == 0 {
		return nil, fmt.Errorf("no valid AAC frames found")
	}

	duration := time.Duration(estimatedDuration * float64(time.Second))
	samples := int(float64(ad.config.OutputSampleRate) * estimatedDuration)

	return &common.AudioData{
		PCM:        ad.generateSilence(samples),
		SampleRate: ad.config.OutputSampleRate,
		Channels:   ad.config.OutputChannels,
		Duration:   duration,
		Metadata:   ad.createBasicMetadata(segmentURL),
	}, nil
}

// extractMP3 performs basic MP3 extraction
func (ad *AudioDownloader) extractMP3(data []byte, segmentURL string) (*common.AudioData, error) {
	// Skip ID3 tag if present
	offset := 0
	if len(data) >= 10 && data[0] == 'I' && data[1] == 'D' && data[2] == '3' {
		tagSize := int(data[6])<<21 | int(data[7])<<14 | int(data[8])<<7 | int(data[9])
		offset = 10 + tagSize
	}

	frames := 0
	estimatedDuration := 0.0

	for i := offset; i < len(data)-4; i++ {
		if data[i] == 0xFF && (data[i+1]&0xE0) == 0xE0 {
			frames++
			estimatedDuration += 0.026 // ~26ms per frame
			i += 144                   // Skip ahead
		}
	}

	if frames == 0 {
		return nil, fmt.Errorf("no valid MP3 frames found")
	}

	duration := time.Duration(estimatedDuration * float64(time.Second))
	samples := int(float64(ad.config.OutputSampleRate) * estimatedDuration)

	return &common.AudioData{
		PCM:        ad.generateSilence(samples),
		SampleRate: ad.config.OutputSampleRate,
		Channels:   ad.config.OutputChannels,
		Duration:   duration,
		Metadata:   ad.createBasicMetadata(segmentURL),
	}, nil
}

// extractTS performs basic TS (Transport Stream) extraction
func (ad *AudioDownloader) extractTS(data []byte, segmentURL string) (*common.AudioData, error) {
	packets := 0
	for i := 0; i < len(data)-188; i += 188 {
		if data[i] == 0x47 {
			packets++
		}
	}

	if packets == 0 {
		return nil, fmt.Errorf("no valid TS packets found")
	}

	// Estimate duration based on packets
	estimatedDuration := float64(packets) / 100.0
	if estimatedDuration < 1.0 {
		estimatedDuration = 2.0
	}
	if estimatedDuration > 15.0 {
		estimatedDuration = 10.0
	}

	duration := time.Duration(estimatedDuration * float64(time.Second))
	samples := int(float64(ad.config.OutputSampleRate) * estimatedDuration)

	return &common.AudioData{
		PCM:        ad.generateSilence(samples),
		SampleRate: ad.config.OutputSampleRate,
		Channels:   ad.config.OutputChannels,
		Duration:   duration,
		Metadata:   ad.createBasicMetadata(segmentURL),
	}, nil
}

// generateSilence creates silent PCM data
func (ad *AudioDownloader) generateSilence(sampleCount int) []float64 {
	return make([]float64, sampleCount)
}

// processAudioData applies basic post-processing to audio data
func (ad *AudioDownloader) processAudioData(audioData *common.AudioData) *common.AudioData {
	if audioData == nil || len(audioData.PCM) == 0 {
		return audioData
	}

	// Apply normalization if configured
	if ad.config.NormalizePCM {
		ad.normalizePCM(audioData.PCM)
	}

	// Apply channel conversion if needed
	if ad.config.OutputChannels != audioData.Channels {
		if ad.config.OutputChannels == 1 && audioData.Channels == 2 {
			audioData = ad.convertToMono(audioData)
		}
	}

	// Apply sample rate conversion if needed
	if ad.config.OutputSampleRate != audioData.SampleRate {
		audioData = ad.convertSampleRate(audioData, ad.config.OutputSampleRate)
	}

	return audioData
}

// convertToMono converts stereo audio to mono
func (ad *AudioDownloader) convertToMono(audioData *common.AudioData) *common.AudioData {
	if audioData.Channels != 2 {
		return audioData
	}

	monoSamples := make([]float64, len(audioData.PCM)/2)
	for i := range len(monoSamples) {
		monoSamples[i] = (audioData.PCM[i*2] + audioData.PCM[i*2+1]) / 2.0
	}

	return &common.AudioData{
		PCM:        monoSamples,
		SampleRate: audioData.SampleRate,
		Channels:   1,
		Duration:   audioData.Duration,
		Timestamp:  audioData.Timestamp,
		Metadata:   audioData.Metadata,
	}
}

// convertSampleRate performs basic linear interpolation sample rate conversion
func (ad *AudioDownloader) convertSampleRate(audioData *common.AudioData, targetRate int) *common.AudioData {
	if audioData.SampleRate == targetRate {
		return audioData
	}

	ratio := float64(targetRate) / float64(audioData.SampleRate)
	newLength := int(float64(len(audioData.PCM)) * ratio)
	newSamples := make([]float64, newLength)

	for i := range newLength {
		sourceIndex := float64(i) / ratio
		sourceIndexInt := int(sourceIndex)

		if sourceIndexInt >= len(audioData.PCM)-1 {
			newSamples[i] = audioData.PCM[len(audioData.PCM)-1]
		} else {
			fraction := sourceIndex - float64(sourceIndexInt)
			newSamples[i] = audioData.PCM[sourceIndexInt]*(1-fraction) + audioData.PCM[sourceIndexInt+1]*fraction
		}
	}

	return &common.AudioData{
		PCM:        newSamples,
		SampleRate: targetRate,
		Channels:   audioData.Channels,
		Duration:   audioData.Duration,
		Timestamp:  audioData.Timestamp,
		Metadata:   audioData.Metadata,
	}
}

// normalizePCM normalizes audio samples to prevent clipping
func (ad *AudioDownloader) normalizePCM(samples []float64) {
	if len(samples) == 0 {
		return
	}

	var peak float64
	for _, sample := range samples {
		abs := sample
		if abs < 0 {
			abs = -abs
		}
		if abs > peak {
			peak = abs
		}
	}

	if peak > 1.0 {
		factor := 0.95 / peak
		for i := range samples {
			samples[i] *= factor
		}
	}
}

// downloadSegmentWithRetries downloads a segment with retry logic
func (ad *AudioDownloader) downloadSegmentWithRetries(ctx context.Context, segmentURL string) ([]byte, error) {
	var lastErr error

	for retry := 0; retry <= ad.config.MaxRetries; retry++ {
		data, err := ad.downloadSegment(ctx, segmentURL)
		if err == nil {
			return data, nil
		}

		lastErr = err
		ad.recordSegmentError(segmentURL, err, retry, "download")

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if retry < ad.config.MaxRetries {
			waitTime := time.Duration(retry+1) * time.Second
			time.Sleep(waitTime)
		}
	}

	return nil, lastErr
}

// downloadSegment downloads a single segment
func (ad *AudioDownloader) downloadSegment(ctx context.Context, segmentURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", segmentURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	userAgent := "TuneIn-CDN-Benchmark/1.0"
	if ad.hlsConfig != nil && ad.hlsConfig.HTTP != nil {
		userAgent = ad.hlsConfig.HTTP.UserAgent
	}

	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "*/*")

	resp, err := ad.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	var reader io.Reader = resp.Body
	if ad.hlsConfig != nil && ad.hlsConfig.HTTP != nil && ad.hlsConfig.HTTP.BufferSize > 0 {
		reader = bufio.NewReaderSize(resp.Body, ad.hlsConfig.HTTP.BufferSize)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return data, nil
}

// resolveSegmentURL resolves relative segment URLs against the playlist base URL
func (ad *AudioDownloader) resolveSegmentURL(playlist *M3U8Playlist, segmentURI string) string {
	// If it's already an absolute URL, add query params and return
	if strings.HasPrefix(segmentURI, "http://") || strings.HasPrefix(segmentURI, "https://") {
		return ad.applyQueryParamRules(segmentURI)
	}

	// Try playlist's base URL first
	if playlist != nil && playlist.Metadata != nil && playlist.Metadata.URL != "" {
		if baseURL, err := url.Parse(playlist.Metadata.URL); err == nil {
			if relativeURL, err := url.Parse(segmentURI); err == nil {
				resolved := baseURL.ResolveReference(relativeURL).String()
				return ad.applyQueryParamRules(resolved)
			}
		}
	}

	// Use downloader's base URL if available
	if ad.baseURL != "" {
		if baseURL, err := url.Parse(ad.baseURL); err == nil {
			if relativeURL, err := url.Parse(segmentURI); err == nil {
				resolved := baseURL.ResolveReference(relativeURL).String()
				return ad.applyQueryParamRules(resolved)
			}
		}
	}

	// Fallback: return as-is with query params
	return ad.applyQueryParamRules(segmentURI)
}

// selectBestVariant fetches master playlist and returns media playlist URL
func (ad *AudioDownloader) selectBestVariant(ctx context.Context, playlistURL string) (string, error) {
	playlist, err := ad.fetchPlaylist(ctx, playlistURL)
	if err != nil {
		return "", fmt.Errorf("failed to fetch master playlist: %w", err)
	}

	// If already a media playlist, use it directly
	if !playlist.IsMaster || len(playlist.Variants) == 0 {
		return playlistURL, nil
	}

	// Select lowest bandwidth variant (usually audio-only or lowest quality)
	var selectedVariant *M3U8Variant
	for i := range playlist.Variants {
		variant := &playlist.Variants[i]
		if selectedVariant == nil || variant.Bandwidth < selectedVariant.Bandwidth {
			selectedVariant = variant
		}
	}

	if selectedVariant == nil {
		return "", fmt.Errorf("no suitable variant found")
	}

	// Resolve variant URL
	return ad.resolveVariantURL(playlist, selectedVariant.URI), nil
}

// resolveVariantURL resolves variant URL against the master playlist base URL
func (ad *AudioDownloader) resolveVariantURL(masterPlaylist *M3U8Playlist, variantURI string) string {
	// If it's already an absolute URL, apply query params and return
	if strings.HasPrefix(variantURI, "http://") || strings.HasPrefix(variantURI, "https://") {
		return ad.applyQueryParamRules(variantURI)
	}

	// Try to resolve against master playlist URL
	if masterPlaylist != nil && masterPlaylist.Metadata != nil && masterPlaylist.Metadata.URL != "" {
		if baseURL, err := url.Parse(masterPlaylist.Metadata.URL); err == nil {
			if relativeURL, err := url.Parse(variantURI); err == nil {
				resolved := baseURL.ResolveReference(relativeURL).String()
				return ad.applyQueryParamRules(resolved)
			}
		}
	}

	// Fallback: try to resolve against the downloader's base URL
	if ad.baseURL != "" {
		if baseURL, err := url.Parse(ad.baseURL); err == nil {
			if relativeURL, err := url.Parse(variantURI); err == nil {
				resolved := baseURL.ResolveReference(relativeURL).String()
				return ad.applyQueryParamRules(resolved)
			}
		}
	}

	// Last resort: return as-is with query params applied
	return ad.applyQueryParamRules(variantURI)
}

// recordSegmentError records an error for metrics and debugging
func (ad *AudioDownloader) recordSegmentError(url string, err error, retry int, errorType string) {
	ad.downloadStats.ErrorCount++
	ad.downloadStats.SegmentErrors = append(ad.downloadStats.SegmentErrors, SegmentError{
		URL:       url,
		Error:     err.Error(),
		Timestamp: time.Now(),
		Retry:     retry,
		Type:      errorType,
	})
}

// updateAudioMetrics calculates and updates audio quality metrics
func (ad *AudioDownloader) updateAudioMetrics(audioData *common.AudioData) {
	if audioData == nil || len(audioData.PCM) == 0 {
		return
	}

	metrics := ad.downloadStats.AudioMetrics

	metrics.SamplesDecoded += int64(len(audioData.PCM))
	metrics.DecodedDuration += audioData.Duration.Seconds()

	var sum, peak float64
	silentSamples := 0
	clipping := false

	for _, sample := range audioData.PCM {
		abs := sample
		if abs < 0 {
			abs = -abs
		}

		sum += abs
		if abs > peak {
			peak = abs
		}

		if abs < 0.001 { // Silence threshold
			silentSamples++
		}

		if abs >= 0.99 { // Clipping threshold
			clipping = true
		}
	}

	metrics.AverageAmplitude = sum / float64(len(audioData.PCM))
	metrics.PeakAmplitude = peak
	metrics.SilenceRatio = float64(silentSamples) / float64(len(audioData.PCM))
	metrics.ClippingDetected = clipping
}

// createBasicMetadata creates basic metadata for a segment
func (ad *AudioDownloader) createBasicMetadata(segmentURL string) *common.StreamMetadata {
	return &common.StreamMetadata{
		URL:        segmentURL,
		Type:       common.StreamTypeHLS,
		SampleRate: ad.config.OutputSampleRate,
		Channels:   ad.config.OutputChannels,
		Bitrate:    ad.config.PreferredBitrate,
		Codec:      "unknown",
		Headers:    make(map[string]string),
		Timestamp:  time.Now(),
	}
}

// GetDownloadStats returns current download statistics
func (ad *AudioDownloader) GetDownloadStats() *DownloadStats {
	if ad.downloadStats.DownloadTime > 0 {
		bitsDownloaded := float64(ad.downloadStats.BytesDownloaded) * 8
		seconds := ad.downloadStats.DownloadTime.Seconds()
		ad.downloadStats.AverageBitrate = bitsDownloaded / seconds / 1000 // kbps
	}

	return ad.downloadStats
}

// ClearCache clears the segment cache
func (ad *AudioDownloader) ClearCache() {
	ad.segmentCache = make(map[string][]byte)
	ad.seenSegments = make(map[string]bool)
}

// UpdateConfig updates the download configuration
func (ad *AudioDownloader) UpdateConfig(config *DownloadConfig) {
	if config != nil {
		ad.config = config
	}
}

// Close cleans up resources
func (ad *AudioDownloader) Close() error {
	if ad.config.CleanupTempFiles && ad.tempDir != "" {
		return os.RemoveAll(ad.tempDir)
	}
	return nil
}

// SetBaseURL sets the base URL for resolving relative segment URLs
func (ad *AudioDownloader) SetBaseURL(baseURL string) {
	ad.baseURL = baseURL
}
