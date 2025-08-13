package icecast

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/audio/transcode"
	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// AudioDownloader handles continuous streaming download from ICEcast servers
type AudioDownloader struct {
	client        *http.Client
	icecastConfig *Config
	tempDir       string
	downloadStats *DownloadStats
	config        *DownloadConfig
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
	TargetDuration   time.Duration `json:"target_duration"`
	PreferredBitrate int           `json:"preferred_bitrate"`
	OutputSampleRate int           `json:"output_sample_rate"`
	OutputChannels   int           `json:"output_channels"`
	NormalizePCM     bool          `json:"normalize_pcm"`
	ResampleQuality  string        `json:"resample_quality"`
	CleanupTempFiles bool          `json:"cleanup_temp_files"`
	// ICEcast specific settings
	InitialTimeout       time.Duration `json:"initial_timeout"`        // Timeout for first byte
	StreamReadTimeout    time.Duration `json:"stream_read_timeout"`    // Timeout for individual reads
	ConnectionKeepAlive  time.Duration `json:"connection_keep_alive"`  // Keep connection alive
	MaxReconnectAttempts int           `json:"max_reconnect_attempts"` // Max reconnection attempts
	ReconnectDelay       time.Duration `json:"reconnect_delay"`        // Delay between reconnects
	// Query parameter configuration
	QueryParamRules   []QueryParamRule  `json:"query_param_rules"`   // Rules for adding query parameters
	GlobalQueryParams map[string]string `json:"global_query_params"` // Query parameters to add to all requests
}

// DownloadStats tracks download performance
type DownloadStats struct {
	SegmentsDownloaded int           `json:"segments_downloaded"`
	BytesDownloaded    int64         `json:"bytes_downloaded"`
	DownloadTime       time.Duration `json:"download_time"`
	DecodeTime         time.Duration `json:"decode_time"`
	ErrorCount         int           `json:"error_count"`
	AverageBitrate     float64       `json:"average_bitrate"`
	AudioMetrics       *AudioMetrics `json:"audio_metrics,omitempty"`
	// ICEcast specific stats
	ConnectionAttempts int           `json:"connection_attempts"`
	TimeToFirstByte    time.Duration `json:"time_to_first_byte"`
	Reconnections      int           `json:"reconnections"`
	LiveStartTime      *time.Time    `json:"live_start_time,omitempty"`
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

// NewAudioDownloader creates a new ICEcast audio downloader optimized for live streaming
func NewAudioDownloader(config *Config) *AudioDownloader {
	if config == nil {
		config = DefaultConfig()
	}

	// Create optimized HTTP client for ICEcast streaming
	client := &http.Client{
		Timeout: 0, // No global timeout for streaming
		Transport: &http.Transport{
			MaxIdleConns:          10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 15 * time.Second, // Quick response headers
			ExpectContinueTimeout: 1 * time.Second,
			DisableCompression:    false,
			DisableKeepAlives:     false,
			MaxIdleConnsPerHost:   5,
		},
	}

	now := time.Now()
	return &AudioDownloader{
		client:        client,
		icecastConfig: config,
		downloadStats: &DownloadStats{
			AudioMetrics:  &AudioMetrics{},
			LiveStartTime: &now,
		},
		config: DefaultDownloadConfig(),
	}
}

// DefaultDownloadConfig returns default download configuration optimized for ICEcast
func DefaultDownloadConfig() *DownloadConfig {
	return &DownloadConfig{
		MaxSegments:          10,
		SegmentTimeout:       10 * time.Second,
		MaxRetries:           3,
		TargetDuration:       240 * time.Second,
		PreferredBitrate:     128,
		OutputSampleRate:     44100,
		OutputChannels:       1,
		NormalizePCM:         true,
		ResampleQuality:      "medium",
		CleanupTempFiles:     true,
		InitialTimeout:       30 * time.Second,        // 30s timeout for first byte
		StreamReadTimeout:    30 * time.Second,        // 30s timeout for reads
		ConnectionKeepAlive:  60 * time.Second,        // Keep connection alive
		MaxReconnectAttempts: 5,                       // Max 3 reconnection attempts
		ReconnectDelay:       2 * time.Second,         // 5s delay between reconnects
		QueryParamRules:      []QueryParamRule{},      // Empty by default
		GlobalQueryParams:    make(map[string]string), // Empty by default
	}
}

// DefaultSoundstackConfig returns a config with soundstack/adzwizz query parameters
func DefaultSoundstackConfig() *DownloadConfig {
	config := DefaultDownloadConfig()

	// Add soundstack/adzwizz rule
	config.QueryParamRules = []QueryParamRule{
		{
			HostPatterns: []string{"cdnstream1.com", "soundstack", "adzwizz"},
			QueryParams: map[string]string{
				"aw_0_1st.premium":           "true",
				"partnerID":                  "BotTIStream",
				"playerid":                   "BotTIStream",
				"aw_0_1st.ads_partner_alias": "bot.TIStream",
			},
		},
	}

	return config
}

// DownloadAudioSample downloads a continuous stream of audio for the specified duration
func (d *AudioDownloader) DownloadAudioSample(ctx context.Context, streamURL string, targetDuration time.Duration) (*common.AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component":       "icecast_downloader",
		"function":        "DownloadAudioSample",
		"url":             streamURL,
		"target_duration": targetDuration.Seconds(),
	})

	logger.Debug("Starting ICEcast live stream download")

	// Apply configured query parameters
	streamURL = d.applyQueryParamRules(streamURL)

	var audioData *common.AudioData
	var err error

	// Try downloading with reconnection logic
	for attempt := 0; attempt <= d.config.MaxReconnectAttempts; attempt++ {
		d.downloadStats.ConnectionAttempts++

		if attempt > 0 {
			logger.Debug("Reconnecting to ICEcast stream", logging.Fields{
				"attempt":      attempt + 1,
				"max_attempts": d.config.MaxReconnectAttempts + 1,
			})

			// Wait before reconnecting
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(d.config.ReconnectDelay):
			}
		}

		audioData, err = d.downloadStreamWithTimeout(ctx, streamURL, targetDuration, logger)
		if err == nil {
			logger.Debug("ICEcast stream download completed successfully", logging.Fields{
				"attempt":  attempt + 1,
				"samples":  len(audioData.PCM),
				"duration": audioData.Duration.Seconds(),
			})
			return audioData, nil
		}

		logger.Warn("ICEcast download attempt failed", logging.Fields{
			"attempt": attempt + 1,
			"error":   err.Error(),
		})

		// If it's a context cancellation, don't retry
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if attempt < d.config.MaxReconnectAttempts {
			d.downloadStats.Reconnections++
		}
	}

	return nil, fmt.Errorf("failed to download ICEcast stream after %d attempts: %w",
		d.config.MaxReconnectAttempts+1, err)
}

// downloadStreamWithTimeout downloads the stream with proper timeout handling
func (d *AudioDownloader) downloadStreamWithTimeout(ctx context.Context, streamURL string, targetDuration time.Duration, logger logging.Logger) (*common.AudioData, error) {
	// Target duration + reasonable buffer for connection/processing
	downloadTimeout := targetDuration + (400 * time.Second)
	downloadCtx, cancel := context.WithTimeout(ctx, downloadTimeout)
	defer cancel()

	logger.Debug("Establishing ICEcast connection", logging.Fields{
		"timeout_seconds": downloadTimeout.Seconds(),
		"target_duration": targetDuration.Seconds(),
	})

	// Create HTTP request for streaming
	req, err := http.NewRequestWithContext(downloadCtx, "GET", streamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create streaming request: %w", err)
	}

	// Set headers optimized for ICEcast streaming
	req.Header.Set("User-Agent", d.icecastConfig.HTTP.UserAgent)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Icy-MetaData", "1") // Request ICEcast metadata

	// Start connection with timeout for initial response
	connectionStart := time.Now()

	logger.Debug("Connecting to ICEcast stream...")

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to establish streaming connection: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("streaming request failed with status %d: %s", resp.StatusCode, resp.Status)
	}

	// Record time to first byte
	ttfb := time.Since(connectionStart)
	d.downloadStats.TimeToFirstByte = ttfb

	logger.Debug("ICEcast connection established", logging.Fields{
		"status_code":  resp.StatusCode,
		"content_type": resp.Header.Get("Content-Type"),
		"ttfb_ms":      ttfb.Milliseconds(),
		"icy_br":       resp.Header.Get("icy-br"),
		"icy_name":     resp.Header.Get("icy-name"),
	})

	// Now use the same download context for streaming
	return d.streamAudioDataOptimized(downloadCtx, resp, targetDuration, logger)
}

// streamAudioDataOptimized performs optimized continuous reading with proper timeout handling
func (d *AudioDownloader) streamAudioDataOptimized(ctx context.Context, resp *http.Response, targetDuration time.Duration, logger logging.Logger) (*common.AudioData, error) {
	// Calculate target bytes more accurately based on ICEcast bitrate
	estimatedBitrate := 128 // Default
	if icyBr := resp.Header.Get("icy-br"); icyBr != "" {
		if br, err := fmt.Sscanf(icyBr, "%d", &estimatedBitrate); err == nil && br > 0 {
			logger.Debug("Using ICEcast reported bitrate", logging.Fields{"bitrate": estimatedBitrate})
		}
	}

	bytesPerSecond := (estimatedBitrate * 1000) / 8 // Convert kbps to bytes/sec
	targetBytes := int(targetDuration.Seconds() * float64(bytesPerSecond))
	bufferMultiplier := 1.5 // 50% buffer
	targetBytesWithBuffer := int(float64(targetBytes) * bufferMultiplier)

	logger.Debug("Starting optimized ICEcast streaming", logging.Fields{
		"estimated_bitrate_kbps": estimatedBitrate,
		"bytes_per_second":       bytesPerSecond,
		"target_bytes":           targetBytes,
		"target_with_buffer":     targetBytesWithBuffer,
	})

	var audioData []byte
	startTime := time.Now()
	readCount := 0

	// Use dynamic buffer size based on bitrate
	baseBufferSize := 32768     // 32KB base
	if bytesPerSecond > 20000 { // For higher bitrates
		baseBufferSize = 65536 // 64KB
	}
	bufferSize := max(d.icecastConfig.Audio.BufferSize, baseBufferSize)

	// Create a buffered reader for efficiency
	reader := resp.Body

streamLoop:
	for len(audioData) < targetBytesWithBuffer {
		select {
		case <-ctx.Done():
			logger.Debug("Streaming context cancelled", logging.Fields{
				"bytes_collected": len(audioData),
				"target_bytes":    targetBytesWithBuffer,
			})
			if len(audioData) < targetBytes/4 { // Less than 25% collected
				return nil, ctx.Err()
			}
			// We have enough data, break and process
			break streamLoop
		default:
		}

		readCount++
		buffer := make([]byte, bufferSize)

		// Set read deadline for this specific read
		readDeadline := time.Now().Add(d.config.StreamReadTimeout)
		if conn, ok := reader.(interface{ SetReadDeadline(time.Time) error }); ok {
			conn.SetReadDeadline(readDeadline)
		}

		n, err := reader.Read(buffer)

		if err != nil {
			elapsed := time.Since(startTime)

			if err == io.EOF {
				logger.Debug("ICEcast stream ended", logging.Fields{
					"bytes_collected": len(audioData),
					"elapsed_seconds": elapsed.Seconds(),
				})
				break
			}

			// Log the error but check if we have enough data
			logger.Warn("Read error during streaming", logging.Fields{
				"error":           err.Error(),
				"bytes_so_far":    len(audioData),
				"read_count":      readCount,
				"target_duration": targetDuration.Seconds(),
				"elapsed_seconds": elapsed.Seconds(),
			})

			// If we have substantial data and we're close to target duration, continue
			minDataThreshold := int(float64(targetBytes) * 0.6) // At least 60% of target
			minTimeThreshold := targetDuration * 70 / 100       // At least 70% of target time

			if len(audioData) >= minDataThreshold && elapsed >= minTimeThreshold {
				logger.Debug("Continuing with partial data due to read error", logging.Fields{
					"data_ratio": fmt.Sprintf("%.1f%%", float64(len(audioData))/float64(targetBytes)*100),
					"time_ratio": fmt.Sprintf("%.1f%%", elapsed.Seconds()/targetDuration.Seconds()*100),
				})
				break
			}

			return nil, fmt.Errorf("streaming read failed: %w", err)
		}

		if n > 0 {
			audioData = append(audioData, buffer[:n]...)

			// Log progress every 2 seconds of collection or every 50 reads
			if readCount%50 == 0 || time.Since(startTime) >= time.Duration(len(audioData)/bytesPerSecond)*time.Second {
				elapsed := time.Since(startTime)
				progress := (float64(len(audioData)) / float64(targetBytes)) * 100
				rate := float64(len(audioData)) / elapsed.Seconds() / 1024 // KB/s
				estimatedTimeRemaining := (float64(targetBytes-len(audioData)) / float64(bytesPerSecond))

				logger.Debug("ICEcast streaming progress", logging.Fields{
					"bytes_collected":         len(audioData),
					"target_bytes":            targetBytes,
					"progress_pct":            fmt.Sprintf("%.1f%%", progress),
					"rate_kbps":               fmt.Sprintf("%.1f", rate),
					"elapsed_sec":             elapsed.Seconds(),
					"estimated_remaining_sec": fmt.Sprintf("%.1f", estimatedTimeRemaining),
				})
			}
		}

		// Check if we've collected enough data based on time
		elapsed := time.Since(startTime)
		// Exit when we've reached target duration AND have reasonable amount of data
		if elapsed >= targetDuration {
			minDataForTime := int(float64(targetBytes) * 0.7) // At least 70% of expected data
			if len(audioData) >= minDataForTime {
				logger.Debug("Target duration reached with sufficient data", logging.Fields{
					"elapsed_seconds":   elapsed.Seconds(),
					"target_duration":   targetDuration.Seconds(),
					"bytes_collected":   len(audioData),
					"target_bytes":      targetBytes,
					"data_completeness": fmt.Sprintf("%.1f%%", float64(len(audioData))/float64(targetBytes)*100),
				})
				break streamLoop
			}
		}

		// FIX: Extended time allowance - allow up to 125% of target duration if we still need more data
		maxDuration := targetDuration + (targetDuration / 4) // 125% of target duration
		if elapsed >= maxDuration {
			logger.Debug("Maximum duration reached, stopping collection", logging.Fields{
				"elapsed_seconds":   elapsed.Seconds(),
				"max_duration":      maxDuration.Seconds(),
				"bytes_collected":   len(audioData),
				"data_completeness": fmt.Sprintf("%.1f%%", float64(len(audioData))/float64(targetBytes)*100),
			})
			break streamLoop
		}

		// Secondary condition: if we have significantly more data than expected, we can stop
		if len(audioData) >= targetBytesWithBuffer {
			logger.Debug("Buffer threshold reached", logging.Fields{
				"bytes_collected":    len(audioData),
				"target_with_buffer": targetBytesWithBuffer,
				"elapsed_seconds":    elapsed.Seconds(),
			})
			break streamLoop
		}
	}

	finalElapsed := time.Since(startTime)
	d.downloadStats.BytesDownloaded += int64(len(audioData))
	d.downloadStats.DownloadTime += finalElapsed

	// Calculate actual bitrate
	if finalElapsed > 0 {
		bitsDownloaded := float64(len(audioData)) * 8
		seconds := finalElapsed.Seconds()
		d.downloadStats.AverageBitrate = bitsDownloaded / seconds / 1000 // kbps
	}

	logger.Debug("ICEcast streaming collection completed", logging.Fields{
		"final_bytes":         len(audioData),
		"target_bytes":        targetBytes,
		"total_reads":         readCount,
		"elapsed_sec":         finalElapsed.Seconds(),
		"target_sec":          targetDuration.Seconds(),
		"avg_rate_kbps":       fmt.Sprintf("%.1f", d.downloadStats.AverageBitrate),
		"data_efficiency_pct": fmt.Sprintf("%.1f%%", (float64(len(audioData))/float64(targetBytes))*100),
		"time_efficiency_pct": fmt.Sprintf("%.1f%%", finalElapsed.Seconds()/targetDuration.Seconds()*100),
	})

	// Process the collected audio data
	return d.processStreamedAudioWithFFmpeg(audioData, resp.Request.URL.String(), logger)
}

// applyQueryParamRules applies configured query parameter rules to a URL
func (d *AudioDownloader) applyQueryParamRules(targetURL string) string {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return targetURL
	}

	query := parsedURL.Query()

	// First apply global query parameters
	for key, value := range d.config.GlobalQueryParams {
		query.Set(key, value)
	}

	// Then apply rule-based query parameters
	for _, rule := range d.config.QueryParamRules {
		if d.matchesRule(parsedURL, rule) {
			for key, value := range rule.QueryParams {
				query.Set(key, value)
			}
		}
	}

	parsedURL.RawQuery = query.Encode()
	return parsedURL.String()
}

// matchesRule checks if a URL matches a query parameter rule
func (d *AudioDownloader) matchesRule(parsedURL *url.URL, rule QueryParamRule) bool {
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

// processStreamedAudioWithFFmpeg converts streamed audio bytes using FFmpeg decoder
func (d *AudioDownloader) processStreamedAudioWithFFmpeg(audioBytes []byte, url string, logger logging.Logger) (*common.AudioData, error) {
	if len(audioBytes) == 0 {
		return nil, fmt.Errorf("no audio data received from stream")
	}

	logger.Debug("Processing streamed audio with FFmpeg decoder", logging.Fields{
		"raw_bytes": len(audioBytes),
	})

	// FIX: Clean and validate the audio stream before decoding
	cleanedAudio, err := d.cleanICEcastStream(audioBytes, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to clean ICEcast stream: %w", err)
	}

	if len(cleanedAudio) == 0 {
		return nil, fmt.Errorf("no valid audio data found after cleaning stream")
	}

	logger.Debug("Stream cleaned for decoding", logging.Fields{
		"original_bytes": len(audioBytes),
		"cleaned_bytes":  len(cleanedAudio),
		"reduction_pct":  fmt.Sprintf("%.1f%%", float64(len(audioBytes)-len(cleanedAudio))/float64(len(audioBytes))*100),
	})

	decodeStartTime := time.Now()

	// Use FFmpeg decoder to properly decode the cleaned data
	ad, err := d.icecastConfig.AudioDecoder.DecodeBytes(cleanedAudio)
	if err != nil {
		// If cleaning didn't work, try with original data as fallback
		logger.Warn("Cleaned audio decode failed, trying original data", logging.Fields{
			"cleaned_size":  len(cleanedAudio),
			"original_size": len(audioBytes),
			"error":         err.Error(),
		})

		ad, err = d.icecastConfig.AudioDecoder.DecodeBytes(audioBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to decode streamed audio with FFmpeg: %w", err)
		}
	}

	var audioData *common.AudioData
	if commonAudio, ok := ad.(*common.AudioData); ok {
		audioData = commonAudio
	} else {
		// Use reflection to convert unknown AudioData type to common.AudioData
		audioData = common.ConvertToAudioData(ad)
		if audioData == nil {
			return nil, fmt.Errorf("decoder returned unexpected type: %T", ad)
		}
	}

	d.downloadStats.DecodeTime += time.Since(decodeStartTime)
	d.updateAudioMetrics(audioData)

	logger.Debug("FFmpeg decoding completed", logging.Fields{
		"decoded_samples":     len(audioData.PCM),
		"decoded_duration":    audioData.Duration.Seconds(),
		"decoded_channels":    audioData.Channels,
		"decoded_sample_rate": audioData.SampleRate,
		"decode_time_ms":      d.downloadStats.DecodeTime.Milliseconds(),
	})

	// Create final AudioData with proper metadata
	finalAudioData := &common.AudioData{
		PCM:        audioData.PCM,
		SampleRate: audioData.SampleRate,
		Channels:   audioData.Channels,
		Duration:   audioData.Duration,
		Timestamp:  *d.downloadStats.LiveStartTime,
		Metadata:   d.convertMetadata(audioData.Metadata, url),
	}

	logger.Debug("ICEcast streaming audio processing completed", logging.Fields{
		"final_samples":      len(finalAudioData.PCM),
		"final_duration_sec": finalAudioData.Duration.Seconds(),
		"final_sample_rate":  finalAudioData.SampleRate,
		"final_channels":     finalAudioData.Channels,
		"live_start_time":    finalAudioData.Timestamp.Format(time.RFC3339),
	})

	return finalAudioData, nil
}

// convertMetadata converts metadata and ensures proper ICEcast metadata
func (d *AudioDownloader) convertMetadata(metadata *common.StreamMetadata, url string) *common.StreamMetadata {
	if metadata == nil {
		// Create basic metadata if none provided
		return &common.StreamMetadata{
			URL:         url,
			Type:        common.StreamTypeICEcast,
			ContentType: "audio/mpeg",
			Format:      "mp3",
			Codec:       "mp3",
			SampleRate:  44100,
			Channels:    2,
			Bitrate:     128,
			Timestamp:   time.Now(),
		}
	}

	// Ensure ICEcast specific fields are set
	result := &common.StreamMetadata{
		URL:         url,
		Type:        common.StreamTypeICEcast,
		Format:      metadata.Format,
		Bitrate:     metadata.Bitrate,
		SampleRate:  metadata.SampleRate,
		Channels:    metadata.Channels,
		Codec:       metadata.Codec,
		ContentType: metadata.ContentType,
		Title:       metadata.Title,
		Artist:      metadata.Artist,
		Genre:       metadata.Genre,
		Station:     metadata.Station,
		Headers:     make(map[string]string),
		Timestamp:   *d.downloadStats.LiveStartTime,
	}

	// Copy headers if they exist
	if metadata.Headers != nil {
		maps.Copy(result.Headers, metadata.Headers)
	}

	return result
}

// updateAudioMetrics calculates and updates audio quality metrics
func (d *AudioDownloader) updateAudioMetrics(audioData *common.AudioData) {
	if audioData == nil || len(audioData.PCM) == 0 {
		return
	}

	metrics := d.downloadStats.AudioMetrics

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

// GetDownloadStats returns the current download statistics
func (d *AudioDownloader) GetDownloadStats() *DownloadStats {
	return d.downloadStats
}

// DownloadAudioSampleDirect downloads ICEcast audio using FFmpeg directly with reconnection
// This bypasses all the complex ICEcast parsing and uses FFmpeg's native streaming support
func (d *AudioDownloader) DownloadAudioSampleDirect(ctx context.Context, streamURL string, targetDuration time.Duration) (*common.AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component":       "icecast_downloader",
		"function":        "DownloadAudioSampleDirect",
		"url":             streamURL,
		"target_duration": targetDuration.Seconds(),
		"method":          "ffmpeg_direct",
	})

	logger.Debug("Starting direct FFmpeg ICEcast audio download")

	// Apply query parameters if needed
	processedURL := d.applyQueryParamRules(streamURL)

	// Create decoder with configuration matching our output requirements
	decoderConfig := transcode.DefaultDecoderConfig()

	// Apply ICEcast config values if available
	if d.icecastConfig != nil && d.icecastConfig.MetadataExtractor != nil && d.icecastConfig.MetadataExtractor.DefaultValues != nil {
		if sampleRate, ok := d.icecastConfig.MetadataExtractor.DefaultValues["sample_rate"].(int); ok && sampleRate > 0 {
			decoderConfig.TargetSampleRate = sampleRate
		}
		if channels, ok := d.icecastConfig.MetadataExtractor.DefaultValues["channels"].(int); ok && channels > 0 {
			decoderConfig.TargetChannels = channels
		}
	}

	// Override with our downloader config if available
	if d.config != nil {
		decoderConfig.TargetSampleRate = d.config.OutputSampleRate
		decoderConfig.TargetChannels = d.config.OutputChannels

		// Set duration limit
		decoderConfig.MaxDuration = targetDuration

		// Set timeout based on target duration + buffer for reconnections
		decoderConfig.Timeout = targetDuration + (60 * time.Second)
	}

	// FIX: Use stream type instead of custom options
	// Create decoder
	decoder := transcode.NewDecoder(decoderConfig)
	defer decoder.Close()

	logger.Debug("Created FFmpeg decoder with ICEcast stream type", logging.Fields{
		"target_sample_rate": decoderConfig.TargetSampleRate,
		"target_channels":    decoderConfig.TargetChannels,
		"max_duration":       decoderConfig.MaxDuration.Seconds(),
		"timeout":            decoderConfig.Timeout.Seconds(),
		"stream_type":        "icecast",
	})

	// Use FFmpeg to decode directly from URL with ICEcast optimizations
	startTime := time.Now()
	result, err := decoder.DecodeURL(processedURL, targetDuration, "icecast")
	if err != nil {
		logger.Error(err, "FFmpeg direct decode failed")
		return nil, fmt.Errorf("failed to decode ICEcast stream with FFmpeg: %w", err)
	}

	downloadTime := time.Since(startTime)

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
			URL:         streamURL,
			Type:        common.StreamTypeICEcast,
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
		audioData.Metadata = d.createBasicMetadata(streamURL)
	}

	// Update download stats
	d.downloadStats.SegmentsDownloaded = 1                          // FFmpeg handles streaming internally
	d.downloadStats.BytesDownloaded = int64(len(audioData.PCM) * 8) // Estimate based on PCM data
	d.downloadStats.DownloadTime = downloadTime

	logger.Debug("Direct FFmpeg ICEcast download completed", logging.Fields{
		"actual_duration": audioData.Duration.Seconds(),
		"target_duration": targetDuration.Seconds(),
		"samples":         len(audioData.PCM),
		"sample_rate":     audioData.SampleRate,
		"channels":        audioData.Channels,
		"download_time":   downloadTime.Seconds(),
	})

	return audioData, nil
}

// createBasicMetadata creates basic metadata for ICEcast streams
func (d *AudioDownloader) createBasicMetadata(streamURL string) *common.StreamMetadata {
	metadata := &common.StreamMetadata{
		URL:        streamURL,
		Type:       common.StreamTypeICEcast,
		SampleRate: d.config.OutputSampleRate,
		Channels:   d.config.OutputChannels,
		Bitrate:    d.config.PreferredBitrate,
		Headers:    make(map[string]string),
		Timestamp:  time.Now(),
	}

	// Try to infer format from URL
	if strings.Contains(strings.ToLower(streamURL), ".mp3") {
		metadata.Format = "mp3"
		metadata.Codec = "mp3"
		metadata.ContentType = "audio/mpeg"
	} else if strings.Contains(strings.ToLower(streamURL), ".aac") {
		metadata.Format = "aac"
		metadata.Codec = "aac"
		metadata.ContentType = "audio/aac"
	} else {
		// Default to MP3 for ICEcast streams
		metadata.Format = "mp3"
		metadata.Codec = "mp3"
		metadata.ContentType = "audio/mpeg"
	}

	return metadata
}

// DownloadAudioSampleWithRetry downloads audio with automatic retry logic (legacy method)
func (d *AudioDownloader) DownloadAudioSampleWithRetry(ctx context.Context, url string, targetDuration time.Duration, maxRetries int) (*common.AudioData, error) {
	// Update config with provided retry count
	originalRetries := d.config.MaxReconnectAttempts
	d.config.MaxReconnectAttempts = maxRetries
	defer func() {
		d.config.MaxReconnectAttempts = originalRetries
	}()

	return d.DownloadAudioSample(ctx, url, targetDuration)
}

// cleanICEcastStream removes ICY metadata, ads, and invalid data from the raw stream
func (d *AudioDownloader) cleanICEcastStream(rawData []byte, logger logging.Logger) ([]byte, error) {
	logger.Debug("Starting ICEcast stream cleaning")

	var cleanedData []byte
	pos := 0
	validFrames := 0
	skippedBytes := 0

	for pos < len(rawData)-4 {
		// Look for valid MP3 frame headers
		if d.isValidMP3Frame(rawData[pos:]) {
			// Found valid MP3 frame, extract it
			frameSize := d.getMP3FrameSize(rawData[pos:])
			if frameSize > 0 && pos+frameSize <= len(rawData) {
				cleanedData = append(cleanedData, rawData[pos:pos+frameSize]...)
				pos += frameSize
				validFrames++
				continue
			}
		}

		// Look for valid AAC frame headers
		if d.isValidAACFrame(rawData[pos:]) {
			frameSize := d.getAACFrameSize(rawData[pos:])
			if frameSize > 0 && pos+frameSize <= len(rawData) {
				cleanedData = append(cleanedData, rawData[pos:pos+frameSize]...)
				pos += frameSize
				validFrames++
				continue
			}
		}

		// Skip invalid byte
		pos++
		skippedBytes++

		// If we've skipped too many bytes without finding valid frames,
		// try to find the next sync word
		if skippedBytes > 1024 && validFrames == 0 {
			syncPos := d.findNextSyncWord(rawData[pos:])
			if syncPos > 0 {
				pos += syncPos
				skippedBytes = 0
			}
		}
	}

	logger.Debug("ICEcast stream cleaning completed", logging.Fields{
		"original_size":  len(rawData),
		"cleaned_size":   len(cleanedData),
		"valid_frames":   validFrames,
		"skipped_bytes":  skippedBytes,
		"efficiency_pct": fmt.Sprintf("%.1f%%", float64(len(cleanedData))/float64(len(rawData))*100),
	})

	// If we couldn't find enough valid frames, return original data
	// (maybe it's a format we don't recognize but FFmpeg can handle)
	if validFrames < 10 {
		logger.Warn("Few valid frames found, returning original data", logging.Fields{
			"valid_frames": validFrames,
		})
		return rawData, nil
	}

	return cleanedData, nil
}

// isValidMP3Frame checks if data starts with a valid MP3 frame header
func (d *AudioDownloader) isValidMP3Frame(data []byte) bool {
	if len(data) < 4 {
		return false
	}

	// Check for MP3 sync word (11 bits of 1s)
	if data[0] != 0xFF {
		return false
	}

	if (data[1] & 0xE0) != 0xE0 {
		return false
	}

	// Check MPEG version (should not be reserved)
	mpegVersion := (data[1] >> 3) & 0x03
	if mpegVersion == 1 { // Reserved
		return false
	}

	// Check layer (should not be reserved)
	layer := (data[1] >> 1) & 0x03
	if layer == 0 { // Reserved
		return false
	}

	// Check bitrate (should not be free or reserved)
	bitrate := (data[2] >> 4) & 0x0F
	if bitrate == 0 || bitrate == 15 { // Free or reserved
		return false
	}

	// Check sample rate (should not be reserved)
	sampleRate := (data[2] >> 2) & 0x03
	if sampleRate == 3 { // Reserved
		return false
	}

	return true
}

// isValidAACFrame checks if data starts with a valid AAC ADTS frame header
func (d *AudioDownloader) isValidAACFrame(data []byte) bool {
	if len(data) < 7 {
		return false
	}

	// Check for AAC ADTS sync word
	if data[0] != 0xFF {
		return false
	}

	if (data[1] & 0xF0) != 0xF0 {
		return false
	}

	// Additional ADTS validation
	if (data[1] & 0x06) != 0x00 { // Should be MPEG-4
		return false
	}

	return true
}

// getMP3FrameSize calculates the size of an MP3 frame
func (d *AudioDownloader) getMP3FrameSize(data []byte) int {
	if len(data) < 4 || !d.isValidMP3Frame(data) {
		return 0
	}

	// Extract header info
	bitrate := (data[2] >> 4) & 0x0F
	sampleRate := (data[2] >> 2) & 0x03
	padding := (data[2] >> 1) & 0x01

	// Bitrate table (simplified)
	bitrateTable := map[int]int{
		1: 32, 2: 40, 3: 48, 4: 56, 5: 64, 6: 80, 7: 96,
		8: 112, 9: 128, 10: 160, 11: 192, 12: 224, 13: 256, 14: 320,
	}

	// Sample rate table (simplified for MPEG-1 Layer III)
	sampleRateTable := map[int]int{0: 44100, 1: 48000, 2: 32000}

	bitrateKbps := bitrateTable[int(bitrate)]
	sampleRateHz := sampleRateTable[int(sampleRate)]

	if bitrateKbps == 0 || sampleRateHz == 0 {
		return 0
	}

	// Calculate frame size
	frameSize := (144 * bitrateKbps * 1000) / sampleRateHz
	if padding > 0 {
		frameSize++
	}

	return frameSize
}

// getAACFrameSize calculates the size of an AAC ADTS frame
func (d *AudioDownloader) getAACFrameSize(data []byte) int {
	if len(data) < 7 || !d.isValidAACFrame(data) {
		return 0
	}

	// Extract frame length from ADTS header
	frameLength := ((int(data[3]) & 0x03) << 11) | (int(data[4]) << 3) | ((int(data[5]) >> 5) & 0x07)

	if frameLength < 7 || frameLength > 8192 { // Reasonable bounds
		return 0
	}

	return frameLength
}

// findNextSyncWord finds the position of the next potential MP3/AAC sync word
func (d *AudioDownloader) findNextSyncWord(data []byte) int {
	for i := 0; i < len(data)-1; i++ {
		if data[i] == 0xFF {
			// Check for MP3 sync
			if (data[i+1] & 0xE0) == 0xE0 {
				return i
			}
			// Check for AAC sync
			if (data[i+1] & 0xF0) == 0xF0 {
				return i
			}
		}
	}
	return -1
}
