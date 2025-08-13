package hls

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// extractContentPatterns extracts content patterns from the playlist
func (pa *PlaylistAnalyzer) extractContentPatterns(playlist *M3U8Playlist) *ContentPatterns {
	patterns := &ContentPatterns{
		Categories:   make([]string, 0),
		AdPatterns:   make([]string, 0),
		TimePatterns: make([]string, 0),
	}

	categoryMap := make(map[string]int)
	adPatternMap := make(map[string]bool)
	timePatternMap := make(map[string]bool)

	// Extract patterns from segments
	for _, segment := range playlist.Segments {
		title := segment.Title

		// Extract categories
		if strings.Contains(title, "CATEGORY:") {
			if start := strings.Index(title, "CATEGORY:"); start != -1 {
				start += len("CATEGORY:")
				var category string
				if end := strings.Index(title[start:], ","); end != -1 {
					category = title[start : start+end]
				} else {
					category = title[start:]
				}
				if category != "" {
					categoryMap[category]++
				}
			}
		}

		// Extract ad patterns
		if strings.Contains(title, "AD_BREAK") {
			adPatternMap["ad_breaks"] = true
		}
		if strings.Contains(title, "CUE-OUT") {
			adPatternMap["cue_out"] = true
		}
		if strings.Contains(title, "CUE-IN") {
			adPatternMap["cue_in"] = true
		}

		// Extract time patterns
		if strings.Contains(title, "PDT:") {
			timePatternMap["program_date_time"] = true
		}
	}

	// Find primary genre (most common category)
	var primaryGenre string
	maxCount := 0
	for category, count := range categoryMap {
		patterns.Categories = append(patterns.Categories, category)
		if count > maxCount {
			maxCount = count
			primaryGenre = category
		}
	}
	patterns.PrimaryGenre = primaryGenre

	// Convert ad patterns
	for pattern := range adPatternMap {
		patterns.AdPatterns = append(patterns.AdPatterns, pattern)
	}

	// Convert time patterns
	for pattern := range timePatternMap {
		patterns.TimePatterns = append(patterns.TimePatterns, pattern)
	}

	// Extract station info from headers
	if playlist.Headers != nil {
		if station, exists := playlist.Headers["icy-name"]; exists {
			patterns.StationInfo = station
		}
	}

	return patterns
}

// CodecExtractor provides utilities for extracting codec information
type CodecExtractor struct{}

// NewCodecExtractor creates a new codec extractor
func NewCodecExtractor() *CodecExtractor {
	return &CodecExtractor{}
}

// ExtractAudioCodec extracts the primary audio codec from a codec string
func (ce *CodecExtractor) ExtractAudioCodec(codecString string) string {
	if codecString == "" {
		return ""
	}

	codecs := strings.Split(codecString, ",")
	for _, codec := range codecs {
		codec = strings.TrimSpace(codec)
		codec = strings.Trim(codec, "\"")

		// Check for various audio codec patterns
		if strings.HasPrefix(codec, "mp4a.40.2") {
			return "aac-lc"
		} else if strings.HasPrefix(codec, "mp4a.40.5") {
			return "aac-he"
		} else if strings.HasPrefix(codec, "mp4a.40.29") {
			return "aac-he-v2"
		} else if strings.HasPrefix(codec, "mp4a") {
			return "aac"
		} else if strings.HasPrefix(codec, "mp3") {
			return "mp3"
		} else if strings.Contains(strings.ToLower(codec), "aac") {
			return "aac"
		} else if strings.Contains(strings.ToLower(codec), "mp3") {
			return "mp3"
		}
	}

	return ""
}

// ExtractVideoCodec extracts the primary video codec from a codec string
func (ce *CodecExtractor) ExtractVideoCodec(codecString string) string {
	if codecString == "" {
		return ""
	}

	codecs := strings.Split(codecString, ",")
	for _, codec := range codecs {
		codec = strings.TrimSpace(codec)
		codec = strings.Trim(codec, "\"")

		// Check for various video codec patterns
		if strings.HasPrefix(codec, "avc1") {
			return "h264"
		} else if strings.HasPrefix(codec, "hev1") || strings.HasPrefix(codec, "hvc1") {
			return "h265"
		} else if strings.HasPrefix(codec, "vp09") {
			return "vp9"
		} else if strings.HasPrefix(codec, "av01") {
			return "av1"
		}
	}

	return ""
}

// BitrateExtractor provides utilities for extracting bitrate information
type BitrateExtractor struct{}

// NewBitrateExtractor creates a new bitrate extractor
func NewBitrateExtractor() *BitrateExtractor {
	return &BitrateExtractor{}
}

// ExtractFromURL attempts to extract bitrate from URL patterns
func (be *BitrateExtractor) ExtractFromURL(url string) int {
	// Common patterns for bitrate in URLs
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`/(\d+)k/`),      // /96k/
		regexp.MustCompile(`/(\d+)/`),       // /96/
		regexp.MustCompile(`_(\d+)\.`),      // _96.aac
		regexp.MustCompile(`_(\d+)k\.`),     // _96k.aac
		regexp.MustCompile(`(\d+)kbps`),     // 96kbps
		regexp.MustCompile(`bitrate=(\d+)`), // bitrate=96
	}

	for _, pattern := range patterns {
		if matches := pattern.FindStringSubmatch(url); len(matches) > 1 {
			if bitrate, err := strconv.Atoi(matches[1]); err == nil {
				// Reasonable bitrate range check
				if bitrate >= 8 && bitrate <= 10000 {
					return bitrate
				}
			}
		}
	}

	return 0
}

// ExtractFromBandwidth converts bandwidth (bps) to bitrate (kbps)
func (be *BitrateExtractor) ExtractFromBandwidth(bandwidth int) int {
	if bandwidth <= 0 {
		return 0
	}
	return bandwidth / 1000
}

// QualityAnalyzer provides utilities for analyzing stream quality
type QualityAnalyzer struct{}

// NewQualityAnalyzer creates a new quality analyzer
func NewQualityAnalyzer() *QualityAnalyzer {
	return &QualityAnalyzer{}
}

// QualityMetrics contains quality analysis results
type QualityMetrics struct {
	AudioQuality   string  `json:"audio_quality"`
	VideoQuality   string  `json:"video_quality,omitempty"`
	OverallQuality string  `json:"overall_quality"`
	Bitrate        int     `json:"bitrate"`
	SampleRate     int     `json:"sample_rate,omitempty"`
	Channels       int     `json:"channels,omitempty"`
	Resolution     string  `json:"resolution,omitempty"`
	FrameRate      float64 `json:"frame_rate,omitempty"`
	QualityScore   float64 `json:"quality_score"`
}

// AnalyzeQuality analyzes the quality of a stream based on metadata
func (qa *QualityAnalyzer) AnalyzeQuality(metadata *common.StreamMetadata) *QualityMetrics {
	metrics := &QualityMetrics{
		Bitrate:    metadata.Bitrate,
		SampleRate: metadata.SampleRate,
		Channels:   metadata.Channels,
	}

	// Analyze audio quality
	metrics.AudioQuality = qa.classifyAudioQuality(metadata.Bitrate, metadata.SampleRate, metadata.Channels)

	// Extract video info from headers if available
	if resolution, exists := metadata.Headers["resolution"]; exists {
		metrics.Resolution = resolution
		metrics.VideoQuality = qa.classifyVideoQuality(resolution, metadata.Bitrate)
	}

	if frameRateStr, exists := metadata.Headers["frame_rate"]; exists {
		if frameRate, err := strconv.ParseFloat(frameRateStr, 64); err == nil {
			metrics.FrameRate = frameRate
		}
	}

	// Calculate overall quality
	metrics.OverallQuality = qa.calculateOverallQuality(metrics)
	metrics.QualityScore = qa.calculateQualityScore(metrics)

	return metrics
}

// classifyAudioQuality classifies audio quality based on bitrate, sample rate, and channels
func (qa *QualityAnalyzer) classifyAudioQuality(bitrate, sampleRate, channels int) string {
	// Score based on bitrate
	bitrateScore := 0
	if bitrate >= 320 {
		bitrateScore = 5
	} else if bitrate >= 256 {
		bitrateScore = 4
	} else if bitrate >= 192 {
		bitrateScore = 3
	} else if bitrate >= 128 {
		bitrateScore = 2
	} else if bitrate >= 96 {
		bitrateScore = 1
	}

	// Score based on sample rate
	sampleRateScore := 0
	if sampleRate >= 48000 {
		sampleRateScore = 2
	} else if sampleRate >= 44100 {
		sampleRateScore = 1
	}

	// Score based on channels
	channelScore := 0
	if channels >= 2 {
		channelScore = 1
	}

	totalScore := bitrateScore + sampleRateScore + channelScore

	switch {
	case totalScore >= 7:
		return "excellent"
	case totalScore >= 5:
		return "high"
	case totalScore >= 3:
		return "medium"
	case totalScore >= 1:
		return "low"
	default:
		return "poor"
	}
}

// classifyVideoQuality classifies video quality based on resolution and bitrate
func (qa *QualityAnalyzer) classifyVideoQuality(resolution string, bitrate int) string {
	switch {
	case strings.Contains(resolution, "1920x1080") || strings.Contains(resolution, "1080"):
		if bitrate >= 3000 {
			return "1080p-high"
		}
		return "1080p"
	case strings.Contains(resolution, "1280x720") || strings.Contains(resolution, "720"):
		if bitrate >= 2000 {
			return "720p-high"
		}
		return "720p"
	case strings.Contains(resolution, "640x360") || strings.Contains(resolution, "360"):
		return "360p"
	case strings.Contains(resolution, "426x240") || strings.Contains(resolution, "240"):
		return "240p"
	default:
		return "unknown"
	}
}

// calculateOverallQuality determines overall quality based on individual metrics
func (qa *QualityAnalyzer) calculateOverallQuality(metrics *QualityMetrics) string {
	audioQualities := map[string]int{
		"excellent": 5,
		"high":      4,
		"medium":    3,
		"low":       2,
		"poor":      1,
	}

	videoQualities := map[string]int{
		"1080p-high": 5,
		"1080p":      4,
		"720p-high":  4,
		"720p":       3,
		"360p":       2,
		"240p":       1,
		"unknown":    0,
	}

	audioScore := audioQualities[metrics.AudioQuality]
	videoScore := videoQualities[metrics.VideoQuality]

	// If no video, base on audio only
	if metrics.VideoQuality == "" {
		switch {
		case audioScore >= 5:
			return "excellent"
		case audioScore >= 4:
			return "high"
		case audioScore >= 3:
			return "medium"
		case audioScore >= 2:
			return "low"
		default:
			return "poor"
		}
	}

	// Combined audio/video score
	totalScore := (float64)(audioScore+videoScore) / 2.0
	switch {
	case totalScore >= 4.5:
		return "excellent"
	case totalScore >= 3.5:
		return "high"
	case totalScore >= 2.5:
		return "medium"
	case totalScore >= 1.5:
		return "low"
	default:
		return "poor"
	}
}

// calculateQualityScore calculates a numerical quality score (0-100)
func (qa *QualityAnalyzer) calculateQualityScore(metrics *QualityMetrics) float64 {
	score := 0.0

	// Bitrate contribution (0-40 points)
	if metrics.Bitrate > 0 {
		score += float64(metrics.Bitrate) / 10
		if score > 40 {
			score = 40
		}
	}

	// Sample rate contribution (0-20 points)
	if metrics.SampleRate >= 48000 {
		score += 20
	} else if metrics.SampleRate >= 44100 {
		score += 15
	} else if metrics.SampleRate >= 22050 {
		score += 10
	}

	// Channel contribution (0-10 points)
	if metrics.Channels >= 2 {
		score += 10
	} else if metrics.Channels == 1 {
		score += 5
	}

	// Video resolution contribution (0-30 points)
	if metrics.Resolution != "" {
		if strings.Contains(metrics.Resolution, "1920x1080") {
			score += 30
		} else if strings.Contains(metrics.Resolution, "1280x720") {
			score += 25
		} else if strings.Contains(metrics.Resolution, "640x360") {
			score += 15
		} else if strings.Contains(metrics.Resolution, "426x240") {
			score += 10
		}
	}

	// Cap at 100
	if score > 100 {
		score = 100
	}

	return score
}

// StreamHealthChecker provides utilities for checking stream health
type StreamHealthChecker struct {
	config *Config
}

// NewStreamHealthChecker creates a new stream health checker
func NewStreamHealthChecker(config *Config) *StreamHealthChecker {
	if config == nil {
		config = DefaultConfig()
	}
	return &StreamHealthChecker{config: config}
}

// HealthStatus represents the health status of a stream
type HealthStatus struct {
	IsHealthy          bool          `json:"is_healthy"`
	Score              float64       `json:"score"`
	Issues             []HealthIssue `json:"issues,omitempty"`
	Recommendations    []string      `json:"recommendations,omitempty"`
	LastChecked        time.Time     `json:"last_checked"`
	ResponseTime       time.Duration `json:"response_time"`
	PlaylistSize       int           `json:"playlist_size"`
	SegmentCount       int           `json:"segment_count"`
	VariantCount       int           `json:"variant_count"`
	EstimatedBandwidth int           `json:"estimated_bandwidth"`
}

// HealthIssue represents a specific health issue
type HealthIssue struct {
	Severity    string `json:"severity"` // critical, warning, info
	Category    string `json:"category"` // performance, format, metadata
	Description string `json:"description"`
	Impact      string `json:"impact"`
}

// CheckStreamHealth performs comprehensive health check on a playlist
func (shc *StreamHealthChecker) CheckStreamHealth(playlist *M3U8Playlist, metadata *common.StreamMetadata, responseTime time.Duration) *HealthStatus {
	status := &HealthStatus{
		LastChecked:     time.Now(),
		ResponseTime:    responseTime,
		PlaylistSize:    len(playlist.Segments) + len(playlist.Variants),
		SegmentCount:    len(playlist.Segments),
		VariantCount:    len(playlist.Variants),
		Issues:          make([]HealthIssue, 0),
		Recommendations: make([]string, 0),
	}

	score := 100.0

	// Check response time
	if responseTime > 5*time.Second {
		score -= 20
		status.Issues = append(status.Issues, HealthIssue{
			Severity:    "warning",
			Category:    "performance",
			Description: fmt.Sprintf("Slow response time: %v", responseTime),
			Impact:      "May cause buffering issues",
		})
	}

	// Check playlist validity
	if !playlist.IsValid {
		score -= 50
		status.Issues = append(status.Issues, HealthIssue{
			Severity:    "critical",
			Category:    "format",
			Description: "Invalid M3U8 playlist format",
			Impact:      "Stream may not play correctly",
		})
	}

	// Check for empty playlists
	if len(playlist.Segments) == 0 && len(playlist.Variants) == 0 {
		score -= 40
		status.Issues = append(status.Issues, HealthIssue{
			Severity:    "critical",
			Category:    "format",
			Description: "Empty playlist - no segments or variants",
			Impact:      "No content available to play",
		})
	}

	// Check bitrate consistency for variants
	if len(playlist.Variants) > 1 {
		bandwidths := make([]int, len(playlist.Variants))
		for i, variant := range playlist.Variants {
			bandwidths[i] = variant.Bandwidth
		}

		if !shc.isBandwidthProgression(bandwidths) {
			score -= 10
			status.Issues = append(status.Issues, HealthIssue{
				Severity:    "warning",
				Category:    "format",
				Description: "Inconsistent bandwidth progression in variants",
				Impact:      "May cause adaptive streaming issues",
			})
			status.Recommendations = append(status.Recommendations, "Ensure variants have logical bandwidth progression")
		}
	}

	// Check metadata completeness
	metadataScore := shc.checkMetadataCompleteness(metadata)
	score -= (100 - metadataScore) * 0.2 // 20% weight for metadata

	// Check segment duration consistency
	if len(playlist.Segments) > 0 {
		durationConsistency := shc.checkSegmentDurationConsistency(playlist.Segments)
		if durationConsistency < 0.8 {
			score -= 15
			status.Issues = append(status.Issues, HealthIssue{
				Severity:    "warning",
				Category:    "format",
				Description: "Inconsistent segment durations",
				Impact:      "May cause playback stuttering",
			})
		}
	}

	// Estimate bandwidth
	if len(playlist.Variants) > 0 {
		maxBandwidth := 0
		for _, variant := range playlist.Variants {
			if variant.Bandwidth > maxBandwidth {
				maxBandwidth = variant.Bandwidth
			}
		}
		status.EstimatedBandwidth = maxBandwidth
	} else if metadata.Bitrate > 0 {
		status.EstimatedBandwidth = metadata.Bitrate * 1000
	}

	status.Score = score
	status.IsHealthy = score >= 70.0

	// Add general recommendations
	if status.Score < 90 {
		status.Recommendations = append(status.Recommendations, "Consider optimizing playlist structure")
	}
	if responseTime > 2*time.Second {
		status.Recommendations = append(status.Recommendations, "Improve CDN response times")
	}

	return status
}

// isBandwidthProgression checks if bandwidths follow a logical progression
func (shc *StreamHealthChecker) isBandwidthProgression(bandwidths []int) bool {
	if len(bandwidths) < 2 {
		return true
	}

	// Sort to check progression
	sorted := make([]int, len(bandwidths))
	copy(sorted, bandwidths)

	// Simple bubble sort for small arrays
	for i := 0; i < len(sorted)-1; i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			if sorted[j] > sorted[j+1] {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}

	// Check if there's reasonable progression (each step should be meaningful)
	for i := 1; i < len(sorted); i++ {
		ratio := float64(sorted[i]) / float64(sorted[i-1])
		if ratio < 1.2 || ratio > 5.0 { // Too close or too far apart
			return false
		}
	}

	return true
}

// checkMetadataCompleteness returns a score (0-100) for metadata completeness
func (shc *StreamHealthChecker) checkMetadataCompleteness(metadata *common.StreamMetadata) float64 {
	score := 0.0

	// Essential fields
	if metadata.Codec != "" {
		score += 25
	}
	if metadata.Bitrate > 0 {
		score += 25
	}
	if metadata.SampleRate > 0 {
		score += 20
	}
	if metadata.Channels > 0 {
		score += 15
	}

	// Optional but valuable fields
	if metadata.Genre != "" {
		score += 5
	}
	if metadata.Station != "" {
		score += 5
	}
	if metadata.ContentType != "" {
		score += 5
	}

	return score
}

// checkSegmentDurationConsistency returns consistency score (0-1)
func (shc *StreamHealthChecker) checkSegmentDurationConsistency(segments []M3U8Segment) float64 {
	if len(segments) < 2 {
		return 1.0
	}

	// Calculate average duration
	totalDuration := 0.0
	for _, segment := range segments {
		totalDuration += segment.Duration
	}
	avgDuration := totalDuration / float64(len(segments))

	// Calculate variance
	variance := 0.0
	for _, segment := range segments {
		diff := segment.Duration - avgDuration
		variance += diff * diff
	}
	variance /= float64(len(segments))

	// Calculate coefficient of variation
	if avgDuration == 0 {
		return 0.0
	}

	stdDev := variance
	cv := stdDev / avgDuration

	// Convert to consistency score (lower CV = higher consistency)
	if cv < 0.1 {
		return 1.0
	} else if cv < 0.3 {
		return 0.8
	} else if cv < 0.5 {
		return 0.6
	} else {
		return 0.4
	}
}

// PerformanceProfiler provides utilities for performance profiling
type PerformanceProfiler struct {
	startTime time.Time
	events    []ProfileEvent
}

// ProfileEvent represents a profiling event
type ProfileEvent struct {
	Name      string                 `json:"name"`
	Timestamp time.Time              `json:"timestamp"`
	Duration  time.Duration          `json:"duration"`
	Details   map[string]interface{} `json:"details,omitempty"`
}

// NewPerformanceProfiler creates a new performance profiler
func NewPerformanceProfiler() *PerformanceProfiler {
	return &PerformanceProfiler{
		startTime: time.Now(),
		events:    make([]ProfileEvent, 0),
	}
}

// StartEvent starts timing an event
func (pp *PerformanceProfiler) StartEvent(name string) *EventTimer {
	return &EventTimer{
		profiler: pp,
		name:     name,
		start:    time.Now(),
	}
}

// EventTimer tracks the duration of an event
type EventTimer struct {
	profiler *PerformanceProfiler
	name     string
	start    time.Time
	details  map[string]interface{}
}

// AddDetail adds detail information to the event
func (et *EventTimer) AddDetail(key string, value interface{}) {
	if et.details == nil {
		et.details = make(map[string]interface{})
	}
	et.details[key] = value
}

// End stops timing the event and records it
func (et *EventTimer) End() {
	duration := time.Since(et.start)
	event := ProfileEvent{
		Name:      et.name,
		Timestamp: et.start,
		Duration:  duration,
		Details:   et.details,
	}
	et.profiler.events = append(et.profiler.events, event)
}

// GetProfile returns the complete performance profile
func (pp *PerformanceProfiler) GetProfile() *PerformanceProfile {
	totalDuration := time.Since(pp.startTime)

	profile := &PerformanceProfile{
		TotalDuration: totalDuration,
		EventCount:    len(pp.events),
		Events:        pp.events,
		Summary:       pp.generateSummary(),
	}

	return profile
}

// PerformanceProfile contains complete performance information
type PerformanceProfile struct {
	TotalDuration time.Duration       `json:"total_duration"`
	EventCount    int                 `json:"event_count"`
	Events        []ProfileEvent      `json:"events"`
	Summary       *PerformanceSummary `json:"summary"`
}

// PerformanceSummary contains summary statistics
type PerformanceSummary struct {
	SlowestEvent    string         `json:"slowest_event"`
	SlowestDuration time.Duration  `json:"slowest_duration"`
	FastestEvent    string         `json:"fastest_event"`
	FastestDuration time.Duration  `json:"fastest_duration"`
	AverageDuration time.Duration  `json:"average_duration"`
	EventBreakdown  map[string]int `json:"event_breakdown"`
}

// generateSummary generates performance summary statistics
func (pp *PerformanceProfiler) generateSummary() *PerformanceSummary {
	if len(pp.events) == 0 {
		return &PerformanceSummary{
			EventBreakdown: make(map[string]int),
		}
	}

	summary := &PerformanceSummary{
		EventBreakdown: make(map[string]int),
	}

	var totalDuration time.Duration
	summary.SlowestDuration = pp.events[0].Duration
	summary.FastestDuration = pp.events[0].Duration
	summary.SlowestEvent = pp.events[0].Name
	summary.FastestEvent = pp.events[0].Name

	for _, event := range pp.events {
		totalDuration += event.Duration
		summary.EventBreakdown[event.Name]++

		if event.Duration > summary.SlowestDuration {
			summary.SlowestDuration = event.Duration
			summary.SlowestEvent = event.Name
		}

		if event.Duration < summary.FastestDuration {
			summary.FastestDuration = event.Duration
			summary.FastestEvent = event.Name
		}
	}

	summary.AverageDuration = totalDuration / time.Duration(len(pp.events))

	return summary
}

// ValidationError represents validation errors in HLS processing
type ValidationError struct {
	Field   string
	Value   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s' with value '%s': %s", e.Field, e.Value, e.Message)
}

// PlaylistValidator provides validation utilities for HLS playlists
type PlaylistValidator struct {
	config *Config
}

// NewPlaylistValidator creates a new playlist validator
func NewPlaylistValidator(config *Config) *PlaylistValidator {
	if config == nil {
		config = DefaultConfig()
	}
	return &PlaylistValidator{config: config}
}

// ValidatePlaylist validates an M3U8 playlist structure
func (pv *PlaylistValidator) ValidatePlaylist(playlist *M3U8Playlist) []ValidationError {
	var errors []ValidationError

	// Basic structure validation
	if !playlist.IsValid {
		errors = append(errors, ValidationError{
			Field:   "IsValid",
			Value:   "false",
			Message: "playlist is marked as invalid",
		})
	}

	// Version validation
	if playlist.Version < 1 || playlist.Version > 10 {
		errors = append(errors, ValidationError{
			Field:   "Version",
			Value:   strconv.Itoa(playlist.Version),
			Message: "version should be between 1 and 10",
		})
	}

	// Target duration validation for media playlists
	if !playlist.IsMaster && playlist.TargetDuration <= 0 {
		errors = append(errors, ValidationError{
			Field:   "TargetDuration",
			Value:   strconv.Itoa(playlist.TargetDuration),
			Message: "target duration must be positive for media playlists",
		})
	}

	// Segment validation
	for i, segment := range playlist.Segments {
		if errs := pv.validateSegment(segment, i); len(errs) > 0 {
			errors = append(errors, errs...)
		}
	}

	// Variant validation
	for i, variant := range playlist.Variants {
		if errs := pv.validateVariant(variant, i); len(errs) > 0 {
			errors = append(errors, errs...)
		}
	}

	return errors
}

// validateSegment validates a single segment
func (pv *PlaylistValidator) validateSegment(segment M3U8Segment, index int) []ValidationError {
	var errors []ValidationError
	prefix := fmt.Sprintf("Segment[%d]", index)

	// URI validation
	if segment.URI == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".URI",
			Value:   "",
			Message: "segment URI cannot be empty",
		})
	} else if pv.config.Parser.ValidateURIs {
		if _, err := url.Parse(segment.URI); err != nil {
			errors = append(errors, ValidationError{
				Field:   prefix + ".URI",
				Value:   segment.URI,
				Message: "invalid URI format: " + err.Error(),
			})
		}
	}

	// Duration validation
	if segment.Duration < 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".Duration",
			Value:   fmt.Sprintf("%.2f", segment.Duration),
			Message: "segment duration cannot be negative",
		})
	}

	return errors
}

// validateVariant validates a single variant
func (pv *PlaylistValidator) validateVariant(variant M3U8Variant, index int) []ValidationError {
	var errors []ValidationError
	prefix := fmt.Sprintf("Variant[%d]", index)

	// URI validation
	if variant.URI == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".URI",
			Value:   "",
			Message: "variant URI cannot be empty",
		})
	} else if pv.config.Parser.ValidateURIs {
		if _, err := url.Parse(variant.URI); err != nil {
			errors = append(errors, ValidationError{
				Field:   prefix + ".URI",
				Value:   variant.URI,
				Message: "invalid URI format: " + err.Error(),
			})
		}
	}

	// Bandwidth validation
	if variant.Bandwidth <= 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".Bandwidth",
			Value:   strconv.Itoa(variant.Bandwidth),
			Message: "variant bandwidth must be positive",
		})
	}

	// Frame rate validation
	if variant.FrameRate < 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".FrameRate",
			Value:   fmt.Sprintf("%.2f", variant.FrameRate),
			Message: "frame rate cannot be negative",
		})
	}

	return errors
}

// URLResolver provides utilities for resolving HLS URLs
type URLResolver struct {
	baseURL *url.URL
}

// NewURLResolver creates a new URL resolver
func NewURLResolver(baseURL string) (*URLResolver, error) {
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, baseURL,
			common.ErrCodeInvalidFormat, "invalid base URL", nil)
	}
	return &URLResolver{baseURL: parsedURL}, nil
}

// ResolveURL resolves a relative URL against the base URL
func (ur *URLResolver) ResolveURL(relativeURL string) string {
	// If it's already an absolute URL, return as-is
	if strings.HasPrefix(relativeURL, "http://") || strings.HasPrefix(relativeURL, "https://") {
		return relativeURL
	}

	// Parse relative URI
	rel, err := url.Parse(relativeURL)
	if err != nil {
		return relativeURL // Return original if can't parse
	}

	// Resolve relative to base
	resolved := ur.baseURL.ResolveReference(rel)
	return resolved.String()
}

// ResolveSegmentURLs resolves all segment URLs in a playlist
func (ur *URLResolver) ResolveSegmentURLs(playlist *M3U8Playlist) []string {
	urls := make([]string, 0, len(playlist.Segments))
	for _, segment := range playlist.Segments {
		urls = append(urls, ur.ResolveURL(segment.URI))
	}
	return urls
}

// ResolveVariantURLs resolves all variant URLs in a playlist
func (ur *URLResolver) ResolveVariantURLs(playlist *M3U8Playlist) []string {
	urls := make([]string, 0, len(playlist.Variants))
	for _, variant := range playlist.Variants {
		urls = append(urls, ur.ResolveURL(variant.URI))
	}
	return urls
}

// PlaylistAnalyzer provides analysis utilities for HLS playlists
type PlaylistAnalyzer struct{}

// NewPlaylistAnalyzer creates a new playlist analyzer
func NewPlaylistAnalyzer() *PlaylistAnalyzer {
	return &PlaylistAnalyzer{}
}

// AnalyzePlaylist provides comprehensive analysis of a playlist
func (pa *PlaylistAnalyzer) AnalyzePlaylist(playlist *M3U8Playlist) *PlaylistAnalysis {
	analysis := &PlaylistAnalysis{
		TotalSegments:     len(playlist.Segments),
		TotalVariants:     len(playlist.Variants),
		IsMaster:          playlist.IsMaster,
		IsLive:            playlist.IsLive,
		Version:           playlist.Version,
		TargetDuration:    playlist.TargetDuration,
		MediaSequence:     playlist.MediaSequence,
		EstimatedDuration: pa.calculateTotalDuration(playlist.Segments),
	}

	// Analyze segments
	if len(playlist.Segments) > 0 {
		analysis.SegmentAnalysis = pa.analyzeSegments(playlist.Segments)
	}

	// Analyze variants
	if len(playlist.Variants) > 0 {
		analysis.VariantAnalysis = pa.analyzeVariants(playlist.Variants)
	}

	// Extract content patterns
	analysis.ContentPatterns = pa.extractContentPatterns(playlist)

	return analysis
}

// PlaylistAnalysis contains the results of playlist analysis
type PlaylistAnalysis struct {
	TotalSegments     int              `json:"total_segments"`
	TotalVariants     int              `json:"total_variants"`
	IsMaster          bool             `json:"is_master"`
	IsLive            bool             `json:"is_live"`
	Version           int              `json:"version"`
	TargetDuration    int              `json:"target_duration"`
	MediaSequence     int              `json:"media_sequence"`
	EstimatedDuration time.Duration    `json:"estimated_duration"`
	SegmentAnalysis   *SegmentAnalysis `json:"segment_analysis,omitempty"`
	VariantAnalysis   *VariantAnalysis `json:"variant_analysis,omitempty"`
	ContentPatterns   *ContentPatterns `json:"content_patterns,omitempty"`
}

// SegmentAnalysis contains analysis of playlist segments
type SegmentAnalysis struct {
	AverageDuration    float64       `json:"average_duration"`
	MinDuration        float64       `json:"min_duration"`
	MaxDuration        float64       `json:"max_duration"`
	TotalDuration      time.Duration `json:"total_duration"`
	HasAdBreaks        bool          `json:"has_ad_breaks"`
	HasDiscontinuities bool          `json:"has_discontinuities"`
	CategoryCount      int           `json:"category_count"`
}

// VariantAnalysis contains analysis of playlist variants
type VariantAnalysis struct {
	MinBandwidth           int            `json:"min_bandwidth"`
	MaxBandwidth           int            `json:"max_bandwidth"`
	AvgBandwidth           int            `json:"avg_bandwidth"`
	CodecDistribution      map[string]int `json:"codec_distribution"`
	ResolutionDistribution map[string]int `json:"resolution_distribution"`
	RecommendedVariant     *M3U8Variant   `json:"recommended_variant,omitempty"`
}

// ContentPatterns contains detected content patterns
type ContentPatterns struct {
	PrimaryGenre string   `json:"primary_genre,omitempty"`
	Categories   []string `json:"categories,omitempty"`
	StationInfo  string   `json:"station_info,omitempty"`
	AdPatterns   []string `json:"ad_patterns,omitempty"`
	TimePatterns []string `json:"time_patterns,omitempty"`
}

// calculateTotalDuration calculates the total duration of all segments
func (pa *PlaylistAnalyzer) calculateTotalDuration(segments []M3U8Segment) time.Duration {
	var total float64
	for _, segment := range segments {
		total += segment.Duration
	}
	return time.Duration(total * float64(time.Second))
}

// analyzeSegments provides detailed analysis of segments
func (pa *PlaylistAnalyzer) analyzeSegments(segments []M3U8Segment) *SegmentAnalysis {
	if len(segments) == 0 {
		return nil
	}

	analysis := &SegmentAnalysis{}

	var totalDuration float64
	minDuration := segments[0].Duration
	maxDuration := segments[0].Duration
	categoryCount := make(map[string]bool)

	for _, segment := range segments {
		totalDuration += segment.Duration

		if segment.Duration < minDuration {
			minDuration = segment.Duration
		}
		if segment.Duration > maxDuration {
			maxDuration = segment.Duration
		}

		// Check for ad breaks
		if strings.Contains(segment.Title, "AD_BREAK") ||
			strings.Contains(segment.Title, "CUE-OUT") ||
			strings.Contains(segment.Title, "CUE-IN") {
			analysis.HasAdBreaks = true
		}

		// Check for discontinuities
		if strings.Contains(segment.Title, "DISCONTINUITY") {
			analysis.HasDiscontinuities = true
		}

		// Extract categories
		if strings.Contains(segment.Title, "CATEGORY:") {
			if start := strings.Index(segment.Title, "CATEGORY:"); start != -1 {
				start += len("CATEGORY:")
				var category string
				if end := strings.Index(segment.Title[start:], ","); end != -1 {
					category = segment.Title[start : start+end]
				} else {
					category = segment.Title[start:]
				}
				if category != "" {
					categoryCount[category] = true
				}
			}
		}
	}

	analysis.AverageDuration = totalDuration / float64(len(segments))
	analysis.MinDuration = minDuration
	analysis.MaxDuration = maxDuration
	analysis.TotalDuration = time.Duration(totalDuration * float64(time.Second))
	analysis.CategoryCount = len(categoryCount)

	return analysis
}

// analyzeVariants provides detailed analysis of variants
func (pa *PlaylistAnalyzer) analyzeVariants(variants []M3U8Variant) *VariantAnalysis {
	if len(variants) == 0 {
		return nil
	}

	analysis := &VariantAnalysis{
		CodecDistribution:      make(map[string]int),
		ResolutionDistribution: make(map[string]int),
	}

	minBandwidth := variants[0].Bandwidth
	maxBandwidth := variants[0].Bandwidth
	totalBandwidth := 0
	var recommendedVariant *M3U8Variant

	for i := range variants {
		variant := &variants[i]

		if variant.Bandwidth < minBandwidth {
			minBandwidth = variant.Bandwidth
		}
		if variant.Bandwidth > maxBandwidth {
			maxBandwidth = variant.Bandwidth
			recommendedVariant = variant
		}

		totalBandwidth += variant.Bandwidth

		// Codec distribution
		if variant.Codecs != "" {
			codecs := strings.Split(variant.Codecs, ",")
			for _, codec := range codecs {
				codec = strings.TrimSpace(codec)
				analysis.CodecDistribution[codec]++
			}
		}

		// Resolution distribution
		if variant.Resolution != "" {
			analysis.ResolutionDistribution[variant.Resolution]++
		}
	}

	analysis.MinBandwidth = minBandwidth
	analysis.MaxBandwidth = maxBandwidth
	analysis.AvgBandwidth = totalBandwidth / len(variants)
	analysis.RecommendedVariant = recommendedVariant

	return analysis
}
