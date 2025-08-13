package stream

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// Manager orchestrates complex stream operations like parallel audio extraction
// and multi-stream synchronization. It uses Factory and Detector to handle
// different stream types transparently.
type Manager struct {
	factory *Factory
	config  *ManagerConfig
}

// ManagerConfig holds configuration for the stream manager
type ManagerConfig struct {
	// Timeout for individual stream operations
	StreamTimeout time.Duration `json:"stream_timeout"`
	// Overall timeout for parallel operations
	OverallTimeout time.Duration `json:"overall_timeout"`
	// Maximum number of concurrent streams
	MaxConcurrentStreams int `json:"max_concurrent_streams"`
	// Buffer size for result channels
	ResultBufferSize int `json:"result_buffer_size"`
}

// AudioExtractionResult represents the result of extracting audio from a single stream
type AudioExtractionResult struct {
	URL             string                 `json:"url"`
	AudioData       *common.AudioData      `json:"audio_data,omitempty"`
	Metadata        *common.StreamMetadata `json:"metadata,omitempty"`
	Error           error                  `json:"error,omitempty"`
	StartTime       time.Time              `json:"start_time"`
	EndTime         time.Time              `json:"end_time"`
	Duration        time.Duration          `json:"duration"`
	StreamType      common.StreamType      `json:"stream_type"`
	TimeToFirstByte time.Duration          `json:"time_to_first_byte"`
	ConnectionTime  time.Duration          `json:"connection_time"`
}

// ParallelExtractionResult contains results from parallel audio extraction
type ParallelExtractionResult struct {
	Results           []*AudioExtractionResult `json:"results"`
	TotalDuration     time.Duration            `json:"total_duration"`
	SuccessfulStreams int                      `json:"successful_streams"`
	FailedStreams     int                      `json:"failed_streams"`
	MaxTimeDiff       time.Duration            `json:"max_time_diff"` // Max difference between start times
}

// NewManager creates a new stream manager with default configuration
func NewManager() *Manager {
	return NewManagerWithConfig(nil)
}

// NewManagerWithConfig creates a new stream manager with custom configuration
func NewManagerWithConfig(config *ManagerConfig) *Manager {
	if config == nil {
		config = &ManagerConfig{
			StreamTimeout:        300 * time.Second,
			OverallTimeout:       600 * time.Second,
			MaxConcurrentStreams: 10,
			ResultBufferSize:     10,
		}
	}

	return &Manager{
		factory: NewFactory(),
		config:  config,
	}
}

// ExtractAudioParallel extracts audio from multiple streams simultaneously
// This is designed for fingerprinting scenarios where you need synchronized
// audio data from multiple sources
func (m *Manager) ExtractAudioParallel(ctx context.Context, urls []string, targetDuration time.Duration) (*ParallelExtractionResult, error) {
	if len(urls) == 0 {
		return nil, fmt.Errorf("no URLs provided")
	}

	if len(urls) > m.config.MaxConcurrentStreams {
		return nil, fmt.Errorf("too many streams: %d > %d", len(urls), m.config.MaxConcurrentStreams)
	}

	logger := logging.WithFields(logging.Fields{
		"component":       "stream_manager",
		"function":        "ExtractAudioParallel",
		"stream_count":    len(urls),
		"target_duration": targetDuration.Seconds(),
	})

	logger.Debug("Starting parallel audio extraction")

	// Create overall timeout context
	overallCtx, cancel := context.WithTimeout(ctx, m.config.OverallTimeout)
	defer cancel()

	// Prepare synchronization
	var wg sync.WaitGroup
	resultChan := make(chan *AudioExtractionResult, len(urls))

	// Record the start time for synchronization analysis
	globalStartTime := time.Now()

	// Start all extractions simultaneously
	for i, url := range urls {
		wg.Add(1)
		go func(index int, streamURL string) {
			defer wg.Done()

			// Create individual stream timeout context
			streamCtx, streamCancel := context.WithTimeout(overallCtx, m.config.StreamTimeout)
			defer streamCancel()

			result := m.extractSingleStream(streamCtx, streamURL, targetDuration, index)
			resultChan <- result
		}(i, url)
	}

	// Wait for all extractions to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	results := make([]*AudioExtractionResult, 0, len(urls))
	for result := range resultChan {
		results = append(results, result)
	}

	// Analyze results
	totalDuration := time.Since(globalStartTime)
	successCount := 0
	failedCount := 0
	var minStartTime, maxStartTime time.Time

	for i, result := range results {
		if result.Error == nil {
			successCount++
		} else {
			failedCount++
		}

		// Track timing for synchronization analysis
		if i == 0 {
			minStartTime = result.StartTime
			maxStartTime = result.StartTime
		} else {
			if result.StartTime.Before(minStartTime) {
				minStartTime = result.StartTime
			}
			if result.StartTime.After(maxStartTime) {
				maxStartTime = result.StartTime
			}
		}
	}

	maxTimeDiff := maxStartTime.Sub(minStartTime)

	logger.Debug("Parallel audio extraction completed", logging.Fields{
		"total_duration":     totalDuration.Milliseconds(),
		"successful_streams": successCount,
		"failed_streams":     failedCount,
		"max_time_diff_ms":   maxTimeDiff.Milliseconds(),
	})

	return &ParallelExtractionResult{
		Results:           results,
		TotalDuration:     totalDuration,
		SuccessfulStreams: successCount,
		FailedStreams:     failedCount,
		MaxTimeDiff:       maxTimeDiff,
	}, nil
}

// extractSingleStream extracts audio from a single stream
func (m *Manager) extractSingleStream(ctx context.Context, url string, targetDuration time.Duration, index int) *AudioExtractionResult {
	startTime := time.Now()

	logger := logging.WithFields(logging.Fields{
		"component":    "stream_manager",
		"function":     "extractSingleStream",
		"stream_index": index,
		"url":          url,
	})

	result := &AudioExtractionResult{
		URL:       url,
		StartTime: startTime,
	}

	// Detect and create handler
	handler, err := m.factory.DetectAndCreate(ctx, url)
	if err != nil {
		result.Error = fmt.Errorf("failed to create handler: %w", err)
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}
	defer handler.Close()

	result.StreamType = handler.Type()

	connectionStart := time.Now()

	// Connect to stream
	if err := handler.Connect(ctx, url); err != nil {
		result.Error = fmt.Errorf("failed to connect to stream: %w", err)
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		result.ConnectionTime = time.Since(connectionStart)
		return result
	}

	// Connection successful
	result.ConnectionTime = time.Since(connectionStart)

	// Measure TTFB
	ttfbStart := time.Now()

	// Try to read a small amount of data to measure TTFB
	firstByteData, err := m.measureFirstByte(ctx, handler)

	if err != nil {
		// If we can't measure TTFB, fall back to connection time
		logger.Warn("Could not measure TTFB, using connection time", logging.Fields{
			"error": err.Error(),
		})
		result.TimeToFirstByte = result.ConnectionTime
	} else {
		result.TimeToFirstByte = time.Since(ttfbStart)
		logger.Debug("TTFB measured successfully", logging.Fields{
			"ttfb_ms":     result.TimeToFirstByte.Milliseconds(),
			"first_bytes": len(firstByteData),
		})
	}

	logger.Debug("Stream connection established", logging.Fields{
		"connection_time_ms": result.ConnectionTime.Milliseconds(),
		"ttfb_ms":            result.TimeToFirstByte.Milliseconds(),
		"ttfb_delta_ms":      (result.TimeToFirstByte - result.ConnectionTime).Milliseconds(),
	})

	// Get metadata
	metadata, err := handler.GetMetadata()
	if err != nil {
		logger.Warn("Failed to get metadata, continuing without it", logging.Fields{
			"error": err.Error(),
		})
	} else {
		result.Metadata = metadata
	}

	// Extract audio data with specified duration
	audioData, err := handler.ReadAudioWithDuration(ctx, targetDuration)
	if err != nil {
		result.Error = fmt.Errorf("failed to read audio: %w", err)
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	result.AudioData = audioData
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	logger.Debug("Stream extraction completed", logging.Fields{
		"audio_samples":      len(audioData.PCM),
		"audio_duration_sec": audioData.Duration.Seconds(),
		"extraction_time_ms": result.Duration.Milliseconds(),
	})

	return result
}

// measureFirstByte attempts to read the first few bytes from a stream to measure TTFB
func (m *Manager) measureFirstByte(ctx context.Context, handler common.StreamHandler) ([]byte, error) {
	// Create a short timeout context for TTFB measurement
	ttfbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Try to get the HTTP response body for direct reading
	if httpHandler, ok := handler.(interface{ GetResponse() *http.Response }); ok {
		resp := httpHandler.GetResponse()
		if resp != nil && resp.Body != nil {
			// Read just a few bytes to trigger TTFB
			buffer := make([]byte, 64)
			n, err := resp.Body.Read(buffer)
			if err != nil && err != io.EOF {
				return nil, err
			}
			return buffer[:n], nil
		}
	}

	// Fallback: try to read a very small amount using ReadAudioWithDuration
	// This is less accurate but works with any handler
	smallDuration := 100 * time.Millisecond
	audioData, err := handler.ReadAudioWithDuration(ttfbCtx, smallDuration)
	if err != nil {
		return nil, err
	}

	// Convert some PCM samples back to bytes as an approximation
	if len(audioData.PCM) > 0 {
		return []byte{0x1}, nil // Just signal that we got data
	}

	return nil, fmt.Errorf("no data received")
}

// ExtractAudioSequential extracts audio from multiple streams one after another
// This is useful for testing or when you don't need temporal synchronization
func (m *Manager) ExtractAudioSequential(ctx context.Context, urls []string, targetDuration time.Duration) (*ParallelExtractionResult, error) {
	if len(urls) == 0 {
		return nil, fmt.Errorf("no URLs provided")
	}

	logger := logging.WithFields(logging.Fields{
		"component":       "stream_manager",
		"function":        "ExtractAudioSequential",
		"stream_count":    len(urls),
		"target_duration": targetDuration.Seconds(),
	})

	logger.Debug("Starting sequential audio extraction")

	globalStartTime := time.Now()
	results := make([]*AudioExtractionResult, 0, len(urls))

	for i, url := range urls {
		// Create individual stream timeout context
		streamCtx, streamCancel := context.WithTimeout(ctx, m.config.StreamTimeout)

		result := m.extractSingleStream(streamCtx, url, targetDuration, i)
		results = append(results, result)

		streamCancel()

		// Check if overall context is cancelled
		if ctx.Err() != nil {
			break
		}
	}

	// Analyze results
	totalDuration := time.Since(globalStartTime)
	successCount := 0
	failedCount := 0

	for _, result := range results {
		if result.Error == nil {
			successCount++
		} else {
			failedCount++
		}
	}

	logger.Debug("Sequential audio extraction completed", logging.Fields{
		"total_duration":     totalDuration.Milliseconds(),
		"successful_streams": successCount,
		"failed_streams":     failedCount,
	})

	return &ParallelExtractionResult{
		Results:           results,
		TotalDuration:     totalDuration,
		SuccessfulStreams: successCount,
		FailedStreams:     failedCount,
		MaxTimeDiff:       0, // No timing difference in sequential mode
	}, nil
}

// ValidateExtractionResults validates that extraction results are suitable for fingerprinting
func (m *Manager) ValidateExtractionResults(results *ParallelExtractionResult, minSuccessfulStreams int, maxTimeDiffMs int64) error {
	if results.SuccessfulStreams < minSuccessfulStreams {
		return fmt.Errorf("insufficient successful streams: %d < %d", results.SuccessfulStreams, minSuccessfulStreams)
	}

	if results.MaxTimeDiff.Milliseconds() > maxTimeDiffMs {
		return fmt.Errorf("timing difference too large: %dms > %dms", results.MaxTimeDiff.Milliseconds(), maxTimeDiffMs)
	}

	// Validate audio data consistency
	var referenceSampleRate int
	var referenceChannels int

	for i, result := range results.Results {
		if result.Error != nil {
			continue
		}

		if result.AudioData == nil {
			return fmt.Errorf("stream %d: missing audio data", i)
		}

		if len(result.AudioData.PCM) == 0 {
			return fmt.Errorf("stream %d: empty audio data", i)
		}

		// Check for consistency across streams
		if referenceSampleRate == 0 {
			referenceSampleRate = result.AudioData.SampleRate
			referenceChannels = result.AudioData.Channels
		} else {
			if result.AudioData.SampleRate != referenceSampleRate {
				return fmt.Errorf("stream %d: sample rate mismatch: %d != %d", i, result.AudioData.SampleRate, referenceSampleRate)
			}
			if result.AudioData.Channels != referenceChannels {
				return fmt.Errorf("stream %d: channel count mismatch: %d != %d", i, result.AudioData.Channels, referenceChannels)
			}
		}
	}

	return nil
}

// GetFactory returns the underlying factory for advanced usage
func (m *Manager) GetFactory() *Factory {
	return m.factory
}

// GetConfig returns the current manager configuration
func (m *Manager) GetConfig() *ManagerConfig {
	return m.config
}

// UpdateConfig updates the manager configuration
func (m *Manager) UpdateConfig(config *ManagerConfig) {
	if config != nil {
		m.config = config
	}
}
