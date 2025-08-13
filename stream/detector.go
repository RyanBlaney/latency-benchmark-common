package stream

import (
	"context"
	"net/http"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"github.com/RyanBlaney/latency-benchmark-common/stream/hls"
	"github.com/RyanBlaney/latency-benchmark-common/stream/icecast"
)

// Detector implements the StreamDetector interface and provides stream type
// detection and lightweight probing capabilities. It acts as an orchestrator
// that delegates stream-specific operations to the appropriate packages.
//
// The detector maintains an HTTP client with configurable timeout and redirect
// behavior, which is shared across all detection operations for consistency.
type Detector struct {
	client        *http.Client
	hlsConfig     *hls.Config
	icecastConfig *icecast.Config
}

// Config holds configuration for the stream detector
type Config struct {
	TimeoutSeconds int             `json:"timeout_seconds"`
	MaxRedirects   int             `json:"max_redirects"`
	HLS            *hls.Config     `json:"hls,omitempty"`
	ICEcast        *icecast.Config `json:"icecast,omitempty"`
}

// NewDetector creates a new stream detector with default configuration:
// - 10 second timeout
// - Maximum of 3 redirects
// - Default HLS configuration
//
// This is suitable for most general-purpose stream detection scenarios.
func NewDetector() *Detector {
	return &Detector{
		client: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				// Follow up to 3 redirects
				if len(via) >= 3 {
					return http.ErrUseLastResponse
				}
				return nil
			},
		},
		hlsConfig: nil,
	}
}

// NewDetectorWithConfig creates a new stream detector with custom configuration.
// This provides full control over detection behavior including:
// - HTTP client timeouts and redirect behavior
// - HLS-specific detection and parsing settings
// - ICEcast-specific settings (when implemented)
//
// Parameters:
//   - config: Complete detector configuration, or nil for defaults
//
// Use this when you need:
// - Custom timeout and redirect behavior
// - Specific HLS detection patterns or metadata extraction rules
// - Custom HTTP headers or buffer sizes for stream-specific operations
//
// Example:
//
//	config := &stream.Config{
//	    TimeoutSeconds: 15,
//	    MaxRedirects: 5,
//	    HLS: &hls.Config{
//	        Detection: &hls.DetectionConfig{TimeoutSeconds: 10},
//	        HTTP: &hls.HTTPConfig{BufferSize: 32768},
//	    },
//	}
//	detector := stream.NewDetectorWithConfig(config)
func NewDetectorWithConfig(config *Config) *Detector {
	// Set defaults if config is nil
	if config == nil {
		config = &Config{
			TimeoutSeconds: 10,
			MaxRedirects:   3,
			HLS:            nil, // Use package defaults
		}
	}

	// Apply defaults for missing fields
	timeoutSeconds := config.TimeoutSeconds
	if timeoutSeconds <= 0 {
		timeoutSeconds = 10
	}

	maxRedirects := config.MaxRedirects
	if maxRedirects < 0 {
		maxRedirects = 3
	}

	return &Detector{
		client: &http.Client{
			Timeout: time.Duration(timeoutSeconds) * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= maxRedirects {
					return http.ErrUseLastResponse
				}
				return nil
			},
		},
		hlsConfig: config.HLS,
	}
}

// DetectType determines the stream type for the given URL by delegating to
// stream-specific detection functions. It implements a fallback strategy:
//
//  1. Try HLS detection first (with configuration if available)
//  2. Fall back to ICEcast detection
//  3. Return unsupported if no match
//
// This method does not return an error for unsupported stream types; it only
// returns errors for actual failure conditions (network errors, etc.).
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - streamURL: The URL to analyze for stream type detection
//
// Returns:
//   - StreamType: The detected stream type or StreamTypeUnsupported
//   - error: Only for actual failures, not for unsupported types
//
// Example:
//
//	detector := stream.NewDetector()
//	streamType, err := detector.DetectType(ctx, "https://example.com/playlist.m3u8")
//	if err != nil {
//	    // Handle detection failure
//	}
//	if streamType == common.StreamTypeHLS {
//	    // Handle HLS stream
//	}
func (d *Detector) DetectType(ctx context.Context, streamURL string) (common.StreamType, error) {
	// Try HLS detection - use configuration if available
	var streamType common.StreamType
	var err error

	if d.hlsConfig != nil {
		streamType, err = hls.DetectTypeWithConfig(ctx, streamURL, d.hlsConfig)
	} else {
		streamType, err = hls.DetectType(ctx, d.client, streamURL)
	}

	if err == nil && streamType == common.StreamTypeHLS {
		return common.StreamTypeHLS, nil
	}

	// Try ICEcast detection
	if d.icecastConfig != nil {
		streamType, err = icecast.DetectTypeWithConfig(ctx, streamURL, d.icecastConfig)
	} else {
		streamType, err = icecast.DetectType(ctx, d.client, streamURL)
	}

	if err == nil && streamType == common.StreamTypeICEcast {
		return common.StreamTypeICEcast, nil
	}

	return common.StreamTypeUnsupported, nil
}

// ProbeStream performs a lightweight probe to gather basic stream metadata.
// It first detects the stream type, then delegates to the appropriate
// stream-specific probing function to extract detailed metadata.
//
// This method provides a unified interface for metadata extraction across
// different stream types while letting each package handle its own
// stream-specific logic and metadata extraction strategies.
//
// The probe operation is designed to be lightweight and fast, typically
// using HEAD requests or minimal content parsing to gather essential
// metadata without downloading significant amounts of stream data.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - streamURL: The URL to probe for metadata
//
// Returns:
//   - *StreamMetadata: Extracted metadata including stream type, format,
//     bitrate, codec, station info, and other stream-specific properties
//   - error: Returns error if stream type detection fails, probing fails,
//     or if the stream type is unsupported
//
// Example:
//
//	detector := stream.NewDetector()
//	metadata, err := detector.ProbeStream(ctx, "https://live.example.com/stream")
//	if err != nil {
//	    // Handle probing failure
//	}
//	fmt.Printf("Stream: %s (%s) - %d kbps\n",
//	    metadata.Station, metadata.Type, metadata.Bitrate)
func (d *Detector) ProbeStream(ctx context.Context, streamURL string) (*common.StreamMetadata, error) {
	streamType, err := d.DetectType(ctx, streamURL)
	if err != nil {
		return nil, err
	}

	// Delegate entirely to the appropriate package for stream-specific probing
	switch streamType {
	case common.StreamTypeHLS:
		if d.hlsConfig != nil {
			return hls.ProbeStreamWithConfig(ctx, streamURL, d.hlsConfig)
		}
		return hls.ProbeStream(ctx, d.client, streamURL)

	case common.StreamTypeICEcast:
		if d.icecastConfig != nil {
			return icecast.ProbeStreamWithConfig(ctx, streamURL, d.icecastConfig)
		}
		return icecast.ProbeStream(ctx, d.client, streamURL)

	default:
		return nil, common.NewStreamError(
			streamType, streamURL, common.ErrCodeUnsupported,
			"unsupported stream type for probing", nil,
		)
	}
}
