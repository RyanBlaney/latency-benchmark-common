package common

import (
	"context"
	"io"
	"net/http"
	"time"
)

// StreamType represents the type of an audio stream ('hls', 'icecast', 'unsupported')
type StreamType string

const (
	StreamTypeHLS         StreamType = "hls"
	StreamTypeICEcast     StreamType = "icecast"
	StreamTypeUnsupported StreamType = "unsupported"
)

// StreamMetadata contains metadata and info about the stream
// TODO: ensure metadata matches all possible content types
type StreamMetadata struct {
	URL         string            `json:"url"`
	Type        StreamType        `json:"type"`
	Format      string            `json:"format"`
	Bitrate     int               `json:"bitrate,omitempty"`
	SampleRate  int               `json:"sample_rate,omitempty"`
	Channels    int               `json:"channels,omitempty"`
	Codec       string            `json:"codec,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Title       string            `json:"title,omitempty"`
	Artist      string            `json:"artist,omitempty"`
	Genre       string            `json:"genre,omitempty"`
	Station     string            `json:"station,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Timestamp   time.Time         `json:"timestamp"`
}

// AudioData represents processed audio data
type AudioData struct {
	PCM        []float64       `json:"-"` // Raw PCM data
	SampleRate int             `json:"sample_rate"`
	Channels   int             `json:"channels"`
	Duration   time.Duration   `json:"duration"`
	Timestamp  time.Time       `json:"timestamp"`
	Metadata   *StreamMetadata `json:"metadata,omitempty"`
}

// StreamStats containing streaming statistics
type StreamStats struct {
	BytesReceived    int64         `json:"bytes_received"`
	SegmentsReceived int           `json:"segments_received,omitempty"` // HLS specific
	Dropouts         int           `json:"dropouts"`
	AverageBitrate   float64       `json:"average_bitrate"`
	ConnectionTime   time.Duration `json:"connection_time"`
	FirstByteTime    time.Duration `json:"first_byte_time"`
	BufferHealth     float64       `json:"buffer_health"` // 0.0-1.0
}

// StreamHandler defines the interface for handling different stream types
type StreamHandler interface {
	// Type returns the stream type this handler supports
	Type() StreamType

	// CanHandle determines if this handler can process the given URL
	CanHandle(ctx context.Context, url string) bool

	// Connect establishes connection to the stream
	Connect(ctx context.Context, url string) error

	// GetMetadata retrieves stream metadata
	GetMetadata() (*StreamMetadata, error)

	// ReadAudio reads audio data from the stream
	ReadAudio(ctx context.Context) (*AudioData, error)

	// ReadAudio reads a fixed length of audio data from the stream
	ReadAudioWithDuration(ctx context.Context, duration time.Duration) (*AudioData, error)

	// GetStats returns current streaming statistics
	GetStats() *StreamStats

	// GetClient returns the HTTP Client
	GetClient() *http.Client

	// Close closes the stream connection
	Close() error
}

// StreamDetector defines the interface for detecting stream types
type StreamDetector interface {
	// DetectType determines the stream type for URL, headers, and stream parsing
	DetectType(ctx context.Context, url string) (StreamType, error)

	// ProbeStream performs a lightweight probe to gather basic info. Requires the stream to support HTTP headers
	ProbeStream(ctx context.Context, url string) (*StreamMetadata, error)
}

// StreamManager defines the interface for managing multiple streams
type StreamManager interface {
	// CreateHandler creates a handler for the given stream type
	CreateHandler(streamType StreamType) (StreamHandler, error)

	// DetectAndCreate detects stream type and creates appropriate handler
	DetectAndCreate(ctx context.Context, url string) (StreamHandler, error)

	// RegisterHandler registers a custom stream handler
	RegisterHandler(streamType StreamType, handler StreamHandler) error

	// SupportedTypes returns a list of supported stream types
	SupportedTypes() []StreamType
}

// StreamValidator defines validation interface
type StreamValidator interface {
	// ValidateURL validates if URL is accessible and valid
	ValidateURL(ctx context.Context, url string) error

	// ValidateStream validates stream content and format
	ValidateStream(ctx context.Context, handler StreamHandler) error

	// ValidateAudio validates audio data quality
	ValidateAudio(data *AudioData) error
}

// AudioDecoder defines the interface for audio decoding implementations
// This allows the stream package to use different audio decoders (gmf, ffmpeg, etc.)
// without depending on any specific implementation
//
// Here is the structure of AudioData (the `any` type):
// - PCM        []float64
// - SampleRate int
// - Channels   int
// - Duration   time.Duration
// - Timestamp  time.Time
// - Metadata   *StreamMetadata
type AudioDecoder interface {
	// DecodeBytes decodes audio data from a byte slice
	// Returns decoded PCM audio data ready for processing
	DecodeBytes(data []byte) (any, error)

	// DecodeReader decodes audio data from an io.Reader
	// Useful for streaming audio data
	DecodeReader(reader io.Reader) (any, error)

	// GetConfig returns decoder configuration information
	// Useful for debugging and compatibility checks
	GetConfig() map[string]any
}
