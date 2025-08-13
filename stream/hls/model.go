package hls

import (
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// M3U8Playlist represents a parsed M3U8 playlist
type M3U8Playlist struct {
	IsValid        bool                   `json:"is_valid"`
	IsMaster       bool                   `json:"is_master"`
	IsLive         bool                   `json:"is_live"`
	Version        int                    `json:"version"`
	TargetDuration int                    `json:"target_duration"`
	MediaSequence  int                    `json:"media_sequence"`
	Segments       []M3U8Segment          `json:"segments"`
	Variants       []M3U8Variant          `json:"variants"`
	Headers        map[string]string      `json:"headers"`
	Metadata       *common.StreamMetadata `json:"metadata,omitempty"`
}

// M3U8Segment represents an individual HLS media segment
type M3U8Segment struct {
	URI       string  `json:"uri"`
	Duration  float64 `json:"duration"`
	Title     string  `json:"title,omitempty"`
	ByteRange string  `json:"byte_range,omitempty"`
}

// M3U8Variant represents a stream variant (different quality/bitrate)
type M3U8Variant struct {
	URI        string  `json:"uri"`
	Bandwidth  int     `json:"bandwidth"`
	Codecs     string  `json:"codecs,omitempty"`
	Resolution string  `json:"resolution,omitempty"`
	FrameRate  float64 `json:"frame_rate,omitempty"`
}
