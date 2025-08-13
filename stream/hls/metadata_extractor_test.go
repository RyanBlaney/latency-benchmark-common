package hls

import (
	"regexp"
	"strings"
	"testing"

	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetadataExtractor(t *testing.T) {
	extractor := NewMetadataExtractor()

	assert.NotNil(t, extractor)
	assert.NotEmpty(t, extractor.urlPatterns)
	assert.NotEmpty(t, extractor.headerMappings)
	assert.NotEmpty(t, extractor.segmentAnalyzers)
}

func TestExtractMetadata(t *testing.T) {
	extractor := NewMetadataExtractor()

	t.Run("basic metadata extraction", func(t *testing.T) {
		playlist := &M3U8Playlist{
			IsValid:  true,
			IsMaster: false,
			IsLive:   true,
			Version:  3,
			Headers: map[string]string{
				"icy-name":    "Test Station",
				"icy-genre":   "Rock",
				"icy-bitrate": "128",
			},
			Segments: []M3U8Segment{
				{URI: "segment1.ts", Duration: 10.0, Title: ""},
				{URI: "segment2.ts", Duration: 10.0, Title: ""},
			},
		}

		streamURL := "https://example.com/128k/playlist.m3u8"

		metadata := extractor.ExtractMetadata(playlist, streamURL)

		require.NotNil(t, metadata)
		assert.Equal(t, common.StreamTypeHLS, metadata.Type)
		assert.Equal(t, streamURL, metadata.URL)
		assert.Equal(t, "Test Station", metadata.Station)
		assert.Equal(t, "Rock", metadata.Genre)
		assert.Equal(t, 128, metadata.Bitrate)      // From URL pattern and headers
		assert.Equal(t, "aac", metadata.Codec)      // Default
		assert.Equal(t, 2, metadata.Channels)       // Default
		assert.Equal(t, 44100, metadata.SampleRate) // Default
		assert.NotNil(t, metadata.Headers)
		assert.NotZero(t, metadata.Timestamp)
	})

	t.Run("master playlist with variants", func(t *testing.T) {
		playlist := &M3U8Playlist{
			IsValid:  true,
			IsMaster: true,
			Version:  3,
			Headers:  make(map[string]string),
			Variants: []M3U8Variant{
				{URI: "480p.m3u8", Bandwidth: 1280000, Codecs: "avc1.42e00a,mp4a.40.2"},
				{URI: "720p.m3u8", Bandwidth: 2560000, Codecs: "avc1.42e00a,mp4a.40.2"},
				{URI: "1080p.m3u8", Bandwidth: 5000000, Codecs: "avc1.42e00a,mp4a.40.2"},
			},
		}

		streamURL := "https://example.com/master.m3u8"

		metadata := extractor.ExtractMetadata(playlist, streamURL)

		require.NotNil(t, metadata)
		assert.Equal(t, 5000, metadata.Bitrate) // From highest bandwidth variant
		assert.Equal(t, "aac", metadata.Codec)  // Extracted from codecs string
		assert.Equal(t, "5000000", metadata.Headers["best_variant_bandwidth"])
		assert.Equal(t, "3", metadata.Headers["variant_count"])
		assert.Equal(t, "avc1.42e00a,mp4a.40.2", metadata.Headers["codecs"])
	})
}

func TestExtractFromURL(t *testing.T) {
	extractor := NewMetadataExtractor()

	testCases := []struct {
		name     string
		url      string
		expected map[string]interface{}
	}{
		{
			name: "bitrate from path",
			url:  "https://example.com/128k/playlist.m3u8",
			expected: map[string]interface{}{
				"bitrate": 128,
			},
		},
		{
			name: "AAC codec from path",
			url:  "https://example.com/aac/playlist.m3u8",
			expected: map[string]interface{}{
				"codec": "aac",
			},
		},
		{
			name: "MP3 codec from path",
			url:  "https://example.com/mp3/playlist.m3u8",
			expected: map[string]interface{}{
				"codec": "mp3",
			},
		},
		{
			name: "station name from path",
			url:  "https://example.com/cool_station/master.m3u8",
			expected: map[string]interface{}{
				"station": "Cool Station",
			},
		},
		{
			name: "genre from path",
			url:  "https://example.com/music/playlist.m3u8",
			expected: map[string]interface{}{
				"genre": "Music",
			},
		},
		{
			name: "bitrate from filename",
			url:  "https://example.com/96.aac/playlist.m3u8",
			expected: map[string]interface{}{
				"bitrate": 96,
				"codec":   "aac",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &common.StreamMetadata{
				Headers: make(map[string]string),
			}

			extractor.extractFromURL(tc.url, metadata)

			if expectedBitrate, ok := tc.expected["bitrate"]; ok {
				assert.Equal(t, expectedBitrate, metadata.Bitrate)
			}
			if expectedCodec, ok := tc.expected["codec"]; ok {
				assert.Equal(t, expectedCodec, metadata.Codec)
			}
			if expectedStation, ok := tc.expected["station"]; ok {
				assert.Equal(t, expectedStation, metadata.Station)
			}
			if expectedGenre, ok := tc.expected["genre"]; ok {
				assert.Equal(t, expectedGenre, metadata.Genre)
			}
		})
	}
}

func TestExtractFromHeaders(t *testing.T) {
	extractor := NewMetadataExtractor()

	headers := map[string]string{
		"icy-name":        "Test Radio Station",
		"icy-genre":       "Jazz",
		"icy-description": "Great Jazz Music",
		"icy-bitrate":     "192",
		"icy-samplerate":  "48000",
		"content-type":    "application/vnd.apple.mpegurl",
		"server":          "Icecast 2.4.4",
	}

	metadata := &common.StreamMetadata{
		Headers: make(map[string]string),
	}

	extractor.extractFromHeaders(headers, metadata)

	assert.Equal(t, "Test Radio Station", metadata.Station)
	assert.Equal(t, "Jazz", metadata.Genre)
	assert.Equal(t, "Great Jazz Music", metadata.Title)
	assert.Equal(t, 192, metadata.Bitrate)
	assert.Equal(t, 48000, metadata.SampleRate)
	assert.Equal(t, "application/vnd.apple.mpegurl", metadata.ContentType)
	assert.Equal(t, "Icecast 2.4.4", metadata.Headers["server"])
}

func TestExtractFromPlaylistStructure(t *testing.T) {
	extractor := NewMetadataExtractor()

	playlist := &M3U8Playlist{
		IsValid:        true,
		IsMaster:       false,
		IsLive:         true,
		Version:        4,
		TargetDuration: 10,
		MediaSequence:  12345,
	}

	metadata := &common.StreamMetadata{
		Headers: make(map[string]string),
	}

	extractor.extractFromPlaylistStructure(playlist, metadata)

	assert.Equal(t, "true", metadata.Headers["is_live"])
	assert.Equal(t, "false", metadata.Headers["is_master"])
	assert.Equal(t, "4", metadata.Headers["hls_version"])
	assert.Equal(t, "10", metadata.Headers["target_duration"])
	assert.Equal(t, "12345", metadata.Headers["media_sequence"])
}

func TestExtractFromSegments(t *testing.T) {
	extractor := NewMetadataExtractor()

	segments := []M3U8Segment{
		{
			URI:      "seg1.ts",
			Duration: 10.0,
			Title:    "CATEGORY:music,PDT:2023-01-01T00:00:00Z",
		},
		{
			URI:      "seg2.ts",
			Duration: 10.0,
			Title:    "CATEGORY:music,AD_BREAK_START",
		},
		{
			URI:      "seg3.ts",
			Duration: 10.0,
			Title:    "CATEGORY:news,CUE-IN",
		},
	}

	metadata := &common.StreamMetadata{
		Headers: make(map[string]string),
	}

	extractor.extractFromSegments(segments, metadata)

	assert.Equal(t, "3", metadata.Headers["segment_count"])
	assert.Equal(t, "true", metadata.Headers["has_ad_breaks"])
	assert.Contains(t, metadata.Headers["content_categories"], "music")
	assert.Contains(t, metadata.Headers["content_categories"], "news")
}

func TestExtractFromVariants(t *testing.T) {
	extractor := NewMetadataExtractor()

	variants := []M3U8Variant{
		{
			URI:        "low.m3u8",
			Bandwidth:  500000,
			Codecs:     "mp4a.40.2",
			Resolution: "640x360",
		},
		{
			URI:        "high.m3u8",
			Bandwidth:  2000000,
			Codecs:     "avc1.42e00a,mp4a.40.2",
			Resolution: "1280x720",
			FrameRate:  29.97,
		},
	}

	metadata := &common.StreamMetadata{
		Headers: make(map[string]string),
	}

	extractor.extractFromVariants(variants, metadata)

	assert.Equal(t, "2", metadata.Headers["variant_count"])
	assert.Equal(t, 2000, metadata.Bitrate) // From highest bandwidth
	assert.Equal(t, "aac", metadata.Codec)  // Extracted from codecs
	assert.Equal(t, "2000000", metadata.Headers["best_variant_bandwidth"])
	assert.Equal(t, "1280x720", metadata.Headers["resolution"])
	assert.Equal(t, "29.97", metadata.Headers["frame_rate"])
}

func TestExtractCodecFromString(t *testing.T) {
	extractor := NewMetadataExtractor()

	testCases := []struct {
		name     string
		codecs   string
		expected string
	}{
		{
			name:     "AAC LC",
			codecs:   "mp4a.40.2",
			expected: "aac",
		},
		{
			name:     "AAC HE",
			codecs:   "mp4a.40.5",
			expected: "aac",
		},
		{
			name:     "video and audio",
			codecs:   "avc1.42e00a,mp4a.40.2",
			expected: "aac",
		},
		{
			name:     "MP3",
			codecs:   "mp3",
			expected: "mp3",
		},
		{
			name:     "unknown",
			codecs:   "unknown.codec",
			expected: "",
		},
		{
			name:     "empty",
			codecs:   "",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := extractor.extractCodecFromString(tc.codecs)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestSetDefaults(t *testing.T) {
	extractor := NewMetadataExtractor()

	t.Run("empty metadata gets defaults", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Headers: make(map[string]string),
		}

		extractor.setDefaults(metadata)

		assert.Equal(t, "aac", metadata.Codec)
		assert.Equal(t, 2, metadata.Channels)
		assert.Equal(t, 44100, metadata.SampleRate)
		assert.Equal(t, 0, metadata.Bitrate) // No variants, so no default bitrate
	})

	t.Run("existing values not overridden", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Codec:      "mp3",
			Channels:   1,
			SampleRate: 48000,
			Bitrate:    256,
			Headers:    make(map[string]string),
		}

		extractor.setDefaults(metadata)

		assert.Equal(t, "mp3", metadata.Codec)
		assert.Equal(t, 1, metadata.Channels)
		assert.Equal(t, 48000, metadata.SampleRate)
		assert.Equal(t, 256, metadata.Bitrate)
	})

	t.Run("bitrate default with variants", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Headers: map[string]string{
				"variant_count": "2",
			},
		}

		extractor.setDefaults(metadata)

		assert.Equal(t, 96, metadata.Bitrate) // Default when variants exist
	})
}

func TestAddURLPattern(t *testing.T) {
	extractor := NewMetadataExtractor()
	initialCount := len(extractor.urlPatterns)

	pattern := URLPattern{
		Pattern:     regexp.MustCompile(`/custom/(\d+)`),
		Priority:    200,
		Description: "Custom test pattern",
		Extractor: func(matches []string, metadata *common.StreamMetadata) {
			metadata.Station = "Custom Station"
		},
	}

	extractor.AddURLPattern(pattern)

	assert.Len(t, extractor.urlPatterns, initialCount+1)

	// Test the pattern works
	metadata := &common.StreamMetadata{Headers: make(map[string]string)}
	extractor.extractFromURL("https://example.com/custom/123", metadata)
	assert.Equal(t, "Custom Station", metadata.Station)
}

func TestAddHeaderMapping(t *testing.T) {
	extractor := NewMetadataExtractor()
	initialCount := len(extractor.headerMappings)

	mapping := HeaderMapping{
		HeaderKey:   "x-custom-quality",
		MetadataKey: "custom_quality",
		Transformer: func(value string) any {
			return strings.ToUpper(value)
		},
	}

	extractor.AddHeaderMapping(mapping)

	assert.Len(t, extractor.headerMappings, initialCount+1)

	// Test the mapping works
	headers := map[string]string{
		"x-custom-quality": "high",
	}
	metadata := &common.StreamMetadata{Headers: make(map[string]string)}
	extractor.extractFromHeaders(headers, metadata)
	assert.Equal(t, "high", metadata.Headers["custom_quality"])
}

func TestAddSegmentAnalyzer(t *testing.T) {
	extractor := NewMetadataExtractor()
	initialCount := len(extractor.segmentAnalyzers)

	analyzer := SegmentAnalyzer{
		Name:     "custom_analyzer",
		Priority: 150,
		Analyzer: func(segments []M3U8Segment, metadata *common.StreamMetadata) {
			metadata.Headers["custom_analysis"] = "completed"
		},
	}

	extractor.AddSegmentAnalyzer(analyzer)

	assert.Len(t, extractor.segmentAnalyzers, initialCount+1)

	// Test the analyzer works
	segments := []M3U8Segment{{URI: "test.ts", Duration: 10.0}}
	metadata := &common.StreamMetadata{Headers: make(map[string]string)}
	extractor.extractFromSegments(segments, metadata)
	assert.Equal(t, "completed", metadata.Headers["custom_analysis"])
}

func TestDefaultURLPatterns(t *testing.T) {
	extractor := NewMetadataExtractor()

	// Test that default patterns are registered
	assert.NotEmpty(t, extractor.urlPatterns)

	// Test some specific URL patterns
	testCases := []struct {
		url      string
		expected map[string]interface{}
	}{
		{
			url: "https://example.com/96k/playlist.m3u8",
			expected: map[string]interface{}{
				"bitrate": 96,
			},
		},
		{
			url: "https://example.com/aac_adts/playlist.m3u8",
			expected: map[string]interface{}{
				"codec":  "aac",
				"format": "adts",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.url, func(t *testing.T) {
			metadata := &common.StreamMetadata{Headers: make(map[string]string)}
			extractor.extractFromURL(tc.url, metadata)

			if expectedBitrate, ok := tc.expected["bitrate"]; ok {
				assert.Equal(t, expectedBitrate, metadata.Bitrate)
			}
			if expectedCodec, ok := tc.expected["codec"]; ok {
				assert.Equal(t, expectedCodec, metadata.Codec)
			}
			if expectedFormat, ok := tc.expected["format"]; ok {
				assert.Equal(t, expectedFormat, metadata.Format)
			}
		})
	}
}

func TestDefaultHeaderMappings(t *testing.T) {
	extractor := NewMetadataExtractor()

	// Test that default mappings are registered
	assert.NotEmpty(t, extractor.headerMappings)

	// Test specific header mappings
	headers := map[string]string{
		"icy-name":                             "Test Station",
		"icy-genre":                            "Rock",
		"icy-bitrate":                          "128",
		"x-tunein-playlist-available-duration": "3600",
	}

	metadata := &common.StreamMetadata{Headers: make(map[string]string)}
	extractor.extractFromHeaders(headers, metadata)

	assert.Equal(t, "Test Station", metadata.Station)
	assert.Equal(t, "Rock", metadata.Genre)
	assert.Equal(t, 128, metadata.Bitrate)
	assert.Equal(t, "3600", metadata.Headers["available_duration"])
}

func TestDefaultSegmentAnalyzers(t *testing.T) {
	extractor := NewMetadataExtractor()

	// Test that default analyzers are registered
	assert.NotEmpty(t, extractor.segmentAnalyzers)

	t.Run("content categories analyzer", func(t *testing.T) {
		segments := []M3U8Segment{
			{URI: "seg1.ts", Title: "CATEGORY:music"},
			{URI: "seg2.ts", Title: "CATEGORY:music"},
			{URI: "seg3.ts", Title: "CATEGORY:news"},
		}

		metadata := &common.StreamMetadata{Headers: make(map[string]string)}
		extractor.extractFromSegments(segments, metadata)

		assert.Equal(t, "music", metadata.Genre) // Most common category
		assert.Contains(t, metadata.Headers["content_categories"], "music")
		assert.Contains(t, metadata.Headers["content_categories"], "news")
	})

	t.Run("ad breaks analyzer", func(t *testing.T) {
		segments := []M3U8Segment{
			{URI: "seg1.ts", Title: "normal content"},
			{URI: "seg2.ts", Title: "AD_BREAK_START"},
			{URI: "seg3.ts", Title: "CUE-OUT:30.0"},
		}

		metadata := &common.StreamMetadata{Headers: make(map[string]string)}
		extractor.extractFromSegments(segments, metadata)

		assert.Equal(t, "true", metadata.Headers["has_ad_breaks"])
	})

	t.Run("program datetime analyzer", func(t *testing.T) {
		segments := []M3U8Segment{
			{URI: "seg1.ts", Title: "PDT:2023-01-01T12:00:00Z"},
			{URI: "seg2.ts", Title: "PDT:2023-01-01T12:00:10Z"},
		}

		metadata := &common.StreamMetadata{Headers: make(map[string]string)}
		extractor.extractFromSegments(segments, metadata)

		assert.Equal(t, "2023-01-01T12:00:10Z", metadata.Headers["program_date_time"])
	})
}

func TestMetadataExtractionIntegration(t *testing.T) {
	extractor := NewMetadataExtractor()

	// Create a comprehensive test playlist
	playlist := &M3U8Playlist{
		IsValid:        true,
		IsMaster:       false,
		IsLive:         true,
		Version:        3,
		TargetDuration: 10,
		MediaSequence:  1000,
		Headers: map[string]string{
			"icy-name":    "Amazing Radio",
			"icy-genre":   "Pop",
			"icy-bitrate": "128",
			"server":      "Icecast",
		},
		Segments: []M3U8Segment{
			{URI: "seg1.aac", Duration: 10.0, Title: "CATEGORY:music"},
			{URI: "seg2.aac", Duration: 10.0, Title: "CATEGORY:music,AD_BREAK_START"},
			{URI: "seg3.aac", Duration: 10.0, Title: "CUE-IN"},
		},
		Variants: []M3U8Variant{
			{URI: "low.m3u8", Bandwidth: 64000, Codecs: "mp4a.40.2"},
			{URI: "high.m3u8", Bandwidth: 128000, Codecs: "mp4a.40.2"},
		},
	}

	streamURL := "https://example.com/radio/128k/aac/playlist.m3u8"

	metadata := extractor.ExtractMetadata(playlist, streamURL)

	require.NotNil(t, metadata)

	// Verify comprehensive metadata extraction
	assert.Equal(t, common.StreamTypeHLS, metadata.Type)
	assert.Equal(t, streamURL, metadata.URL)
	assert.Equal(t, "Amazing Radio", metadata.Station)
	assert.Equal(t, "music", metadata.Genre)
	assert.Greater(t, metadata.Bitrate, 0) // Should get bitrate from URL or variants
	assert.Equal(t, "aac", metadata.Codec)
	assert.Equal(t, 2, metadata.Channels)
	assert.Equal(t, 44100, metadata.SampleRate)

	// Verify playlist structure metadata
	assert.Equal(t, "true", metadata.Headers["is_live"])
	assert.Equal(t, "false", metadata.Headers["is_master"])
	assert.Equal(t, "3", metadata.Headers["segment_count"])
	assert.Equal(t, "2", metadata.Headers["variant_count"])
	assert.Equal(t, "true", metadata.Headers["has_ad_breaks"])

	// Verify it has a timestamp
	assert.NotZero(t, metadata.Timestamp)
}
