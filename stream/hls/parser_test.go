package hls

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewParser(t *testing.T) {
	parser := NewParser()

	assert.NotNil(t, parser)
	assert.NotNil(t, parser.tagHandlers)
	assert.NotEmpty(t, parser.tagHandlers)

	// Verify some default tag handlers are registered
	assert.Contains(t, parser.tagHandlers, "#EXT-X-VERSION")
	assert.Contains(t, parser.tagHandlers, "#EXTINF")
	assert.Contains(t, parser.tagHandlers, "#EXT-X-TARGETDURATION")
}

func TestNewConfigurableParser(t *testing.T) {
	config := &ParserConfig{
		StrictMode:         true,
		MaxSegmentAnalysis: 20,
		CustomTagHandlers: map[string]string{
			"#EXT-X-CUSTOM": "custom_handler",
		},
	}

	parser := NewConfigurableParser(config)

	assert.NotNil(t, parser)
	assert.NotNil(t, parser.Parser)
	assert.Equal(t, config, parser.config)
}

func TestNewConfigurableParserWithNilConfig(t *testing.T) {
	parser := NewConfigurableParser(nil)

	assert.NotNil(t, parser)
	assert.NotNil(t, parser.config)
	assert.Equal(t, DefaultConfig().Parser, parser.config)
}

func TestParseM3U8Content(t *testing.T) {
	parser := NewParser()

	t.Run("valid media playlist", func(t *testing.T) {
		reader := strings.NewReader(TestM3U8MediaPlaylist)

		playlist, err := parser.ParseM3U8Content(reader)

		require.NoError(t, err)
		require.NotNil(t, playlist)
		assert.True(t, playlist.IsValid)
		assert.False(t, playlist.IsMaster)
		assert.False(t, playlist.IsLive) // Has EXT-X-ENDLIST
		assert.Equal(t, 3, playlist.Version)
		assert.Equal(t, 10, playlist.TargetDuration)
		assert.Equal(t, 0, playlist.MediaSequence)
		assert.Len(t, playlist.Segments, 3)
		assert.Empty(t, playlist.Variants)

		// Check first segment
		seg := playlist.Segments[0]
		assert.Equal(t, "segment0.ts", seg.URI)
		assert.Equal(t, 9.009, seg.Duration)
	})

	t.Run("valid master playlist", func(t *testing.T) {
		reader := strings.NewReader(TestM3U8MasterPlaylist)

		playlist, err := parser.ParseM3U8Content(reader)

		require.NoError(t, err)
		require.NotNil(t, playlist)
		assert.True(t, playlist.IsValid)
		assert.True(t, playlist.IsMaster)
		assert.Equal(t, 3, playlist.Version)
		assert.Empty(t, playlist.Segments)
		assert.Len(t, playlist.Variants, 3)

		// Check first variant
		variant := playlist.Variants[0]
		assert.Equal(t, "480p.m3u8", variant.URI)
		assert.Equal(t, 1280000, variant.Bandwidth)
		assert.Equal(t, "avc1.42e00a,mp4a.40.2", variant.Codecs)
		assert.Equal(t, "852x480", variant.Resolution)
	})

	t.Run("live playlist", func(t *testing.T) {
		reader := strings.NewReader(TestM3U8LivePlaylist)

		playlist, err := parser.ParseM3U8Content(reader)

		require.NoError(t, err)
		require.NotNil(t, playlist)
		assert.True(t, playlist.IsValid)
		assert.False(t, playlist.IsMaster)
		assert.True(t, playlist.IsLive) // No EXT-X-ENDLIST
		assert.Equal(t, 3, playlist.Version)
		assert.Equal(t, 10, playlist.TargetDuration)
		assert.Equal(t, 123456, playlist.MediaSequence)
		assert.Len(t, playlist.Segments, 3)
	})

	t.Run("playlist with ad breaks", func(t *testing.T) {
		reader := strings.NewReader(TestM3U8WithAdBreaks)

		playlist, err := parser.ParseM3U8Content(reader)

		require.NoError(t, err)
		require.NotNil(t, playlist)
		assert.True(t, playlist.IsValid)
		assert.Len(t, playlist.Segments, 3)

		// Check that ad break information is in segment titles
		assert.Contains(t, playlist.Segments[1].Title, "Ad Content")
		assert.Contains(t, playlist.Segments[2].Title, "Back to Content")
	})

	t.Run("audio-only playlist", func(t *testing.T) {
		reader := strings.NewReader(TestM3U8AudioOnly)

		playlist, err := parser.ParseM3U8Content(reader)

		require.NoError(t, err)
		require.NotNil(t, playlist)
		assert.True(t, playlist.IsValid)
		assert.Len(t, playlist.Segments, 3)

		// Check segments have category information
		for _, segment := range playlist.Segments {
			assert.Contains(t, segment.Title, "CATEGORY:")
		}
	})

	t.Run("missing header", func(t *testing.T) {
		invalidContent := `#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXTINF:10.0,
segment.ts`

		reader := strings.NewReader(invalidContent)
		playlist, err := parser.ParseM3U8Content(reader)

		assert.Error(t, err)
		assert.Nil(t, playlist)
		assert.Contains(t, err.Error(), "#EXTM3U")
	})

	t.Run("empty content", func(t *testing.T) {
		reader := strings.NewReader("")

		playlist, err := parser.ParseM3U8Content(reader)

		assert.Error(t, err)
		assert.Nil(t, playlist)
		assert.Contains(t, err.Error(), "empty playlist")
	})
}

func TestParseTag(t *testing.T) {
	parser := NewParser()
	playlist := &M3U8Playlist{
		Headers: make(map[string]string),
	}
	context := &ParseContext{}

	t.Run("version tag", func(t *testing.T) {
		err := parser.parseTag("#EXT-X-VERSION:3", playlist, context)

		assert.NoError(t, err)
		assert.Equal(t, 3, playlist.Version)
	})

	t.Run("target duration tag", func(t *testing.T) {
		err := parser.parseTag("#EXT-X-TARGETDURATION:10", playlist, context)

		assert.NoError(t, err)
		assert.Equal(t, 10, playlist.TargetDuration)
	})

	t.Run("media sequence tag", func(t *testing.T) {
		err := parser.parseTag("#EXT-X-MEDIA-SEQUENCE:1000", playlist, context)

		assert.NoError(t, err)
		assert.Equal(t, 1000, playlist.MediaSequence)
	})

	t.Run("extinf tag", func(t *testing.T) {
		err := parser.parseTag("#EXTINF:9.009,Sample Title", playlist, context)

		assert.NoError(t, err)
		assert.NotNil(t, context.CurrentSegment)
		assert.Equal(t, 9.009, context.CurrentSegment.Duration)
		assert.Equal(t, "Sample Title", context.CurrentSegment.Title)
	})

	t.Run("stream-inf tag", func(t *testing.T) {
		err := parser.parseTag("#EXT-X-STREAM-INF:BANDWIDTH=1280000,CODECS=\"avc1.42e00a,mp4a.40.2\",RESOLUTION=852x480", playlist, context)

		assert.NoError(t, err)
		assert.NotNil(t, context.CurrentVariant)
		assert.Equal(t, 1280000, context.CurrentVariant.Bandwidth)
		assert.Equal(t, "avc1.42e00a,mp4a.40.2", context.CurrentVariant.Codecs)
		assert.Equal(t, "852x480", context.CurrentVariant.Resolution)
	})

	t.Run("endlist tag", func(t *testing.T) {
		err := parser.parseTag("#EXT-X-ENDLIST", playlist, context)

		assert.NoError(t, err)
		assert.False(t, playlist.IsLive)
		assert.Equal(t, "true", playlist.Headers["has_endlist"])
	})

	t.Run("unknown tag", func(t *testing.T) {
		err := parser.parseTag("#EXT-X-UNKNOWN:value", playlist, context)

		assert.NoError(t, err)
		assert.Equal(t, "value", playlist.Headers["custom_unknown"])
	})
}

func TestHandleURI(t *testing.T) {
	parser := NewParser()
	playlist := &M3U8Playlist{
		Segments: []M3U8Segment{},
		Variants: []M3U8Variant{},
	}

	t.Run("segment URI", func(t *testing.T) {
		context := &ParseContext{
			CurrentSegment: &M3U8Segment{
				Duration: 10.0,
				Title:    "Test Segment",
			},
		}

		err := parser.handleURI("segment1.ts", playlist, context)

		assert.NoError(t, err)
		assert.Len(t, playlist.Segments, 1)
		assert.Equal(t, "segment1.ts", playlist.Segments[0].URI)
		assert.Equal(t, 10.0, playlist.Segments[0].Duration)
		assert.Equal(t, "Test Segment", playlist.Segments[0].Title)
		assert.Nil(t, context.CurrentSegment)
	})

	t.Run("variant URI", func(t *testing.T) {
		context := &ParseContext{
			CurrentVariant: &M3U8Variant{
				Bandwidth: 1280000,
				Codecs:    "avc1.42e00a,mp4a.40.2",
			},
		}

		err := parser.handleURI("480p.m3u8", playlist, context)

		assert.NoError(t, err)
		assert.Len(t, playlist.Variants, 1)
		assert.Equal(t, "480p.m3u8", playlist.Variants[0].URI)
		assert.Equal(t, 1280000, playlist.Variants[0].Bandwidth)
		assert.Equal(t, "avc1.42e00a,mp4a.40.2", playlist.Variants[0].Codecs)
		assert.Nil(t, context.CurrentVariant)
		assert.True(t, playlist.IsMaster)
	})

	t.Run("orphan URI", func(t *testing.T) {
		context := &ParseContext{}

		err := parser.handleURI("orphan.ts", playlist, context)

		assert.NoError(t, err)
		assert.Len(t, playlist.Segments, 2) // Previous segment + this one
		assert.Equal(t, "orphan.ts", playlist.Segments[1].URI)
		assert.Equal(t, 0.0, playlist.Segments[1].Duration)
	})
}

func TestParseAttributes(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected map[string]string
	}{
		{
			name:  "simple attributes",
			input: "BANDWIDTH=1280000,RESOLUTION=852x480",
			expected: map[string]string{
				"BANDWIDTH":  "1280000",
				"RESOLUTION": "852x480",
			},
		},
		{
			name:  "quoted attributes",
			input: "BANDWIDTH=1280000,CODECS=\"avc1.42e00a,mp4a.40.2\"",
			expected: map[string]string{
				"BANDWIDTH": "1280000",
				"CODECS":    "\"avc1.42e00a,mp4a.40.2\"",
			},
		},
		{
			name:  "mixed attributes",
			input: "BANDWIDTH=1280000,CODECS=\"avc1.42e00a,mp4a.40.2\",RESOLUTION=852x480,FRAME-RATE=29.97",
			expected: map[string]string{
				"BANDWIDTH":  "1280000",
				"CODECS":     "\"avc1.42e00a,mp4a.40.2\"",
				"RESOLUTION": "852x480",
				"FRAME-RATE": "29.97",
			},
		},
		{
			name:  "quoted with commas",
			input: "NAME=\"Station, The Best\",TYPE=AUDIO",
			expected: map[string]string{
				"NAME": "\"Station, The Best\"",
				"TYPE": "AUDIO",
			},
		},
		{
			name:     "empty string",
			input:    "",
			expected: map[string]string{},
		},
		{
			name:  "single attribute",
			input: "BANDWIDTH=1280000",
			expected: map[string]string{
				"BANDWIDTH": "1280000",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := parseAttributes(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractAttributeValue(t *testing.T) {
	attrString := "BANDWIDTH=1280000,CODECS=\"avc1.42e00a,mp4a.40.2\",RESOLUTION=852x480"

	testCases := []struct {
		key      string
		expected string
	}{
		{"BANDWIDTH", "1280000"},
		{"CODECS", "avc1.42e00a,mp4a.40.2"}, // Quotes should be stripped
		{"RESOLUTION", "852x480"},
		{"NONEXISTENT", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			result := extractAttributeValue(attrString, tc.key)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestRegisterTagHandler(t *testing.T) {
	parser := NewParser()
	initialCount := len(parser.tagHandlers)

	customHandler := TagHandler{
		Name:        "#EXT-X-CUSTOM",
		Description: "Custom test handler",
		Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
			playlist.Headers["custom_value"] = value
			return nil
		},
	}

	parser.RegisterTagHandler(customHandler)

	assert.Len(t, parser.tagHandlers, initialCount+1)
	assert.Contains(t, parser.tagHandlers, "#EXT-X-CUSTOM")

	// Test the custom handler works
	playlist := &M3U8Playlist{Headers: make(map[string]string)}
	context := &ParseContext{}

	err := parser.parseTag("#EXT-X-CUSTOM:test_value", playlist, context)
	assert.NoError(t, err)
	assert.Equal(t, "test_value", playlist.Headers["custom_value"])
}

func TestGetRegisteredTags(t *testing.T) {
	parser := NewParser()

	tags := parser.GetRegisteredTags()

	assert.NotEmpty(t, tags)
	assert.Contains(t, tags, "#EXT-X-VERSION")
	assert.Contains(t, tags, "#EXTINF")
	assert.Contains(t, tags, "#EXT-X-TARGETDURATION")
	assert.Contains(t, tags, "#EXT-X-STREAM-INF")
}

func TestConfigurableParserCustomTags(t *testing.T) {
	config := &ParserConfig{
		CustomTagHandlers: map[string]string{
			"#EXT-X-CUSTOM-1": "handler1",
			"#EXT-X-CUSTOM-2": "handler2",
		},
	}

	parser := NewConfigurableParser(config)

	// Test that custom tag handlers are registered
	tags := parser.GetRegisteredTags()
	assert.Contains(t, tags, "#EXT-X-CUSTOM-1")
	assert.Contains(t, tags, "#EXT-X-CUSTOM-2")

	// Test parsing with custom tags
	playlist := &M3U8Playlist{Headers: make(map[string]string)}
	context := &ParseContext{}

	err := parser.parseTag("#EXT-X-CUSTOM-1:value1", playlist, context)
	assert.NoError(t, err)
	assert.Equal(t, "value1", playlist.Headers["custom_custom-1"])

	err = parser.parseTag("#EXT-X-CUSTOM-2:value2", playlist, context)
	assert.NoError(t, err)
	assert.Equal(t, "value2", playlist.Headers["custom_custom-2"])
}

func TestSpecialTags(t *testing.T) {
	parser := NewParser()

	t.Run("discontinuity tag", func(t *testing.T) {
		playlist := &M3U8Playlist{Headers: make(map[string]string)}
		context := &ParseContext{
			CurrentSegment: &M3U8Segment{},
		}

		err := parser.parseTag("#EXT-X-DISCONTINUITY", playlist, context)

		assert.NoError(t, err)
		assert.Contains(t, context.CurrentSegment.Title, "DISCONTINUITY")
	})

	t.Run("cue-out tag", func(t *testing.T) {
		playlist := &M3U8Playlist{Headers: make(map[string]string)}
		context := &ParseContext{
			CurrentSegment: &M3U8Segment{},
		}

		err := parser.parseTag("#EXT-X-CUE-OUT:30.0", playlist, context)

		assert.NoError(t, err)
		assert.Contains(t, context.CurrentSegment.Title, "AD_BREAK_START")
	})

	t.Run("cue-in tag", func(t *testing.T) {
		playlist := &M3U8Playlist{Headers: make(map[string]string)}
		context := &ParseContext{
			CurrentSegment: &M3U8Segment{},
		}

		err := parser.parseTag("#EXT-X-CUE-IN", playlist, context)

		assert.NoError(t, err)
		assert.Contains(t, context.CurrentSegment.Title, "AD_BREAK_END")
	})

	t.Run("program-date-time tag", func(t *testing.T) {
		playlist := &M3U8Playlist{Headers: make(map[string]string)}
		context := &ParseContext{
			CurrentSegment: &M3U8Segment{},
		}

		err := parser.parseTag("#EXT-X-PROGRAM-DATE-TIME:2023-01-01T12:00:00Z", playlist, context)

		assert.NoError(t, err)
		assert.Contains(t, context.CurrentSegment.Title, "PDT:2023-01-01T12:00:00Z")
	})

	t.Run("byterange tag", func(t *testing.T) {
		playlist := &M3U8Playlist{Headers: make(map[string]string)}
		context := &ParseContext{
			CurrentSegment: &M3U8Segment{},
		}

		err := parser.parseTag("#EXT-X-BYTERANGE:1024@2048", playlist, context)

		assert.NoError(t, err)
		assert.Equal(t, "1024@2048", context.CurrentSegment.ByteRange)
	})
}

func TestTuneInSpecificTags(t *testing.T) {
	parser := NewParser()

	t.Run("tunein available duration", func(t *testing.T) {
		playlist := &M3U8Playlist{Headers: make(map[string]string)}
		context := &ParseContext{}

		err := parser.parseTag("#EXT-X-COM-TUNEIN-AVAIL-DUR:3600", playlist, context)

		assert.NoError(t, err)
		assert.Equal(t, "3600", playlist.Headers["tunein_available_duration"])
	})

	t.Run("content category tag", func(t *testing.T) {
		playlist := &M3U8Playlist{Headers: make(map[string]string)}
		context := &ParseContext{
			CurrentSegment: &M3U8Segment{},
		}

		err := parser.parseTag("#EXT-X-CONTENT-CATEGORY:music", playlist, context)

		assert.NoError(t, err)
		assert.Contains(t, context.CurrentSegment.Title, "CATEGORY:music")
	})
}

func TestComplexPlaylistParsing(t *testing.T) {
	parser := NewParser()

	t.Run("playlist with mixed content", func(t *testing.T) {
		complexPlaylist := `#EXTM3U
#EXT-X-VERSION:4
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:100
#EXT-X-DISCONTINUITY-SEQUENCE:5
#EXT-X-START:TIME-OFFSET=10.5,PRECISE=YES
#EXTINF:10.0,Sample Title
#EXT-X-PROGRAM-DATE-TIME:2023-01-01T12:00:00Z
#EXT-X-BYTERANGE:1024@0
segment1.ts
#EXT-X-DISCONTINUITY
#EXTINF:9.5,Another Title
segment2.ts
#EXT-X-CUE-OUT:30.0
#EXTINF:10.0,Ad Segment
ad_segment.ts
#EXT-X-CUE-IN
#EXTINF:10.0,Back to Content
segment3.ts
#EXT-X-ENDLIST`

		reader := strings.NewReader(complexPlaylist)
		playlist, err := parser.ParseM3U8Content(reader)

		require.NoError(t, err)
		require.NotNil(t, playlist)
		assert.True(t, playlist.IsValid)
		assert.False(t, playlist.IsLive)
		assert.Equal(t, 4, playlist.Version)
		assert.Equal(t, 10, playlist.TargetDuration)
		assert.Equal(t, 100, playlist.MediaSequence)
		assert.Len(t, playlist.Segments, 4)

		// Check specific segment details
		assert.Equal(t, "segment1.ts", playlist.Segments[0].URI)
		assert.Contains(t, playlist.Segments[0].Title, "Sample Title")
		assert.Equal(t, "1024@0", playlist.Segments[0].ByteRange)

		// Check ad break segments
		assert.Contains(t, playlist.Segments[2].Title, "Ad Segment")
		assert.Contains(t, playlist.Segments[3].Title, "Back to Content")

		// Check headers
		assert.Equal(t, "5", playlist.Headers["discontinuity_sequence"])
		assert.Equal(t, "10.5", playlist.Headers["start_time_offset"])
	})
}

func TestParserErrorHandling(t *testing.T) {
	parser := NewParser()

	t.Run("malformed extinf", func(t *testing.T) {
		playlist := &M3U8Playlist{Headers: make(map[string]string)}
		context := &ParseContext{}

		// Should not error, just parse what it can
		err := parser.parseTag("#EXTINF:invalid,title", playlist, context)

		assert.NoError(t, err)
		assert.NotNil(t, context.CurrentSegment)
		assert.Equal(t, "title", context.CurrentSegment.Title)
		assert.Equal(t, 0.0, context.CurrentSegment.Duration)
	})

	t.Run("malformed stream-inf", func(t *testing.T) {
		playlist := &M3U8Playlist{Headers: make(map[string]string)}
		context := &ParseContext{}

		// Should not error, just parse what it can
		err := parser.parseTag("#EXT-X-STREAM-INF:BANDWIDTH=invalid", playlist, context)

		assert.NoError(t, err)
		assert.NotNil(t, context.CurrentVariant)
		assert.Equal(t, 0, context.CurrentVariant.Bandwidth)
	})
}
