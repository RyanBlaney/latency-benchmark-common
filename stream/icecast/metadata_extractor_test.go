package icecast

import (
	"net/http"
	"strings"
	"testing"

	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetadataExtractor(t *testing.T) {
	extractor := NewMetadataExtractor()

	assert.NotNil(t, extractor)
	assert.NotEmpty(t, extractor.headerMappings)
}

func TestExtractMetadata(t *testing.T) {
	extractor := NewMetadataExtractor()

	t.Run("basic metadata extraction", func(t *testing.T) {
		headers := http.Header{}
		for key, value := range TestICEcastHeaders {
			headers.Set(key, value)
		}

		streamURL := TestCDNStreamURL

		metadata := extractor.ExtractMetadata(headers, streamURL)

		require.NotNil(t, metadata)
		assert.Equal(t, common.StreamTypeICEcast, metadata.Type)
		assert.Equal(t, streamURL, metadata.URL)
		assert.Equal(t, "Test Radio Station", metadata.Station)
		assert.Equal(t, "Rock", metadata.Genre)
		assert.Equal(t, 128, metadata.Bitrate)
		assert.Equal(t, "mp3", metadata.Codec)      // Default
		assert.Equal(t, 2, metadata.Channels)       // Default
		assert.Equal(t, 44100, metadata.SampleRate) // Default
		assert.NotNil(t, metadata.Headers)
		assert.NotZero(t, metadata.Timestamp)
	})

	t.Run("extraction with minimal headers", func(t *testing.T) {
		headers := http.Header{}
		headers.Set("content-type", "audio/mpeg")
		headers.Set("icy-name", "Simple Station")

		streamURL := "http://example.com/stream.mp3"

		metadata := extractor.ExtractMetadata(headers, streamURL)

		require.NotNil(t, metadata)
		assert.Equal(t, common.StreamTypeICEcast, metadata.Type)
		assert.Equal(t, streamURL, metadata.URL)
		assert.Equal(t, "Simple Station", metadata.Station)
		assert.Equal(t, "mp3", metadata.Codec)
		assert.Equal(t, "mp3", metadata.Format)
		assert.Equal(t, "audio/mpeg", metadata.ContentType)
	})

	t.Run("extraction with no headers", func(t *testing.T) {
		headers := http.Header{}
		streamURL := TestCDNStreamURL

		metadata := extractor.ExtractMetadata(headers, streamURL)

		require.NotNil(t, metadata)
		assert.Equal(t, common.StreamTypeICEcast, metadata.Type)
		assert.Equal(t, streamURL, metadata.URL)
		assert.Equal(t, "mp3", metadata.Codec)      // Default
		assert.Equal(t, 2, metadata.Channels)       // Default
		assert.Equal(t, 44100, metadata.SampleRate) // Default
	})
}

func TestExtractFromHeaders(t *testing.T) {
	extractor := NewMetadataExtractor()

	headers := map[string]string{
		"icy-name":        "Test Radio Station",
		"icy-genre":       "Jazz",
		"icy-description": "Great Jazz Music",
		"icy-url":         "http://testradio.example.com",
		"icy-br":          "192",
		"icy-sr":          "48000",
		"icy-channels":    "2",
		"icy-metaint":     "16000",
		"icy-pub":         "1",
		"content-type":    "audio/mpeg",
		"server":          "Icecast 2.4.4",
	}

	metadata := &common.StreamMetadata{
		Headers: make(map[string]string),
	}

	// Convert to http.Header
	httpHeaders := http.Header{}
	for key, value := range headers {
		httpHeaders.Set(key, value)
	}

	extractor.extractFromHeaders(httpHeaders, metadata)

	assert.Equal(t, "Test Radio Station", metadata.Station)
	assert.Equal(t, "Jazz", metadata.Genre)
	assert.Equal(t, "Great Jazz Music", metadata.Title)
	assert.Equal(t, 192, metadata.Bitrate)
	assert.Equal(t, 48000, metadata.SampleRate)
	assert.Equal(t, 2, metadata.Channels)
	assert.Equal(t, "audio/mpeg", metadata.ContentType)
	assert.Equal(t, "mp3", metadata.Codec)
	assert.Equal(t, "mp3", metadata.Format)
	assert.Equal(t, "http://testradio.example.com", metadata.Headers["icy-url"])
	assert.Equal(t, "Icecast 2.4.4", metadata.Headers["server"])
	assert.Equal(t, "16000", metadata.Headers["icy-metaint"])
	assert.Equal(t, "1", metadata.Headers["icy-pub"])
}

func TestExtractCodecFromContentType(t *testing.T) {
	extractor := NewMetadataExtractor()

	testCases := []struct {
		name           string
		contentType    string
		expectedCodec  string
		expectedFormat string
	}{
		{
			name:           "audio/mpeg",
			contentType:    "audio/mpeg",
			expectedCodec:  "mp3",
			expectedFormat: "mp3",
		},
		{
			name:           "audio/mp3",
			contentType:    "audio/mp3",
			expectedCodec:  "mp3",
			expectedFormat: "mp3",
		},
		{
			name:           "audio/mpeg with charset",
			contentType:    "audio/mpeg; charset=utf-8",
			expectedCodec:  "mp3",
			expectedFormat: "mp3",
		},
		{
			name:           "empty content type",
			contentType:    "",
			expectedCodec:  "",
			expectedFormat: "",
		},
		{
			name:           "unknown content type",
			contentType:    "application/unknown",
			expectedCodec:  "",
			expectedFormat: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &common.StreamMetadata{
				ContentType: tc.contentType,
				Headers:     make(map[string]string),
			}

			extractor.extractCodecFromContentType(metadata)

			assert.Equal(t, tc.expectedCodec, metadata.Codec)
			assert.Equal(t, tc.expectedFormat, metadata.Format)
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

		assert.Equal(t, "mp3", metadata.Codec)
		assert.Equal(t, 2, metadata.Channels)
		assert.Equal(t, 44100, metadata.SampleRate)
		assert.Equal(t, "mp3", metadata.Format)
	})

	t.Run("existing values not overridden", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Codec:      "existing",
			Channels:   1,
			SampleRate: 48000,
			Bitrate:    256,
			Format:     "existing",
			Headers:    make(map[string]string),
		}

		extractor.setDefaults(metadata)

		assert.Equal(t, "existing", metadata.Codec)
		assert.Equal(t, 1, metadata.Channels)
		assert.Equal(t, 48000, metadata.SampleRate)
		assert.Equal(t, 256, metadata.Bitrate)
		assert.Equal(t, "existing", metadata.Format)
	})
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
	headers := http.Header{}
	headers.Set("x-custom-quality", "high")
	metadata := &common.StreamMetadata{Headers: make(map[string]string)}
	extractor.extractFromHeaders(headers, metadata)
	assert.Equal(t, "high", strings.ToLower(metadata.Headers["custom_quality"]))
}

func TestDefaultHeaderMappings(t *testing.T) {
	extractor := NewMetadataExtractor()

	// Test that default mappings are registered
	assert.NotEmpty(t, extractor.headerMappings)

	// Test specific header mappings
	headers := http.Header{}
	headers.Set("icy-name", "Test Station")
	headers.Set("icy-genre", "Rock")
	headers.Set("icy-br", "128")
	headers.Set("icy-sr", "44100")
	headers.Set("icy-channels", "2")

	metadata := &common.StreamMetadata{Headers: make(map[string]string)}
	extractor.extractFromHeaders(headers, metadata)

	assert.Equal(t, "Test Station", metadata.Station)
	assert.Equal(t, "Rock", metadata.Genre)
	assert.Equal(t, 128, metadata.Bitrate)
	assert.Equal(t, 44100, metadata.SampleRate)
	assert.Equal(t, 2, metadata.Channels)
}

func TestConfigurableMetadataExtractor(t *testing.T) {
	config := &MetadataExtractorConfig{
		EnableHeaderMappings: true,
		EnableICYMetadata:    true,
		CustomHeaderMappings: []CustomHeaderMapping{
			{
				HeaderKey:   "x-test-bitrate",
				MetadataKey: "bitrate",
				Transform:   "int",
				Description: "Test header mapping",
			},
		},
		DefaultValues: map[string]any{
			"codec":       "mp3",
			"sample_rate": 48000,
			"channels":    1,
		},
	}

	extractor := NewConfigurableMetadataExtractor(config)
	assert.NotNil(t, extractor)
	assert.NotNil(t, extractor.MetadataExtractor)
	assert.Equal(t, config, extractor.config)
}

func TestConfigurableMetadataExtractorWithNilConfig(t *testing.T) {
	extractor := NewConfigurableMetadataExtractor(nil)
	assert.NotNil(t, extractor)
	assert.NotNil(t, extractor.config)
	assert.Equal(t, DefaultConfig().MetadataExtractor, extractor.config)
}

func TestCreateCustomTransformer(t *testing.T) {
	config := &MetadataExtractorConfig{}
	extractor := NewConfigurableMetadataExtractor(config)

	t.Run("int transform", func(t *testing.T) {
		transformer := extractor.createCustomTransformer("int")
		result := transformer("123")
		assert.Equal(t, 123, result)

		result = transformer("invalid")
		assert.Nil(t, result)
	})

	t.Run("float transform", func(t *testing.T) {
		transformer := extractor.createCustomTransformer("float")
		result := transformer("123.45")
		assert.Equal(t, 123.45, result)

		result = transformer("invalid")
		assert.Nil(t, result)
	})

	t.Run("bool transform", func(t *testing.T) {
		transformer := extractor.createCustomTransformer("bool")
		result := transformer("true")
		assert.Equal(t, true, result)

		result = transformer("invalid")
		assert.Nil(t, result)
	})

	t.Run("string transforms", func(t *testing.T) {
		lower := extractor.createCustomTransformer("lower")
		assert.Equal(t, "test", lower("TEST"))

		upper := extractor.createCustomTransformer("upper")
		assert.Equal(t, "TEST", upper("test"))

		title := extractor.createCustomTransformer("title")
		assert.Equal(t, "Test String", title("test string"))

		trim := extractor.createCustomTransformer("trim")
		assert.Equal(t, "test", trim("  test  "))

		none := extractor.createCustomTransformer("unknown")
		assert.Equal(t, "unchanged", none("unchanged"))
	})
}

func TestSetMetadataField(t *testing.T) {
	config := &MetadataExtractorConfig{}
	extractor := NewConfigurableMetadataExtractor(config)

	metadata := &common.StreamMetadata{
		Headers: make(map[string]string),
	}

	// Test known fields
	extractor.setMetadataField(metadata, "station", "Test Station")
	assert.Equal(t, "Test Station", metadata.Station)

	extractor.setMetadataField(metadata, "genre", "Rock")
	assert.Equal(t, "Rock", metadata.Genre)

	extractor.setMetadataField(metadata, "title", "Song Title")
	assert.Equal(t, "Song Title", metadata.Title)

	extractor.setMetadataField(metadata, "artist", "Artist Name")
	assert.Equal(t, "Artist Name", metadata.Artist)

	extractor.setMetadataField(metadata, "bitrate", 128)
	assert.Equal(t, 128, metadata.Bitrate)

	extractor.setMetadataField(metadata, "sample_rate", 44100)
	assert.Equal(t, 44100, metadata.SampleRate)

	extractor.setMetadataField(metadata, "channels", 2)
	assert.Equal(t, 2, metadata.Channels)

	extractor.setMetadataField(metadata, "codec", "mp3")
	assert.Equal(t, "mp3", metadata.Codec)

	extractor.setMetadataField(metadata, "format", "mp3")
	assert.Equal(t, "mp3", metadata.Format)

	extractor.setMetadataField(metadata, "content_type", "audio/mpeg")
	assert.Equal(t, "audio/mpeg", metadata.ContentType)

	// Test unknown field goes to headers
	extractor.setMetadataField(metadata, "custom_field", "custom_value")
	assert.Equal(t, "custom_value", metadata.Headers["custom_field"])
}

func TestApplyDefaults(t *testing.T) {
	config := &MetadataExtractorConfig{
		DefaultValues: map[string]any{
			"codec":       "mp3",
			"channels":    2,
			"sample_rate": 44100,
			"bitrate":     128,
			"station":     "Default Station",
		},
	}

	extractor := NewConfigurableMetadataExtractor(config)

	t.Run("apply defaults to empty metadata", func(t *testing.T) {
		metadata := &common.StreamMetadata{}
		extractor.applyDefaults(metadata)

		assert.Equal(t, "mp3", metadata.Codec)
		assert.Equal(t, 2, metadata.Channels)
		assert.Equal(t, 44100, metadata.SampleRate)
		assert.Equal(t, 128, metadata.Bitrate)
		assert.Equal(t, "Default Station", metadata.Station)
	})

	t.Run("don't override existing values", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Codec:      "existing",
			Channels:   1,
			SampleRate: 48000,
			Bitrate:    256,
			Station:    "Existing Station",
		}
		extractor.applyDefaults(metadata)

		assert.Equal(t, "existing", metadata.Codec)
		assert.Equal(t, 1, metadata.Channels)
		assert.Equal(t, 48000, metadata.SampleRate)
		assert.Equal(t, 256, metadata.Bitrate)
		assert.Equal(t, "Existing Station", metadata.Station)
	})

	t.Run("handle float64 defaults", func(t *testing.T) {
		config := &MetadataExtractorConfig{
			DefaultValues: map[string]any{
				"channels":    float64(2),
				"sample_rate": float64(44100),
				"bitrate":     float64(128),
			},
		}

		extractor := NewConfigurableMetadataExtractor(config)
		metadata := &common.StreamMetadata{}
		extractor.applyDefaults(metadata)

		assert.Equal(t, 2, metadata.Channels)
		assert.Equal(t, 44100, metadata.SampleRate)
		assert.Equal(t, 128, metadata.Bitrate)
	})
}

func TestParseICYTitle(t *testing.T) {
	testCases := []struct {
		name           string
		icyTitle       string
		expectedArtist string
		expectedTitle  string
	}{
		{
			name:           "standard format",
			icyTitle:       "Artist - Song Title",
			expectedArtist: "Artist",
			expectedTitle:  "Song Title",
		},
		{
			name:           "colon separator",
			icyTitle:       "Artist: Song Title",
			expectedArtist: "Artist",
			expectedTitle:  "Song Title",
		},
		{
			name:           "pipe separator",
			icyTitle:       "Artist | Song Title",
			expectedArtist: "Artist",
			expectedTitle:  "Song Title",
		},
		{
			name:           "no separator",
			icyTitle:       "Just a Title",
			expectedArtist: "",
			expectedTitle:  "Just a Title",
		},
		{
			name:           "empty string",
			icyTitle:       "",
			expectedArtist: "",
			expectedTitle:  "",
		},
		{
			name:           "whitespace only",
			icyTitle:       "   ",
			expectedArtist: "",
			expectedTitle:  "",
		},
		{
			name:           "multiple separators",
			icyTitle:       "Artist - Album - Song Title",
			expectedArtist: "Artist",
			expectedTitle:  "Album - Song Title",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			artist, title := ParseICYTitle(tc.icyTitle)
			assert.Equal(t, tc.expectedArtist, artist)
			assert.Equal(t, tc.expectedTitle, title)
		})
	}
}

func TestUpdateWithICYMetadata(t *testing.T) {
	extractor := NewMetadataExtractor()

	t.Run("update with artist and title", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Headers: make(map[string]string),
		}

		extractor.UpdateWithICYMetadata(metadata, "The Beatles - Hey Jude")

		assert.Equal(t, "The Beatles", metadata.Artist)
		assert.Equal(t, "Hey Jude", metadata.Title)
		assert.Equal(t, "The Beatles - Hey Jude", metadata.Headers["icy_current_title"])
		assert.Equal(t, "The Beatles", metadata.Headers["icy_current_artist"])
		assert.Equal(t, "Hey Jude", metadata.Headers["icy_current_song"])
		assert.NotZero(t, metadata.Timestamp)
	})

	t.Run("update with title only", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Headers: make(map[string]string),
		}

		extractor.UpdateWithICYMetadata(metadata, "Station Jingle")

		assert.Equal(t, "", metadata.Artist)
		assert.Equal(t, "Station Jingle", metadata.Title)
		assert.Equal(t, "Station Jingle", metadata.Headers["icy_current_title"])
		assert.Equal(t, "Station Jingle", metadata.Headers["icy_current_song"])
	})

	t.Run("update with empty title", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Headers: make(map[string]string),
		}

		extractor.UpdateWithICYMetadata(metadata, "")

		// Should not update anything
		assert.Equal(t, "", metadata.Artist)
		assert.Equal(t, "", metadata.Title)
	})
}

func TestExtractAudioInfo(t *testing.T) {
	extractor := NewMetadataExtractor()

	t.Run("parse audio info header", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Headers: map[string]string{
				"icy-audio-info": "ice-samplerate=44100;ice-bitrate=128;ice-channels=2",
			},
		}

		extractor.ExtractAudioInfo(metadata)

		assert.Equal(t, 44100, metadata.SampleRate)
		assert.Equal(t, 128, metadata.Bitrate)
		assert.Equal(t, 2, metadata.Channels)
	})

	t.Run("don't override existing values", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			SampleRate: 48000,
			Bitrate:    256,
			Channels:   1,
			Headers: map[string]string{
				"icy-audio-info": "ice-samplerate=44100;ice-bitrate=128;ice-channels=2",
			},
		}

		extractor.ExtractAudioInfo(metadata)

		// Should not override existing values
		assert.Equal(t, 48000, metadata.SampleRate)
		assert.Equal(t, 256, metadata.Bitrate)
		assert.Equal(t, 1, metadata.Channels)
	})

	t.Run("malformed audio info", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Headers: map[string]string{
				"icy-audio-info": "invalid format",
			},
		}

		extractor.ExtractAudioInfo(metadata)

		// Should not crash or set invalid values
		assert.Equal(t, 0, metadata.SampleRate)
		assert.Equal(t, 0, metadata.Bitrate)
		assert.Equal(t, 0, metadata.Channels)
	})
}

func TestGetSupportedHeaders(t *testing.T) {
	extractor := NewMetadataExtractor()

	headers := extractor.GetSupportedHeaders()

	assert.NotEmpty(t, headers)
	assert.Contains(t, headers, "icy-name")
	assert.Contains(t, headers, "icy-genre")
	assert.Contains(t, headers, "icy-br")
	assert.Contains(t, headers, "content-type")
}

func TestGetHeaderMapping(t *testing.T) {
	extractor := NewMetadataExtractor()

	t.Run("existing mapping", func(t *testing.T) {
		mapping, found := extractor.GetHeaderMapping("icy-name")
		assert.True(t, found)
		assert.Equal(t, "icy-name", mapping.HeaderKey)
	})

	t.Run("non-existing mapping", func(t *testing.T) {
		_, found := extractor.GetHeaderMapping("non-existent")
		assert.False(t, found)
	})
}

func TestRemoveHeaderMapping(t *testing.T) {
	extractor := NewMetadataExtractor()
	initialCount := len(extractor.headerMappings)

	// Remove an existing mapping
	removed := extractor.RemoveHeaderMapping("icy-name")
	assert.True(t, removed)
	assert.Len(t, extractor.headerMappings, initialCount-1)

	// Try to remove non-existing mapping
	removed = extractor.RemoveHeaderMapping("non-existent")
	assert.False(t, removed)
	assert.Len(t, extractor.headerMappings, initialCount-1)
}

func TestValidateMetadata(t *testing.T) {
	extractor := NewMetadataExtractor()

	t.Run("valid metadata", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			SampleRate: 44100,
			Channels:   2,
			Bitrate:    128,
			Codec:      "mp3",
		}

		issues := extractor.ValidateMetadata(metadata)
		assert.Empty(t, issues)
	})

	t.Run("unusual sample rate", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			SampleRate: 1000, // Too low
		}

		issues := extractor.ValidateMetadata(metadata)
		assert.Contains(t, issues, "unusual sample rate")
	})

	t.Run("too many channels", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Channels: 10, // Too many
		}

		issues := extractor.ValidateMetadata(metadata)
		assert.Contains(t, issues, "unusual channel count")
	})

	t.Run("unusual bitrate", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Bitrate: 5000, // Too high
		}

		issues := extractor.ValidateMetadata(metadata)
		assert.Contains(t, issues, "unusual bitrate")
	})

	t.Run("missing codec", func(t *testing.T) {
		metadata := &common.StreamMetadata{
			Codec: "",
		}

		issues := extractor.ValidateMetadata(metadata)
		assert.Contains(t, issues, "missing codec information")
	})
}

func TestMetadataExtractionIntegration(t *testing.T) {
	extractor := NewMetadataExtractor()

	// Create comprehensive test headers
	headers := http.Header{}
	for key, value := range TestICEcastHeaders {
		headers.Set(key, value)
	}

	streamURL := TestCDNStreamURL

	metadata := extractor.ExtractMetadata(headers, streamURL)

	require.NotNil(t, metadata)

	// Verify comprehensive metadata extraction
	assert.Equal(t, common.StreamTypeICEcast, metadata.Type)
	assert.Equal(t, streamURL, metadata.URL)
	assert.Equal(t, "Test Radio Station", metadata.Station)
	assert.Equal(t, "Rock", metadata.Genre)
	assert.Equal(t, 128, metadata.Bitrate)
	assert.Equal(t, "mp3", metadata.Codec)
	assert.Equal(t, 2, metadata.Channels)
	assert.Equal(t, 44100, metadata.SampleRate)

	// Verify headers are stored
	assert.Equal(t, "Test Radio Station", metadata.Headers["icy-name"])
	assert.Equal(t, "Rock", metadata.Headers["icy-genre"])
	assert.Equal(t, "128", metadata.Headers["icy-br"])
	assert.Equal(t, "audio/mpeg", metadata.Headers["content-type"])

	// Verify it has a timestamp
	assert.NotZero(t, metadata.Timestamp)
}
