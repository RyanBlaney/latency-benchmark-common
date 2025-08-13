package icecast

// Test URLs for ICEcast streams used across all test files
var (
	// Source stream URL - SkyView Networks MSNBC stream
	TestSRCStreamURL = "http://stream1.skyviewnetworks.com:8010/MSNBC"

	// CDN stream URL - TuneIn CDN stream
	TestCDNStreamURL = "https://tunein.cdnstream1.com/3511_96.mp3"

	// Additional test URLs for various scenarios
	// TODO: Replace with more real valid ICEcast URLs when available
	TestValidICEcastURLs = []string{
		TestSRCStreamURL,
		TestCDNStreamURL,
	}

	// Invalid URLs for negative testing
	TestInvalidICEcastURLs = []string{
		"not-a-url",
		"ftp://example.com/file.mp3",
		"https://example.com/file.m3u8", // HLS URL
		"https://example.com/video.mp4", // Video file
		"",
		"mailto:test@example.com",
		"file:///local/file.mp3",
	}

	// Sample HTTP headers for testing metadata extraction
	TestICEcastHeaders = map[string]string{
		"icy-name":        "Test Radio Station",
		"icy-genre":       "Rock",
		"icy-description": "The best rock music 24/7",
		"icy-url":         "https://testradio.example.com",
		"icy-br":          "128",
		"icy-sr":          "44100",
		"icy-channels":    "2",
		"icy-metaint":     "16000",
		"icy-pub":         "1",
		"icy-notice1":     "This stream requires a license",
		"icy-notice2":     "SHOUTcast Distributed Network Audio Server",
		"icy-version":     "2.4.4",
		"content-type":    "audio/mpeg",
		"server":          "Icecast 2.4.4",
		"connection":      "close",
		"cache-control":   "no-cache",
		"expires":         "Mon, 26 Jul 1997 05:00:00 GMT",
		"pragma":          "no-cache",
	}

	// Alternative MP3 headers for different test scenarios
	TestICEcastMP3Headers128 = map[string]string{
		"icy-name":     "MP3 Test Station 128k",
		"icy-genre":    "Jazz",
		"icy-br":       "128",
		"icy-sr":       "44100",
		"icy-channels": "2",
		"content-type": "audio/mpeg",
		"server":       "Icecast 2.4.4",
	}

	TestICEcastMP3Headers64 = map[string]string{
		"icy-name":     "MP3 Test Station 64k",
		"icy-genre":    "Rock",
		"icy-br":       "64",
		"icy-sr":       "44100",
		"icy-channels": "2",
		"content-type": "audio/mp3",
		"server":       "Icecast 2.4.4",
	}

	// Headers without ICY metadata
	TestBasicAudioHeaders = map[string]string{
		"content-type": "audio/mpeg",
		"server":       "Apache/2.4.41",
		"connection":   "keep-alive",
	}

	// Sample ICY metadata strings for testing
	TestICYMetadataStrings = []string{
		"StreamTitle='Artist - Song Title';",
		"StreamTitle='Madonna - Like a Prayer';StreamUrl='http://example.com';",
		"StreamTitle='The Beatles - Hey Jude';",
		"StreamTitle='';", // Empty title
		"StreamTitle='Radio Station Jingle';StreamUrl='';",
		"StreamTitle='News Update - Breaking News';",
		"StreamTitle='Commercial Break';",
		"StreamTitle='Live DJ Set by DJ Name';",
	}

	// Sample raw MP3 audio data for testing (mock MP3 frame headers)
	TestMP3AudioData = []byte{
		0xFF, 0xFB, 0x90, 0x00, // MP3 frame header (MPEG-1 Layer 3, 128kbps, 44.1kHz, stereo)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	// Another MP3 frame for testing
	TestMP3AudioData64k = []byte{
		0xFF, 0xFB, 0x50, 0x00, // MP3 frame header (MPEG-1 Layer 3, 64kbps, 44.1kHz, stereo)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	// Mock PCM audio data (16-bit signed samples)
	TestPCMData = []float64{
		0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.4, 0.3, 0.2, 0.1,
		0.0, -0.1, -0.2, -0.3, -0.4, -0.5, -0.4, -0.3, -0.2, -0.1,
		0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 0.8, 0.6, 0.4, 0.2,
		0.0, -0.2, -0.4, -0.6, -0.8, -1.0, -0.8, -0.6, -0.4, -0.2,
	}

	// Silent PCM data for testing silence detection
	TestSilentPCMData = []float64{
		0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
		0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
		0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
		0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
	}

	// Sample content types for testing (MP3 only for now)
	TestValidContentTypes = []string{
		"audio/mpeg",
		"audio/mp3",
	}

	// Invalid content types for testing
	TestInvalidContentTypes = []string{
		"audio/aac",       // Not supported yet
		"audio/ogg",       // Not supported yet
		"application/ogg", // Not supported yet
		"video/mp4",
		"text/html",
		"application/json",
		"image/jpeg",
		"text/plain",
		"application/xml",
	}

	// Sample server headers for ICEcast/SHOUTcast servers
	TestICEcastServerHeaders = []string{
		"Icecast 2.4.4",
		"Icecast/2.4.4",
		"SHOUTcast Distributed Network Audio Server/v2.5.5",
		"SHOUTcast/v2.5.5",
		"Icecast 2.5.0",
	}
)

