package hls

import (
	"maps"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var titleCaser = cases.Title(language.English)

// MetadataExtractor handles extraction of metadata from HLS playlists and URLs
type MetadataExtractor struct {
	urlPatterns      []URLPattern
	headerMappings   []HeaderMapping
	segmentAnalyzers []SegmentAnalyzer
}

// URLPattern defines patterns for extracting metadata from URLs
type URLPattern struct {
	Pattern     *regexp.Regexp
	Extractor   func(matches []string, metadata *common.StreamMetadata)
	Priority    int
	Description string
}

// HeaderMapping defines how to extract metadata from HTTP headers
type HeaderMapping struct {
	HeaderKey   string
	MetadataKey string
	Transformer func(value string) any
}

// SegmentAnalyzer defines how to analyze playlist segments for metadata
type SegmentAnalyzer struct {
	Name     string
	Analyzer func(segments []M3U8Segment, metadata *common.StreamMetadata)
	Priority int
}

// NewMetadataExtractor creates a new metadata extractor with default patterns
func NewMetadataExtractor() *MetadataExtractor {
	extractor := &MetadataExtractor{
		urlPatterns:      make([]URLPattern, 0),
		headerMappings:   make([]HeaderMapping, 0),
		segmentAnalyzers: make([]SegmentAnalyzer, 0),
	}

	// Register default URL patterns
	extractor.registerDefaultURLPatterns()

	// Register default header mappings
	extractor.registerDefaultHeaderMappings()

	// Register default segment analyzers
	extractor.registerDefaultSegmentAnalyzers()

	return extractor
}

// ExtractMetadata extracts comprehensive metadata from playlist and URL
func (me *MetadataExtractor) ExtractMetadata(playlist *M3U8Playlist, streamURL string) *common.StreamMetadata {
	metadata := &common.StreamMetadata{
		URL:       streamURL,
		Type:      common.StreamTypeHLS,
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
	}

	// Copy playlist headers to metadata
	if playlist.Headers != nil {
		maps.Copy(metadata.Headers, playlist.Headers)
	}

	// Extract from URL patterns
	me.extractFromURL(streamURL, metadata)

	// Extract from HTTP headers
	me.extractFromHeaders(playlist.Headers, metadata)

	// Extract from playlist structure
	me.extractFromPlaylistStructure(playlist, metadata)

	// Extract from segments
	me.extractFromSegments(playlist.Segments, metadata)

	// Extract from variants
	me.extractFromVariants(playlist.Variants, metadata)

	// Set intelligent defaults
	me.setDefaults(metadata)

	return metadata
}

// registerDefaultURLPatterns registers common URL patterns for metadata extraction
func (me *MetadataExtractor) registerDefaultURLPatterns() {
	patterns := []URLPattern{
		{
			Pattern:     regexp.MustCompile(`/(\d+)k?/`),
			Description: "Bitrate from path (e.g., /96k/, /128/)",
			Priority:    100,
			Extractor: func(matches []string, metadata *common.StreamMetadata) {
				if len(matches) > 1 {
					if bitrate, err := strconv.Atoi(matches[1]); err == nil {
						metadata.Bitrate = bitrate
					}
				}
			},
		},
		{
			Pattern:     regexp.MustCompile(`/aac_adts/`),
			Description: "AAC ADTS codec format",
			Priority:    90,
			Extractor: func(matches []string, metadata *common.StreamMetadata) {
				metadata.Codec = "aac"
				metadata.Format = "adts"
			},
		},
		{
			Pattern:     regexp.MustCompile(`/aac/`),
			Description: "AAC codec",
			Priority:    80,
			Extractor: func(matches []string, metadata *common.StreamMetadata) {
				metadata.Codec = "aac"
			},
		},
		{
			Pattern:     regexp.MustCompile(`/mp3/`),
			Description: "MP3 codec",
			Priority:    80,
			Extractor: func(matches []string, metadata *common.StreamMetadata) {
				metadata.Codec = "mp3"
			},
		},
		{
			Pattern:     regexp.MustCompile(`/(\d+)\.aac/`),
			Description: "Bitrate from AAC filename",
			Priority:    95,
			Extractor: func(matches []string, metadata *common.StreamMetadata) {
				if len(matches) > 1 {
					if bitrate, err := strconv.Atoi(matches[1]); err == nil {
						metadata.Bitrate = bitrate
						metadata.Codec = "aac"
					}
				}
			},
		},
		{
			Pattern:     regexp.MustCompile(`/(news|music|sports|talk|podcasts?)/`),
			Description: "Content genre from path",
			Priority:    70,
			Extractor: func(matches []string, metadata *common.StreamMetadata) {
				if len(matches) > 1 {
					metadata.Genre = titleCaser.String(matches[1])
				}
			},
		},
		{
			Pattern:     regexp.MustCompile(`/([^/]+)/(master|playlist)\.m3u8`),
			Description: "Station/stream name from path",
			Priority:    60,
			Extractor: func(matches []string, metadata *common.StreamMetadata) {
				if len(matches) > 1 && metadata.Station == "" {
					station := strings.ReplaceAll(matches[1], "_", " ")
					station = titleCaser.String(station)
					metadata.Station = station
				}
			},
		},
	}

	for _, pattern := range patterns {
		me.AddURLPattern(pattern)
	}
}

// registerDefaultHeaderMappings registers common header to metadata mappings
func (me *MetadataExtractor) registerDefaultHeaderMappings() {
	mappings := []HeaderMapping{
		{
			HeaderKey:   "icy-name",
			MetadataKey: "station",
			Transformer: func(value string) any { return value },
		},
		{
			HeaderKey:   "icy-genre",
			MetadataKey: "genre",
			Transformer: func(value string) any { return value },
		},
		{
			HeaderKey:   "icy-description",
			MetadataKey: "title",
			Transformer: func(value string) any { return value },
		},
		{
			HeaderKey:   "icy-bitrate",
			MetadataKey: "bitrate",
			Transformer: func(value string) any {
				if bitrate, err := strconv.Atoi(value); err == nil {
					return bitrate
				}
				return nil
			},
		},
		{
			HeaderKey:   "icy-samplerate",
			MetadataKey: "sample_rate",
			Transformer: func(value string) any {
				if rate, err := strconv.Atoi(value); err == nil {
					return rate
				}
				return nil
			},
		},
		{
			HeaderKey:   "content-type",
			MetadataKey: "content_type",
			Transformer: func(value string) any { return value },
		},
		{
			HeaderKey:   "server",
			MetadataKey: "server",
			Transformer: func(value string) any { return value },
		},
		{
			HeaderKey:   "x-tunein-playlist-available-duration",
			MetadataKey: "available_duration",
			Transformer: func(value string) any {
				if duration, err := strconv.Atoi(value); err == nil {
					return duration
				}
				return nil
			},
		},
	}

	for _, mapping := range mappings {
		me.AddHeaderMapping(mapping)
	}
}

// registerDefaultSegmentAnalyzers registers analyzers for playlist segments
func (me *MetadataExtractor) registerDefaultSegmentAnalyzers() {
	analyzers := []SegmentAnalyzer{
		{
			Name:     "content_categories",
			Priority: 100,
			Analyzer: func(segments []M3U8Segment, metadata *common.StreamMetadata) {
				categories := make(map[string]int)

				// Analyze first 10 segments for patterns
				maxSegments := len(segments)
				maxSegments = min(maxSegments, 10)

				for i := 0; i < maxSegments; i++ {
					segment := segments[i]

					// Extract categories from segment titles
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
								categories[category]++
							}
						}
					}
				}

				// Set primary category (most common)
				var primaryCategory string
				maxCount := 0
				for category, count := range categories {
					if count > maxCount {
						maxCount = count
						primaryCategory = category
					}
				}

				if primaryCategory != "" {
					metadata.Genre = primaryCategory

					// Store all categories in headers
					categoryList := make([]string, 0, len(categories))
					for category := range categories {
						categoryList = append(categoryList, category)
					}
					metadata.Headers["content_categories"] = strings.Join(categoryList, ",")
				}
			},
		},
		{
			Name:     "ad_breaks",
			Priority: 90,
			Analyzer: func(segments []M3U8Segment, metadata *common.StreamMetadata) {
				hasAdBreaks := false

				for _, segment := range segments {
					if strings.Contains(segment.Title, "AD_BREAK_START") ||
						strings.Contains(segment.Title, "AD_BREAK_END") ||
						strings.Contains(segment.Title, "CUE-OUT") ||
						strings.Contains(segment.Title, "CUE-IN") {
						hasAdBreaks = true
						break
					}
				}

				if hasAdBreaks {
					metadata.Headers["has_ad_breaks"] = "true"
				}
			},
		},
		{
			Name:     "program_datetime",
			Priority: 80,
			Analyzer: func(segments []M3U8Segment, metadata *common.StreamMetadata) {
				var latestPDT string

				for _, segment := range segments {
					if strings.Contains(segment.Title, "PDT:") {
						if start := strings.Index(segment.Title, "PDT:"); start != -1 {
							start += len("PDT:")
							var pdt string
							if end := strings.Index(segment.Title[start:], ","); end != -1 {
								pdt = segment.Title[start : start+end]
							} else {
								pdt = segment.Title[start:]
							}
							if pdt != "" {
								latestPDT = pdt
							}
						}
					}
				}

				if latestPDT != "" {
					metadata.Headers["program_date_time"] = latestPDT
				}
			},
		},
	}

	for _, analyzer := range analyzers {
		me.AddSegmentAnalyzer(analyzer)
	}
}

// extractFromURL extracts metadata using registered URL patterns
func (me *MetadataExtractor) extractFromURL(streamURL string, metadata *common.StreamMetadata) {
	for _, pattern := range me.urlPatterns {
		if matches := pattern.Pattern.FindStringSubmatch(streamURL); matches != nil {
			pattern.Extractor(matches, metadata)
		}
	}
}

// extractFromHeaders extracts metadata from HTTP headers
func (me *MetadataExtractor) extractFromHeaders(headers map[string]string, metadata *common.StreamMetadata) {
	if headers == nil {
		return
	}

	for _, mapping := range me.headerMappings {
		if value, exists := headers[mapping.HeaderKey]; exists {
			if transformed := mapping.Transformer(value); transformed != nil {
				switch mapping.MetadataKey {
				case "station":
					metadata.Station = transformed.(string)
				case "genre":
					metadata.Genre = transformed.(string)
				case "title":
					metadata.Title = transformed.(string)
				case "bitrate":
					metadata.Bitrate = transformed.(int)
				case "sample_rate":
					metadata.SampleRate = transformed.(int)
				case "content_type":
					metadata.ContentType = transformed.(string)
				default:
					// Store in headers for custom fields
					metadata.Headers[mapping.MetadataKey] = value
				}
			}
		}
	}
}

// extractFromPlaylistStructure extracts metadata from playlist structure
func (me *MetadataExtractor) extractFromPlaylistStructure(playlist *M3U8Playlist, metadata *common.StreamMetadata) {
	// Determine if live stream
	metadata.Headers["is_live"] = strconv.FormatBool(playlist.IsLive)
	metadata.Headers["is_master"] = strconv.FormatBool(playlist.IsMaster)

	if playlist.Version > 0 {
		metadata.Headers["hls_version"] = strconv.Itoa(playlist.Version)
	}

	if playlist.TargetDuration > 0 {
		metadata.Headers["target_duration"] = strconv.Itoa(playlist.TargetDuration)
	}

	if playlist.MediaSequence > 0 {
		metadata.Headers["media_sequence"] = strconv.Itoa(playlist.MediaSequence)
	}
}

// extractFromSegments extracts metadata from playlist segments
func (me *MetadataExtractor) extractFromSegments(segments []M3U8Segment, metadata *common.StreamMetadata) {
	if len(segments) == 0 {
		return
	}

	metadata.Headers["segment_count"] = strconv.Itoa(len(segments))

	// Run all segment analyzers
	for _, analyzer := range me.segmentAnalyzers {
		analyzer.Analyzer(segments, metadata)
	}
}

// extractFromVariants extracts metadata from playlist variants
func (me *MetadataExtractor) extractFromVariants(variants []M3U8Variant, metadata *common.StreamMetadata) {
	if len(variants) == 0 {
		return
	}

	metadata.Headers["variant_count"] = strconv.Itoa(len(variants))

	// Find the best variant (highest bandwidth) for metadata
	var bestVariant *M3U8Variant
	for i := range variants {
		variant := &variants[i]
		if bestVariant == nil || variant.Bandwidth > bestVariant.Bandwidth {
			bestVariant = variant
		}
	}

	if bestVariant != nil {
		// Extract bitrate (convert from bps to kbps)
		if metadata.Bitrate == 0 && bestVariant.Bandwidth > 0 {
			metadata.Bitrate = bestVariant.Bandwidth / 1000
		}

		// Extract codec information
		if bestVariant.Codecs != "" && metadata.Codec == "" {
			metadata.Codec = me.extractCodecFromString(bestVariant.Codecs)
		}

		// Store variant information
		metadata.Headers["best_variant_bandwidth"] = strconv.Itoa(bestVariant.Bandwidth)
		if bestVariant.Resolution != "" {
			metadata.Headers["resolution"] = bestVariant.Resolution
		}
		if bestVariant.FrameRate > 0 {
			metadata.Headers["frame_rate"] = strconv.FormatFloat(bestVariant.FrameRate, 'f', 2, 64)
		}
		if bestVariant.Codecs != "" {
			metadata.Headers["codecs"] = bestVariant.Codecs
		}
	}
}

// extractCodecFromString extracts audio codec from codec string
func (me *MetadataExtractor) extractCodecFromString(codecString string) string {
	for codec := range strings.SplitSeq(codecString, ",") {
		codec = strings.TrimSpace(codec)
		if strings.HasPrefix(codec, "mp4a") {
			return "aac"
		} else if strings.HasPrefix(codec, "mp3") {
			return "mp3"
		} else if strings.Contains(codec, "aac") {
			return "aac"
		}
	}
	return ""
}

// setDefaults sets intelligent defaults for missing metadata
func (me *MetadataExtractor) setDefaults(metadata *common.StreamMetadata) {
	// Set default codec if not determined
	if metadata.Codec == "" {
		metadata.Codec = "aac" // Most common for HLS
	}

	// Set default channels if not specified
	if metadata.Channels == 0 {
		metadata.Channels = 2 // Stereo default
	}

	// Set default sample rate if not specified
	if metadata.SampleRate == 0 {
		metadata.SampleRate = 44100 // CD quality default
	}

	// Set default bitrate if not determined and we have variants
	if metadata.Bitrate == 0 {
		if variantCount := metadata.Headers["variant_count"]; variantCount != "" {
			metadata.Bitrate = 96 // Reasonable default for audio streams
		}
	}
}

// AddURLPattern adds a new URL pattern for metadata extraction
func (me *MetadataExtractor) AddURLPattern(pattern URLPattern) {
	me.urlPatterns = append(me.urlPatterns, pattern)
}

// AddHeaderMapping adds a new header mapping
func (me *MetadataExtractor) AddHeaderMapping(mapping HeaderMapping) {
	me.headerMappings = append(me.headerMappings, mapping)
}

// AddSegmentAnalyzer adds a new segment analyzer
func (me *MetadataExtractor) AddSegmentAnalyzer(analyzer SegmentAnalyzer) {
	me.segmentAnalyzers = append(me.segmentAnalyzers, analyzer)
}
