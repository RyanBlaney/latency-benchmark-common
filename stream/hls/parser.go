package hls

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
)

// Parser handles M3U8 playlist parsing
type Parser struct {
	tagHandlers map[string]TagHandler
}

// TagHandler defines how to handle specific M3U8 tags
type TagHandler struct {
	Name        string
	Handler     func(value string, playlist *M3U8Playlist, context *ParseContext) error
	Description string
}

// ParseContext holds the current parsing state
type ParseContext struct {
	CurrentSegment *M3U8Segment
	CurrentVariant *M3U8Variant
	LineNumber     int
}

// NewParser creates a new M3U8 parser with default tag handlers
func NewParser() *Parser {
	parser := &Parser{
		tagHandlers: make(map[string]TagHandler),
	}

	// Register default tag handlers
	parser.registerDefaultTagHandlers()

	return parser
}

func NewConfigurableParser(config *ParserConfig) *ConfigurableParser {
	if config == nil {
		config = DefaultConfig().Parser
	}

	parser := &ConfigurableParser{
		Parser: NewParser(),
		config: config,
	}

	// Register custom tag handlers from configuration
	parser.registerCustomTagHandlers()

	return parser
}

// Add this new method to ConfigurableParser
func (cp *ConfigurableParser) registerCustomTagHandlers() {
	for tagName, handlerName := range cp.config.CustomTagHandlers {
		// Create a generic handler that stores the value in playlist headers
		handler := TagHandler{
			Name:        tagName,
			Description: fmt.Sprintf("Custom handler: %s", handlerName),
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				// Store custom tag values in playlist headers with a prefix
				key := fmt.Sprintf("custom_%s", strings.ToLower(strings.TrimPrefix(tagName, "#EXT-X-")))
				playlist.Headers[key] = value
				return nil
			},
		}
		cp.RegisterTagHandler(handler)
	}
}

// ParseM3U8Content parses M3U8 playlist content from an io.Reader
func (p *Parser) ParseM3U8Content(reader io.Reader) (*M3U8Playlist, error) {
	playlist := &M3U8Playlist{
		Segments: make([]M3U8Segment, 0),
		Variants: make([]M3U8Variant, 0),
		Headers:  make(map[string]string),
	}

	scanner := bufio.NewScanner(reader)
	context := &ParseContext{
		LineNumber: 0,
	}

	if !scanner.Scan() {
		return nil, common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "empty playlist", nil)
	}

	context.LineNumber++

	// First line must be #EXTM3U
	firstLine := strings.TrimSpace(scanner.Text())
	if firstLine != "#EXTM3U" {
		return nil, common.NewStreamErrorWithFields(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "invalid M3U8 format: missing #EXTM3U header", nil,
			logging.Fields{"line_number": context.LineNumber})

	}

	playlist.IsValid = true

	for scanner.Scan() {
		context.LineNumber++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments (except M3U8 tags)
		if line == "" || (strings.HasPrefix(line, "#") && !strings.HasPrefix(line, "#EXT")) {
			continue
		}

		// Parse M3U8 tags
		if strings.HasPrefix(line, "#EXT") {
			if err := p.parseTag(line, playlist, context); err != nil {
				return nil, common.NewStreamErrorWithFields(common.StreamTypeHLS, "",
					common.ErrCodeInvalidFormat, "invalid M3U8 format: missing #EXTM3U header", nil,
					logging.Fields{"line_number": context.LineNumber})
			}
		} else {
			// This is a URI line
			if err := p.handleURI(line, playlist, context); err != nil {
				return nil, common.NewStreamErrorWithFields(common.StreamTypeHLS, "",
					common.ErrCodeInvalidFormat, "invalid M3U8 format: missing #EXTM3U header", nil,
					logging.Fields{"line_number": context.LineNumber})
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, common.NewStreamError(common.StreamTypeHLS, "",
			common.ErrCodeInvalidFormat, "error reading playlist", err)
	}

	// Post-processing
	p.postProcess(playlist)

	return playlist, nil
}

// parseTag parses individual M3U8 tags using registered handlers
func (p *Parser) parseTag(line string, playlist *M3U8Playlist, context *ParseContext) error {
	parts := strings.SplitN(line, ":", 2)
	tag := parts[0]
	value := ""
	if len(parts) > 1 {
		value = parts[1]
	}

	// Check for registered handler
	if handler, exists := p.tagHandlers[tag]; exists {
		return handler.Handler(value, playlist, context)
	}

	// Handle unknown/custom tags
	return p.handleUnknownTag(tag, value, playlist)
}

// handleURI processes URI lines
func (p *Parser) handleURI(uri string, playlist *M3U8Playlist, context *ParseContext) error {
	if context.CurrentSegment != nil {
		// Complete the current segment
		context.CurrentSegment.URI = uri
		playlist.Segments = append(playlist.Segments, *context.CurrentSegment)
		context.CurrentSegment = nil
	} else if context.CurrentVariant != nil {
		// Complete the current variant
		context.CurrentVariant.URI = uri
		playlist.Variants = append(playlist.Variants, *context.CurrentVariant)
		context.CurrentVariant = nil
		playlist.IsMaster = true
	} else {
		// URI without preceding tag - might be valid in some cases
		// Create a basic segment
		segment := M3U8Segment{
			URI:      uri,
			Duration: 0,
		}
		playlist.Segments = append(playlist.Segments, segment)
	}

	return nil
}

// handleUnknownTag handles unknown or custom tags
func (p *Parser) handleUnknownTag(tag, value string, playlist *M3U8Playlist) error {
	// Store custom tags in headers for debugging/extensibility
	if cleanTag, found := strings.CutPrefix(tag, "#EXT-X-"); found {
		playlist.Headers["custom_"+strings.ToLower(cleanTag)] = value
	} else if cleanTag, found := strings.CutPrefix(tag, "#EXT"); found {
		playlist.Headers["ext_"+strings.ToLower(cleanTag)] = value
	}
	return nil
}

// postProcess performs post-parsing processing
func (p *Parser) postProcess(playlist *M3U8Playlist) {
	// Determine if this is a live stream (no EXT-X-ENDLIST tag)
	if _, hasEndList := playlist.Headers["has_endlist"]; !hasEndList {
		playlist.IsLive = true
	}
}

// registerDefaultTagHandlers registers all standard M3U8 tag handlers
func (p *Parser) registerDefaultTagHandlers() {
	handlers := []TagHandler{
		{
			Name:        "#EXT-X-VERSION",
			Description: "Playlist version",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				if v, err := strconv.Atoi(value); err == nil {
					playlist.Version = v
				}
				return nil
			},
		},
		{
			Name:        "#EXT-X-TARGETDURATION",
			Description: "Target segment duration",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				if v, err := strconv.Atoi(value); err == nil {
					playlist.TargetDuration = v
				}
				return nil
			},
		},
		{
			Name:        "#EXT-X-MEDIA-SEQUENCE",
			Description: "Media sequence number",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				if v, err := strconv.Atoi(value); err == nil {
					playlist.MediaSequence = v
				}
				return nil
			},
		},
		{
			Name:        "#EXT-X-DISCONTINUITY-SEQUENCE",
			Description: "Discontinuity sequence number",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				playlist.Headers["discontinuity_sequence"] = value
				return nil
			},
		},
		{
			Name:        "#EXT-X-START",
			Description: "Playback start point",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				if strings.Contains(value, "TIME-OFFSET=") {
					offsetStr := extractAttributeValue(value, "TIME-OFFSET")
					if offsetStr != "" {
						playlist.Headers["start_time_offset"] = offsetStr
					}
				}
				return nil
			},
		},
		{
			Name:        "#EXT-X-ENDLIST",
			Description: "End of playlist marker",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				playlist.IsLive = false
				playlist.Headers["has_endlist"] = "true"
				return nil
			},
		},
		{
			Name:        "#EXTINF",
			Description: "Segment information",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				// Start a new segment
				context.CurrentSegment = &M3U8Segment{}

				// Parse duration and title
				parts := strings.SplitN(value, ",", 2)
				if len(parts) > 0 {
					if duration, err := strconv.ParseFloat(parts[0], 64); err == nil {
						context.CurrentSegment.Duration = duration
					}
				}
				if len(parts) > 1 {
					context.CurrentSegment.Title = parts[1]
				}

				return nil
			},
		},
		{
			Name:        "#EXT-X-BYTERANGE",
			Description: "Byte range for segment",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				if context.CurrentSegment != nil {
					context.CurrentSegment.ByteRange = value
				}
				return nil
			},
		},
		{
			Name:        "#EXT-X-PROGRAM-DATE-TIME",
			Description: "Program date and time",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				if context.CurrentSegment != nil {
					p.appendToSegmentTitle(context.CurrentSegment, "PDT:"+value)
				}
				return nil
			},
		},
		{
			Name:        "#EXT-X-CUE-OUT",
			Description: "Ad break start",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				if context.CurrentSegment != nil {
					p.appendToSegmentTitle(context.CurrentSegment, "AD_BREAK_START")
				}
				return nil
			},
		},
		{
			Name:        "#EXT-X-CUE-IN",
			Description: "Ad break end",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				if context.CurrentSegment != nil {
					p.appendToSegmentTitle(context.CurrentSegment, "AD_BREAK_END")
				}
				return nil
			},
		},
		{
			Name:        "#EXT-X-DISCONTINUITY",
			Description: "Content discontinuity",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				if context.CurrentSegment != nil {
					p.appendToSegmentTitle(context.CurrentSegment, "DISCONTINUITY")
				}
				return nil
			},
		},
		{
			Name:        "#EXT-X-STREAM-INF",
			Description: "Stream variant information",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				// Start a new variant stream
				context.CurrentVariant = &M3U8Variant{}

				// Parse stream info attributes
				attrs := parseAttributes(value)

				if bandwidth, exists := attrs["BANDWIDTH"]; exists {
					if b, err := strconv.Atoi(bandwidth); err == nil {
						context.CurrentVariant.Bandwidth = b
					}
				}

				if codecs, exists := attrs["CODECS"]; exists {
					context.CurrentVariant.Codecs = strings.Trim(codecs, "\"")
				}

				if resolution, exists := attrs["RESOLUTION"]; exists {
					context.CurrentVariant.Resolution = resolution
				}

				if frameRate, exists := attrs["FRAME-RATE"]; exists {
					if f, err := strconv.ParseFloat(frameRate, 64); err == nil {
						context.CurrentVariant.FrameRate = f
					}
				}

				return nil
			},
		},
		// TuneIn specific tags
		{
			Name:        "#EXT-X-COM-TUNEIN-AVAIL-DUR",
			Description: "TuneIn available duration",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				playlist.Headers["tunein_available_duration"] = value
				return nil
			},
		},
		// Handle content categories in EXTINF titles
		{
			Name:        "#EXT-X-CONTENT-CATEGORY",
			Description: "Content category information",
			Handler: func(value string, playlist *M3U8Playlist, context *ParseContext) error {
				if context.CurrentSegment != nil {
					p.appendToSegmentTitle(context.CurrentSegment, "CATEGORY:"+value)
				}
				return nil
			},
		},
	}

	for _, handler := range handlers {
		p.RegisterTagHandler(handler)
	}
}

// appendToSegmentTitle appends information to segment title
func (p *Parser) appendToSegmentTitle(segment *M3U8Segment, info string) {
	if segment.Title == "" {
		segment.Title = info
	} else {
		segment.Title += "," + info
	}
}

// extractAttributeValue extracts a specific attribute value from a string
func extractAttributeValue(attrString, key string) string {
	attrs := parseAttributes(attrString)
	if value, exists := attrs[key]; exists {
		return strings.Trim(value, "\"")
	}
	return ""
}

// parseAttributes parses M3U8 attribute strings like 'BANDWIDTH=1280000,CODECS="avc1.42e00a,mp4a.40.2"'
func parseAttributes(attrString string) map[string]string {
	attrs := make(map[string]string)

	// Split by comma, but be careful with quoted values
	var parts []string
	var current strings.Builder
	inQuotes := false

	for _, char := range attrString {
		switch char {
		case '"':
			inQuotes = !inQuotes
			current.WriteRune(char)
		case ',':
			if inQuotes {
				current.WriteRune(char)
			} else {
				parts = append(parts, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(char)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	// Parse key=value pairs
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			attrs[kv[0]] = kv[1]
		}
	}

	return attrs
}

// RegisterTagHandler registers a new tag handler
func (p *Parser) RegisterTagHandler(handler TagHandler) {
	p.tagHandlers[handler.Name] = handler
}

// GetRegisteredTags returns a list of all registered tag handlers
func (p *Parser) GetRegisteredTags() []string {
	tags := make([]string, 0, len(p.tagHandlers))
	for tag := range p.tagHandlers {
		tags = append(tags, tag)
	}
	return tags
}
