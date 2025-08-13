package transcode

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
)

// AudioData represents decoded audio data
type AudioData struct {
	PCM        []float64       `json:"-"` // Raw PCM data
	SampleRate int             `json:"sample_rate"`
	Channels   int             `json:"channels"`
	Duration   time.Duration   `json:"duration"`
	Timestamp  time.Time       `json:"timestamp"`
	Metadata   *StreamMetadata `json:"metadata,omitempty"`
}

// StreamMetadata represents metadata about the audio stream/file
type StreamMetadata struct {
	URL         string            `json:"url"`
	Type        string            `json:"type"`
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

// DecoderConfig holds decoder configuration
type DecoderConfig struct {
	TargetSampleRate int           `json:"target_sample_rate"`
	TargetChannels   int           `json:"target_channels"`
	OutputFormat     string        `json:"output_format"`
	MaxDuration      time.Duration `json:"max_duration"`
	ResampleQuality  string        `json:"resample_quality"` // "fast", "medium", "high"
	FFmpegPath       string        `json:"ffmpeg_path"`      // Path to ffmpeg binary
	FFprobePath      string        `json:"ffprobe_path"`     // Path to ffprobe binary
	Timeout          time.Duration `json:"timeout"`          // Timeout for ffmpeg operations
	// Normalization options
	EnableNormalization bool    `json:"enable_normalization"`
	NormalizationMethod string  `json:"normalization_method"` // "loudnorm", "dynaudnorm", "compand"
	TargetLUFS          float64 `json:"target_lufs"`          // -23.0 for broadcast, -16.0 for streaming
	TargetPeak          float64 `json:"target_peak"`          // -2.0
	LoudnessRange       float64 `json:"loudness_range"`       // 7.0 typical
}

// DefaultDecoderConfig returns default decoder configuration
func DefaultDecoderConfig() *DecoderConfig {
	return &DecoderConfig{
		TargetSampleRate:    44100,
		TargetChannels:      1, // Mono for fingerprinting
		OutputFormat:        "f64le",
		MaxDuration:         0, // No limit
		ResampleQuality:     "medium",
		FFmpegPath:          "ffmpeg",  // Assume in PATH
		FFprobePath:         "ffprobe", // Assume in PATH
		Timeout:             30 * time.Second,
		EnableNormalization: true,
		NormalizationMethod: "loudnorm",
		TargetLUFS:          -23.0, // EBU R128 standard
		TargetPeak:          -2.0,  // True peak limit
		LoudnessRange:       7.0,   // Loudness range
	}
}

// ContentOptimizedDecoderConfig returns a decoder configuration based on the content type.
//
// The main difference is in the normalization method:
func ContentOptimizedDecoderConfig(contentType string) *DecoderConfig {
	config := DefaultDecoderConfig()

	switch contentType {
	case "music":
		config.NormalizationMethod = "loudnorm"
		config.TargetLUFS = -16.0 // Streaming standard
		config.TargetPeak = -1.0
		config.LoudnessRange = 8.0

	case "speech", "news", "talk":
		config.NormalizationMethod = "dynaudnorm"
		config.TargetLUFS = -20.0
		config.TargetPeak = -3.0
		config.LoudnessRange = 5.0

	case "sports":
		config.NormalizationMethod = "compand"
		config.TargetLUFS = -18.0
		config.TargetPeak = -2.0
		config.LoudnessRange = 10.0

	default:
		// Use defaults
	}

	return config
}

// Decoder handles audio decoding using FFmpeg
type Decoder struct {
	config *DecoderConfig
}

// AudioMetadata holds detected audio properties from FFprobe
type AudioMetadata struct {
	SampleRate int     `json:"sample_rate"`
	Channels   int     `json:"channels"`
	Codec      string  `json:"codec"`
	Duration   float64 `json:"duration"`
	Bitrate    int     `json:"bitrate"`
	Format     string  `json:"format"`
}

// NewDecoder creates a new audio decoder
func NewDecoder(config *DecoderConfig) *Decoder {
	if config == nil {
		config = DefaultDecoderConfig()
	}
	return &Decoder{config: config}
}

func NewNormalizingDecoder(contentType string) *Decoder {
	config := ContentOptimizedDecoderConfig(contentType)
	return NewDecoder(config)
}

// DecodeFile decodes an audio file and returns PCM data
func (d *Decoder) DecodeFile(filename string) (*AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "audio_decoder",
		"function":  "DecodeFile",
		"filename":  filename,
	})

	logger.Debug("Starting audio file decode")

	// Probe the file to get format info
	metadata, err := d.probeAudioFile(filename)
	if err != nil {
		logger.Error(err, "Failed to probe audio file")
		return nil, err
	}

	logger.Debug("Audio metadata detected", logging.Fields{
		"input_sample_rate": metadata.SampleRate,
		"input_channels":    metadata.Channels,
		"input_codec":       metadata.Codec,
		"input_duration":    metadata.Duration,
		"input_bitrate":     metadata.Bitrate,
	})

	// Decode with proper parameters
	return d.decodeFileWithFFmpeg(filename, metadata)
}

// DecodeBytes decodes audio from byte slice
// Returns AudioData (`any` is for package independence)
func (d *Decoder) DecodeBytes(data []byte) (any, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "audio_decoder",
		"function":  "DecodeBytes",
		"data_size": len(data),
	})

	logger.Debug("Starting audio bytes decode")

	if len(data) == 0 {
		return nil, fmt.Errorf("empty audio data")
	}

	// Probe the input to get format info
	metadata, err := d.probeAudioMetadata(data)
	if err != nil {
		logger.Error(err, "Failed to probe audio metadata")
		return nil, err
	}

	logger.Debug("Audio metadata detected", logging.Fields{
		"input_sample_rate": metadata.SampleRate,
		"input_channels":    metadata.Channels,
		"input_codec":       metadata.Codec,
		"input_duration":    metadata.Duration,
		"input_bitrate":     metadata.Bitrate,
		"input_format":      metadata.Format,
	})

	// Decode with proper parameters
	audioData, err := d.decodeWithFFmpeg(data, metadata)
	if err != nil {
		return nil, err
	}

	// Add normalization metadata if it was applied
	if d.config.EnableNormalization && audioData.Metadata != nil {
		if audioData.Metadata.Headers == nil {
			audioData.Metadata.Headers = make(map[string]string)
		}
		audioData.Metadata.Headers["normalization_applied"] = "true"
		audioData.Metadata.Headers["normalization_method"] = d.config.NormalizationMethod
		audioData.Metadata.Headers["target_lufs"] = fmt.Sprintf("%.1f", d.config.TargetLUFS)
		audioData.Metadata.Headers["target_peak"] = fmt.Sprintf("%.1f", d.config.TargetPeak)
	}

	return audioData, nil
}

// DecodeReader decodes audio from an io.Reader
// Returns `AudioData` (`any` is for package independence)
func (d *Decoder) DecodeReader(reader io.Reader) (any, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "audio_decoder",
		"function":  "DecodeReader",
	})

	// Read all data from reader
	data, err := io.ReadAll(reader)
	if err != nil {
		logger.Error(err, "Failed to read data from reader")
		return nil, err
	}

	logger.Debug("Data read from reader", logging.Fields{
		"data_size": len(data),
	})

	return d.DecodeBytes(data)
}

// GetConfig returns decoder configuration information
func (d *Decoder) GetConfig() map[string]any {
	return map[string]any{
		"target_sample_rate": d.config.TargetSampleRate,
		"target_channels":    d.config.TargetChannels,
		"output_format":      d.config.OutputFormat,
		"max_duration":       d.config.MaxDuration,
		"resample_quality":   d.config.ResampleQuality,
		"ffmpeg_path":        d.config.FFmpegPath,
		"ffprobe_path":       d.config.FFprobePath,
		"timeout":            d.config.Timeout,
	}
}

// DecodeURL decodes audio directly from a URL using FFmpeg (for HLS, HTTP streams, etc.)
func (d *Decoder) DecodeURL(url string, duration time.Duration, streamType string) (*AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component":   "audio_decoder",
		"function":    "DecodeURL",
		"url":         url,
		"duration":    duration.Seconds(),
		"stream_type": streamType,
	})

	logger.Debug("Starting URL decode with FFmpeg")

	// Build ffmpeg command for URL input
	args := []string{
		"-v", "error", // Suppress verbose output
	}

	// Add stream-type specific flags
	switch streamType {
	case "icecast":
		// Add reconnection flags for Icecast streams
		args = append(args,
			"-re",
			"-reconnect", "1",
			"-reconnect_at_eof", "1",
			"-reconnect_streamed", "1",
			"-reconnect_delay_max", "2",
			"-fflags", "+genpts+igndts", // Generate PTS, ignore DTS for live
			"-rw_timeout", "30000000", // 30 second read timeout
			"-timeout", "60000000", // 60 second total timeout
		)
	case "hls":
		args = append(args,
			"-protocol_whitelist", "file,http,https,tcp,tls",
			"-timeout", "60000000",
			"-rw_timeout", "30000000",
			"-user_agent", "Mozilla/5.0",
			"-live_start_index", "-3", // Force latest segment
			"-fflags", "+genpts+igndts+flush_packets",
			"-avoid_negative_ts", "disabled",
			"-probesize", "32",
			"-analyzeduration", "0",
		)
	default:
		// For unknown stream types, log a warning but continue
		logger.Debug("Unknown stream type, using default settings", logging.Fields{
			"stream_type": streamType,
		})
	}

	// Add input URL
	args = append(args, "-i", url)

	// Add duration limit if specified
	if duration > 0 {
		args = append(args, "-t", fmt.Sprintf("%.3f", duration.Seconds()))
	}

	switch streamType {
	case "hls":
		// For HLS: explicitly select first audio stream, ignore video
		args = append(args, "-map", "0:a:0")
	case "icecast":
		// ICEcast is audio-only, but be explicit
		args = append(args, "-map", "0:a:0?")
	default:
		// Try to find audio stream
		args = append(args, "-map", "0:a:0?")
	}

	// Add output format parameters
	args = append(args,
		"-vn",         // No video
		"-f", "f64le", // Output raw float64 little-endian
		"-ac", strconv.Itoa(d.config.TargetChannels), // Target channels
		"-ar", strconv.Itoa(d.config.TargetSampleRate), // Target sample rate
	)

	var audioFilters []string

	// Always add resampling for consistency
	audioFilters = append(audioFilters, fmt.Sprintf("aresample=%d:resampler=soxr", d.config.TargetSampleRate))

	// Add normalization if enabled
	if d.config.EnableNormalization {
		normFilter := d.buildNormalizationFilter()
		if normFilter != "" {
			audioFilters = append(audioFilters, normFilter)
		}
	}

	// Apply all audio filters
	if len(audioFilters) > 0 {
		args = append(args, "-af", strings.Join(audioFilters, ","))
	}

	// Output to stdout
	args = append(args, "pipe:1")

	// Create command with timeout
	timeout := duration + (30 * time.Second) // Buffer time
	if d.config.Timeout > 0 && d.config.Timeout > timeout {
		timeout = d.config.Timeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, d.config.FFmpegPath, args...)

	logger.Debug("Running FFmpeg URL command", logging.Fields{
		"command": fmt.Sprintf("%s %s", d.config.FFmpegPath, strings.Join(args, " ")),
		"timeout": timeout.Seconds(),
	})

	startTime := time.Now()
	output, err := cmd.Output()
	decodeTime := time.Since(startTime)

	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			logger.Error(err, "FFmpeg URL decode failed", logging.Fields{
				"stderr": string(exitError.Stderr),
			})
			return nil, fmt.Errorf("ffmpeg URL decode failed: %w, stderr: %s", err, string(exitError.Stderr))
		}
		return nil, fmt.Errorf("ffmpeg URL decode failed: %w", err)
	}

	logger.Debug("FFmpeg URL decode completed", logging.Fields{
		"output_bytes": len(output),
		"decode_time":  decodeTime.Seconds(),
	})

	// Convert output to AudioData
	samples := d.bytesToFloat64(output)
	if len(samples) == 0 {
		return nil, fmt.Errorf("no audio samples decoded from URL")
	}

	// Calculate actual duration
	samplesPerChannel := len(samples) / d.config.TargetChannels
	actualDuration := time.Duration(samplesPerChannel) * time.Second / time.Duration(d.config.TargetSampleRate)

	logger.Debug("URL decode processing completed", logging.Fields{
		"samples":            len(samples),
		"samples_per_ch":     samplesPerChannel,
		"actual_duration":    actualDuration.Seconds(),
		"requested_duration": duration.Seconds(),
		"sample_rate":        d.config.TargetSampleRate,
		"channels":           d.config.TargetChannels,
	})

	// Create metadata for URL-based decode
	metadata := &StreamMetadata{
		URL:        url,
		Type:       streamType, // Use the provided stream type
		Format:     d.config.OutputFormat,
		SampleRate: d.config.TargetSampleRate,
		Channels:   d.config.TargetChannels,
		Codec:      "decoded",
		Headers:    make(map[string]string),
		Timestamp:  time.Now(),
	}

	// Add stream type to metadata headers
	metadata.Headers["stream_type"] = streamType

	// Add normalization info if applied
	if d.config.EnableNormalization {
		metadata.Headers["normalization_applied"] = "true"
		metadata.Headers["normalization_method"] = d.config.NormalizationMethod
		metadata.Headers["target_lufs"] = fmt.Sprintf("%.1f", d.config.TargetLUFS)
	}

	return &AudioData{
		PCM:        samples,
		SampleRate: d.config.TargetSampleRate,
		Channels:   d.config.TargetChannels,
		Duration:   actualDuration,
		Timestamp:  time.Now(),
		Metadata:   metadata,
	}, nil
}

// ProbeURL probes a URL to extract audio metadata without decoding the entire stream
func (d *Decoder) ProbeURL(ctx context.Context, url string) (*AudioMetadata, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "audio_decoder",
		"function":  "ProbeURL",
		"url":       url,
	})

	logger.Debug("Starting URL probe with FFprobe")

	// Create timeout context if not already set
	probeTimeout := 15 * time.Second
	if d.config.Timeout > 0 && d.config.Timeout < probeTimeout {
		probeTimeout = d.config.Timeout
	}

	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	// Build ffprobe command for URL
	args := []string{
		"-v", "quiet", // Suppress verbose output
		"-print_format", "json", // JSON output
		"-show_streams",          // Show stream info
		"-select_streams", "a:0", // First audio stream only
		"-analyzeduration", "10000000", // 10 seconds analysis
		"-probesize", "5000000", // 5MB probe size
		url,
	}

	cmd := exec.CommandContext(probeCtx, d.config.FFprobePath, args...)

	logger.Debug("Running FFprobe URL command", logging.Fields{
		"command": fmt.Sprintf("%s %s", d.config.FFprobePath, url),
		"timeout": probeTimeout.Seconds(),
	})

	output, err := cmd.Output()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			logger.Error(err, "FFprobe URL failed", logging.Fields{
				"stderr": string(exitError.Stderr),
			})
			return nil, fmt.Errorf("ffprobe URL failed: %w, stderr: %s", err, string(exitError.Stderr))
		}
		return nil, fmt.Errorf("ffprobe URL failed: %w", err)
	}

	// Parse ffprobe JSON output
	metadata, err := d.parseFFprobeOutput(output)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ffprobe output: %w", err)
	}

	logger.Debug("FFprobe URL completed successfully", logging.Fields{
		"sample_rate": metadata.SampleRate,
		"channels":    metadata.Channels,
		"codec":       metadata.Codec,
		"bitrate":     metadata.Bitrate,
		"format":      metadata.Format,
	})

	return metadata, nil
}

// probeAudioFile uses ffprobe to get audio information from a file
func (d *Decoder) probeAudioFile(filename string) (*AudioMetadata, error) {
	args := []string{
		"-v", "quiet", // Suppress verbose output
		"-print_format", "json", // JSON output
		"-show_streams",          // Show stream info
		"-select_streams", "a:0", // First audio stream only
		filename,
	}

	cmd := exec.Command(d.config.FFprobePath, args...)

	// Set timeout
	if d.config.Timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), d.config.Timeout)
		defer cancel()
		cmd = exec.CommandContext(ctx, d.config.FFprobePath, args...)
	}

	output, err := cmd.Output()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("ffprobe failed: %w, stderr: %s", err, string(exitError.Stderr))
		}
		return nil, fmt.Errorf("ffprobe failed: %w", err)
	}

	// Parse ffprobe JSON output
	return d.parseFFprobeOutput(output)
}

// probeAudioMetadata uses ffprobe to get input audio information from bytes
func (d *Decoder) probeAudioMetadata(data []byte) (*AudioMetadata, error) {
	args := []string{
		"-v", "quiet", // Suppress verbose output
		"-print_format", "json", // JSON output
		"-show_streams",          // Show stream info
		"-select_streams", "a:0", // First audio stream only
		"pipe:0", // Input from stdin
	}

	cmd := exec.Command(d.config.FFprobePath, args...)
	cmd.Stdin = bytes.NewReader(data)

	// Set timeout
	if d.config.Timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), d.config.Timeout)
		defer cancel()
		cmd = exec.CommandContext(ctx, d.config.FFprobePath, args...)
		cmd.Stdin = bytes.NewReader(data)
	}

	output, err := cmd.Output()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("ffprobe failed: %w, stderr: %s", err, string(exitError.Stderr))
		}
		return nil, fmt.Errorf("ffprobe failed: %w", err)
	}

	// Parse ffprobe JSON output
	return d.parseFFprobeOutput(output)
}

// parseFFprobeOutput parses ffprobe JSON to extract audio metadata
func (d *Decoder) parseFFprobeOutput(jsonData []byte) (*AudioMetadata, error) {
	var probe struct {
		Streams []struct {
			CodecType     string `json:"codec_type"`
			CodecName     string `json:"codec_name"`
			SampleRate    string `json:"sample_rate"`
			Channels      int    `json:"channels"`
			Duration      string `json:"duration"`
			BitRate       string `json:"bit_rate"`
			CodecLongName string `json:"codec_long_name"`
		} `json:"streams"`
	}

	if err := json.Unmarshal(jsonData, &probe); err != nil {
		return nil, fmt.Errorf("failed to parse ffprobe output: %w", err)
	}

	if len(probe.Streams) == 0 {
		return nil, fmt.Errorf("no audio streams found")
	}

	stream := probe.Streams[0]

	// Validate that this is an audio stream
	if stream.CodecType != "audio" {
		return nil, fmt.Errorf("stream is not audio type: %s", stream.CodecType)
	}

	// Parse sample rate
	sampleRate, err := strconv.Atoi(stream.SampleRate)
	if err != nil {
		sampleRate = 44100 // Fallback to common sample rate
	}

	// Parse duration
	duration, err := strconv.ParseFloat(stream.Duration, 64)
	if err != nil {
		duration = 0
	}

	// Parse bitrate
	bitrate, err := strconv.Atoi(stream.BitRate)
	if err != nil {
		bitrate = 0
	}

	// Validate channels
	if stream.Channels <= 0 || stream.Channels > 8 {
		return nil, fmt.Errorf("invalid channel count: %d", stream.Channels)
	}

	return &AudioMetadata{
		SampleRate: sampleRate,
		Channels:   stream.Channels,
		Codec:      stream.CodecName,
		Duration:   duration,
		Bitrate:    bitrate,
		Format:     stream.CodecLongName,
	}, nil
}

// decodeFileWithFFmpeg performs the actual audio decoding from a file
func (d *Decoder) decodeFileWithFFmpeg(filename string, metadata *AudioMetadata) (*AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "audio_decoder",
		"function":  "decodeFileWithFFmpeg",
		"filename":  filename,
	})

	// Build ffmpeg command with detected parameters
	args := d.buildFFmpegArgs(metadata)
	args = append([]string{"-i", filename}, args...) // Prepend input file
	args = append(args, "pipe:1")                    // Output to stdout

	cmd := exec.Command(d.config.FFmpegPath, args...)

	// Set timeout
	if d.config.Timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), d.config.Timeout)
		defer cancel()
		cmd = exec.CommandContext(ctx, d.config.FFmpegPath, args...)
	}

	logger.Debug("Running ffmpeg command", logging.Fields{
		"args": strings.Join(args, " "),
	})

	output, err := cmd.Output()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			logger.Error(err, "Ffmpeg decode failed", logging.Fields{
				"stderr": string(exitError.Stderr),
			})
		}
		return nil, fmt.Errorf("ffmpeg decode failed: %w", err)
	}

	return d.processFFmpegOutput(output, metadata, filename, logger)
}

// decodeWithFFmpeg performs the actual audio decoding from bytes
func (d *Decoder) decodeWithFFmpeg(data []byte, metadata *AudioMetadata) (*AudioData, error) {
	logger := logging.WithFields(logging.Fields{
		"component": "audio_decoder",
		"function":  "decodeWithFFmpeg",
	})

	// Build ffmpeg command with detected parameters
	args := d.buildFFmpegArgs(metadata)
	args = append([]string{"-i", "pipe:0"}, args...) // Prepend input from stdin
	args = append(args, "pipe:1")                    // Output to stdout

	cmd := exec.Command(d.config.FFmpegPath, args...)
	cmd.Stdin = bytes.NewReader(data)

	// Set timeout
	if d.config.Timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), d.config.Timeout)
		defer cancel()
		cmd = exec.CommandContext(ctx, d.config.FFmpegPath, args...)
		cmd.Stdin = bytes.NewReader(data)
	}

	logger.Debug("Running ffmpeg command", logging.Fields{
		"args": strings.Join(args, " "),
	})

	output, err := cmd.Output()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			logger.Error(err, "Ffmpeg decode failed", logging.Fields{
				"stderr": string(exitError.Stderr),
			})
		}
		return nil, fmt.Errorf("ffmpeg decode failed: %w", err)
	}

	return d.processFFmpegOutput(output, metadata, "", logger)
}

// buildFFmpegArgs builds the ffmpeg arguments based on configuration and metadata
func (d *Decoder) buildFFmpegArgs(metadata *AudioMetadata) []string {
	args := []string{
		"-f", "f64le", // Output raw float64 little-endian
		"-ac", strconv.Itoa(d.config.TargetChannels), // Target channels
		"-ar", strconv.Itoa(d.config.TargetSampleRate), // Target sample rate
	}

	// Add resampling quality if specified
	if d.config.ResampleQuality != "" && metadata.SampleRate != d.config.TargetSampleRate {
		switch d.config.ResampleQuality {
		case "fast":
			args = append(args, "-af", "aresample=resampler=soxr:precision=16")
		case "medium":
			args = append(args, "-af", "aresample=resampler=soxr:precision=20")
		case "high":
			args = append(args, "-af", "aresample=resampler=soxr:precision=28")
		}
	}

	// Add max duration limit if specified
	if d.config.MaxDuration > 0 {
		args = append(args, "-t", fmt.Sprintf("%.2f", d.config.MaxDuration.Seconds()))
	}

	if d.config.EnableNormalization {
		normFilter := d.buildNormalizationFilter()
		if normFilter != "" {
			// Combine with existing filters or add new filter chain
			if slices.Contains(args, "-af") {
				// Append to existing -af
				for i, arg := range args {
					if arg == "-af" && i+1 < len(args) {
						args[i+1] = args[i+1] + "," + normFilter
						break
					}
				}
			} else {
				args = append(args, "-af", normFilter)
			}
		}
	}

	// Suppress ffmpeg output
	args = append(args, "-v", "error")

	return args
}

// buildNormalizationFilter builds the arguments based on the `DecoderConfig` for a normalization filter
// buildNormalizationFilter builds the arguments based on the `DecoderConfig` for a normalization filter
func (d *Decoder) buildNormalizationFilter() string {
	switch d.config.NormalizationMethod {
	case "loudnorm":
		// EBU R128 loudness normalization
		return fmt.Sprintf("loudnorm=I=%.1f:TP=%.1f:LRA=%.1f",
			d.config.TargetLUFS,
			d.config.TargetPeak,
			d.config.LoudnessRange)

	case "dynaudnorm":
		// Dynamic audio normalization - FIXED: use colons instead of semicolon
		return "dynaudnorm=p=0.95:m=10:s=12"

	case "compand":
		// Compressor/limiter
		return fmt.Sprintf("compand=0.1,0.3:-90/-90,-%.1f/-%.1f,0/0:6:0:-90:0.1",
			math.Abs(d.config.TargetPeak),
			math.Abs(d.config.TargetPeak))

	default:
		return ""
	}
}

// processFFmpegOutput processes the raw output from ffmpeg
func (d *Decoder) processFFmpegOutput(output []byte, inputMetadata *AudioMetadata, sourceURL string, logger logging.Logger) (*AudioData, error) {
	// Convert raw bytes to []float64
	samples := d.bytesToFloat64(output)
	if len(samples) == 0 {
		return nil, fmt.Errorf("no audio samples decoded")
	}

	// Calculate duration based on output samples
	samplesPerChannel := len(samples) / d.config.TargetChannels
	duration := time.Duration(samplesPerChannel) * time.Second / time.Duration(d.config.TargetSampleRate)

	logger.Debug("FFmpeg decode completed successfully", logging.Fields{
		"input_sample_rate":  inputMetadata.SampleRate,
		"input_channels":     inputMetadata.Channels,
		"input_codec":        inputMetadata.Codec,
		"input_duration":     inputMetadata.Duration,
		"output_samples":     len(samples),
		"output_sample_rate": d.config.TargetSampleRate,
		"output_channels":    d.config.TargetChannels,
		"output_duration":    duration.Seconds(),
	})

	// Create optional metadata (audio decoder only knows about the decoding process)
	var metadata *StreamMetadata
	if sourceURL != "" || inputMetadata.Codec != "" {
		metadata = &StreamMetadata{
			URL:         sourceURL,
			Type:        "decoded",
			Format:      d.config.OutputFormat,
			Bitrate:     inputMetadata.Bitrate,
			SampleRate:  d.config.TargetSampleRate,
			Channels:    d.config.TargetChannels,
			Codec:       inputMetadata.Codec,
			ContentType: d.getContentTypeFromCodec(inputMetadata.Codec),
			Headers:     make(map[string]string),
			Timestamp:   time.Now(),
		}
	}

	return &AudioData{
		PCM:        samples,
		SampleRate: d.config.TargetSampleRate,
		Channels:   d.config.TargetChannels,
		Duration:   duration,
		Timestamp:  time.Now(),
		Metadata:   metadata, // nil for simple decode operations
	}, nil
}

// getContentTypeFromCodec maps codec to content type
func (d *Decoder) getContentTypeFromCodec(codec string) string {
	switch codec {
	case "aac":
		return "audio/aac"
	case "mp3":
		return "audio/mpeg"
	case "flac":
		return "audio/flac"
	case "ogg":
		return "audio/ogg"
	case "opus":
		return "audio/opus"
	default:
		return "audio/unknown"
	}
}

// bytesToFloat64 converts raw float64 bytes to []float64
func (d *Decoder) bytesToFloat64(data []byte) []float64 {
	if len(data)%8 != 0 {
		// Trim to multiple of 8 bytes
		data = data[:len(data)-(len(data)%8)]
	}

	if len(data) == 0 {
		return nil
	}

	sampleCount := len(data) / 8
	samples := make([]float64, sampleCount)

	for i := range sampleCount {
		// Convert 8 bytes to float64 (little-endian)
		bits := binary.LittleEndian.Uint64(data[i*8 : i*8+8])
		samples[i] = math.Float64frombits(bits)
	}

	return samples
}

// ValidateConfig validates the decoder configuration
func (d *Decoder) ValidateConfig() error {
	if d.config.TargetSampleRate <= 0 {
		return fmt.Errorf("target sample rate must be positive: %d", d.config.TargetSampleRate)
	}

	if d.config.TargetChannels <= 0 || d.config.TargetChannels > 8 {
		return fmt.Errorf("target channels must be between 1 and 8: %d", d.config.TargetChannels)
	}

	if d.config.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive: %v", d.config.Timeout)
	}

	// Check if ffmpeg and ffprobe are available
	if err := d.checkFFmpegAvailability(); err != nil {
		return fmt.Errorf("ffmpeg not available: %w", err)
	}

	return nil
}

// checkFFmpegAvailability checks if ffmpeg and ffprobe are available
func (d *Decoder) checkFFmpegAvailability() error {
	// Check ffmpeg
	cmd := exec.Command(d.config.FFmpegPath, "-version")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ffmpeg not found at %s: %w", d.config.FFmpegPath, err)
	}

	// Check ffprobe
	cmd = exec.Command(d.config.FFprobePath, "-version")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ffprobe not found at %s: %w", d.config.FFprobePath, err)
	}

	return nil
}

// GetSupportedFormats returns a list of formats supported by this decoder
func (d *Decoder) GetSupportedFormats() []string {
	return []string{
		"aac", "mp3", "wav", "flac", "ogg", "opus", "m4a", "wma",
		"ts", "m3u8", "webm", "mp4", "mov", "avi", "mkv",
		// FFmpeg supports many more formats
	}
}

// GetInfo returns information about the audio decoder (alias for GetConfig)
func (d *Decoder) GetInfo() map[string]any {
	return d.GetConfig()
}

// Close cleans up any resources (no-op for FFmpeg decoder)
func (d *Decoder) Close() error {
	// FFmpeg decoder doesn't maintain persistent resources
	return nil
}
