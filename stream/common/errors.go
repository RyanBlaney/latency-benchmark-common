package common

import (
	"maps"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
)

// StreamError represents stream-related errors with integrated logging
type StreamError struct {
	Type    StreamType     `json:"type"`
	URL     string         `json:"url"`
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Cause   error          `json:"-"`
	Fields  logging.Fields `json:"fields,omitempty"`
}

func (e *StreamError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

func (e *StreamError) Unwrap() error {
	return e.Cause
}

// Log logs this error using the global logger
func (e *StreamError) Log() {
	fields := logging.Fields{
		"stream_type": string(e.Type),
		"url":         e.URL,
		"error_code":  e.Code,
	}

	maps.Copy(fields, e.Fields)

	logging.Error(e.Cause, e.Message, fields)
}

// LogWith logs this error using a specific logger
func (e *StreamError) LogWith(logger logging.Logger) {
	fields := logging.Fields{
		"stream_type": string(e.Type),
		"url":         e.URL,
		"error_code":  e.Code,
	}

	// Add any additional fields
	maps.Copy(fields, e.Fields)

	logger.Error(e.Cause, e.Message, fields)
}

// Common error codes
const (
	ErrCodeConnection    = "CONNECTION_FAILED" // Fixed typo
	ErrCodeTimeout       = "TIMEOUT"
	ErrCodeInvalidFormat = "INVALID_FORMAT"
	ErrCodeDecoding      = "DECODING_FAILED"
	ErrCodeMetadata      = "METADATA_ERROR"
	ErrCodeUnsupported   = "UNSUPPORTED_STREAM"
)

// NewStreamError creates a new stream error
func NewStreamError(streamType StreamType, url, code, message string, cause error) *StreamError {
	return &StreamError{
		Type:    streamType,
		URL:     url,
		Code:    code,
		Message: message,
		Cause:   cause,
		Fields:  make(logging.Fields),
	}
}

// NewStreamErrorWithFields creates a new stream error with additional fields
func NewStreamErrorWithFields(streamType StreamType, url, code, message string, cause error, fields logging.Fields) *StreamError {
	return &StreamError{
		Type:    streamType,
		URL:     url,
		Code:    code,
		Message: message,
		Cause:   cause,
		Fields:  fields,
	}
}
