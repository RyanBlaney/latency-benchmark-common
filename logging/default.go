package logging

import (
	"context"
	"fmt"
	"log"
	"maps"
	"os"
)

// DefaultLogger is a colored logger implementation using Go's standard log package
// Debug/Info -> stdout (no color)
// Warn -> stderr (yellow)
// Error -> stderr (red)
// Fatal -> stderr (bold red)
type DefaultLogger struct {
	stdoutLogger *log.Logger
	stderrLogger *log.Logger
	level        Level
	fields       Fields
	useColors    bool
}

// NewDefaultLogger creates a new default logger with colored output
func NewDefaultLogger() *DefaultLogger {
	return &DefaultLogger{
		stdoutLogger: log.New(os.Stdout, "", log.LstdFlags),
		stderrLogger: log.New(os.Stderr, "", log.LstdFlags),
		level:        InfoLevel,
		fields:       make(Fields),
		useColors:    isTerminal(),
	}
}

// NewDefaultLoggerNoColor creates a new default logger without colored output
func NewDefaultLoggerNoColor() *DefaultLogger {
	return &DefaultLogger{
		stdoutLogger: log.New(os.Stdout, "", log.LstdFlags),
		stderrLogger: log.New(os.Stderr, "", log.LstdFlags),
		level:        InfoLevel,
		fields:       make(Fields),
		useColors:    false,
	}
}

// isTerminal checks if the logger is running in a terminal that supports colors
func isTerminal() bool {
	// TODO: realistic check for TTY
	// Simple check - in a real implementation you might use a library like isatty
	// For now, just check if we have a TTY
	if fileInfo, _ := os.Stdout.Stat(); fileInfo != nil {
		return (fileInfo.Mode() & os.ModeCharDevice) != 0
	}
	return false
}

func (d *DefaultLogger) formatMessage(level Level, err error, msg string, fields ...Fields) string {
	// Merge all fields
	allFields := make(Fields)
	maps.Copy(allFields, d.fields)
	for _, f := range fields {
		maps.Copy(allFields, f)
	}

	// Build the message
	logMsg := fmt.Sprintf("[%s] %s", level.String(), msg)

	if err != nil {
		logMsg += fmt.Sprintf(": %v", err)
	}

	if len(allFields) > 0 {
		logMsg += fmt.Sprintf(" %+v", allFields)
	}

	// Apply colors if enabled
	if d.useColors {
		switch level {
		case WarnLevel:
			logMsg = ColorYellow + logMsg + ColorReset
		case ErrorLevel:
			logMsg = ColorRed + logMsg + ColorReset
		case FatalLevel:
			logMsg = ColorBold + ColorRed + logMsg + ColorReset
		}
	}

	return logMsg
}

func (d *DefaultLogger) log(level Level, err error, msg string, fields ...Fields) {
	if level < d.level {
		return
	}

	formattedMsg := d.formatMessage(level, err, msg, fields...)

	switch level {
	case DebugLevel, InfoLevel:
		d.stdoutLogger.Println(formattedMsg)
	case WarnLevel, ErrorLevel:
		d.stderrLogger.Println(formattedMsg)
	case FatalLevel:
		d.stderrLogger.Println(formattedMsg)
		os.Exit(1)
	}
}

func (d *DefaultLogger) Debug(msg string, fields ...Fields) {
	d.log(DebugLevel, nil, msg, fields...)
}

func (d *DefaultLogger) Info(msg string, fields ...Fields) {
	d.log(InfoLevel, nil, msg, fields...)
}

func (d *DefaultLogger) Warn(msg string, fields ...Fields) {
	d.log(WarnLevel, nil, msg, fields...)
}

func (d *DefaultLogger) Error(err error, msg string, fields ...Fields) {
	d.log(ErrorLevel, err, msg, fields...)
}

func (d *DefaultLogger) Fatal(err error, msg string, fields ...Fields) {
	d.log(FatalLevel, err, msg, fields...)
}

func (d *DefaultLogger) WithFields(fields Fields) Logger {
	newFields := make(Fields)
	maps.Copy(newFields, d.fields)
	maps.Copy(newFields, fields)

	return &DefaultLogger{
		stdoutLogger: d.stdoutLogger,
		stderrLogger: d.stderrLogger,
		level:        d.level,
		fields:       newFields,
		useColors:    d.useColors,
	}
}

func (d *DefaultLogger) WithContext(ctx context.Context) Logger {
	// Extract fields from context if any
	if fields, ok := ctx.Value("logger_fields").(Fields); ok {
		return d.WithFields(fields)
	}
	return d
}

func (d *DefaultLogger) SetLevel(level Level) {
	d.level = level
}

// NoOpLogger is a logger that does nothing - useful for testing or when logging is disabled
// TODO: explain why this is useful
type NoOpLogger struct{}

func (n *NoOpLogger) Debug(msg string, fields ...Fields)            {}
func (n *NoOpLogger) Info(msg string, fields ...Fields)             {}
func (n *NoOpLogger) Warn(msg string, fields ...Fields)             {}
func (n *NoOpLogger) Error(err error, msg string, fields ...Fields) {}
func (n *NoOpLogger) Fatal(err error, msg string, fields ...Fields) {}
func (n *NoOpLogger) WithFields(fields Fields) Logger               { return n }
func (n *NoOpLogger) WithContext(ctx context.Context) Logger        { return n }
func (n *NoOpLogger) SetLevel(level Level)                          {}
