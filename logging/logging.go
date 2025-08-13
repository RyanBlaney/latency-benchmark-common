package logging

import (
	"context"
	"maps"
	"reflect"
)

// ANSI color codes for terminal output
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorYellow = "\033[33m"
	ColorBold   = "\033[1m"
)

// Level represents log levels
type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	case FatalLevel:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// Fields represents structured logging fields
type Fields map[string]any

// Logger defines the interface that the library expects for logging
type Logger interface {
	Debug(msg string, fields ...Fields)
	Info(msg string, fields ...Fields)
	Warn(msg string, fields ...Fields)
	Error(err error, msg string, fields ...Fields)
	Fatal(err error, msg string, fields ...Fields)

	// WithFields returns a logger with preset fields
	WithFields(fields Fields) Logger

	// WithContext returns a logger that can extract fields from context
	WithContext(ctx context.Context) Logger

	// SetLevel sets the minimum log level
	SetLevel(level Level)
}

var globalLogger Logger = NewDefaultLogger()

// SetGlobalLogger sets the global logger instance
func SetGlobalLogger(logger Logger) {
	if logger == nil {
		globalLogger = &NoOpLogger{}
	} else {
		globalLogger = logger
	}
}

// GetGlobalLogger returns the current global logger
func GetGlobalLogger() Logger {
	return globalLogger
}

// LoggerFromAppLogger creates a library logger from an application logger
// This allows the library to remain standalone while integrating with an existing logger
//
// Example integration:
// existingAppLogger := existinglog.NewLogWithFields(existinglog.Fields{"component": "hls"})
// logging.SetGlobalLogger(logging.LoggerFromAppLogger(existingAppLogger))
func LoggerFromAppLogger(appLogger any) Logger {
	if appLogger == nil {
		return NewDefaultLogger()
	}

	// Check if the app logger uses our interface directly
	if logger, ok := appLogger.(Logger); ok {
		return logger
	}

	// Check if it implements the interface methods
	if hasMethod(appLogger, "Debug") && hasMethod(appLogger, "Info") &&
		hasMethod(appLogger, "Error") && hasMethod(appLogger, "WithFields") {
		return &AppLoggerAdapter{appLogger: appLogger}
	}

	// Fallback to default logger
	return NewDefaultLogger()
}

// hasMethod checks if an interface has a method using reflection
func hasMethod(obj any, methodName string) bool {
	if obj == nil {
		return false
	}

	objType := reflect.TypeOf(obj)
	if objType == nil {
		return false
	}

	// Check if it's a pointer type
	if objType.Kind() == reflect.Ptr {
		objType = objType.Elem()
	}

	// Look for the method
	_, found := objType.MethodByName(methodName)
	return found
}

// AppLoggerAdapter adapts an application logger to our interface
type AppLoggerAdapter struct {
	appLogger any
}

func (a *AppLoggerAdapter) Debug(msg string, fields ...Fields) {
	// Check if Debug method exists at runtime
	if debugger, ok := a.appLogger.(interface{ Debug(string, ...any) }); ok {
		if len(fields) > 0 {
			// Merge all fields into a single map for logging
			allFields := make(map[string]any)
			for _, fieldMap := range fields {
				maps.Copy(allFields, fieldMap)
			}
			debugger.Debug("%s %+v", msg, allFields)
		} else {
			debugger.Debug("%s", msg)
		}
	}
	// If Debug method doesn't exist, silently ignore
}

func (a *AppLoggerAdapter) Info(msg string, fields ...Fields) {
	// Check if Info method exists at runtime
	if infoer, ok := a.appLogger.(interface{ Info(string, ...any) }); ok {
		if len(fields) > 0 {
			// Merge all fields into a single map for logging
			allFields := make(map[string]any)
			for _, fieldMap := range fields {
				maps.Copy(allFields, fieldMap)
			}
			infoer.Info("%s %+v", msg, allFields)
		} else {
			infoer.Info("%s", msg)
		}
	}
}

func (a *AppLoggerAdapter) Warn(msg string, fields ...Fields) {
	// Try Warn method first
	if warner, ok := a.appLogger.(interface{ Warn(string, ...any) }); ok {
		if len(fields) > 0 {
			allFields := make(map[string]any)
			for _, fieldMap := range fields {
				maps.Copy(allFields, fieldMap)
			}
			warner.Warn("%s %+v", msg, allFields)
		} else {
			warner.Warn("%s", msg)
		}
		return
	}

	// Fallback to Info with WARN prefix if Warn method doesn't exist
	if infoer, ok := a.appLogger.(interface{ Info(string, ...any) }); ok {
		if len(fields) > 0 {
			allFields := make(map[string]any)
			for _, fieldMap := range fields {
				maps.Copy(allFields, fieldMap)
			}
			infoer.Info("WARN: %s %+v", msg, allFields)
		} else {
			infoer.Info("WARN: %s", msg)
		}
	}
}

func (a *AppLoggerAdapter) Error(err error, msg string, fields ...Fields) {
	if errorer, ok := a.appLogger.(interface{ Error(error, ...any) }); ok {
		if len(fields) > 0 {
			allFields := make(map[string]any)
			for _, fieldMap := range fields {
				maps.Copy(allFields, fieldMap)
			}
			errorer.Error(err, "%s %+v", msg, allFields)
		} else {
			errorer.Error(err, msg)
		}
	}
}

func (a *AppLoggerAdapter) Fatal(err error, msg string, fields ...Fields) {
	// Try Fatal method first
	if fataler, ok := a.appLogger.(interface{ Fatal(string, ...any) }); ok {
		if len(fields) > 0 {
			allFields := make(map[string]any)
			for _, fieldMap := range fields {
				maps.Copy(allFields, fieldMap)
			}
			fataler.Fatal("%s: %v %+v", msg, err, allFields)
		} else {
			fataler.Fatal("%s: %v", msg, err)
		}
		return
	}

	// Fallback to Error method if Fatal doesn't exist
	if errorer, ok := a.appLogger.(interface{ Error(error, ...any) }); ok {
		if len(fields) > 0 {
			allFields := make(map[string]any)
			for _, fieldMap := range fields {
				maps.Copy(allFields, fieldMap)
			}
			errorer.Error(err, "FATAL: %s %+v", msg, allFields)
		} else {
			errorer.Error(err, "FATAL: %s", msg)
		}
	}
	// Note: We don't call os.Exit here since the app logger should handle that
}

func (a *AppLoggerAdapter) WithFields(fields Fields) Logger {
	// Try to use the app logger's WithFields if available
	if fielder, ok := a.appLogger.(interface{ WithFields(any) any }); ok {
		newAppLogger := fielder.WithFields(fields)
		return &AppLoggerAdapter{appLogger: newAppLogger}
	}
	// Fallback: return same adapter (fields would be ignored)
	return a
}

func (a *AppLoggerAdapter) WithContext(ctx context.Context) Logger {
	// Extract fields from context if any
	if fields, ok := ctx.Value("logger_fields").(Fields); ok {
		return a.WithFields(fields)
	}
	return a
}

func (a *AppLoggerAdapter) SetLevel(level Level) {
	// Try to set level if the app logger supports it
	if leveler, ok := a.appLogger.(interface{ SetLevel(any) }); ok {
		leveler.SetLevel(level)
	}
}

// Package-level logging functions that use the global logger
func Debug(msg string, fields ...Fields) {
	globalLogger.Debug(msg, fields...)
}

func Info(msg string, fields ...Fields) {
	globalLogger.Info(msg, fields...)
}

func Warn(msg string, fields ...Fields) {
	globalLogger.Warn(msg, fields...)
}

func Error(err error, msg string, fields ...Fields) {
	globalLogger.Error(err, msg, fields...)
}

func Fatal(err error, msg string, fields ...Fields) {
	globalLogger.Fatal(err, msg, fields...)
}

func WithFields(fields Fields) Logger {
	return globalLogger.WithFields(fields)
}

func WithContext(ctx context.Context) Logger {
	return globalLogger.WithContext(ctx)
}

func SetLevel(level Level) {
	globalLogger.SetLevel(level)
}

// DisableColors globally disables color output for the default logger
func DisableColors() {
	if defaultLogger, ok := globalLogger.(*DefaultLogger); ok {
		defaultLogger.useColors = false
	}
}

// EnableColors globally enables color output for the default logger
func EnableColors() {
	if defaultLogger, ok := globalLogger.(*DefaultLogger); ok {
		defaultLogger.useColors = true
	}
}
