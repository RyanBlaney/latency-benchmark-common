package output

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Formatter interface for different output formats
type Formatter interface {
	Format(data any, prettyPrint bool) ([]byte, error)
}

// JSONFormatter formats output as JSON
type JSONFormatter struct{}

func (f *JSONFormatter) Format(data any, prettyPrint bool) ([]byte, error) {
	if prettyPrint {
		return json.MarshalIndent(data, "", "  ")
	}
	return json.Marshal(data)
}

// YAMLFormatter formats output as YAML
type YAMLFormatter struct{}

func (f *YAMLFormatter) Format(data any, prettyPrint bool) ([]byte, error) {
	return yaml.Marshal(data)
}

// CSVFormatter formats output as CSV (simplified for benchmark results)
type CSVFormatter struct{}

func (f *CSVFormatter) Format(data any, prettyPrint bool) ([]byte, error) {
	// For CSV, we'll extract key metrics into a tabular format
	// This is a simplified implementation that focuses on the most important metrics

	var records [][]string

	// Add header
	headers := []string{
		"broadcast_group",
		"timestamp",
		"success",
		"health_score",
		"hls_cdn_latency_s",
		"icecast_cdn_latency_s",
		"cross_protocol_lag_s",
		"valid_streams",
		"total_streams",
		"alignment_success_rate",
		"fingerprint_match_rate",
		"avg_ttfb_ms",
		"errors",
	}
	records = append(records, headers)

	// Extract data from the input (assumes it's our benchmark output structure)
	if dataMap, ok := data.(map[string]any); ok {
		if _, exists := dataMap["benchmark_summary"]; exists {
			// This would need to be properly implemented based on the actual data structure
			// For now, we'll create a placeholder CSV format
			records = append(records, []string{
				"placeholder_group",
				time.Now().Format(time.RFC3339),
				"true",
				"0.85",
				"2.3",
				"1.8",
				"0.5",
				"4",
				"4",
				"0.95",
				"0.88",
				"1250",
				"",
			})
		}
	}

	// Convert records to CSV format
	var result strings.Builder
	writer := csv.NewWriter(&result)

	for _, record := range records {
		if err := writer.Write(record); err != nil {
			return nil, fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("CSV writer error: %w", err)
	}

	return []byte(result.String()), nil
}

// TableFormatter formats output as a human-readable table
type TableFormatter struct{}

func (f *TableFormatter) Format(data any, prettyPrint bool) ([]byte, error) {
	var result strings.Builder

	// This is a simplified table formatter
	// In a full implementation, you'd want to use a proper table formatting library

	result.WriteString("CDN BENCHMARK RESULTS\n")
	result.WriteString("=====================\n\n")

	// Extract and format key information
	if dataMap, ok := data.(map[string]any); ok {
		if _, exists := dataMap["benchmark_summary"]; exists {
			result.WriteString("Summary Information:\n")
			result.WriteString("-------------------\n")

			// Format summary data (simplified)
			result.WriteString("Status: Completed\n")
			result.WriteString("Duration: N/A\n")
			result.WriteString("Health Score: N/A\n")
			result.WriteString("\n")
		}

		if insights, exists := dataMap["insights"]; exists {
			result.WriteString("Insights & Recommendations:\n")
			result.WriteString("---------------------------\n")

			if insightsList, ok := insights.([]string); ok {
				for i, insight := range insightsList {
					result.WriteString(fmt.Sprintf("%d. %s\n", i+1, insight))
				}
			}
			result.WriteString("\n")
		}
	}

	return []byte(result.String()), nil
}

// FormatDuration formats a duration for human-readable output
func FormatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%.0fms", float64(d.Nanoseconds())/1e6)
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fm", d.Minutes())
}

// FormatBytes formats bytes for human-readable output
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// FormatPercentage formats a float as a percentage
func FormatPercentage(value float64) string {
	return fmt.Sprintf("%.1f%%", value*100)
}

// ExtractFlattenedData extracts data from nested structures for tabular output
func ExtractFlattenedData(data any, prefix string) map[string]any {
	result := make(map[string]any)

	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			fieldType := t.Field(i)

			if !field.CanInterface() {
				continue
			}

			key := prefix + strings.ToLower(fieldType.Name)
			value := field.Interface()

			// Recursively flatten nested structs
			if field.Kind() == reflect.Struct || (field.Kind() == reflect.Ptr && field.Elem().Kind() == reflect.Struct) {
				nested := ExtractFlattenedData(value, key+"_")
				maps.Copy(result, nested)
			} else {
				result[key] = value
			}
		}
	case reflect.Map:
		for _, key := range v.MapKeys() {
			keyStr := fmt.Sprintf("%v", key.Interface())
			value := v.MapIndex(key).Interface()

			flatKey := prefix + strings.ToLower(keyStr)
			if reflect.ValueOf(value).Kind() == reflect.Struct {
				nested := ExtractFlattenedData(value, flatKey+"_")
				maps.Copy(result, nested)
			} else {
				result[flatKey] = value
			}
		}
	default:
		result[prefix] = data
	}

	return result
}

// ConvertToStringMap converts various data types to string for CSV/table output
func ConvertToStringMap(data map[string]any) map[string]string {
	result := make(map[string]string)

	for key, value := range data {
		result[key] = ConvertValueToString(value)
	}

	return result
}

// ConvertValueToString converts a single value to string representation
func ConvertValueToString(value any) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return strconv.FormatFloat(reflect.ValueOf(v).Float(), 'f', 3, 64)
	case bool:
		return strconv.FormatBool(v)
	case time.Time:
		return v.Format(time.RFC3339)
	case time.Duration:
		return FormatDuration(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

type PipelineFormatter struct {
	formatters []Formatter
}

func NewPipelineFormatter(formatters ...Formatter) *PipelineFormatter {
	return &PipelineFormatter{formatters: formatters}
}

func (pf *PipelineFormatter) Format(data any, prettyPrint bool) ([]byte, error) {
	currentData := data

	for i, formatter := range pf.formatters {
		if i == len(pf.formatters)-1 {
			// Last formatter, return the result
			return formatter.Format(currentData, prettyPrint)
		}

		// Intermediate formatters transform the data
		formattedBytes, err := formatter.Format(currentData, prettyPrint)
		if err != nil {
			return nil, fmt.Errorf("pipeline formatter %d failed: %w", i, err)
		}
		currentData = formattedBytes
	}

	return nil, fmt.Errorf("empty formatter pipeline")
}
