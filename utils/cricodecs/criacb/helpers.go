package criacb

import (
	"fmt"
	"math"
)

func getBytesField(row map[string]interface{}, key string) ([]byte, error) {
	val, ok := row[key]
	if !ok {
		return nil, fmt.Errorf("field %s not found", key)
	}

	if b, ok := val.([]byte); ok {
		return b, nil
	}

	return nil, fmt.Errorf("field %s is not bytes", key)
}

func getStringField(row map[string]interface{}, key string) string {
	val, ok := row[key]
	if !ok {
		return ""
	}

	if s, ok := val.(string); ok {
		return s
	}

	return ""
}

func getIntField(row map[string]interface{}, key string) int {
	val, ok := row[key]
	if !ok {
		return 0
	}

	switch v := val.(type) {
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	}

	return 0
}

func align(alignment, offset uint32) uint32 {
	return uint32(math.Ceil(float64(offset)/float64(alignment))) * alignment
}
