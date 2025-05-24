package json

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

func TestSpec_ValidateSpec(t *testing.T) {
	tests := []struct {
		name    string
		spec    *Spec
		wantErr bool
	}{
		{
			name: "valid spec",
			spec: &Spec{
				DataType: "JSON",
				Value:    mustNullableAny(map[string]any{"key": "value"}),
				Schema:   `{"type": "object", "properties": {"key": {"type": "string"}}}`,
			},
			wantErr: false,
		},
		{
			name: "invalid data type",
			spec: &Spec{
				DataType: "String",
				Value:    mustNullableAny(map[string]any{"key": "value"}),
				Schema:   `{"type": "object", "properties": {"key": {"type": "string"}}}`,
			},
			wantErr: true,
		},
		{
			name: "missing data type",
			spec: &Spec{
				Value:  mustNullableAny(map[string]any{"key": "value"}),
				Schema: `{"type": "object", "properties": {"key": {"type": "string"}}}`,
			},
			wantErr: true,
		},
		{
			name: "missing schema",
			spec: &Spec{
				DataType: "JSON",
				Value:    mustNullableAny(map[string]any{"key": "value"}),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := tt.spec.ValidateSpec()
			if tt.wantErr {
				assert.NotEmpty(t, errs)
			} else {
				assert.Empty(t, errs)
			}
		})
	}
}

func TestSpec_GetValue(t *testing.T) {
	tests := []struct {
		name     string
		spec     *Spec
		expected any
	}{
		{
			name: "valid JSON value",
			spec: &Spec{
				DataType: "JSON",
				Value:    mustNullableAny(map[string]any{"key": "value"}),
				Schema:   `{"type": "object", "properties": {"key": {"type": "string"}}}`,
			},
			expected: map[string]any{"key": "value"},
		},
		{
			name: "nil value",
			spec: &Spec{
				DataType: "JSON",
				Value:    types.NullableAny{},
				Schema:   `{"type": "object", "properties": {"key": {"type": "string"}}}`,
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.spec.GetValue()
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestSpec_ValidateValue(t *testing.T) {
	tests := []struct {
		name    string
		spec    *Spec
		value   types.NullableAny
		wantErr bool
	}{
		{
			name: "valid JSON value",
			spec: &Spec{
				DataType: "JSON",
				Schema:   `{"type": "object", "properties": {"key": {"type": "string"}}}`,
			},
			value:   mustNullableAny(map[string]any{"key": "value"}),
			wantErr: false,
		},
		{
			name: "invalid JSON value",
			spec: &Spec{
				DataType: "JSON",
				Schema:   `{"type": "object", "properties": {"key": {"type": "string"}}}`,
			},
			value:   mustNullableAny(map[string]any{"key": 42}),
			wantErr: true,
		},
		{
			name: "nil value",
			spec: &Spec{
				DataType: "JSON",
				Schema:   `{"type": "object", "properties": {"key": {"type": "string"}}}`,
			},
			value:   types.NullableAny{},
			wantErr: false,
		},
		{
			name: "complex nested object",
			spec: &Spec{
				DataType: "JSON",
				Schema:   `{"type": "object", "properties": {"nested": {"type": "object", "properties": {"array": {"type": "array", "items": {"type": "string"}}}}}}`,
			},
			value:   mustNullableAny(map[string]any{"nested": map[string]any{"array": []any{"one", "two", "three"}}}),
			wantErr: false,
		},
		{
			name: "invalid complex nested object",
			spec: &Spec{
				DataType: "JSON",
				Schema:   `{"type": "object", "properties": {"nested": {"type": "object", "properties": {"array": {"type": "array", "items": {"type": "string"}}}}}}`,
			},
			value:   mustNullableAny(map[string]any{"nested": map[string]any{"array": []any{1, 2, 3}}}),
			wantErr: true,
		},
		{
			name: "large integer",
			spec: &Spec{
				DataType: "JSON",
				Schema:   `{"type": "object", "properties": {"key": {"type": "integer"}}}`,
			},
			value:   mustNullableAny(map[string]any{"key": int64(9007199254740991)}), // 2^53 - 1
			wantErr: false,
		},
		{
			name: "very large integer",
			spec: &Spec{
				DataType: "JSON",
				Schema:   `{"type": "object", "properties": {"key": {"type": "integer"}}}`,
			},
			value:   mustNullableAny(map[string]any{"key": int64(9007199254740992)}), // 2^53
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.spec.ValidateValue(tt.value)
			if tt.wantErr {
				assert.Error(t, err)
				var appErr apperrors.Error
				assert.ErrorAs(t, err, &appErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLoadJSONSpec(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "valid JSON spec",
			data:    []byte(`{"dataType": "JSON", "value": {"key": "value"}, "schema": "{\"type\": \"object\", \"properties\": {\"key\": {\"type\": \"string\"}}}"}`),
			wantErr: false,
		},
		{
			name:    "invalid json",
			data:    []byte(`invalid json`),
			wantErr: true,
		},
		{
			name:    "invalid schema",
			data:    []byte(`{"dataType": "JSON", "value": {"key": "value"}, "schema": "invalid schema"}`),
			wantErr: true,
		},
		{
			name:    "value not matching schema",
			data:    []byte(`{"dataType": "JSON", "value": {"key": 42}, "schema": "{\"type\": \"object\", \"properties\": {\"key\": {\"type\": \"string\"}}}"}`),
			wantErr: true,
		},
		{
			name:    "complex nested object",
			data:    []byte(`{"dataType": "JSON", "value": {"nested": {"array": ["one", "two", "three"]}}, "schema": "{\"type\": \"object\", \"properties\": {\"nested\": {\"type\": \"object\", \"properties\": {\"array\": {\"type\": \"array\", \"items\": {\"type\": \"string\"}}}}}}"}`),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadJSONSpec(tt.data)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, got)
				spec, ok := got.(*Spec)
				assert.True(t, ok)
				assert.Equal(t, "JSON", spec.DataType)
			}
		})
	}
}

func TestSpec_GetMIMEType(t *testing.T) {
	spec := &Spec{}
	assert.Equal(t, "application/json", spec.GetMIMEType())
}

// Helper function to create a NullableAny value, panicking if there's an error
func mustNullableAny(v any) types.NullableAny {
	na, err := types.NullableAnyFrom(v)
	if err != nil {
		panic(err)
	}
	return na
}
