package string

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
				DataType: "String",
				Value:    mustNullableAny("test"),
			},
			wantErr: false,
		},
		{
			name: "invalid data type",
			spec: &Spec{
				DataType: "Integer",
				Value:    mustNullableAny("test"),
			},
			wantErr: true,
		},
		{
			name: "missing data type",
			spec: &Spec{
				Value: mustNullableAny("test"),
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
			name: "valid string value",
			spec: &Spec{
				DataType: "String",
				Value:    mustNullableAny("test"),
			},
			expected: "test",
		},
		{
			name: "nil value",
			spec: &Spec{
				DataType: "String",
				Value:    types.NullableAny{},
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
			name: "valid string",
			spec: &Spec{
				DataType: "String",
			},
			value:   mustNullableAny("test"),
			wantErr: false,
		},
		{
			name: "invalid type",
			spec: &Spec{
				DataType: "String",
			},
			value:   mustNullableAny(42),
			wantErr: true,
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

func TestLoadStringSpec(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "valid string spec",
			data:    []byte(`{"dataType": "String", "value": "test"}`),
			wantErr: false,
		},
		{
			name:    "invalid json",
			data:    []byte(`invalid json`),
			wantErr: true,
		},
		{
			name:    "invalid value type",
			data:    []byte(`{"dataType": "String", "value": 42}`),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadStringSpec(tt.data)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, got)
				spec, ok := got.(*Spec)
				assert.True(t, ok)
				assert.Equal(t, "String", spec.DataType)
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
