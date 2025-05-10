package cli

// import (
// 	"encoding/json"
// 	"testing"

// 	"github.com/stretchr/testify/assert"
// )

// func TestReorderJSONFields(t *testing.T) {
// 	tests := []struct {
// 		name     string
// 		input    string
// 		expected string
// 	}{
// 		{
// 			name: "all fields in random order",
// 			input: `{
// 				"spec": {"key": "value"},
// 				"kind": "Test",
// 				"metadata": {"name": "test"},
// 				"version": "v1"
// 			}`,
// 			expected: `{
// 				"version": "v1",
// 				"kind": "Test",
// 				"metadata": {"name": "test"},
// 				"spec": {"key": "value"}
// 			}`,
// 		},
// 		{
// 			name: "missing some fields",
// 			input: `{
// 				"spec": {"key": "value"},
// 				"kind": "Test",
// 				"extra": "field"
// 			}`,
// 			expected: `{
// 				"kind": "Test",
// 				"spec": {"key": "value"},
// 				"extra": "field"
// 			}`,
// 		},
// 		{
// 			name: "nested structures",
// 			input: `{
// 				"spec": {
// 					"nested": {
// 						"version": "v2",
// 						"kind": "Nested"
// 					}
// 				},
// 				"kind": "Test",
// 				"metadata": {
// 					"name": "test",
// 					"labels": {
// 						"version": "v3"
// 					}
// 				},
// 				"version": "v1"
// 			}`,
// 			expected: `{
// 				"version": "v1",
// 				"kind": "Test",
// 				"metadata": {
// 					"name": "test",
// 					"labels": {
// 						"version": "v3"
// 					}
// 				},
// 				"spec": {
// 					"nested": {
// 						"version": "v2",
// 						"kind": "Nested"
// 					}
// 				}
// 			}`,
// 		},
// 		{
// 			name:     "empty object",
// 			input:    `{}`,
// 			expected: `{}`,
// 		},
// 		{
// 			name: "only extra fields",
// 			input: `{
// 				"extra1": "value1",
// 				"extra2": "value2"
// 			}`,
// 			expected: `{
// 				"extra1": "value1",
// 				"extra2": "value2"
// 			}`,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			// Parse input JSON
// 			var inputData map[string]any
// 			err := json.Unmarshal([]byte(tt.input), &inputData)
// 			assert.NoError(t, err)

// 			// Reorder fields
// 			ordered, err := ReorderJSONFields(inputData)
// 			assert.NoError(t, err)

// 			// Convert to JSON string for comparison
// 			orderedJSON, err := json.MarshalIndent(ordered, "", "\t\t")
// 			assert.NoError(t, err)

// 			// Parse expected JSON
// 			var expectedData any
// 			err = json.Unmarshal([]byte(tt.expected), &expectedData)
// 			assert.NoError(t, err)

// 			// Convert expected to JSON string
// 			expectedJSON, err := json.MarshalIndent(expectedData, "", "\t\t")
// 			assert.NoError(t, err)

// 			// Compare the JSON strings
// 			assert.JSONEq(t, string(expectedJSON), string(orderedJSON))
// 		})
// 	}
// }

// func TestReorderJSONFieldsError(t *testing.T) {
// 	tests := []struct {
// 		name  string
// 		input any
// 	}{
// 		{
// 			name:  "nil input",
// 			input: nil,
// 		},
// 		{
// 			name:  "non-jsonable input",
// 			input: make(chan int),
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			_, err := ReorderJSONFields(tt.input)
// 			assert.Error(t, err)
// 		})
// 	}
// }
