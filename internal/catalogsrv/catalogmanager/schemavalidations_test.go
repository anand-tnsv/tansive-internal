package catalogmanager

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/jackc/pgtype"
	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/validationerrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/yaml"
)

func TestYamlToJson(t *testing.T) {
	y := `
version: v1
kind: ParameterSchema
metadata:
  name: example
  catalog: example-catalog
  path: /example
spec:
  dataType: Integer
  validation:
    minValue: 1
    maxValue: 10
  default: 5
`
	j, err := yaml.YAMLToJSON([]byte(y))
	if assert.NoError(t, err) {
		assert.NotEmpty(t, j)

		var prettyJSON []byte
		var err error
		// Indent the raw JSON
		buffer := bytes.NewBuffer(prettyJSON)
		err = json.Indent(buffer, j, "", "    ")
		if assert.NoError(t, err) {
			t.Logf("\n%s", buffer.String())
		}
	}
}

// Tests each section of a Parameter resource for validation errors
func TestNewParameterSchema(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string
		expected string
	}{
		{
			name: "valid resource",
			yamlData: `
version: v1
kind: ParameterSchema
metadata:
  name: example
  catalog: example-catalog
  path: /example
spec:
  dataType: Integer
  validation:
    minValue: 1
    maxValue: 10
  default: 5
`,
			expected: "",
		},
		{
			name: "missing required version",
			yamlData: `
kind: ParameterSchema
metadata:
  name: example
  catalog: example-catalog
  path: /example
spec:
  dataType: Integer
  validation:
    minValue: 1
    maxValue: 10
  default: 5
`,
			expected: schemaerr.ErrMissingRequiredAttribute("version").Error(),
		},
		{
			name: "bad name format",
			yamlData: `
version: v1
kind: ParameterSchema
metadata:
  name: Invalid Name!
  catalog: example-catalog
  path: /example
spec:
  dataType: Integer
  validation:
    minValue: 1
    maxValue: 10
  default: 5
`,
			expected: schemaerr.ErrInvalidNameFormat("metadata.name", "Invalid Name!").Error(),
		},
		{
			name: "bad dataType",
			yamlData: `
version: v1
kind: ParameterSchema
metadata:
  name: example
  catalog: example-catalog
  path: /example
spec:
  dataType: InvalidType
  validation:
    minValue: 1
    maxValue: 10
  default: 5
`,
			expected: schemaerr.ErrUnsupportedDataType("spec.dataType", "InvalidType").Error(),
		},
		{
			name: "bad default value",
			yamlData: `
version: v1
kind: ParameterSchema
metadata:
  name: example
  catalog: example-catalog
  path: /example
spec:
  dataType: Integer
  validation:
    minValue: 1
    maxValue: 10
  default: 11
`,
			expected: schemaerr.ValidationError{
				Field:  "default",
				ErrStr: validationerrors.ErrValueAboveMax.Error(),
			}.Error(),
		},
		{
			name: "bad validation values",
			yamlData: `
version: v1
kind: ParameterSchema
metadata:
  name: example
  catalog: example-catalog
  path: /example
spec:
  dataType: Integer
  validation:
    minValue: 1
    maxValue: -1
  default: 5
`,
			expected: schemaerr.ErrMaxValueLessThanMinValue("validation.maxValue").Error(),
		},
	}
	// Run tests
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")
	// Set the tenant ID and project ID in the context
	ctx = catcommon.SetTenantIdInContext(ctx, tenantID)
	ctx = catcommon.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = db.DB(ctx).DeleteTenant(ctx, tenantID)
	})
	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)

	// create catalog example-catalog
	cat := &models.Catalog{
		Name:        "example-catalog",
		Description: "An example catalog",
		Info:        pgtype.JSONB{Status: pgtype.Null},
		ProjectID:   projectID,
	}
	err = db.DB(ctx).CreateCatalog(ctx, cat)
	assert.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newDb()
			t.Cleanup(func() {
				db.DB(ctx).Close(ctx)
			})

			tenantID := types.TenantId("TABCDE")
			projectID := types.ProjectId("PABCDE")
			// Set the tenant ID and project ID in the context
			ctx = catcommon.SetTenantIdInContext(ctx, tenantID)
			ctx = catcommon.SetProjectIdInContext(ctx, projectID)
			jsonData, err := yaml.YAMLToJSON([]byte(tt.yamlData))
			if assert.NoError(t, err) {
				_, err := NewSchema(ctx, jsonData, nil)
				errStr := ""
				if err != nil {
					errStr = err.Error()
				}
				if errStr != tt.expected {
					t.Errorf("got %v, want %v", err, tt.expected)
				}
			}
		})
	}
}

func TestNewCollectionSchema(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string
		expected string
	}{
		{
			name: "valid collection schema with schema",
			yamlData: `
version: v1
kind: CollectionSchema
metadata:
  name: app-config-collection
  catalog: my-catalog
  path: /valid/path
spec:
  parameters:
    maxRetries:
      schema: integer-param-schema
      default: 5
  collections:
    databaseConfig:
      schema: database-config-collection
`,
			expected: "",
		},
		{
			name: "valid collection schema with dataType",
			yamlData: `
version: v1
kind: CollectionSchema
metadata:
  name: app-config-collection
  catalog: my-catalog
  path: /valid/path
spec:
  parameters:
    maxRetries:
      dataType: Integer
      default: 5
  collections:
    databaseConfig:
      schema: database-config-collection
`,
			expected: "",
		},
		{
			name: "missing required version",
			yamlData: `
kind: CollectionSchema
metadata:
  name: app-config-collection
  catalog: my-catalog
  path: /valid/path
spec:
  parameters:
    maxRetries:
      dataType: Integer
      default: 5
  collections:
    databaseConfig:
      schema: database-config-collection
`,
			expected: schemaerr.ErrMissingRequiredAttribute("version").Error(),
		},
		{
			name: "missing both schema and dataType",
			yamlData: `
version: v1
kind: CollectionSchema
metadata:
  name: app-config-collection
  catalog: my-catalog
  path: /valid/path
spec:
  parameters:
    maxRetries:
      default: 5
  collections:
    databaseConfig:
      schema: database-config-collection
`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrMissingSchemaOrType("spec.parameters.maxRetries.schema"),
				schemaerr.ErrMissingSchemaOrType("spec.parameters.maxRetries.dataType"),
			}.Error(),
		},
		{
			name: "invalid name format in metadata",
			yamlData: `
version: v1
kind: CollectionSchema
metadata:
  name: Invalid Name!
  catalog: my-catalog
  path: /valid/path
spec:
  parameters:
    maxRetries:
      schema: integer-param-schema
      default: 5
  collections:
    databaseConfig:
      schema: database-config-collection
`,
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrInvalidNameFormat("metadata.name", "Invalid Name!"),
			}.Error(),
		},
	}
	// Run tests
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")
	// Set the tenant ID and project ID in the context
	ctx = catcommon.SetTenantIdInContext(ctx, tenantID)
	ctx = catcommon.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = db.DB(ctx).DeleteTenant(ctx, tenantID)
	})
	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)

	// create catalog example-catalog
	cat := &models.Catalog{
		Name:        "my-catalog",
		Description: "An example catalog",
		Info:        pgtype.JSONB{Status: pgtype.Null},
		ProjectID:   projectID,
	}
	err = db.DB(ctx).CreateCatalog(ctx, cat)
	assert.NoError(t, err)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newDb()
			t.Cleanup(func() {
				db.DB(ctx).Close(ctx)
			})
			tenantID := types.TenantId("TABCDE")
			projectID := types.ProjectId("PABCDE")
			// Set the tenant ID and project ID in the context
			ctx = catcommon.SetTenantIdInContext(ctx, tenantID)
			ctx = catcommon.SetProjectIdInContext(ctx, projectID)
			jsonData, err := yaml.YAMLToJSON([]byte(tt.yamlData))
			if assert.NoError(t, err) {
				_, err := NewSchema(ctx, jsonData, nil)
				errStr := ""
				if err != nil {
					errStr = err.Error()
				}
				if errStr != tt.expected {
					t.Errorf("got %v, want %v", err, tt.expected)
				}
			}
		})
	}
}
