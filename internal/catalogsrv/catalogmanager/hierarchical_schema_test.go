package catalogmanager

import (
	"encoding/json"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schemamanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/config"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"sigs.k8s.io/yaml"
)

func TestSaveHierarchicalSchema(t *testing.T) {
	if !config.HierarchicalSchemas {
		t.Skip("Hierarchical schemas are not enabled")
	}
	emptyCollection1Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: valid
		catalog: example-catalog
		path: /
	`
	emptyCollection2Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		path: /valid
	`
	emptyCollection3Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: another
		catalog: example-catalog
		path: /
	`
	emptyCollection4Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		path: /another
	`
	emptyCollection5Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: collection
		catalog: example-catalog
		path: /another
	`
	emptyCollection6Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		path: /another/collection
	`
	validParamYaml := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				  path: /valid/path
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5
	`
	validCollectionYaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`

	nonExistentParamYaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: non-existent-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`

	nonExistentDataTypeYaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: non-existent-param-schema
				default: 8
			maxDelay:
				dataType: InvalidType
				default: 1000
	`
	invalidParameterPath := `
	version: v1
	kind: ParameterSchema
	metadata:
		name: integer-param-schema
		catalog: example-catalog
		path: /invalid/path
	spec:
		dataType: Integer
		validation:
		minValue: 1
		maxValue: 10
		default: 5
	`
	invalidCollectionPath := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /invalid/path
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`

	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	replaceTabsWithSpaces(&emptyCollection1Yaml)
	replaceTabsWithSpaces(&emptyCollection2Yaml)
	replaceTabsWithSpaces(&emptyCollection3Yaml)
	replaceTabsWithSpaces(&emptyCollection4Yaml)
	replaceTabsWithSpaces(&emptyCollection5Yaml)
	replaceTabsWithSpaces(&emptyCollection6Yaml)
	replaceTabsWithSpaces(&validParamYaml)
	replaceTabsWithSpaces(&validCollectionYaml)
	replaceTabsWithSpaces(&nonExistentParamYaml)
	replaceTabsWithSpaces(&nonExistentDataTypeYaml)
	replaceTabsWithSpaces(&invalidParameterPath)
	replaceTabsWithSpaces(&invalidCollectionPath)

	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("PABCDE")
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

	varId, err := db.DB(ctx).GetVariantIDFromName(ctx, cat.CatalogID, catcommon.DefaultVariant)
	assert.NoError(t, err)

	// create a workspace
	ws := &models.Workspace{
		Info:        pgtype.JSONB{Status: pgtype.Null},
		BaseVersion: 1,
		VariantID:   varId,
	}
	err = db.DB(ctx).CreateWorkspace(ctx, ws)
	assert.NoError(t, err)

	// create the empty collections
	jsonData, err := yaml.YAMLToJSON([]byte(emptyCollection1Yaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection3Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection4Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection5Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection6Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// Create the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		if assert.NoError(t, err) {
			err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
			if assert.NoError(t, err) {
				// try to save again
				err = SaveSchema(ctx, r, WithErrorIfEqualToExisting(), WithWorkspaceID(ws.WorkspaceID))
				if assert.Error(t, err) {
					assert.ErrorIs(t, err, ErrAlreadyExists)
				}
				// create another object with same spec but at different path. Should not create a duplicate hash
				rNew, err := NewSchema(ctx, jsonData, &schemamanager.SchemaMetadata{
					Name: "example-new",
					Path: "/another/path",
				})
				if assert.NoError(t, err) {
					err = SaveSchema(ctx, rNew, WithWorkspaceID(ws.WorkspaceID))
					if assert.NoError(t, err) {
						assert.Equal(t, r.StorageRepresentation().GetHash(), rNew.StorageRepresentation().GetHash())
					}
				}
				// load the resource from the database
				m := r.Metadata()
				lr, err := GetSchemaByHash(ctx, r.StorageRepresentation().GetHash(), &m)
				if assert.NoError(t, err) { // Check if no error occurred
					assert.NotNil(t, lr)                                                                       // Check if the loaded resource is not nil
					assert.Equal(t, r.Kind(), lr.Kind())                                                       // Check if the kind matches
					assert.Equal(t, r.Version(), lr.Version())                                                 // Check if the version matches
					assert.Equal(t, r.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash()) // Check if the hashes match
				}
				// load object by path
				var tp catcommon.CatalogObjectType
				if r.Kind() == "CollectionSchema" {
					tp = catcommon.CatalogObjectTypeCollectionSchema
				} else if r.Kind() == "ParameterSchema" {
					tp = catcommon.CatalogObjectTypeParameterSchema
				}
				lr, err = GetSchema(ctx, tp, &m, WithWorkspaceID(ws.WorkspaceID))
				if assert.NoError(t, err) {
					assert.NotNil(t, lr)
					assert.Equal(t, r.Kind(), lr.Kind())
					assert.Equal(t, r.Version(), lr.Version())
					assert.Equal(t, r.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
				}
			}
		}
	}
	// Create the collection
	// unmarshal the yaml of the param schema
	param := make(map[string]any)
	yaml.Unmarshal([]byte(validParamYaml), &param)
	collection := make(map[string]any)
	yaml.Unmarshal([]byte(validCollectionYaml), &collection)
	// create the collection schema
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.NoError(t, err) {
			// load the collection schema
			m := collectionSchema.Metadata()
			lr, err := GetSchemaByHash(ctx, collectionSchema.StorageRepresentation().GetHash(), &m)
			if assert.NoError(t, err) {
				assert.NotNil(t, lr)
				assert.Equal(t, collectionSchema.Kind(), lr.Kind())
				assert.Equal(t, collectionSchema.Version(), lr.Version())
				assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
			}
			// load by path
			lr, err = GetSchema(ctx, catcommon.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
			if assert.NoError(t, err) {
				assert.NotNil(t, lr)
				assert.Equal(t, collectionSchema.Kind(), lr.Kind())
				assert.Equal(t, collectionSchema.Version(), lr.Version())
				assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
			}
		}
	}
	// change the base path of the collection schema
	collectionSchema.SetPath("/another/collection/path")
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	if assert.Error(t, err) {
		t.Logf("Error: %v", err)
	}

	// revert the path
	collectionSchema.SetPath("/valid/path")
	// change default value to a string
	collection["spec"].(map[string]any)["parameters"].(map[string]any)["maxRetries"].(map[string]any)["default"] = "five"
	jsonData, err = json.Marshal(collection)
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}

	// create a collection with a non-existent parameter schema
	jsonData, err = yaml.YAMLToJSON([]byte(nonExistentParamYaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}

	// create a collection with a non-existent data type
	jsonData, err = yaml.YAMLToJSON([]byte(nonExistentDataTypeYaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}

	// create a parameter with an invalid path
	jsonData, err = yaml.YAMLToJSON([]byte(invalidParameterPath))
	require.NoError(t, err)
	parameterSchema, err := NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}

	// create a collection with an invalid path
	jsonData, err = yaml.YAMLToJSON([]byte(invalidCollectionPath))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}
}

func TestSaveHierarchicalValue(t *testing.T) {
	if !config.HierarchicalSchemas {
		t.Skip("Hierarchical schemas are not enabled")
	}
	emptyCollection1Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: valid
		catalog: example-catalog
		path: /
	`
	emptyCollection2Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		path: /valid
	`

	validParamYaml := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				  path: /valid/path
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5
	`
	validCollectionYaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	validValueYaml := `
	version: v1
	kind: Value
	metadata:
		catalog: example-catalog
		variant: default
		collection: /valid/path/app-config-collection
	spec:
		maxRetries: 5
		maxDelay: 2000
	`

	invalidDataTypeYaml := `
	version: v1
	kind: Value
	metadata:
		catalog: example-catalog
		variant: default
		collection: /valid/path/app-config-collection
	spec:
		maxRetries: 5
		maxDelay: two_thousand
	`
	invalidParamYaml := `
	version: v1
	kind: Value
	metadata:
		catalog: example-catalog
		variant: default
		collection: /valid/path/app-config-collection
	spec:
		maxRetries: 5000
		maxDelay: 2000
	`
	invalidPathYaml := `
	version: v1
	kind: Value
	metadata:
		catalog: example-catalog
		variant: default
		collection: /invalidpath/app-config-collection
	spec:
		maxRetries: 5
		maxDelay: 1000
	`

	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	replaceTabsWithSpaces(&emptyCollection1Yaml)
	replaceTabsWithSpaces(&emptyCollection2Yaml)
	replaceTabsWithSpaces(&validParamYaml)
	replaceTabsWithSpaces(&validCollectionYaml)
	replaceTabsWithSpaces(&validValueYaml)
	replaceTabsWithSpaces(&invalidDataTypeYaml)
	replaceTabsWithSpaces(&invalidParamYaml)
	replaceTabsWithSpaces(&invalidPathYaml)

	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("PABCDE")
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

	varId, err := db.DB(ctx).GetVariantIDFromName(ctx, cat.CatalogID, catcommon.DefaultVariant)
	assert.NoError(t, err)

	// create a workspace
	ws := &models.Workspace{
		Info:        pgtype.JSONB{Status: pgtype.Null},
		BaseVersion: 1,
		VariantID:   varId,
	}
	err = db.DB(ctx).CreateWorkspace(ctx, ws)
	assert.NoError(t, err)

	// create the empty collections
	jsonData, err := yaml.YAMLToJSON([]byte(emptyCollection1Yaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// Create the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
	}
	// Create the collection
	// unmarshal the yaml of the param schema
	param := make(map[string]any)
	yaml.Unmarshal([]byte(validParamYaml), &param)
	collection := make(map[string]any)
	yaml.Unmarshal([]byte(validCollectionYaml), &collection)
	// create the collection schema
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
	}
	collectionHash := collectionSchema.StorageRepresentation().GetHash()

	// create a value
	jsonData, err = yaml.YAMLToJSON([]byte(validValueYaml))
	require.NoError(t, err)
	err = SaveValue(ctx, jsonData, nil, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// get the value
	dir, err := getWorkspaceDirs(ctx, ws.WorkspaceID)
	require.NoError(t, err)

	_, err = GetValue(ctx, &ValueMetadata{
		Catalog:    collectionSchema.Catalog(),
		Variant:    collectionSchema.Metadata().Variant,
		Collection: collectionSchema.FullyQualifiedName(),
	},
		dir)
	require.NoError(t, err)

	// load collection by path
	m := collectionSchema.Metadata()
	lr, err := GetSchema(ctx, catcommon.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	require.NotNil(t, lr)
	assert.NotEqual(t, collectionHash, lr.StorageRepresentation().GetHash())

	// create a value with invalid data type
	jsonData, err = yaml.YAMLToJSON([]byte(invalidDataTypeYaml))
	require.NoError(t, err)
	err = SaveValue(ctx, jsonData, nil, WithWorkspaceID(ws.WorkspaceID))
	if assert.Error(t, err) {
		t.Logf("Error: %v", err)
	}

	// create a value with invalid parameter
	jsonData, err = yaml.YAMLToJSON([]byte(invalidParamYaml))
	require.NoError(t, err)
	err = SaveValue(ctx, jsonData, nil, WithWorkspaceID(ws.WorkspaceID))
	if assert.Error(t, err) {
		t.Logf("Error: %v", err)
	}

	// create a value with invalid path
	jsonData, err = yaml.YAMLToJSON([]byte(invalidPathYaml))
	require.NoError(t, err)
	err = SaveValue(ctx, jsonData, nil, WithWorkspaceID(ws.WorkspaceID))
	if assert.Error(t, err) {
		t.Logf("Error: %v", err)
	}
}

func TestReferences(t *testing.T) {
	if !config.HierarchicalSchemas {
		t.Skip("Hierarchical schemas are not enabled")
	}
	emptyCollection1Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: valid
		catalog: example-catalog
		path: /
	`
	emptyCollection2Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		path: /valid
	`
	emptyCollection3Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: anotherpath
		catalog: example-catalog
		path: /valid
	`
	validParamYaml := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				  path: /valid
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5
	`
	updatedParamYaml := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				  path: /valid
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 20
				  default: 2
	`
	validParamYaml2 := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema2
				  catalog: example-catalog
				  path: /valid
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5
	`
	updatedParamAtNewPathYaml := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				  path: /valid/path
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 3
				  default: 2
	`
	updatedParamAtNewPathYaml2 := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				  path: /valid/path
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 20
				  default: 2
	`
	updatedParamYamlAtGrandparent := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				  path: /
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 20
				  default: 2
	`
	validCollectionYaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	validCollectionYaml2 := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			connectionAttempts:
				schema: integer-param-schema2
				default: 3
			connectionDelay:
				schema: integer-param-schema
				default: 7	
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	validCollectionYamlAtNewPath := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/anotherpath
	spec:
		parameters:
			connectionAttempts:
				schema: integer-param-schema2
				default: 3
			connectionDelay:
				schema: integer-param-schema
				default: 7	
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	replaceTabsWithSpaces(&emptyCollection1Yaml)
	replaceTabsWithSpaces(&emptyCollection2Yaml)
	replaceTabsWithSpaces(&emptyCollection3Yaml)
	replaceTabsWithSpaces(&validParamYaml)
	replaceTabsWithSpaces(&updatedParamYaml)
	replaceTabsWithSpaces(&validParamYaml2)
	replaceTabsWithSpaces(&updatedParamAtNewPathYaml)
	replaceTabsWithSpaces(&updatedParamAtNewPathYaml2)
	replaceTabsWithSpaces(&updatedParamYamlAtGrandparent)
	replaceTabsWithSpaces(&validCollectionYaml)
	replaceTabsWithSpaces(&validCollectionYaml2)
	replaceTabsWithSpaces(&validCollectionYamlAtNewPath)

	tenantID := catcommon.TenantId("TABCDE")
	projectID := catcommon.ProjectId("PABCDE")
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

	varId, err := db.DB(ctx).GetVariantIDFromName(ctx, cat.CatalogID, catcommon.DefaultVariant)
	assert.NoError(t, err)

	// create a workspace
	ws := &models.Workspace{
		Info:        pgtype.JSONB{Status: pgtype.Null},
		BaseVersion: 1,
		VariantID:   varId,
	}
	err = db.DB(ctx).CreateWorkspace(ctx, ws)
	assert.NoError(t, err)

	// get the directories for the workspace
	dir, err := getWorkspaceDirs(ctx, ws.WorkspaceID)
	require.NoError(t, err)

	// create the empty collections
	jsonData, err := yaml.YAMLToJSON([]byte(emptyCollection1Yaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection3Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// Create the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	var paramFqn string
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		paramFqn = r.FullyQualifiedName()
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
	}
	// Create the collection
	// unmarshal the yaml of the param schema
	param := make(map[string]any)
	yaml.Unmarshal([]byte(validParamYaml), &param)
	collection := make(map[string]any)
	yaml.Unmarshal([]byte(validCollectionYaml), &collection)
	// create the collection schema
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)

	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getSchemaRefs(ctx, catcommon.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
	}

	// Create the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml2))
	var paramFqn2 string
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		paramFqn2 = r.FullyQualifiedName()
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
	}
	// update the collection schema to include another parameter
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYaml2))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getSchemaRefs(ctx, catcommon.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.Len(t, refs, 2)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn2}, {Name: paramFqn}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn2)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
	}
	// update the collection back
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getSchemaRefs(ctx, catcommon.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn2)
		require.NoError(t, err)
		assert.Len(t, refs, 0)
	}
	// update the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(updatedParamYaml))
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		paramFqn = r.FullyQualifiedName()
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn}})
	}
	// create a collection schema at new path
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYamlAtNewPath))
	require.NoError(t, err)
	collectionSchema2, err := NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema2, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getSchemaRefs(ctx, catcommon.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema2.FullyQualifiedName())
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn2}, {Name: paramFqn}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}, {Name: collectionSchema2.FullyQualifiedName()}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn2)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema2.FullyQualifiedName()}})
	}

	// update the parameter at a new path with lower max value
	jsonData, err = yaml.YAMLToJSON([]byte(updatedParamAtNewPathYaml))
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.Error(t, err)
		t.Logf("Error: %v", err)
	}

	// update the parameter at a new path with higher max value
	jsonData, err = yaml.YAMLToJSON([]byte(updatedParamAtNewPathYaml2))
	var paramFqn3 string
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		paramFqn3 = r.FullyQualifiedName()
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn3)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema2.FullyQualifiedName()}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn2)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema2.FullyQualifiedName()}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema2.FullyQualifiedName())
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn}, {Name: paramFqn2}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn3}})
	}

	// update the parameter at the grandparent path
	jsonData, err = yaml.YAMLToJSON([]byte(updatedParamYamlAtGrandparent))
	var paramFqn4 string
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		paramFqn4 = r.FullyQualifiedName()
		// get all references
		refs, err := getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn4)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn3)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema2.FullyQualifiedName()}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn2)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema2.FullyQualifiedName()}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema2.FullyQualifiedName())
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn}, {Name: paramFqn2}})
		refs, err = getSchemaRefs(ctx, catcommon.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn3}})
	}

}
