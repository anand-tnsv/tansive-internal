package catalogmanager

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/tidwall/sjson"
	"sigs.k8s.io/yaml"
)

func TestCollection(t *testing.T) {

	parameterYaml := `
		version: v1
		kind: ParameterSchema
		metadata:
			name: integer-param-schema
			catalog: example-catalog
		spec:
			dataType: Integer
			validation:
			minValue: 1
			maxValue: 10
			default: 5		
	`
	collectionYaml := `
		version: v1
		kind: CollectionSchema
		metadata:
			name: example-collection-schema
			catalog: example-catalog
			description: An example collection schema
		spec:
			parameters:
				maxAttempts:
					schema: integer-param-schema
				maxRetries:
					schema: integer-param-schema
					default: 5
				maxDelay:
					dataType: Integer
					default: 1000
	`

	anotherCollectionYaml := `
		version: v1
		kind: CollectionSchema
		metadata:
			name: another-collection-schema
			catalog: example-catalog
			description: An example collection schema
		spec:
			parameters:
				maxRetries:
					schema: integer-param-schema
					default: 8
				maxDelay:
					dataType: Integer
					default: 1000
	`
	invalidCollectionvalueYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			invalid: invalid
	`
	validCollectionValueYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: example-collection-schema
	`
	validCollectionValueWithValueYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: example-collection-schema
			values:
				maxRetries: 5
				maxDelay: 1000
	`
	validCollectionWithChangedValueYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: example-collection-schema
			values:
				maxRetries: 3
				maxAttempts: 10
	`
	validCollectionValueWithInvalidValueYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: example-collection-schema
			values:
				maxRetries: 'hello'
				maxDelay: 1000
	`
	validCollectionValueWithInvalidValueYaml2 := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: example-collection-schema
			values:
				maxRetries: 3
				maxDelay: 'hello'
	`
	validCollectionValueWithPartialValueYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: example-collection-schema
			values:
				maxRetries: 3
	`
	validCollectionValueWithPartialValueYaml2 := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: example-collection-schema
			values:
				maxDelay: 500
				nonExistingParameter: 1000
	`

	invalidCollectionValueYaml2 := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: invalid-schema
	`
	collectionWithChangedSchemaYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: another-collection-schema
	`
	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})
	replaceTabsWithSpaces(&parameterYaml)
	replaceTabsWithSpaces(&collectionYaml)
	replaceTabsWithSpaces(&invalidCollectionvalueYaml)
	replaceTabsWithSpaces(&validCollectionValueYaml)
	replaceTabsWithSpaces(&validCollectionValueWithValueYaml)
	replaceTabsWithSpaces(&invalidCollectionValueYaml2)
	replaceTabsWithSpaces(&collectionWithChangedSchemaYaml)
	replaceTabsWithSpaces(&anotherCollectionYaml)
	replaceTabsWithSpaces(&validCollectionValueWithPartialValueYaml)
	replaceTabsWithSpaces(&validCollectionValueWithPartialValueYaml2)
	replaceTabsWithSpaces(&validCollectionValueWithInvalidValueYaml)
	replaceTabsWithSpaces(&validCollectionValueWithInvalidValueYaml2)
	replaceTabsWithSpaces(&validCollectionWithChangedValueYaml)

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

	varId, err := db.DB(ctx).GetVariantIDFromName(ctx, cat.CatalogID, types.DefaultVariant)
	assert.NoError(t, err)

	// create a workspace
	ws := &models.Workspace{
		Label:       "some-label",
		Info:        pgtype.JSONB{Status: pgtype.Null},
		BaseVersion: 1,
		VariantID:   varId,
	}
	err = db.DB(ctx).CreateWorkspace(ctx, ws)
	assert.NoError(t, err)

	// create the parameter schema
	jsonData, err := yaml.YAMLToJSON([]byte(parameterYaml))
	require.NoError(t, err)
	parameterSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection schema
	jsonData, err = yaml.YAMLToJSON([]byte(collectionYaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection
	jsonData, err = yaml.YAMLToJSON([]byte(invalidCollectionvalueYaml))
	require.NoError(t, err)
	collection, err := NewCollectionManager(ctx, jsonData, nil)
	require.Error(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the collection
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionValueYaml))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection with invalid schema
	jsonData, err = yaml.YAMLToJSON([]byte(invalidCollectionValueYaml2))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the valid collection again
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionValueYaml))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID), WithErrorIfEqualToExisting())
	require.ErrorIs(t, err, ErrEqualToExistingObject)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID), WithErrorIfExists())
	require.ErrorIs(t, err, ErrAlreadyExists)

	// check the collection
	m := collection.Metadata()
	validateMetadata(ctx, &m)
	collection, err = LoadCollectionByPath(ctx, &m, WithWorkspaceID(ws.WorkspaceID), SkipCanonicalizePaths())
	require.NoError(t, err)
	assert.NotNil(t, collection)
	assert.Equal(t, "my-collection", collection.Metadata().Name)
	assert.Equal(t, "example-collection-schema", collection.Schema())
	assert.Equal(t, "An example collection", collection.Metadata().Description)
	assert.Equal(t, "/some/random/path", collection.Metadata().Path)
	values := collection.Values()
	assert.NotNil(t, values)
	assert.Equal(t, 3, len(values)) // maxRetries and maxDelay should be set
	var val int
	assert.NoError(t, values["maxRetries"].Value.GetAs(&val))
	assert.Equal(t, 5, val) // default value from schema
	assert.NoError(t, values["maxDelay"].Value.GetAs(&val))
	assert.Equal(t, 1000, val)
	assert.NoError(t, values["maxAttempts"].Value.GetAs(&val))
	assert.Equal(t, 5, val) // default value from parameter schema

	// create the collection with changed values
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionWithChangedValueYaml))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	collection, err = LoadCollectionByPath(ctx, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.NotNil(t, collection)
	assert.Equal(t, "my-collection", collection.Metadata().Name)
	assert.Equal(t, "example-collection-schema", collection.Schema())
	assert.Equal(t, "An example collection", collection.Metadata().Description)
	assert.Equal(t, "/some/random/path", collection.Metadata().Path)
	values = collection.Values()
	assert.NotNil(t, values)
	assert.Equal(t, 3, len(values)) // maxRetries and maxDelay should be set
	assert.NoError(t, values["maxRetries"].Value.GetAs(&val))
	assert.Equal(t, 3, val) // default value from schema
	assert.NoError(t, values["maxDelay"].Value.GetAs(&val))
	assert.Equal(t, 1000, val) // default value from schema
	assert.False(t, values["maxAttempts"].Value.IsNil())
	assert.NoError(t, values["maxAttempts"].Value.GetAs(&val))
	assert.Equal(t, 10, val) // changed value from the new collection

	// create another collection schema
	jsonData, err = yaml.YAMLToJSON([]byte(anotherCollectionYaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection with valid values
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionValueWithValueYaml))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection with partial values
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionValueWithPartialValueYaml))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	collection, err = LoadCollectionByPath(ctx, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.NotNil(t, collection)
	values = collection.Values()
	assert.NotNil(t, values)
	assert.Equal(t, 3, len(values)) // maxRetries and maxDelay should be set
	assert.NoError(t, values["maxRetries"].Value.GetAs(&val))
	assert.Equal(t, 3, val) // default value from schema
	assert.NoError(t, values["maxDelay"].Value.GetAs(&val))
	assert.Equal(t, 1000, val) // default value from schema
	assert.False(t, values["maxAttempts"].Value.IsNil())
	assert.NoError(t, values["maxAttempts"].Value.GetAs(&val))
	assert.Equal(t, 10, val) // changed value from the new collection

	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionValueWithPartialValueYaml2))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	collection, err = LoadCollectionByPath(ctx, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.NotNil(t, collection)
	values = collection.Values()
	assert.NotNil(t, values)
	assert.Equal(t, 3, len(values)) // maxRetries and maxDelay should be set
	assert.NoError(t, values["maxRetries"].Value.GetAs(&val))
	assert.Equal(t, 3, val) // default value from schema
	assert.NoError(t, values["maxDelay"].Value.GetAs(&val))
	assert.Equal(t, 500, val) // default value from schema
	assert.False(t, values["maxAttempts"].Value.IsNil())
	assert.NoError(t, values["maxAttempts"].Value.GetAs(&val))
	assert.Equal(t, 10, val) // changed value from the new collection

	// create the collection with partial values but invalid value
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionValueWithInvalidValueYaml))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the collection with partial values but invalid value 2
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionValueWithInvalidValueYaml2))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the collection with changed schema
	jsonData, err = yaml.YAMLToJSON([]byte(collectionWithChangedSchemaYaml))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.ErrorIs(t, err, ErrSchemaOfCollectionNotMutable)

	// change single collection value
	valAny, _ := types.NullableAnyFrom(7) // new value for maxRetries
	err = UpdateAttributes(ctx, &m, map[string]types.NullableAny{"maxRetries": valAny}, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	collection, err = LoadCollectionByPath(ctx, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.NotNil(t, collection)
	values = collection.Values()
	assert.NotNil(t, values)
	assert.Equal(t, 3, len(values)) // maxRetries and maxDelay should be set
	assert.NoError(t, values["maxRetries"].Value.GetAs(&val))
	assert.Equal(t, 7, val) // updated value from the new collection
	assert.NoError(t, values["maxDelay"].Value.GetAs(&val))
	assert.Equal(t, 500, val)
	assert.False(t, values["maxAttempts"].Value.IsNil())
	assert.NoError(t, values["maxAttempts"].Value.GetAs(&val))
	assert.Equal(t, 10, val) // unchanged value from the new collection

	// Test invalid parameter name
	err = UpdateAttributes(ctx, &m, map[string]types.NullableAny{"invalidParam": valAny}, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// Test invalid value type
	valAny, _ = types.NullableAnyFrom("not-an-integer") // invalid value for maxRetries
	err = UpdateAttributes(ctx, &m, map[string]types.NullableAny{"maxRetries": valAny}, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// set the same value
	valAny, _ = types.NullableAnyFrom(7) // same value for maxRetries
	err = UpdateAttributes(ctx, &m, map[string]types.NullableAny{"maxRetries": valAny}, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
}

func TestCollectionWithNamespaces(t *testing.T) {

	parameterYaml := `
		version: v1
		kind: ParameterSchema
		metadata:
			name: integer-param-schema
			catalog: example-catalog
		spec:
			dataType: Integer
			validation:
			minValue: 1
			maxValue: 10
			default: 5		
	`
	collectionYaml := `
		version: v1
		kind: CollectionSchema
		metadata:
			name: example-collection-schema
			catalog: example-catalog
			Namespace: my-namespace
			description: An example collection schema
		spec:
			parameters:
				maxAttempts:
					schema: integer-param-schema
				maxRetries:
					schema: integer-param-schema
					default: 5
				maxDelay:
					dataType: Integer
					default: 1000
	`

	invalidCollectionvalueYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			invalid: invalid
	`
	validCollectionValueYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: example-collection-schema
	`

	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})
	replaceTabsWithSpaces(&parameterYaml)
	replaceTabsWithSpaces(&collectionYaml)
	replaceTabsWithSpaces(&invalidCollectionvalueYaml)
	replaceTabsWithSpaces(&validCollectionValueYaml)

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

	varId, err := db.DB(ctx).GetVariantIDFromName(ctx, cat.CatalogID, types.DefaultVariant)
	assert.NoError(t, err)

	// create a namespace
	namespace := &models.Namespace{
		Name:        "my-namespace",
		VariantID:   varId,
		Description: "An example namespace for testing",
		Info:        nil,
	}
	err = db.DB(ctx).CreateNamespace(ctx, namespace)
	assert.NoError(t, err)

	// create a workspace
	ws := &models.Workspace{
		Info:        pgtype.JSONB{Status: pgtype.Null},
		BaseVersion: 1,
		VariantID:   varId,
	}
	err = db.DB(ctx).CreateWorkspace(ctx, ws)
	require.NoError(t, err)

	// create the parameter schema
	jsonData, err := yaml.YAMLToJSON([]byte(parameterYaml))
	require.NoError(t, err)
	parameterSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection schema
	jsonData, err = yaml.YAMLToJSON([]byte(collectionYaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection
	jsonData, err = yaml.YAMLToJSON([]byte(invalidCollectionvalueYaml))
	require.NoError(t, err)
	collection, err := NewCollectionManager(ctx, jsonData, nil)
	require.Error(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	//create valid collection
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionValueYaml))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)
	// create the valid collection in valid namespace
	b, err := sjson.Set(string(jsonData), "metadata.namespace", "my-namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	validCollection, err := NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, validCollection, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	// load the collection
	m := validCollection.Metadata()
	collection, err = LoadCollectionByPath(ctx, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.NotNil(t, collection)
	assert.Equal(t, "my-collection", collection.Metadata().Name)
	assert.Equal(t, "example-collection-schema", collection.Schema())
	assert.Equal(t, "An example collection", collection.Metadata().Description)
	assert.Equal(t, "/some/random/path", collection.Metadata().Path)
	values := collection.Values()
	assert.NotNil(t, values)
	assert.Equal(t, 3, len(values)) // maxRetries and maxDelay should be set

	// create valid collection in invalid namespace
	b, err = sjson.Set(string(jsonData), "metadata.namespace", "invalid-namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	_, err = NewCollectionManager(ctx, jsonData, nil)
	require.Error(t, err)
	// create the same parameter schema in the same namespace
	jsonData, err = yaml.YAMLToJSON([]byte(parameterYaml))
	require.NoError(t, err)
	b, err = sjson.Set(string(jsonData), "metadata.namespace", "my-namespace")
	jsonData = []byte(b)
	require.NoError(t, err)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)
	// delete the parameter schema
	dir, err := getWorkspaceDirs(ctx, ws.WorkspaceID)
	require.NoError(t, err)
	m = parameterSchema.Metadata()
	m.Namespace = types.NullString()
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &m, dir)
	require.Error(t, err)
	// delete the collection schema
	m = collectionSchema.Metadata()
	err = DeleteSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m, dir)
	require.Error(t, err)
	// delete the collection
	m = validCollection.Metadata()
	err = DeleteCollection(ctx, &m, WithDirectories(dir))
	require.NoError(t, err)
	// load the collection
	_, err = LoadCollectionByPath(ctx, &m, WithDirectories(dir))
	require.Error(t, err)
	// delete the collection schema
	m = collectionSchema.Metadata()
	err = DeleteSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m, dir)
	require.NoError(t, err)
}
