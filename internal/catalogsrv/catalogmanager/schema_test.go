package catalogmanager

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/schemavalidator"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/tidwall/sjson"
	"sigs.k8s.io/yaml"
)

func TestSaveSchema(t *testing.T) {
	emptyCollection1Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: valid
		catalog: example-catalog
		description: An example collection
	`
	emptyCollection2Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	emptyCollection3Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
			maxDelay:
				dataType: Integer
				default: 2000
	`
	validParamYaml := `
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
	validParamYamlModifiedValidation := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 5
				  default: 5
	`
	invalidDataTypeYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: InvalidInteger
				default: 1000
	`
	invalidDefaultYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 'hello'
			maxDelay:
				dataType: Integer
				default: 1000
	`
	invalidDefaultDataTypeYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 5
			maxDelay:
				dataType: Integer
				default: 'hello'
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
	replaceTabsWithSpaces(&invalidDataTypeYamlCollection)
	replaceTabsWithSpaces(&validParamYamlModifiedValidation)
	replaceTabsWithSpaces(&invalidDefaultYamlCollection)
	replaceTabsWithSpaces(&invalidDefaultDataTypeYamlCollection)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")
	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

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

	// create the same collection again
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithErrorIfExists(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the same collection again with error if equal
	err = SaveSchema(ctx, collectionSchema, WithErrorIfEqualToExisting(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create a collection with no existing parameter schemas
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the parameter schema
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	require.NoError(t, err)
	parameterSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the same schema again
	err = SaveSchema(ctx, parameterSchema, WithErrorIfEqualToExisting(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)
	err = SaveSchema(ctx, parameterSchema, WithErrorIfExists(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the collection with the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the same collection again
	err = SaveSchema(ctx, collectionSchema, WithErrorIfEqualToExisting(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)
	err = SaveSchema(ctx, collectionSchema, WithErrorIfExists(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the same collection again with a different parameter
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection3Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// Load the collection schema
	m := collectionSchema.Metadata()
	lr, err := GetSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, lr.Kind(), collectionSchema.Kind())
	assert.Equal(t, collectionSchema.Version(), lr.Version())
	assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())

	// Load the parameter schema
	m = parameterSchema.Metadata()
	lr, err = GetSchema(ctx, types.CatalogObjectTypeParameterSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, lr.Kind(), parameterSchema.Kind())
	assert.Equal(t, parameterSchema.Version(), lr.Version())
	assert.Equal(t, parameterSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())

	// create a collection with invalid data type
	jsonData, err = yaml.YAMLToJSON([]byte(invalidDataTypeYamlCollection))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create a collection with invalid default value
	jsonData, err = yaml.YAMLToJSON([]byte(invalidDefaultYamlCollection))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create a collection with invalid default value data type
	jsonData, err = yaml.YAMLToJSON([]byte(invalidDefaultDataTypeYamlCollection))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// modify the parameter schema
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYamlModifiedValidation))
	require.NoError(t, err)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// try to delete the parameter schema
	dir, err := getWorkspaceDirs(ctx, ws.WorkspaceID)
	require.NoError(t, err)
	md := parameterSchema.Metadata()
	md.Name = "integer-param-schema" // set name to load the deleted collection schema
	md.Namespace = types.NullString()
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.ErrorIs(t, err, ErrUnableToDeleteParameterWithReferences)

	// delete the collection
	md.Name = "path"
	err = DeleteSchema(ctx, types.CatalogObjectTypeCollectionSchema, &md, dir)
	require.NoError(t, err)
	// delete the parameter schema
	md.Name = "integer-param-schema" // set name to load the deleted collection schema
	md.Namespace = types.NullString()
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.NoError(t, err)
}

func TestSchemaWithNamespaces(t *testing.T) {
	emptyCollection1Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: valid
		catalog: example-catalog
		namespace: my-namespace
		description: An example collection
	`
	emptyCollection2Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		namespace: my-namespace
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	validParamYaml := `
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
	validParamYamlModifiedValidation := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 5
				  default: 5
	`
	invalidDataTypeYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: InvalidInteger
				default: 1000
	`
	invalidDefaultYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 'hello'
			maxDelay:
				dataType: Integer
				default: 1000
	`
	invalidDefaultDataTypeYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 5
			maxDelay:
				dataType: Integer
				default: 'hello'
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
	replaceTabsWithSpaces(&invalidDataTypeYamlCollection)
	replaceTabsWithSpaces(&validParamYamlModifiedValidation)
	replaceTabsWithSpaces(&invalidDefaultYamlCollection)
	replaceTabsWithSpaces(&invalidDefaultDataTypeYamlCollection)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")
	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

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
	assert.NoError(t, err)

	// create the empty collections
	jsonData, err := yaml.YAMLToJSON([]byte(emptyCollection1Yaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// retrieve the collection
	m := collectionSchema.Metadata()
	m.Path = ""
	lr, err := GetSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, lr.Kind(), collectionSchema.Kind())
	assert.Equal(t, collectionSchema.Version(), lr.Version())
	assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
	// Verify the namespace is set correctly
	assert.Equal(t, lr.Metadata().Namespace.String(), "my-namespace", "Expected namespace to be 'my-namespace'")

	// create the same collection again
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithErrorIfExists(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create same collection in the root namespace
	b, err := sjson.Delete(string(jsonData), "metadata.namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithErrorIfExists(), WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	// Verify the root collection is saved correctly
	m = collectionSchema.Metadata()
	m.Path = "" // clear path to load root collection
	lr, err = GetSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())

	// create new namespace called default
	namespace.Name = "default"
	err = db.DB(ctx).CreateNamespace(ctx, namespace)
	assert.NoError(t, err)

	// create the same collection in a different namespace
	b, err = sjson.Set(string(jsonData), "metadata.namespace", "default")
	require.NoError(t, err)
	jsonData = []byte(b)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	// Verify the new namespace collection is saved correctly
	m = collectionSchema.Metadata()
	m.Path = "" // clear path to load the new namespace collection
	lr, err = GetSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
	assert.Equal(t, lr.Metadata().Namespace.String(), "default", "Expected namespace to be 'another'")

	// create a collection with parameters now
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the parameter schema in root namespace
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	require.NoError(t, err)
	parameterSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	// Verify the parameter schema is saved correctly in the root namespace
	m = parameterSchema.Metadata()
	m.Path = "" // clear path to load root parameter schema
	lr, err = GetSchema(ctx, types.CatalogObjectTypeParameterSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, lr.Kind(), parameterSchema.Kind())
	assert.Equal(t, parameterSchema.Version(), lr.Version())
	assert.Equal(t, parameterSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
	// Verify the namespace is not set for root parameter schema
	assert.Equal(t, lr.Metadata().Namespace.String(), "", "Expected root parameter schema to have no namespace")

	// create the collection with the parameter in the namespace
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection in the root namespace
	b, err = sjson.Delete(string(jsonData), "metadata.namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the parameter schema in the namespace with modified max value
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYamlModifiedValidation))
	require.NoError(t, err)
	b, err = sjson.Set(string(jsonData), "metadata.namespace", "my-namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create it with a different name
	b, err = sjson.Set(string(jsonData), "metadata.name", "integer-param-schema-modified")
	require.NoError(t, err)
	jsonData = []byte(b)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	// Verify the parameter schema is saved correctly in the namespace
	m = parameterSchema.Metadata()
	m.Path = "" // clear path to load the namespace parameter schema
	lr, err = GetSchema(ctx, types.CatalogObjectTypeParameterSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, parameterSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
	assert.Equal(t, lr.Metadata().Namespace.String(), "my-namespace", "Expected namespace to be 'my-namespace'")

	// create the collections schema again
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// modify the parameter schema in the root namespace with modified max value when a collection is referring to it
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYamlModifiedValidation))
	require.NoError(t, err)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the same but ignore the conflict.  This will cause revalidation error
	err = SaveSchema(ctx, parameterSchema, IgnoreSchemaSpecChange(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)
	// skip revalidation
	err = SaveSchema(ctx, parameterSchema, IgnoreSchemaSpecChange(), SkipRevalidationOnSchemaChange(), WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collections schema again.  Should error out since the max value in the parameter schema has changed
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// modify the collection schema to reduce the maxRetry value and try again
	b, err = sjson.Set(string(jsonData), "spec.parameters.maxRetries.default", 5)
	require.NoError(t, err)
	jsonData = []byte(b)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// delete the parameter schema
	dir, err := getWorkspaceDirs(ctx, ws.WorkspaceID)
	md := parameterSchema.Metadata()
	md.Name = "integer-param-schema-modified" // set name to load the deleted collection schema
	md.Namespace = types.NullableStringFrom("my-namespace")
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.NoError(t, err)
	// Verify the parameter schema is deleted
	m = parameterSchema.Metadata()
	m.Name = "integer-param-schema-modified" // set name to load the deleted collection schema
	m.Namespace = types.NullableStringFrom("my-namespace")
	m.Path = "" // clear path to load the deleted collection schema
	_, err = GetSchema(ctx, types.CatalogObjectTypeParameterSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// delete the parameter schema that has references
	md.Name = "integer-param-schema" // set name to load the deleted collection schema
	md.Namespace = types.NullString()
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.ErrorIs(t, err, ErrUnableToDeleteParameterWithReferences)

	// delete the collection schema
	md.Name = "valid"
	err = DeleteSchema(ctx, types.CatalogObjectTypeCollectionSchema, &md, dir)
	require.NoError(t, err)
	// Verify the collection schema is deleted
	m = collectionSchema.Metadata()
	m.Name = "valid" // set name to load the deleted collectio n schema
	m.Namespace = types.NullString()
	m.Path = "/" // clear path to load the deleted collection schema
	_, err = GetSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err, "Expected error when loading deleted collection schema")
	md.Name = "path"
	md.Namespace = types.NullableStringFrom("my-namespace")
	err = DeleteSchema(ctx, types.CatalogObjectTypeCollectionSchema, &md, dir)
	require.NoError(t, err)
	// Verify the root collection schema is deleted
	m = collectionSchema.Metadata()
	m.Name = "path"                                        // set name to load the deleted collection schema
	m.Namespace = types.NullableStringFrom("my-namespace") // set namespace to load the deleted collection schema
	m.Path = "/"
	_, err = GetSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err, "Expected error when loading deleted root collection schema")

	// delete the parameter schema that has references
	md.Name = "integer-param-schema" // set name to load the deleted collection schema
	md.Namespace = types.NullString()
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.ErrorIs(t, err, ErrUnableToDeleteParameterWithReferences)

	md.Name = "path"
	md.Namespace = types.NullString()
	err = DeleteSchema(ctx, types.CatalogObjectTypeCollectionSchema, &md, dir)
	require.NoError(t, err)
	md.Name = "integer-param-schema" // set name to load the deleted collection schema
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.NoError(t, err)

	// create two identical parameter schemas in differentnamespaces
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	require.NoError(t, err)
	b, err = sjson.Set(string(jsonData), "metadata.namespace", "my-namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create another identical parameter schema in a different namespace
	b, err = sjson.Delete(string(jsonData), "metadata.namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// delete one
	md.Name = "integer-param-schema" // set name to load the deleted collection schema
	md.Namespace = types.NullableStringFrom("my-namespace")
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.NoError(t, err)
	// Verify the parameter schema in the other namespace is still available
	m = parameterSchema.Metadata()
	m.Name = "integer-param-schema"  // set name to load the deleted collection schema
	m.Namespace = types.NullString() // clear namespace to load the other parameter schema
	m.Path = ""                      // clear path to load the other parameter schema
	_, err = GetSchema(ctx, types.CatalogObjectTypeParameterSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
}

func TestSaveSchemaInVariant(t *testing.T) {
	emptyCollection1Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: valid
		catalog: example-catalog
		description: An example collection
	`
	emptyCollection2Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	validParamYaml := `
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
	validParamYamlModifiedValidation := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 5
				  default: 5
	`
	invalidDataTypeYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: InvalidInteger
				default: 1000
	`
	invalidDefaultYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 'hello'
			maxDelay:
				dataType: Integer
				default: 1000
	`
	invalidDefaultDataTypeYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 5
			maxDelay:
				dataType: Integer
				default: 'hello'
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
	replaceTabsWithSpaces(&invalidDataTypeYamlCollection)
	replaceTabsWithSpaces(&validParamYamlModifiedValidation)
	replaceTabsWithSpaces(&invalidDefaultYamlCollection)
	replaceTabsWithSpaces(&invalidDefaultDataTypeYamlCollection)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")
	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

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
	var _ = varId

	// create the empty collections
	jsonData, err := yaml.YAMLToJSON([]byte(emptyCollection1Yaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.NoError(t, err)

	// create the same collection again
	err = SaveSchema(ctx, collectionSchema)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithErrorIfExists())
	require.Error(t, err)

	// create the same collection again with error if equal
	err = SaveSchema(ctx, collectionSchema, WithErrorIfEqualToExisting())
	require.Error(t, err)

	// create a collection with no existing parameter schemas
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.Error(t, err)

	// create the parameter schema
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	require.NoError(t, err)
	parameterSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema)
	require.NoError(t, err)

	// create the same schema again
	err = SaveSchema(ctx, parameterSchema, WithErrorIfEqualToExisting())
	require.Error(t, err)
	err = SaveSchema(ctx, parameterSchema, WithErrorIfExists())
	require.Error(t, err)

	// create the collection with the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.NoError(t, err)

	// create the same collection again
	err = SaveSchema(ctx, collectionSchema, WithErrorIfEqualToExisting())
	require.Error(t, err)
	err = SaveSchema(ctx, collectionSchema, WithErrorIfExists())
	require.Error(t, err)

	// Load the collection schema
	m := collectionSchema.Metadata()
	lr, err := GetSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m)
	require.NoError(t, err)
	assert.Equal(t, lr.Kind(), collectionSchema.Kind())
	assert.Equal(t, collectionSchema.Version(), lr.Version())
	assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())

	// Load the parameter schema
	m = parameterSchema.Metadata()
	lr, err = GetSchema(ctx, types.CatalogObjectTypeParameterSchema, &m)
	require.NoError(t, err)
	assert.Equal(t, lr.Kind(), parameterSchema.Kind())
	assert.Equal(t, parameterSchema.Version(), lr.Version())
	assert.Equal(t, parameterSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())

	// create a collection with invalid data type
	jsonData, err = yaml.YAMLToJSON([]byte(invalidDataTypeYamlCollection))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.Error(t, err)

	// create a collection with invalid default value
	jsonData, err = yaml.YAMLToJSON([]byte(invalidDefaultYamlCollection))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.Error(t, err)

	// create a collection with invalid default value data type
	jsonData, err = yaml.YAMLToJSON([]byte(invalidDefaultDataTypeYamlCollection))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.Error(t, err)

	// modify the parameter schema
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYamlModifiedValidation))
	require.NoError(t, err)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema)
	require.Error(t, err)

	// try to delete the parameter schema
	dir, err := getVariantDirs(ctx, varId)
	require.NoError(t, err)
	md := parameterSchema.Metadata()
	md.Name = "integer-param-schema" // set name to load the deleted collection schema
	md.Namespace = types.NullString()
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.ErrorIs(t, err, ErrUnableToDeleteParameterWithReferences)

	// delete the collection
	md.Name = "path"
	err = DeleteSchema(ctx, types.CatalogObjectTypeCollectionSchema, &md, dir)
	require.NoError(t, err)
	// delete the parameter schema
	md.Name = "integer-param-schema" // set name to load the deleted collection schema
	md.Namespace = types.NullString()
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.NoError(t, err)
}

func TestSchemaWithNamespacesInVariant(t *testing.T) {
	emptyCollection1Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: valid
		catalog: example-catalog
		namespace: my-namespace
		description: An example collection
	`
	emptyCollection2Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		namespace: my-namespace
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	validParamYaml := `
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
	validParamYamlModifiedValidation := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 5
				  default: 5
	`
	invalidDataTypeYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: InvalidInteger
				default: 1000
	`
	invalidDefaultYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 'hello'
			maxDelay:
				dataType: Integer
				default: 1000
	`
	invalidDefaultDataTypeYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 5
			maxDelay:
				dataType: Integer
				default: 'hello'
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
	replaceTabsWithSpaces(&invalidDataTypeYamlCollection)
	replaceTabsWithSpaces(&validParamYamlModifiedValidation)
	replaceTabsWithSpaces(&invalidDefaultYamlCollection)
	replaceTabsWithSpaces(&invalidDefaultDataTypeYamlCollection)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")
	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

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

	// create the empty collections
	jsonData, err := yaml.YAMLToJSON([]byte(emptyCollection1Yaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.NoError(t, err)

	// retrieve the collection
	m := collectionSchema.Metadata()
	m.Path = ""
	lr, err := GetSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m)
	require.NoError(t, err)
	assert.Equal(t, lr.Kind(), collectionSchema.Kind())
	assert.Equal(t, collectionSchema.Version(), lr.Version())
	assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
	// Verify the namespace is set correctly
	assert.Equal(t, lr.Metadata().Namespace.String(), "my-namespace", "Expected namespace to be 'my-namespace'")

	// create the same collection again
	err = SaveSchema(ctx, collectionSchema)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithErrorIfExists())
	require.Error(t, err)

	// create same collection in the root namespace
	b, err := sjson.Delete(string(jsonData), "metadata.namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithErrorIfExists())
	require.NoError(t, err)
	// Verify the root collection is saved correctly
	m = collectionSchema.Metadata()
	m.Path = "" // clear path to load root collection
	lr, err = GetSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m)
	require.NoError(t, err)
	assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())

	// create new namespace called default
	namespace.Name = "default"
	err = db.DB(ctx).CreateNamespace(ctx, namespace)
	assert.NoError(t, err)

	// create the same collection in a different namespace
	b, err = sjson.Set(string(jsonData), "metadata.namespace", "default")
	require.NoError(t, err)
	jsonData = []byte(b)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.NoError(t, err)
	// Verify the new namespace collection is saved correctly
	m = collectionSchema.Metadata()
	m.Path = "" // clear path to load the new namespace collection
	lr, err = GetSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m)
	require.NoError(t, err)
	assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
	assert.Equal(t, lr.Metadata().Namespace.String(), "default", "Expected namespace to be 'another'")

	// create a collection with parameters now
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.Error(t, err)

	// create the parameter schema in root namespace
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	require.NoError(t, err)
	parameterSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema)
	require.NoError(t, err)
	// Verify the parameter schema is saved correctly in the root namespace
	m = parameterSchema.Metadata()
	m.Path = "" // clear path to load root parameter schema
	lr, err = GetSchema(ctx, types.CatalogObjectTypeParameterSchema, &m)
	require.NoError(t, err)
	assert.Equal(t, lr.Kind(), parameterSchema.Kind())
	assert.Equal(t, parameterSchema.Version(), lr.Version())
	assert.Equal(t, parameterSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
	// Verify the namespace is not set for root parameter schema
	assert.Equal(t, lr.Metadata().Namespace.String(), "", "Expected root parameter schema to have no namespace")

	// create the collection with the parameter in the namespace
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.NoError(t, err)

	// create the collection in the root namespace
	b, err = sjson.Delete(string(jsonData), "metadata.namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.NoError(t, err)

	// create the parameter schema in the namespace with modified max value
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYamlModifiedValidation))
	require.NoError(t, err)
	b, err = sjson.Set(string(jsonData), "metadata.namespace", "my-namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema)
	require.Error(t, err)

	// create it with a different name
	b, err = sjson.Set(string(jsonData), "metadata.name", "integer-param-schema-modified")
	require.NoError(t, err)
	jsonData = []byte(b)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema)
	require.NoError(t, err)
	// Verify the parameter schema is saved correctly in the namespace
	m = parameterSchema.Metadata()
	m.Path = "" // clear path to load the namespace parameter schema
	lr, err = GetSchema(ctx, types.CatalogObjectTypeParameterSchema, &m)
	require.NoError(t, err)
	assert.Equal(t, parameterSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
	assert.Equal(t, lr.Metadata().Namespace.String(), "my-namespace", "Expected namespace to be 'my-namespace'")

	// create the collections schema again
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.NoError(t, err)

	// modify the parameter schema in the root namespace with modified max value when a collection is referring to it
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYamlModifiedValidation))
	require.NoError(t, err)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema)
	require.Error(t, err)

	// create the same but ignore the conflict.  This will cause revalidation error
	err = SaveSchema(ctx, parameterSchema, IgnoreSchemaSpecChange())
	require.Error(t, err)
	// skip revalidation
	err = SaveSchema(ctx, parameterSchema, IgnoreSchemaSpecChange(), SkipRevalidationOnSchemaChange())
	require.NoError(t, err)

	// create the collections schema again.  Should error out since the max value in the parameter schema has changed
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.Error(t, err)

	// modify the collection schema to reduce the maxRetry value and try again
	b, err = sjson.Set(string(jsonData), "spec.parameters.maxRetries.default", 5)
	require.NoError(t, err)
	jsonData = []byte(b)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema)
	require.NoError(t, err)

	// delete the parameter schema
	dir, err := getVariantDirs(ctx, varId)
	require.NoError(t, err)
	md := parameterSchema.Metadata()
	md.Name = "integer-param-schema-modified" // set name to load the deleted collection schema
	md.Namespace = types.NullableStringFrom("my-namespace")
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.NoError(t, err)
	// Verify the parameter schema is deleted
	m = parameterSchema.Metadata()
	m.Name = "integer-param-schema-modified" // set name to load the deleted collection schema
	m.Namespace = types.NullableStringFrom("my-namespace")
	m.Path = "" // clear path to load the deleted collection schema
	_, err = GetSchema(ctx, types.CatalogObjectTypeParameterSchema, &m)
	require.Error(t, err)

	// delete the parameter schema that has references
	md.Name = "integer-param-schema" // set name to load the deleted collection schema
	md.Namespace = types.NullString()
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.ErrorIs(t, err, ErrUnableToDeleteParameterWithReferences)

	// delete the collection schema
	md.Name = "valid"
	err = DeleteSchema(ctx, types.CatalogObjectTypeCollectionSchema, &md, dir)
	require.NoError(t, err)
	// Verify the collection schema is deleted
	m = collectionSchema.Metadata()
	m.Name = "valid" // set name to load the deleted collectio n schema
	m.Namespace = types.NullString()
	m.Path = "/" // clear path to load the deleted collection schema
	_, err = GetSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m)
	require.Error(t, err, "Expected error when loading deleted collection schema")
	md.Name = "path"
	md.Namespace = types.NullableStringFrom("my-namespace")
	err = DeleteSchema(ctx, types.CatalogObjectTypeCollectionSchema, &md, dir)
	require.NoError(t, err)
	// Verify the root collection schema is deleted
	m = collectionSchema.Metadata()
	m.Name = "path"                                        // set name to load the deleted collection schema
	m.Namespace = types.NullableStringFrom("my-namespace") // set namespace to load the deleted collection schema
	m.Path = "/"
	_, err = GetSchema(ctx, types.CatalogObjectTypeCollectionSchema, &m)
	require.Error(t, err, "Expected error when loading deleted root collection schema")

	// delete the parameter schema that has references
	md.Name = "integer-param-schema" // set name to load the deleted collection schema
	md.Namespace = types.NullString()
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.ErrorIs(t, err, ErrUnableToDeleteParameterWithReferences)

	md.Name = "path"
	md.Namespace = types.NullString()
	err = DeleteSchema(ctx, types.CatalogObjectTypeCollectionSchema, &md, dir)
	require.NoError(t, err)
	md.Name = "integer-param-schema" // set name to load the deleted collection schema
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.NoError(t, err)

	// create two identical parameter schemas in differentnamespaces
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	require.NoError(t, err)
	b, err = sjson.Set(string(jsonData), "metadata.namespace", "my-namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema)
	require.NoError(t, err)

	// create another identical parameter schema in a different namespace
	b, err = sjson.Delete(string(jsonData), "metadata.namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema)
	require.NoError(t, err)

	// delete one
	md.Name = "integer-param-schema" // set name to load the deleted collection schema
	md.Namespace = types.NullableStringFrom("my-namespace")
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, &md, dir)
	require.NoError(t, err)
	// Verify the parameter schema in the other namespace is still available
	m = parameterSchema.Metadata()
	m.Name = "integer-param-schema"  // set name to load the deleted collection schema
	m.Namespace = types.NullString() // clear namespace to load the other parameter schema
	m.Path = ""                      // clear path to load the other parameter schema
	_, err = GetSchema(ctx, types.CatalogObjectTypeParameterSchema, &m)
	require.NoError(t, err)
}

func newDb() context.Context {
	ctx := log.Logger.WithContext(context.Background())
	ctx = db.ConnCtx(ctx)
	return ctx
}

func replaceTabsWithSpaces(s *string) {
	*s = strings.ReplaceAll(*s, "\t", "    ")
}
