package db

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dbmanager"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/postgresql"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

// DB_ is an interface for the database connection. It wraps the underlying sql.Conn interface while
// adding the ability to manage scopes.
// The three interfaces are separately initialized to allow for wrapping each interface separately.
// This is particularly useful for caching. ObjectManager is a prime candidate for caching.

type MetadataManager interface {
	//Tenant and Project
	CreateTenant(ctx context.Context, tenantID types.TenantId) error
	GetTenant(ctx context.Context, tenantID types.TenantId) (*models.Tenant, error)
	DeleteTenant(ctx context.Context, tenantID types.TenantId) error
	CreateProject(ctx context.Context, projectID types.ProjectId) error
	GetProject(ctx context.Context, projectID types.ProjectId) (*models.Project, error)
	DeleteProject(ctx context.Context, projectID types.ProjectId) error

	// Catalog
	CreateCatalog(ctx context.Context, catalog *models.Catalog) apperrors.Error
	GetCatalogIDByName(ctx context.Context, catalogName string) (uuid.UUID, apperrors.Error)
	GetCatalog(ctx context.Context, catalogID uuid.UUID, name string) (*models.Catalog, apperrors.Error)
	UpdateCatalog(ctx context.Context, catalog *models.Catalog) apperrors.Error
	DeleteCatalog(ctx context.Context, catalogID uuid.UUID, name string) apperrors.Error

	// Variant
	CreateVariant(ctx context.Context, variant *models.Variant) apperrors.Error
	GetVariant(ctx context.Context, catalogID uuid.UUID, variantID uuid.UUID, name string) (*models.Variant, apperrors.Error)
	GetVariantIDFromName(ctx context.Context, catalogID uuid.UUID, name string) (uuid.UUID, apperrors.Error)
	UpdateVariant(ctx context.Context, variantID uuid.UUID, name string, updatedVariant *models.Variant) apperrors.Error
	DeleteVariant(ctx context.Context, catalogID uuid.UUID, variantID uuid.UUID, name string) apperrors.Error

	// Version
	CreateVersion(ctx context.Context, version *models.Version) error
	GetVersion(ctx context.Context, versionNum int, variantID uuid.UUID) (*models.Version, error)
	GetVersionByLabel(ctx context.Context, label string, variantID uuid.UUID) (*models.Version, error)
	SetVersionLabel(ctx context.Context, versionNum int, variantID uuid.UUID, newLabel string) error
	UpdateVersionDescription(ctx context.Context, versionNum int, variantID uuid.UUID, newDescription string) error
	DeleteVersion(ctx context.Context, versionNum int, variantID uuid.UUID) error
	CountVersionsInVariant(ctx context.Context, variantID uuid.UUID) (int, error)
	GetNamedVersions(ctx context.Context, variantID uuid.UUID) ([]models.Version, error)

	// Workspace
	CreateWorkspace(ctx context.Context, workspace *models.Workspace) apperrors.Error
	DeleteWorkspace(ctx context.Context, workspaceID uuid.UUID) apperrors.Error
	DeleteWorkspaceByLabel(ctx context.Context, variantID uuid.UUID, label string) apperrors.Error
	GetWorkspace(ctx context.Context, workspaceID uuid.UUID) (*models.Workspace, apperrors.Error)
	GetWorkspaceByLabel(ctx context.Context, variantID uuid.UUID, label string) (*models.Workspace, apperrors.Error)
	UpdateWorkspaceLabel(ctx context.Context, workspaceID uuid.UUID, newLabel string) apperrors.Error
	UpdateWorkspace(ctx context.Context, workspace *models.Workspace) apperrors.Error
	GetCatalogForWorkspace(ctx context.Context, workspaceID uuid.UUID) (models.Catalog, apperrors.Error)
	CommitWorkspace(ctx context.Context, workspace *models.Workspace) apperrors.Error

	// Namespace
	CreateNamespace(ctx context.Context, ns *models.Namespace) apperrors.Error
	GetNamespace(ctx context.Context, name string, variantID uuid.UUID) (*models.Namespace, apperrors.Error)
	UpdateNamespace(ctx context.Context, ns *models.Namespace) apperrors.Error
	DeleteNamespace(ctx context.Context, name string, variantID uuid.UUID) apperrors.Error
	ListNamespacesByVariant(ctx context.Context, variantID uuid.UUID) ([]*models.Namespace, apperrors.Error)

	// View
	CreateView(ctx context.Context, view *models.View) apperrors.Error
	GetView(ctx context.Context, viewID uuid.UUID) (*models.View, apperrors.Error)
	GetViewByLabel(ctx context.Context, label string, catalogID uuid.UUID) (*models.View, apperrors.Error)
	UpdateView(ctx context.Context, view *models.View) apperrors.Error
	DeleteView(ctx context.Context, viewID uuid.UUID) apperrors.Error
	DeleteViewByLabel(ctx context.Context, label string, catalogID uuid.UUID) apperrors.Error
	ListViewsByCatalog(ctx context.Context, catalogID uuid.UUID) ([]*models.View, apperrors.Error)

	// ViewToken
	CreateViewToken(ctx context.Context, token *models.ViewToken) apperrors.Error
	GetViewToken(ctx context.Context, tokenID uuid.UUID) (*models.ViewToken, apperrors.Error)
	UpdateViewTokenExpiry(ctx context.Context, tokenID uuid.UUID, expireAt time.Time) apperrors.Error
	DeleteViewToken(ctx context.Context, tokenID uuid.UUID) apperrors.Error

	// SigningKey
	CreateSigningKey(ctx context.Context, key *models.SigningKey) apperrors.Error
	GetSigningKey(ctx context.Context, keyID uuid.UUID) (*models.SigningKey, apperrors.Error)
	GetActiveSigningKey(ctx context.Context) (*models.SigningKey, apperrors.Error)
	UpdateSigningKeyActive(ctx context.Context, keyID uuid.UUID, isActive bool) apperrors.Error
	DeleteSigningKey(ctx context.Context, keyID uuid.UUID) apperrors.Error
}

type ObjectManager interface {
	// Catalog Object
	CreateCatalogObject(ctx context.Context, obj *models.CatalogObject) apperrors.Error
	GetCatalogObject(ctx context.Context, hash string) (*models.CatalogObject, apperrors.Error)
	DeleteCatalogObject(ctx context.Context, t types.CatalogObjectType, hash string) apperrors.Error

	//Collections
	UpsertCollection(ctx context.Context, wc *models.Collection, dir uuid.UUID) (err apperrors.Error)
	GetCollection(ctx context.Context, path string, dir uuid.UUID) (*models.Collection, apperrors.Error)
	GetCollectionObject(ctx context.Context, path string, dir uuid.UUID) (*models.CatalogObject, apperrors.Error)
	UpdateCollection(ctx context.Context, wc *models.Collection, dir uuid.UUID) apperrors.Error
	DeleteCollection(ctx context.Context, path string, dir uuid.UUID) (string, apperrors.Error)
	HasReferencesToCollectionSchema(ctx context.Context, collectionSchema string, dir uuid.UUID) (bool, apperrors.Error)

	// Resource Groups
	UpsertResource(ctx context.Context, rg *models.Resource, directoryID uuid.UUID) apperrors.Error
	GetResource(ctx context.Context, path string, variantID uuid.UUID, directoryID uuid.UUID) (*models.Resource, apperrors.Error)
	GetResourceObject(ctx context.Context, path string, directoryID uuid.UUID) (*models.CatalogObject, apperrors.Error)
	UpdateResource(ctx context.Context, rg *models.Resource, directoryID uuid.UUID) apperrors.Error
	DeleteResource(ctx context.Context, path string, directoryID uuid.UUID) (string, apperrors.Error)
	UpsertResourceObject(ctx context.Context, rg *models.Resource, obj *models.CatalogObject, directoryID uuid.UUID) apperrors.Error

	// Schema Directory
	CreateSchemaDirectory(ctx context.Context, t types.CatalogObjectType, dir *models.SchemaDirectory) apperrors.Error
	SetDirectory(ctx context.Context, t types.CatalogObjectType, id uuid.UUID, dir []byte) apperrors.Error
	GetDirectory(ctx context.Context, t types.CatalogObjectType, id uuid.UUID) ([]byte, apperrors.Error)
	GetSchemaDirectory(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID) (*models.SchemaDirectory, apperrors.Error)
	GetObjectRefByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (*models.ObjectRef, apperrors.Error)
	LoadObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (*models.CatalogObject, apperrors.Error)
	UpdateObjectHashForPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string, hash string) apperrors.Error
	AddOrUpdateObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string, obj models.ObjectRef) apperrors.Error
	AddReferencesToObject(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string, references models.References) apperrors.Error
	GetAllReferences(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (models.References, apperrors.Error)
	DeleteReferenceFromObject(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string, reference string) apperrors.Error
	DeleteObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (types.Hash, apperrors.Error)
	FindClosestObject(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, targetName, startPath string) (string, *models.ObjectRef, apperrors.Error)
	PathExists(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (bool, apperrors.Error)
	DeleteTree(ctx context.Context, directoryIds models.DirectoryIDs, path string) ([]string, apperrors.Error)
	DeleteObjectWithReferences(ctx context.Context, t types.CatalogObjectType, dirIDs models.DirectoryIDs, delPath string, opts ...models.DirectoryObjectDeleteOptions) (string, apperrors.Error)
	DeleteNamespaceObjects(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, namespace string) ([]string, apperrors.Error)
}

type ConnectionManager interface {
	// Scope Management
	AddScopes(ctx context.Context, scopes map[string]string)
	DropScopes(ctx context.Context, scopes []string) error
	AddScope(ctx context.Context, scope, value string)
	DropScope(ctx context.Context, scope string) error
	DropAllScopes(ctx context.Context) error

	// Close the connection to the database.
	Close(ctx context.Context)
}

type DB_ interface {
	MetadataManager
	ObjectManager
	ConnectionManager
}

const (
	Scope_TenantId  string = "hatch.curr_tenantid"
	Scope_ProjectId string = "hatch.curr_projectid"
)

var configuredScopes = []string{
	Scope_TenantId,
	Scope_ProjectId,
}

var pool dbmanager.ScopedDb

func init() {
	ctx := log.Logger.WithContext(context.Background())
	pg := dbmanager.NewScopedDb(ctx, "postgresql", configuredScopes)
	if pg == nil {
		panic("unable to create db pool")
	}
	pool = pg
}

func Conn(ctx context.Context) dbmanager.ScopedConn {
	if pool != nil {
		conn, err := pool.Conn(ctx)
		if err == nil {
			return conn
		}
		log.Ctx(ctx).Error().Err(err).Msg("unable to get db connection")
	}
	return nil
}

type ctxDbKeyType string

const ctxDbKey ctxDbKeyType = "HatchCatalogDb"

func ConnCtx(ctx context.Context) context.Context {
	conn := Conn(ctx)
	return context.WithValue(ctx, ctxDbKey, conn)
}

type hatchCatalogDb struct {
	MetadataManager
	ObjectManager
	ConnectionManager
}

func DB(ctx context.Context) DB_ {
	if conn, ok := ctx.Value(ctxDbKey).(dbmanager.ScopedConn); ok {
		mm, om, cm := postgresql.NewHatchCatalogDb(conn)
		return &hatchCatalogDb{
			MetadataManager:   mm,
			ObjectManager:     om,
			ConnectionManager: cm,
		}
	}
	log.Ctx(ctx).Error().Msg("unable to get db connection from context")
	return nil
}
