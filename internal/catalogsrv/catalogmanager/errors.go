package catalogmanager

import (
	"net/http"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

var (
	ErrCatalogError                           apperrors.Error = apperrors.New("catalog processing failed").SetStatusCode(http.StatusInternalServerError)
	ErrCatalogNotFound                        apperrors.Error = ErrCatalogError.New("catalog not found").SetExpandError(true).SetStatusCode(http.StatusNotFound)
	ErrObjectNotFound                         apperrors.Error = ErrCatalogError.New("object not found").SetStatusCode(http.StatusNotFound)
	ErrParentCollectionSchemaNotFound         apperrors.Error = ErrCatalogError.New("parent collection schema not found").SetStatusCode(http.StatusNotFound)
	ErrUnableToLoadObject                     apperrors.Error = ErrCatalogError.New("failed to load object").SetStatusCode(http.StatusInternalServerError)
	ErrUnableToUpdateObject                   apperrors.Error = ErrCatalogError.New("failed to update object").SetExpandError(true).SetStatusCode(http.StatusInternalServerError)
	ErrUnableToDeleteObject                   apperrors.Error = ErrCatalogError.New("failed to delete object").SetStatusCode(http.StatusInternalServerError)
	ErrAlreadyExists                          apperrors.Error = ErrCatalogError.New("object already exists").SetStatusCode(http.StatusConflict)
	ErrEqualToExistingObject                  apperrors.Error = ErrCatalogError.New("object is identical to existing object").SetStatusCode(http.StatusConflict)
	ErrInvalidSchema                          apperrors.Error = ErrCatalogError.New("invalid schema").SetExpandError(true).SetStatusCode(http.StatusBadRequest)
	ErrEmptyMetadata                          apperrors.Error = ErrCatalogError.New("metadata cannot be empty").SetStatusCode(http.StatusBadRequest)
	ErrInvalidProject                         apperrors.Error = ErrCatalogError.New("invalid project").SetStatusCode(http.StatusBadRequest)
	ErrInvalidCatalog                         apperrors.Error = ErrCatalogError.New("invalid catalog").SetStatusCode(http.StatusBadRequest)
	ErrInvalidVariant                         apperrors.Error = ErrCatalogError.New("invalid variant").SetStatusCode(http.StatusBadRequest)
	ErrInvalidNamespace                       apperrors.Error = ErrCatalogError.New("invalid namespace").SetStatusCode(http.StatusBadRequest)
	ErrInvalidWorkspace                       apperrors.Error = ErrCatalogError.New("invalid workspace").SetStatusCode(http.StatusBadRequest)
	ErrInvalidWorkspaceOrVariant              apperrors.Error = ErrCatalogError.New("invalid workspace or variant").SetStatusCode(http.StatusBadRequest)
	ErrInvalidObject                          apperrors.Error = ErrCatalogError.New("invalid object").SetStatusCode(http.StatusBadRequest)
	ErrInvalidVersion                         apperrors.Error = ErrCatalogError.New("invalid version").SetStatusCode(http.StatusBadRequest)
	ErrVariantNotFound                        apperrors.Error = ErrCatalogError.New("variant not found").SetStatusCode(http.StatusNotFound)
	ErrNamespaceNotFound                      apperrors.Error = ErrCatalogError.New("namespace not found").SetStatusCode(http.StatusNotFound)
	ErrWorkspaceNotFound                      apperrors.Error = ErrCatalogError.New("workspace not found").SetStatusCode(http.StatusNotFound)
	ErrInvalidVersionOrWorkspace              apperrors.Error = ErrCatalogError.New("invalid version or workspace").SetStatusCode(http.StatusBadRequest)
	ErrInvalidCollectionSchema                apperrors.Error = ErrCatalogError.New("invalid collection schema").SetStatusCode(http.StatusBadRequest)
	ErrInvalidCollection                      apperrors.Error = ErrCatalogError.New("invalid collection").SetStatusCode(http.StatusBadRequest)
	ErrSchemaOfCollectionNotMutable           apperrors.Error = ErrCatalogError.New("collection schema cannot be modified").SetStatusCode(http.StatusBadRequest)
	ErrInvalidUUID                            apperrors.Error = ErrCatalogError.New("invalid UUID").SetStatusCode(http.StatusBadRequest)
	ErrNoAncestorReferencesFound              apperrors.Error = ErrUnableToDeleteObject.New("no ancestor references found").SetStatusCode(http.StatusConflict)
	ErrUnableToDeleteParameterWithReferences  apperrors.Error = ErrUnableToDeleteObject.New("parameter schema has existing references in collections").SetStatusCode(http.StatusConflict)
	ErrUnableToDeleteCollectionWithReferences apperrors.Error = ErrUnableToDeleteObject.New("collection schema has existing references in collections").SetStatusCode(http.StatusConflict)
	ErrInvalidParameter                       apperrors.Error = ErrCatalogError.New("invalid parameter").SetStatusCode(http.StatusBadRequest)
	ErrUnableToSaveSchema                     apperrors.Error = ErrCatalogError.New("failed to save schema").SetStatusCode(http.StatusInternalServerError)
	ErrSchemaConflict                         apperrors.Error = ErrCatalogError.New("schema conflicts with existing schema").SetStatusCode(http.StatusConflict)
	ErrInvalidRequest                         apperrors.Error = ErrCatalogError.New("invalid request").SetStatusCode(http.StatusBadRequest)
)
