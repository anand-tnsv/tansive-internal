// Description: This file contains the implementation of the hatchCatalogDb interface for the PostgreSQL database.
package postgresql

import (
	"context"

	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dbmanager"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
)

type hatchCatalogDb struct {
	mm *metadataManager
	om *objectManager
	cm *connectionManager
}

func NewHatchCatalogDb(c dbmanager.ScopedConn) (*metadataManager, *objectManager, *connectionManager) {
	h := &hatchCatalogDb{}
	h.mm = newMetadataManager(c)
	h.om = newObjectManager(c)
	h.cm = newConnectionManager(c)
	h.om.m = h.mm
	return h.mm, h.om, h.cm
}

// getTenantAndProjectFromContext extracts tenant and project IDs from the context
func getTenantAndProjectFromContext(ctx context.Context) (types.TenantId, types.ProjectId, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	projectID := common.ProjectIdFromContext(ctx)

	// Validate tenantID and projectID to ensure they are not empty
	if tenantID == "" {
		return "", "", dberror.ErrMissingTenantID.Err(dberror.ErrInvalidInput)
	}
	if projectID == "" {
		return "", "", dberror.ErrMissingProjecID.Err(dberror.ErrInvalidInput)
	}

	return tenantID, projectID, nil
}
