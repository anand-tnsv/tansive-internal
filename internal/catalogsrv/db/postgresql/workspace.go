package postgresql

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/rs/zerolog/log"
)

// CreateWorkspace inserts a new workspace in the database.
// It automatically assigns a unique workspace ID if one is not provided.
// Returns an error if the label already exists, the label format is invalid,
// the catalog or variant ID is invalid, or there is a database error.
func (mm *metadataManager) CreateWorkspace(ctx context.Context, workspace *models.Workspace) apperrors.Error {
	tenantID := catcommon.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	workspace.TenantID = tenantID
	var workspaceID any
	if workspace.WorkspaceID != uuid.Nil {
		workspaceID = workspace.WorkspaceID
	} else {
		workspaceID = nil
	}
	label := sql.NullString{String: workspace.Label, Valid: workspace.Label != ""}
	query := `
		SELECT workspace_id, label, description, info, base_version,
		       parameters_directory, collections_directory, values_directory,
		       variant_id, tenant_id, created_at, updated_at
		FROM create_workspace($1, $2, $3, $4, $5, $6);
	`

	row := mm.conn().QueryRowContext(ctx, query,
		workspaceID,
		workspace.VariantID,
		string(tenantID),
		label,
		workspace.Description,
		workspace.Info,
	)

	errDb := row.Scan(
		&workspace.WorkspaceID,
		&label,
		&workspace.Description,
		&workspace.Info,
		&workspace.BaseVersion,
		&workspace.ParametersDir,
		&workspace.CollectionsDir,
		&workspace.ValuesDir,
		&workspace.VariantID,
		&workspace.TenantID,
		&workspace.CreatedAt,
		&workspace.UpdatedAt,
	)

	if errDb != nil {
		if errDb == sql.ErrNoRows {
			log.Ctx(ctx).Info().
				Str("label", workspace.Label).
				Str("variant_id", workspace.VariantID.String()).
				Msg("workspace not created")
			return dberror.ErrNotFound
		}

		if pgErr, ok := errDb.(*pgconn.PgError); ok {
			switch {
			case pgErr.Code == "23505" && pgErr.ConstraintName == "workspaces_label_variant_id_tenant_id_key":
				log.Ctx(ctx).Error().Str("label", workspace.Label).
					Str("variant_id", workspace.VariantID.String()).
					Msg("label already exists for the given variant")
				return dberror.ErrAlreadyExists.Msg("label already exists for the given variant")

			case pgErr.Code == "23505" && pgErr.ConstraintName == "workspaces_pkey":
				log.Ctx(ctx).Error().Str("workspace_id", workspace.WorkspaceID.String()).
					Msg("workspace already exists")
				return dberror.ErrAlreadyExists.Msg("workspace already exists")

			case pgErr.Code == "23514" && pgErr.ConstraintName == "workspaces_label_check":
				log.Ctx(ctx).Error().Str("label", workspace.Label).
					Msg("invalid label format")
				return dberror.ErrInvalidInput.Msg("invalid label format")

			case pgErr.ConstraintName == "workspaces_variant_id_tenant_id_fkey":
				log.Ctx(ctx).Info().Str("variant_id", workspace.VariantID.String()).
					Msg("variant not found")
				return dberror.ErrInvalidCatalog

			case pgErr.Code == "P0002":
				log.Ctx(ctx).Info().Str("variant_id", workspace.VariantID.String()).Msg("variant not found")
				return dberror.ErrInvalidVariant
			}

		}

		log.Ctx(ctx).Error().Err(errDb).
			Str("label", workspace.Label).
			Str("variant_id", workspace.VariantID.String()).
			Msg("failed to create workspace")
		return dberror.ErrDatabase.Err(errDb)
	}
	if label.Valid {
		workspace.Label = label.String
	}
	return nil
}

func (mm *metadataManager) DeleteWorkspace(ctx context.Context, workspaceID uuid.UUID) apperrors.Error {
	tenantID := catcommon.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	query := `
		DELETE FROM workspaces
		WHERE workspace_id = $1 AND tenant_id = $2;
	`

	result, err := mm.conn().ExecContext(ctx, query, workspaceID, tenantID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete workspace")
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve result information")
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		log.Ctx(ctx).Info().Str("workspace_id", workspaceID.String()).Str("tenant_id", string(tenantID)).Msg("workspace not found")
	}

	return nil
}

func (mm *metadataManager) DeleteWorkspaceByLabel(ctx context.Context, variantID uuid.UUID, label string) apperrors.Error {
	tenantID := catcommon.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	if variantID == uuid.Nil {
		return dberror.ErrInvalidVariant
	}
	if label == "" {
		return dberror.ErrInvalidInput.Msg("label cannot be empty")
	}

	query := `
		DELETE FROM workspaces
		WHERE label = $1 AND variant_id = $2 AND tenant_id = $3;
	`

	result, err := mm.conn().ExecContext(ctx, query, label, variantID, tenantID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete workspace")
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve result information")
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		log.Ctx(ctx).Info().Str("label", label).Str("variant_id", variantID.String()).Str("tenant_id", string(tenantID)).Msg("workspace not found")
	}

	return nil
}

func (mm *metadataManager) GetWorkspace(ctx context.Context, workspaceID uuid.UUID) (*models.Workspace, apperrors.Error) {
	tenantID := catcommon.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT workspace_id, label, description, info, base_version, parameters_directory, collections_directory, values_directory, variant_id, tenant_id, created_at, updated_at
		FROM workspaces
		WHERE workspace_id = $1 AND tenant_id = $2;
	`

	row := mm.conn().QueryRowContext(ctx, query, workspaceID, tenantID)
	workspace := &models.Workspace{}
	var label sql.NullString
	err := row.Scan(
		&workspace.WorkspaceID, &label, &workspace.Description, &workspace.Info,
		&workspace.BaseVersion, &workspace.ParametersDir, &workspace.CollectionsDir, &workspace.ValuesDir,
		&workspace.VariantID, &workspace.TenantID, &workspace.CreatedAt, &workspace.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("workspace_id", workspaceID.String()).Str("tenant_id", string(tenantID)).Msg("workspace not found")
			return nil, dberror.ErrNotFound.Msg("workspace not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve workspace")
		return nil, dberror.ErrDatabase.Err(err)
	}
	if label.Valid {
		workspace.Label = label.String
	} else {
		workspace.Label = ""
	}
	return workspace, nil
}

func (mm *metadataManager) UpdateWorkspace(ctx context.Context, workspace *models.Workspace) apperrors.Error {
	tenantID := catcommon.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	var label sql.NullString
	if workspace.Label != "" {
		label = sql.NullString{String: workspace.Label, Valid: true}
	}

	query := `
		UPDATE workspaces
		SET label = $1, description = $2, info = $3, base_version = $4,
		parameters_directory = $5, collections_directory = $6, values_directory = $7
		WHERE workspace_id = $8 AND tenant_id = $9;
	`

	result, err := mm.conn().ExecContext(ctx, query,
		label,
		workspace.Description,
		workspace.Info,
		workspace.BaseVersion,
		workspace.ParametersDir,
		workspace.CollectionsDir,
		workspace.ValuesDir,
		workspace.WorkspaceID,
		tenantID,
	)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to update workspace")
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve result information")
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		log.Ctx(ctx).Info().Str("workspace_id", workspace.WorkspaceID.String()).Str("tenant_id", string(tenantID)).Msg("no rows updated")
		return dberror.ErrNotFound.Msg("workspace not found")
	}

	return nil
}

func (mm *metadataManager) GetWorkspaceByLabel(ctx context.Context, variantID uuid.UUID, label string) (*models.Workspace, apperrors.Error) {
	tenantID := catcommon.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	if variantID == uuid.Nil {
		return nil, dberror.ErrInvalidVariant
	}
	if label == "" {
		return nil, dberror.ErrInvalidInput.Msg("label cannot be empty")
	}

	query := `
		SELECT workspace_id, label, description, info, base_version, parameters_directory, collections_directory, values_directory, variant_id, tenant_id, created_at, updated_at
		FROM workspaces
		WHERE label = $1 AND variant_id = $2 AND tenant_id = $3;
	`

	row := mm.conn().QueryRowContext(ctx, query, label, variantID, tenantID)
	workspace := &models.Workspace{}
	err := row.Scan(
		&workspace.WorkspaceID, &workspace.Label, &workspace.Description, &workspace.Info,
		&workspace.BaseVersion, &workspace.ParametersDir, &workspace.CollectionsDir, &workspace.ValuesDir,
		&workspace.VariantID, &workspace.TenantID, &workspace.CreatedAt, &workspace.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("label", label).Str("variant_id", variantID.String()).Str("tenant_id", string(tenantID)).Msg("workspace not found")
			return nil, dberror.ErrNotFound.Msg("workspace not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve workspace")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return workspace, nil
}

func (mm *metadataManager) UpdateWorkspaceLabel(ctx context.Context, workspaceID uuid.UUID, newLabel string) apperrors.Error {
	tenantID := catcommon.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	if newLabel == "" {
		log.Ctx(ctx).Error().Msg("new label cannot be empty")
		return dberror.ErrInvalidInput.Msg("label cannot be empty")
	}

	query := `
		UPDATE workspaces
		SET label = $1, updated_at = NOW()
		WHERE workspace_id = $2 AND tenant_id = $3
		RETURNING workspace_id;
	`

	row := mm.conn().QueryRowContext(ctx, query, newLabel, workspaceID, tenantID)
	var returnedWorkspaceID uuid.UUID
	err := row.Scan(&returnedWorkspaceID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("workspace_id", workspaceID.String()).Str("tenant_id", string(tenantID)).Msg("workspace not found")
			return dberror.ErrNotFound.Msg("workspace not found")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			if pgErr.Code == "23505" && pgErr.ConstraintName == "workspaces_label_variant_id_tenant_id_key" {
				log.Ctx(ctx).Error().Str("label", newLabel).Str("workspace_id", workspaceID.String()).Msg("label already exists for another workspace")
				return dberror.ErrAlreadyExists.Msg("label already exists for another workspace")
			}
			if pgErr.Code == "23514" && pgErr.ConstraintName == "workspaces_label_check" {
				log.Ctx(ctx).Error().Str("label", newLabel).Msg("invalid label format")
				return dberror.ErrInvalidInput.Msg("invalid label format")
			}
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to update workspace label")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

func (mm *metadataManager) GetCatalogForWorkspace(ctx context.Context, workspaceID uuid.UUID) (models.Catalog, apperrors.Error) {
	// get the variant ID and then from variant fetch the catalog id and then the catalog
	tenantID := catcommon.TenantIdFromContext(ctx)
	if tenantID == "" {
		return models.Catalog{}, dberror.ErrMissingTenantID
	}

	query := `
	SELECT c.catalog_id, c.name, c.info
	FROM workspaces w
	JOIN variants v ON w.variant_id = v.variant_id AND w.tenant_id = v.tenant_id
	JOIN catalogs c ON v.catalog_id = c.catalog_id AND v.tenant_id = c.tenant_id
	WHERE w.workspace_id = $1 AND w.tenant_id = $2;
	`
	row := mm.conn().QueryRowContext(ctx, query, workspaceID, tenantID)
	catalog := models.Catalog{}
	var info sql.NullString
	err := row.Scan(&catalog.CatalogID, &catalog.Name, &info)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("workspace_id", workspaceID.String()).Str("tenant_id", string(tenantID)).Msg("catalog not found for workspace")
			return models.Catalog{}, dberror.ErrNotFound.Msg("catalog not found for workspace")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve catalog for workspace")
		return models.Catalog{}, dberror.ErrDatabase.Err(err)
	}
	if info.Valid {
		if err := catalog.Info.Set(info.String); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to parse catalog info JSON")
			return models.Catalog{}, dberror.ErrDatabase.Err(err)
		}
	} else {
		catalog.Info = pgtype.JSONB{}
	}
	return catalog, nil
}

func (mm *metadataManager) CommitWorkspace(ctx context.Context, workspace *models.Workspace) apperrors.Error {
	tenantID := catcommon.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	workspace.TenantID = tenantID

	query := `
		SELECT workspace_id, variant_id
		FROM commit_workspace($1, $2);
	`

	row := mm.conn().QueryRowContext(ctx, query,
		workspace.WorkspaceID,
		string(tenantID),
	)

	err := row.Scan(
		&workspace.WorkspaceID,
		&workspace.VariantID,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().
				Str("workspace_id", workspace.WorkspaceID.String()).
				Msg("workspace not found or not committed")
			return dberror.ErrNotFound.Msg("workspace not found")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			log.Ctx(ctx).Error().
				Str("workspace_id", workspace.WorkspaceID.String()).
				Str("tenant_id", string(tenantID)).
				Str("code", pgErr.Code).
				Str("message", pgErr.Message).
				Msg("pg error during workspace commit")
			return dberror.ErrDatabase.Err(pgErr)
		}
		log.Ctx(ctx).Error().
			Err(err).
			Str("workspace_id", workspace.WorkspaceID.String()).
			Msg("failed to commit workspace")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

/*
	func (mm *metadataManager) CreateWorkspaceWithoutStoredProcedure(ctx context.Context, workspace *models.Workspace) (err apperrors.Error) {
		tenantID := catcommon.TenantIdFromContext(ctx)
		if tenantID == "" {
			return dberror.ErrMissingTenantID
		}
		workspace.TenantID = tenantID

		workspaceID := workspace.WorkspaceID
		if workspaceID == uuid.Nil {
			workspaceID = uuid.New()
		}

		label := sql.NullString{String: workspace.Label, Valid: workspace.Label != ""}
		tx, errdb := mm.conn().BeginTx(ctx, &sql.TxOptions{})
		if errdb != nil {
			log.Ctx(ctx).Error().Err(errdb).Msg("failed to start transaction")
			return dberror.ErrDatabase.Err(errdb)
		}
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					log.Ctx(ctx).Error().Err(rollbackErr).Msg("failed to rollback transaction")
				}
			}
		}()

		query := `
			INSERT INTO workspaces (workspace_id, label, description, info, base_version, variant_id, tenant_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			RETURNING workspace_id;
		`

		row := tx.QueryRowContext(ctx, query, workspaceID, label, workspace.Description, workspace.Info, workspace.BaseVersion, workspace.VariantID, workspace.TenantID)
		var insertedWorkspaceID uuid.UUID
		errDb := row.Scan(&insertedWorkspaceID)
		if errDb != nil {
			if errDb == sql.ErrNoRows {
				log.Ctx(ctx).Info().Str("label", workspace.Label).Str("variant_id", workspace.VariantID.String()).Msg("workspace already exists")
				return dberror.ErrAlreadyExists.Msg("workspace already exists")
			}
			if pgErr, ok := errDb.(*pgconn.PgError); ok {
				if pgErr.Code == "23505" && pgErr.ConstraintName == "workspaces_label_variant_id_tenant_id_key" {
					log.Ctx(ctx).Error().Str("label", workspace.Label).Str("variant_id", workspace.VariantID.String()).Msg("label already exists for the given variant")
					return dberror.ErrAlreadyExists.Msg("label already exists for the given variant")
				}
				if pgErr.Code == "23505" && pgErr.ConstraintName == "workspaces_pkey" {
					log.Ctx(ctx).Error().Str("workspace_id", workspaceID.String()).Msg("workspace already exists")
					return dberror.ErrAlreadyExists.Msg("workspace already exists")
				}
				if pgErr.Code == "23514" && pgErr.ConstraintName == "workspaces_label_check" {
					log.Ctx(ctx).Error().Str("label", workspace.Label).Msg("invalid label format")
					return dberror.ErrInvalidInput.Msg("invalid label format")
				}
				if pgErr.ConstraintName == "workspaces_variant_id_tenant_id_fkey" {
					log.Ctx(ctx).Info().Str("variant_id", workspace.VariantID.String()).Msg("variant not found")
					return dberror.ErrInvalidVariant
				}
			}
			log.Ctx(ctx).Error().Err(errDb).Str("label", workspace.Label).Str("variant_id", workspace.VariantID.String()).Msg("failed to insert workspace")
			return dberror.ErrDatabase.Err(errDb)
		}
		workspace.WorkspaceID = insertedWorkspaceID

		dirs := []struct {
			objType catcommon.CatalogObjectType
			dirID   *uuid.UUID
		}{
			{catcommon.CatalogObjectTypeParameterSchema, &workspace.ParametersDir},
			{catcommon.CatalogObjectTypeCollectionSchema, &workspace.CollectionsDir},
			{catcommon.CatalogObjectTypeCatalogCollection, &workspace.ValuesDir},
		}

		for _, d := range dirs {
			dir := models.SchemaDirectory{
				WorkspaceID: workspace.WorkspaceID,
				VariantID:   workspace.VariantID,
				TenantID:    workspace.TenantID,
				Directory:   []byte("{}"),
			}
			if err := mm.m.createSchemaDirectoryWithTransaction(ctx, d.objType, &dir, tx); err != nil {
				return err
			}
			*d.dirID = dir.DirectoryID
		}

		updateQuery := `
			UPDATE workspaces
			SET parameters_directory = $1, collections_directory = $2, values_directory = $3
			WHERE workspace_id = $4 AND tenant_id = $5;
		`
		_, errDb = tx.ExecContext(ctx, updateQuery,
			workspace.ParametersDir,
			workspace.CollectionsDir,
			workspace.ValuesDir,
			workspace.WorkspaceID,
			workspace.TenantID,
		)
		if errDb != nil {
			tx.Rollback()
			log.Ctx(ctx).Error().Err(errDb).Msg("failed to update workspace with directories")
			return dberror.ErrDatabase.Err(errDb)
		}

		if err := tx.Commit(); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to commit transaction")
			return dberror.ErrDatabase.Err(err)
		}

		return nil
	}
*/
