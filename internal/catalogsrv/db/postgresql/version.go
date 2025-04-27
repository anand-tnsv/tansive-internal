package postgresql

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/catalogsrv/common"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/rs/zerolog/log"
)

func (mm *metadataManager) CreateVersion(ctx context.Context, version *models.Version) (err error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	version.TenantID = tenantID

	tx, err := mm.conn().BeginTx(ctx, nil)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to begin transaction")
		return dberror.ErrDatabase.Err(err)
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Ctx(ctx).Error().Err(rollbackErr).Msg("failed to rollback transaction")
			}
		}
	}()
	errDb := mm.createVersionWithTransaction(ctx, version, tx)
	if errDb != nil {
		tx.Rollback()
		log.Ctx(ctx).Error().Err(errDb).Msg("failed to create version")
		return errDb
	}
	if err := tx.Commit(); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to commit transaction")
		return dberror.ErrDatabase.Err(err)
	}
	return nil
}

func (mm *metadataManager) createVersionWithTransaction(ctx context.Context, version *models.Version, tx *sql.Tx) apperrors.Error {
	label := sql.NullString{String: version.Label, Valid: version.Label != ""}
	query := `
		SELECT version_num, label, description, info, parameters_directory, collections_directory, values_directory, variant_id, tenant_id, created_at, updated_at
		FROM create_version($1, $2, $3, $4, $5, $6);
	`
	var baseVariant any = nil
	row := tx.QueryRowContext(ctx, query, label, version.Description, version.Info, baseVariant, version.VariantID, version.TenantID)
	err := row.Scan(
		&version.VersionNum,
		&label,
		&version.Description,
		&version.Info,
		&version.ParametersDir,
		&version.CollectionsDir,
		&version.ValuesDir,
		&version.VariantID,
		&version.TenantID,
		&version.CreatedAt,
		&version.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("label", version.Label).Str("variant_id", version.VariantID.String()).Msg("version already exists")
			return dberror.ErrAlreadyExists.Msg("version already exists")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			if pgErr.Code == "23505" && pgErr.ConstraintName == "unique_label_variant_tenant" {
				log.Ctx(ctx).Error().Str("label", version.Label).Str("variant_id", version.VariantID.String()).Msg("label already exists for the given variant")
				return dberror.ErrAlreadyExists.Msg("label already exists for the given variant")
			}
			if pgErr.Code == "23514" && pgErr.ConstraintName == "versions_label_check" {
				log.Ctx(ctx).Error().Str("label", version.Label).Msg("invalid label format")
				return dberror.ErrInvalidInput.Msg("invalid label format")
			}
			if pgErr.ConstraintName == "versions_variant_id_tenant_id_fkey" || pgErr.ConstraintName == "version_sequences_variant_id_tenant_id_fkey" {
				log.Ctx(ctx).Info().Str("variant_id", version.VariantID.String()).Msg("variant not found")
				return dberror.ErrInvalidVariant
			}
		}
		log.Ctx(ctx).Error().Err(err).Str("label", version.Label).Str("variant_id", version.VariantID.String()).Msg("failed to insert version")
		return dberror.ErrDatabase.Err(err)
	}

	if label.Valid {
		version.Label = label.String
	} else {
		version.Label = ""
	}

	return nil
}

func (mm *metadataManager) GetVersion(ctx context.Context, versionNum int, variantID uuid.UUID) (*models.Version, error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT version_num, label, description, info, parameters_directory, collections_directory, values_directory, variant_id, tenant_id
		FROM versions
		WHERE version_num = $1 AND variant_id = $2 AND tenant_id = $3;
	`

	row := mm.conn().QueryRowContext(ctx, query, versionNum, variantID, tenantID)
	version := &models.Version{}
	err := row.Scan(&version.VersionNum, &version.Label, &version.Description, &version.Info, &version.ParametersDir, &version.CollectionsDir, &version.ValuesDir, &version.VariantID, &version.TenantID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Int("version_num", versionNum).Str("variant_id", variantID.String()).Msg("version not found")
			return nil, dberror.ErrNotFound.Msg("version not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve version")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return version, nil
}

func (mm *metadataManager) SetVersionLabel(ctx context.Context, versionNum int, variantID uuid.UUID, newLabel string) error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	if newLabel == "" {
		log.Ctx(ctx).Error().Msg("new label cannot be empty")
		return dberror.ErrInvalidInput.Msg("label cannot be empty")
	}

	query := `
		UPDATE versions
		SET label = $1
		WHERE version_num = $2 AND variant_id = $3 AND tenant_id = $4
		RETURNING version_num;
	`

	row := mm.conn().QueryRowContext(ctx, query, newLabel, versionNum, variantID, tenantID)
	var returnedVersionNum int
	err := row.Scan(&returnedVersionNum)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Int("version_num", versionNum).Str("variant_id", variantID.String()).Msg("version not found")
			return dberror.ErrNotFound.Msg("version not found")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			if pgErr.Code == "23505" && pgErr.ConstraintName == "unique_label_variant_tenant" {
				log.Ctx(ctx).Error().Str("label", newLabel).Str("variant_id", variantID.String()).Msg("label already exists for the given variant")
				return dberror.ErrAlreadyExists.Msg("label already exists for the given variant")
			}
			if pgErr.Code == "23514" && pgErr.ConstraintName == "versions_label_check" {
				log.Ctx(ctx).Error().Str("label", newLabel).Msg("invalid label format")
				return dberror.ErrInvalidInput.Msg("invalid label format")
			}
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to update version label")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

func (mm *metadataManager) UpdateVersionDescription(ctx context.Context, versionNum int, variantID uuid.UUID, newDescription string) error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	query := `
		UPDATE versions
		SET description = $1
		WHERE version_num = $2 AND variant_id = $3 AND tenant_id = $4
		RETURNING version_num;
	`

	row := mm.conn().QueryRowContext(ctx, query, newDescription, versionNum, variantID, tenantID)
	var returnedVersionNum int
	err := row.Scan(&returnedVersionNum)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Int("version_num", versionNum).Str("variant_id", variantID.String()).Msg("version not found")
			return dberror.ErrNotFound.Msg("version not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to update version description")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

func (mm *metadataManager) DeleteVersion(ctx context.Context, versionNum int, variantID uuid.UUID) error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	query := `
		DELETE FROM versions
		WHERE version_num = $1 AND variant_id = $2 AND tenant_id = $3;
	`

	result, err := mm.conn().ExecContext(ctx, query, versionNum, variantID, tenantID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete version")
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve result information")
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		log.Ctx(ctx).Info().Int("version_num", versionNum).Str("variant_id", variantID.String()).Msg("version not found")
		return dberror.ErrNotFound.Msg("version not found")
	}

	return nil
}

func (mm *metadataManager) CountVersionsInVariant(ctx context.Context, variantID uuid.UUID) (int, error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return 0, dberror.ErrMissingTenantID
	}

	query := `
		SELECT COUNT(*)
		FROM versions
		WHERE variant_id = $1 AND tenant_id = $2;
	`

	var count int
	err := mm.conn().QueryRowContext(ctx, query, variantID, tenantID).Scan(&count)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to count versions")
		return 0, dberror.ErrDatabase.Err(err)
	}

	return count, nil
}

func (mm *metadataManager) GetNamedVersions(ctx context.Context, variantID uuid.UUID) ([]models.Version, error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT version_num, label, description
		FROM versions
		WHERE variant_id = $1 AND tenant_id = $2 AND label IS NOT NULL;
	`

	rows, err := mm.conn().QueryContext(ctx, query, variantID, tenantID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve named versions")
		return nil, dberror.ErrDatabase.Err(err)
	}
	defer rows.Close()

	var namedVersions []models.Version
	for rows.Next() {
		var version models.Version
		err = rows.Scan(&version.VersionNum, &version.Label, &version.Description)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to scan named version row")
			return nil, dberror.ErrDatabase.Err(err)
		}
		namedVersions = append(namedVersions, version)
	}

	if err = rows.Err(); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("error after scanning named versions")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return namedVersions, nil
}

func (mm *metadataManager) GetVersionByLabel(ctx context.Context, label string, variantID uuid.UUID) (*models.Version, error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT version_num, label, description, info, parameters_directory, collections_directory, values_directory, variant_id, tenant_id
		FROM versions
		WHERE label = $1 AND variant_id = $2 AND tenant_id = $3;
	`

	row := mm.conn().QueryRowContext(ctx, query, label, variantID, tenantID)
	version := &models.Version{}
	err := row.Scan(&version.VersionNum, &version.Label, &version.Description, &version.Info, &version.ParametersDir, &version.CollectionsDir, &version.ValuesDir, &version.VariantID, &version.TenantID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("label", label).Str("variant_id", variantID.String()).Msg("version not found")
			return nil, dberror.ErrNotFound.Msg("version not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve version by label")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return version, nil
}

/*
	func (mm *metadataManager) createVersionWithTransaction(ctx context.Context, version *models.Version, tx *sql.Tx) apperrors.Error {
		label := sql.NullString{String: version.Label, Valid: version.Label != ""}
		query := `
			INSERT INTO versions (label, description, info, variant_id, tenant_id)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING version_num;
		`

		row := tx.QueryRowContext(ctx, query, label, version.Description, version.Info, version.VariantID, version.TenantID)
		var versionNum int
		err := row.Scan(&versionNum)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Ctx(ctx).Info().Str("label", version.Label).Str("variant_id", version.VariantID.String()).Msg("version already exists")
				return dberror.ErrAlreadyExists.Msg("version already exists")
			}
			if pgErr, ok := err.(*pgconn.PgError); ok {
				if pgErr.Code == "23505" && pgErr.ConstraintName == "unique_label_variant_tenant" {
					log.Ctx(ctx).Error().Str("label", version.Label).Str("variant_id", version.VariantID.String()).Msg("label already exists for the given variant")
					return dberror.ErrAlreadyExists.Msg("label already exists for the given variant")
				}
				if pgErr.Code == "23514" && pgErr.ConstraintName == "versions_label_check" {
					log.Ctx(ctx).Error().Str("label", version.Label).Msg("invalid label format")
					return dberror.ErrInvalidInput.Msg("invalid label format")
				}
				if pgErr.ConstraintName == "versions_variant_id_tenant_id_fkey" || pgErr.ConstraintName == "version_sequences_variant_id_tenant_id_fkey" {
					log.Ctx(ctx).Info().Str("variant_id", version.VariantID.String()).Msg("variant not found")
					return dberror.ErrInvalidVariant
				}
			}
			log.Ctx(ctx).Error().Err(err).Str("label", version.Label).Str("variant_id", version.VariantID.String()).Msg("failed to insert version")
			return dberror.ErrDatabase.Err(err)
		}
		version.VersionNum = versionNum

		pd := models.SchemaDirectory{VersionNum: version.VersionNum, VariantID: version.VariantID, TenantID: version.TenantID, Directory: []byte("{}")}
		cd := models.SchemaDirectory{VersionNum: version.VersionNum, VariantID: version.VariantID, TenantID: version.TenantID, Directory: []byte("{}")}
		vd := models.SchemaDirectory{VersionNum: version.VersionNum, VariantID: version.VariantID, TenantID: version.TenantID, Directory: []byte("{}")}

		if err := mm.m.createSchemaDirectoryWithTransaction(ctx, types.CatalogObjectTypeParameterSchema, &pd, tx); err != nil {
			return err
		}
		if err := mm.m.createSchemaDirectoryWithTransaction(ctx, types.CatalogObjectTypeCollectionSchema, &cd, tx); err != nil {
			return err
		}
		if err := mm.m.createSchemaDirectoryWithTransaction(ctx, types.CatalogObjectTypeCatalogCollection, &vd, tx); err != nil {
			return err
		}

		version.ParametersDir = pd.DirectoryID
		version.CollectionsDir = cd.DirectoryID
		version.ValuesDir = vd.DirectoryID

		query = `
			UPDATE versions SET parameters_directory = $1, collections_directory = $2, values_directory = $3
			WHERE version_num = $4 AND variant_id = $5 AND tenant_id = $6;
		`
		_, err = tx.ExecContext(ctx, query, version.ParametersDir, version.CollectionsDir, version.ValuesDir, version.VersionNum, version.VariantID, version.TenantID)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to update version with directories")
			return dberror.ErrDatabase.Err(err)
		}

		return nil
	}
*/
