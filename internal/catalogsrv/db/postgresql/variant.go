package postgresql

import (
	"context"
	"database/sql"

	"github.com/tansive/tansive-internal/internal/common/uuid"
	"github.com/jackc/pgconn"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

// CreateVariant creates a new variant in the database.
// It generates a new UUID for the variant ID and sets the project ID based on the context.
// If a variant with the same name and catalog ID already exists, the insertion is skipped.
// Returns an error if the variant already exists, the variant name format is invalid,
// the catalog ID is invalid, or there is a database error.
func (mm *metadataManager) CreateVariant(ctx context.Context, variant *models.Variant) (err apperrors.Error) {
	// Start a transaction
	tx, errdb := mm.conn().BeginTx(ctx, &sql.TxOptions{})
	if errdb != nil {
		log.Ctx(ctx).Error().Err(errdb).Msg("failed to start transaction")
		return dberror.ErrDatabase.Err(errdb)
	}
	defer func() {
		// Ensure transaction is rolled back if not committed
		if err != nil {
			tx.Rollback()
		}
	}()

	err = mm.createVariantWithTransaction(ctx, variant, tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Commit the transaction if both insertions succeed
	errdb = tx.Commit()
	if errdb != nil {
		log.Ctx(ctx).Error().Err(errdb).Msg("failed to commit transaction")
		return dberror.ErrDatabase.Err(errdb)
	}

	return nil
}

func (mm *metadataManager) createVariantWithTransaction(ctx context.Context, variant *models.Variant, tx *sql.Tx) apperrors.Error {
	tenantID := catcommon.GetTenantID(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	variantID := variant.VariantID
	if variant.VariantID == uuid.Nil {
		variantID = uuid.New()
	}
	rgDirID := uuid.New()
	variant.ResourceDirectoryID = rgDirID
	// Query to insert the variant
	queryVariant := `
		INSERT INTO variants (variant_id, name, description, info, catalog_id, resource_directory, tenant_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (name, catalog_id, tenant_id) DO NOTHING
		RETURNING variant_id, name;
	`

	// Execute variant insertion within the transaction
	row := tx.QueryRowContext(ctx, queryVariant, variantID, variant.Name, variant.Description, variant.Info, variant.CatalogID, rgDirID, tenantID)
	var insertedVariantID uuid.UUID
	var insertedName string
	err := row.Scan(&insertedVariantID, &insertedName)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("name", variant.Name).Str("variant_id", variant.VariantID.String()).Msg("variant already exists")
			return dberror.ErrAlreadyExists.Msg("variant already exists")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			if pgErr.Code == "23514" && pgErr.ConstraintName == "variants_name_check" {
				log.Ctx(ctx).Error().Str("name", variant.Name).Msg("invalid variant name format")
				return dberror.ErrInvalidInput.Msg("invalid variant name format")
			}
			if pgErr.ConstraintName == "variants_catalog_id_fkey" {
				log.Ctx(ctx).Info().Str("catalog_id", variant.CatalogID.String()).Msg("catalog not found")
				return dberror.ErrInvalidCatalog
			}
			if pgErr.Code == "23503" || pgErr.ConstraintName == "variants_catalog_id_tenant_id_fkey" { // Unique constraint violation
				log.Ctx(ctx).Error().Str("name", variant.Name).Msg("catalog not found or invalid")
				return dberror.ErrInvalidCatalog
			}
		}
		log.Ctx(ctx).Error().Err(err).Str("name", variant.Name).Str("variant_id", variant.VariantID.String()).Msg("failed to insert variant")
		return dberror.ErrDatabase.Err(err)
	}

	// Set the variant ID
	variant.VariantID = insertedVariantID

	// Create a default namespace for the variant
	namespace := models.Namespace{
		Name:        catcommon.DefaultNamespace,
		VariantID:   variant.VariantID,
		TenantID:    tenantID,
		Description: "Default namespace for the variant",
		Info:        nil, // Default info as null
	}
	errDb := mm.createNamespaceWithTransaction(ctx, &namespace, tx)
	if errDb != nil {
		log.Ctx(ctx).Error().Err(errDb).Str("variant_id", variant.VariantID.String()).Msg("failed to create default namespace for variant")
		return errDb
	}

	// Create the resourcegroups directory for the variant
	dir := models.SchemaDirectory{
		DirectoryID: rgDirID,
		VariantID:   variant.VariantID,
		TenantID:    tenantID,
		Directory:   []byte("{}"),
	}

	tableName := getSchemaDirectoryTableName(catcommon.CatalogObjectTypeResource)
	if tableName == "" {
		return dberror.ErrInvalidInput.Msg("invalid catalog object type: resource group not supported")
	}

	// Insert the schema directory into the database and get created uuid
	query := ` INSERT INTO ` + tableName + ` (directory_id, variant_id, tenant_id, directory)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (directory_id, tenant_id) DO NOTHING RETURNING directory_id;`

	var directoryID uuid.UUID
	err = tx.QueryRowContext(ctx, query, dir.DirectoryID, dir.VariantID, dir.TenantID, dir.Directory).Scan(&directoryID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("directory_id", dir.DirectoryID.String()).Msg("resource groups directory already exists, skipping")
			return nil
		} else {
			return dberror.ErrDatabase.Err(err)
		}
	}
	dir.DirectoryID = directoryID

	return nil
}

// GetVariant retrieves a variant from the database based on the variant ID or name.
// If both variantID and name are provided, variantID takes precedence.
// Returns the variant if found, or an error if the variant is not found or there is a database error.

func (mm *metadataManager) GetVariant(ctx context.Context, catalogID uuid.UUID, variantID uuid.UUID, name string) (*models.Variant, apperrors.Error) {
	tenantID := catcommon.GetTenantID(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	var query string
	var row *sql.Row

	if variantID != uuid.Nil {
		query = `
			SELECT variant_id, name, description, info, catalog_id, resource_directory
			FROM variants
			WHERE variant_id = $1 AND tenant_id = $2;
		`
		row = mm.conn().QueryRowContext(ctx, query, variantID, tenantID)
	} else if name != "" {
		query = `
			SELECT variant_id, name, description, info, catalog_id, resource_directory
			FROM variants
			WHERE name = $1 AND catalog_id = $2 AND tenant_id = $3;
		`
		row = mm.conn().QueryRowContext(ctx, query, name, catalogID, tenantID)
	} else {
		log.Ctx(ctx).Error().Msg("either variant ID or name must be provided")
		return nil, dberror.ErrInvalidInput.Msg("either variant ID or name must be provided")
	}

	variant := &models.Variant{}
	err := row.Scan(&variant.VariantID, &variant.Name, &variant.Description, &variant.Info, &variant.CatalogID, &variant.ResourceDirectoryID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Msg("variant not found")
			return nil, dberror.ErrNotFound.Msg("variant not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve variant")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return variant, nil
}

// GetVariantByID retrieves a variant by its UUID. This function performs a direct lookup
func (mm *metadataManager) GetVariantByID(ctx context.Context, variantID uuid.UUID) (*models.Variant, apperrors.Error) {
	tenantID := catcommon.GetTenantID(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT variant_id, name, description, info, catalog_id, resource_directory
		FROM variants
		WHERE variant_id = $1 AND tenant_id = $2;
	`
	row := mm.conn().QueryRowContext(ctx, query, variantID, tenantID)
	variant := &models.Variant{}
	err := row.Scan(&variant.VariantID, &variant.Name, &variant.Description, &variant.Info, &variant.CatalogID, &variant.ResourceDirectoryID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Msg("variant not found")
			return nil, dberror.ErrNotFound.Msg("variant not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve variant")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return variant, nil
}

func (mm *metadataManager) GetVariantIDFromName(ctx context.Context, catalogID uuid.UUID, name string) (uuid.UUID, apperrors.Error) {
	tenantID := catcommon.GetTenantID(ctx)
	if tenantID == "" {
		return uuid.Nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT variant_id
		FROM variants
		WHERE name = $1 AND catalog_id = $2 AND tenant_id = $3;
	`

	var variantID uuid.UUID
	err := mm.conn().QueryRowContext(ctx, query, name, catalogID, tenantID).Scan(&variantID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Msg("variant not found")
			return uuid.Nil, dberror.ErrNotFound.Msg("variant not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve variant ID")
		return uuid.Nil, dberror.ErrDatabase.Err(err)
	}

	return variantID, nil
}

// UpdateVariant updates an existing variant in the database based on the variant ID or name.
// If both variantID and name are provided, variantID takes precedence.
// The VariantID and CatalogID fields cannot be updated.
// Returns an error if the variant is not found, the variant name already exists for the given catalog ID,
// the variant name format is invalid, or there is a database error.
func (mm *metadataManager) UpdateVariant(ctx context.Context, variantID uuid.UUID, name string, updatedVariant *models.Variant) apperrors.Error {
	tenantID := catcommon.GetTenantID(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	var query string
	var row *sql.Row

	if variantID != uuid.Nil {
		query = `
			UPDATE variants
			SET name = $1, description = $2, info = $3
			WHERE variant_id = $4 AND catalog_id = $5 AND tenant_id = $6
			RETURNING variant_id;
		`
		row = mm.conn().QueryRowContext(ctx, query, updatedVariant.Name, updatedVariant.Description, updatedVariant.Info, variantID, updatedVariant.CatalogID, tenantID)
	} else if name != "" {
		query = `
			UPDATE variants
			SET name = $1, description = $2, info = $3
			WHERE name = $4 AND catalog_id = $5 AND tenant_id = $6
			RETURNING variant_id;
		`
		row = mm.conn().QueryRowContext(ctx, query, updatedVariant.Name, updatedVariant.Description, updatedVariant.Info, name, updatedVariant.CatalogID, tenantID)
	} else {
		log.Ctx(ctx).Error().Msg("either variant ID or name must be provided")
		return dberror.ErrInvalidInput.Msg("either variant ID or name must be provided")
	}

	var returnedVariantID uuid.UUID
	err := row.Scan(&returnedVariantID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Msg("variant not found or no changes made")
			return dberror.ErrNotFound.Msg("variant not found or no changes made")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			if pgErr.Code == "23505" && pgErr.ConstraintName == "variants_name_catalog_id_tenant_id_key" { // Unique constraint violation
				log.Ctx(ctx).Error().Msg("variant name already exists for the given catalog_id")
				return dberror.ErrAlreadyExists.Msg("variant name already exists for the given catalog_id")
			}
			if pgErr.Code == "23514" && pgErr.ConstraintName == "variants_name_check" { // Check constraint violation code and specific constraint name
				log.Ctx(ctx).Error().Str("name", updatedVariant.Name).Msg("invalid variant name format")
				return dberror.ErrInvalidInput.Msg("invalid variant name format")
			}
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to update variant")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

// DeleteVariant deletes a variant from the database based on the variant ID or name.
// If both variantID and name are provided, variantID takes precedence.
// Returns an error if the variant is not found or there is a database error.
func (mm *metadataManager) DeleteVariant(ctx context.Context, catalogID, variantID uuid.UUID, name string) apperrors.Error {
	tenantID := catcommon.GetTenantID(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	if variantID == uuid.Nil && (catalogID == uuid.Nil || name == "") {
		return dberror.ErrInvalidInput.Msg("either variant ID or name must be provided")
	}

	var query string
	var err error
	var result sql.Result

	if variantID != uuid.Nil {
		query = `
			DELETE FROM variants
			WHERE variant_id = $1 AND catalog_id = $2 AND tenant_id = $3;
		`
		result, err = mm.conn().ExecContext(ctx, query, variantID, catalogID, tenantID)
	} else {
		query = `
			DELETE FROM variants
			WHERE name = $1 AND catalog_id = $2 AND tenant_id = $3;
		`
		result, err = mm.conn().ExecContext(ctx, query, name, catalogID, tenantID)
	}

	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete variant")
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve result information")
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		log.Ctx(ctx).Info().Str("variant_id", variantID.String()).Str("variant_name", name).Str("catalog_id", catalogID.String()).Str("tenant_id", string(tenantID)).Msg("variant not found")
	}

	return nil
}

func (mm *metadataManager) GetMetadataNames(ctx context.Context, catalogID uuid.UUID, variantID uuid.UUID) (string, string, apperrors.Error) {
	tenantID := catcommon.GetTenantID(ctx)
	if tenantID == "" {
		return "", "", dberror.ErrMissingTenantID
	}

	query := `
		SELECT catalog.name, variant.name
		FROM catalog
		JOIN variant ON catalog.catalog_id = variant.catalog_id
		WHERE catalog.catalog_id = $1 AND variant.variant_id = $2 AND variant.tenant_id = $3;
	`

	var catalogName string
	var variantName string
	err := mm.conn().QueryRowContext(ctx, query, catalogID, variantID, tenantID).Scan(&catalogName, &variantName)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Msg("variant not found")
			return "", "", dberror.ErrNotFound.Msg("variant not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve variant")
		return "", "", dberror.ErrDatabase.Err(err)
	}

	return catalogName, variantName, nil
}
