package postgresql

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

func (mm *metadataManager) CreateViewToken(ctx context.Context, token *models.ViewToken) apperrors.Error {
	if err := token.Validate(); err != nil {
		return dberror.ErrInvalidInput.Err(err)
	}

	tenantID, err := getTenantIdFromContext(ctx)
	if err != nil {
		return err
	}

	query := `
		INSERT INTO view_tokens (view_id, tenant_id, expire_at)
		VALUES ($1, $2, $3)
		RETURNING token_id, created_at, updated_at`

	errDb := mm.conn().QueryRowContext(ctx, query,
		token.ViewID, tenantID, token.ExpireAt).
		Scan(&token.TokenID, &token.CreatedAt, &token.UpdatedAt)

	if errDb != nil {
		log.Ctx(ctx).Error().Err(errDb).Msg("failed to create view token")
		return dberror.ErrDatabase.Err(errDb)
	}

	return nil
}

func (mm *metadataManager) GetViewToken(ctx context.Context, tokenID uuid.UUID) (*models.ViewToken, apperrors.Error) {
	tenantID, err := getTenantIdFromContext(ctx)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT token_id, view_id, tenant_id, expire_at, created_at, updated_at
		FROM view_tokens
		WHERE token_id = $1 AND tenant_id = $2`

	token := &models.ViewToken{}
	errDb := mm.conn().QueryRowContext(ctx, query, tokenID, tenantID).
		Scan(&token.TokenID, &token.ViewID, &token.TenantID, &token.ExpireAt, &token.CreatedAt, &token.UpdatedAt)

	if errDb != nil {
		if errDb == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("token_id", tokenID.String()).Msg("view token not found")
			return nil, dberror.ErrNotFound.Msg("view token not found")
		}
		log.Ctx(ctx).Error().Err(errDb).Str("token_id", tokenID.String()).Msg("failed to get view token")
		return nil, dberror.ErrDatabase.Err(errDb)
	}

	return token, nil
}

func (mm *metadataManager) UpdateViewTokenExpiry(ctx context.Context, tokenID uuid.UUID, expireAt time.Time) apperrors.Error {
	tenantID, err := getTenantIdFromContext(ctx)
	if err != nil {
		return err
	}

	query := `
		UPDATE view_tokens
		SET expire_at = $1
		WHERE token_id = $2 AND tenant_id = $3`

	result, errDb := mm.conn().ExecContext(ctx, query, expireAt, tokenID, tenantID)
	if errDb != nil {
		log.Ctx(ctx).Error().Err(errDb).Str("token_id", tokenID.String()).Msg("failed to update view token expiry")
		return dberror.ErrDatabase.Err(errDb)
	}

	rowsAffected, errDb := result.RowsAffected()
	if errDb != nil {
		log.Ctx(ctx).Error().Err(errDb).Msg("failed to get rows affected")
		return dberror.ErrDatabase.Err(errDb)
	}

	if rowsAffected == 0 {
		log.Ctx(ctx).Info().Str("token_id", tokenID.String()).Msg("view token not found")
		return dberror.ErrNotFound.Msg("view token not found")
	}

	return nil
}

func (mm *metadataManager) DeleteViewToken(ctx context.Context, tokenID uuid.UUID) apperrors.Error {
	tenantID, err := getTenantIdFromContext(ctx)
	if err != nil {
		return err
	}

	query := `
		DELETE FROM view_tokens
		WHERE token_id = $1 AND tenant_id = $2`

	result, errDb := mm.conn().ExecContext(ctx, query, tokenID, tenantID)
	if errDb != nil {
		log.Ctx(ctx).Error().Err(errDb).Str("token_id", tokenID.String()).Msg("failed to delete view token")
		return dberror.ErrDatabase.Err(errDb)
	}

	rowsAffected, errDb := result.RowsAffected()
	if errDb != nil {
		log.Ctx(ctx).Error().Err(errDb).Msg("failed to get rows affected")
		return dberror.ErrDatabase.Err(errDb)
	}

	if rowsAffected == 0 {
		log.Ctx(ctx).Info().Str("token_id", tokenID.String()).Msg("view token not found")
		return dberror.ErrNotFound.Msg("view token not found")
	}

	return nil
}
