package policy

import (
	"context"
	"errors"

	json "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/dberror"
	"github.com/tansive/tansive-internal/internal/catalogsrv/db/models"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

type ViewManager interface {
	ID() uuid.UUID
	Name() string
	GetViewDefinition(ctx context.Context) (*ViewDefinition, apperrors.Error)
	GetViewResourcePath(ctx context.Context) (TargetResource, apperrors.Error)
}
type viewManager struct {
	view *models.View
}

func NewViewManagerByViewLabel(ctx context.Context, viewLabel string) (ViewManager, apperrors.Error) {
	catalogID := catcommon.GetCatalogID(ctx)
	if catalogID == uuid.Nil || viewLabel == "" {
		return nil, ErrInvalidView
	}

	view, err := db.DB(ctx).GetViewByLabel(ctx, viewLabel, catalogID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrViewNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load view")
		return nil, ErrUnableToLoadObject.Msg("unable to load view")
	}

	viewManager := &viewManager{view: view}
	return viewManager, nil
}

func NewViewManagerByViewID(ctx context.Context, viewID uuid.UUID) (ViewManager, apperrors.Error) {
	view, err := db.DB(ctx).GetView(ctx, viewID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrViewNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load view")
		return nil, ErrUnableToLoadObject.Msg("unable to load view")
	}
	return &viewManager{view: view}, nil
}

func (v *viewManager) ID() uuid.UUID {
	return v.view.ViewID
}

func (v *viewManager) Name() string {
	return v.view.Label
}

func (v *viewManager) GetViewDefinition(ctx context.Context) (*ViewDefinition, apperrors.Error) {
	var viewDef ViewDefinition
	if err := json.Unmarshal(v.view.Rules, &viewDef); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal view rules")
		return nil, ErrUnableToLoadObject.Msg("unable to unmarshal view rules")
	}
	return &viewDef, nil
}

func (v *viewManager) GetViewResourcePath(ctx context.Context) (TargetResource, apperrors.Error) {
	viewDef, err := v.GetViewDefinition(ctx)
	if err != nil {
		return "", err
	}
	s := "res://views/" + v.view.Label
	path := canonicalizeResourcePath(viewDef.Scope, TargetResource(s))
	return path, nil
}
