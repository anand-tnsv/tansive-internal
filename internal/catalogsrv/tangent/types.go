package tangent

import (
	"context"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

type TangentInfo struct {
	ID                     uuid.UUID            `json:"id"`
	CreatedBy              string               `json:"createdBy"`
	URL                    string               `json:"url"`
	Capabilities           []catcommon.RunnerID `json:"capabilities"`
	PublicKeyAccessKey     []byte               `json:"publicKeyAccessKey"`
	PublicKeyLogSigningKey []byte               `json:"publicKeyLogSigningKey"`
}

type Tangent struct {
	ID uuid.UUID `json:"id"`
	TangentInfo
}

func GetTangentWithCapabilities(ctx context.Context, capabilities []catcommon.RunnerID) (*Tangent, apperrors.Error) {
	// Fake it for now
	return &Tangent{
		ID: uuid.New(),
		TangentInfo: TangentInfo{
			CreatedBy:    "system",
			URL:          "http://local.tansive.dev:8468",
			Capabilities: capabilities,
		},
	}, nil
}
