package tangent

import (
	"context"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/internal/common/uuid"
)

type TangentInfo struct {
	CreatedBy    string               `json:"createdBy"`
	URL          string               `json:"url"`
	Capabilities []catcommon.RunnerID `json:"capabilities"`
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
			URL:          "http://localhost:8195",
			Capabilities: capabilities,
		},
	}, nil
}
