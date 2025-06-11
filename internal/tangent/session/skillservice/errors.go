package skillservice

import (
	"net/http"

	"github.com/tansive/tansive-internal/internal/common/apperrors"
)

var (
	ErrSkillServiceError apperrors.Error = apperrors.New("skill service error").SetStatusCode(http.StatusInternalServerError)
	ErrInvalidRequest    apperrors.Error = ErrSkillServiceError.New("invalid request").SetStatusCode(http.StatusBadRequest)
)
