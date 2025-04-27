package apperrors

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestError(t *testing.T) {
	t.Run("TestError", func(t *testing.T) {
		ErrBaseErr := New("base error")
		assert.Equal(t, "base error", ErrBaseErr.Error())
		assert.Equal(t, "msg", ErrBaseErr.New("msg").Error())
		assert.ErrorIs(t, ErrBaseErr, ErrBaseErr)

		ErrFirstLevel := ErrBaseErr.New("first level")
		assert.Equal(t, "first level", ErrFirstLevel.Error())
		assert.ErrorIs(t, ErrFirstLevel, ErrBaseErr)

		ErrAnotherErr := New("another error")
		ErrWrappedErr := ErrFirstLevel.Err(ErrAnotherErr)
		assert.Equal(t, "first level", ErrWrappedErr.Error())
		assert.ErrorIs(t, ErrWrappedErr, ErrBaseErr)
		assert.ErrorIs(t, ErrWrappedErr, ErrAnotherErr)

		err := errors.New("error")
		ErrWrappedErr = ErrFirstLevel.Err(err)
		assert.Equal(t, "first level", ErrWrappedErr.Error())
		assert.ErrorIs(t, ErrWrappedErr, ErrBaseErr)
		assert.ErrorIs(t, ErrWrappedErr, err)

		ErrWrappedErr = ErrFirstLevel.MsgErr("msg", err)
		assert.Equal(t, "msg", ErrWrappedErr.Error())
		assert.ErrorIs(t, ErrWrappedErr, ErrBaseErr)
		assert.ErrorIs(t, ErrWrappedErr, err)
	})
}
