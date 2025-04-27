package integer

import (
	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog/log"
)

func integerBoundsValidator(fl validator.FieldLevel) (ret bool) {
	var iv Validation
	defer func() {
		if r := recover(); r != nil {
			log.Error().Any("int_bounds_validation", iv).Msg("panic occurred during validation")
			ret = false
			return
		}
	}()
	// Retrieve the parent Validation struct
	iv, ok := fl.Parent().Interface().(Validation)
	if !ok {
		return false
	}

	// If minValue is present, maxValue must be greater than minValue
	if iv.MinValue != nil && iv.MaxValue != nil && *iv.MinValue > *iv.MaxValue {
		return false
	}

	// If all conditions pass, return true indicating bounds are valid
	return true
}

// integerStepValidator validates the Step field in Validation
func integerStepValidator(fl validator.FieldLevel) (ret bool) {
	var iv Validation
	defer func() {
		if r := recover(); r != nil {
			log.Error().Any("int_step_validation", iv).Msg("panic occurred during validation")
			ret = false
			return
		}
	}()
	// Retrieve the parent Validation struct
	iv, ok := fl.Parent().Interface().(Validation)
	if !ok {
		return false
	}

	// Step should be included only if minValue is present and Step is positive
	// If stepPresent is true and Step is greater than zero, minValue must be present
	if iv.Step != nil && *iv.Step > 0 && iv.MinValue == nil {
		return false
	}

	// Step should be included only if maxValue is present if Step is negative
	// If stepPresent is true and Step is less than zero, maxValue must be present
	if iv.Step != nil && *iv.Step < 0 && iv.MaxValue == nil {
		return false
	}

	// Step should not be zero if it is present
	if iv.Step != nil && *iv.Step == 0 {
		return false
	}

	// If Step is positive, and both minValue and maxValue are present,
	// ensure that minValue + Step does not exceed maxValue
	if iv.Step != nil && *iv.Step > 0 && iv.MaxValue != nil && iv.MinValue != nil && *iv.MinValue+*iv.Step > *iv.MaxValue {
		return false
	}

	// If Step is negative, and both minValue and maxValue are present,
	// ensure that minValue + Step does not exceed maxValue
	if iv.Step != nil && *iv.Step < 0 && iv.MinValue != nil && iv.MaxValue != nil && *iv.MaxValue+*iv.Step < *iv.MinValue {
		return false
	}

	if iv.Step != nil && *iv.Step > 0 && iv.MinValue != nil && iv.MaxValue != nil && (*iv.MaxValue-*iv.MinValue)%*iv.Step != 0 {
		return false
	}

	if iv.Step != nil && *iv.Step < 0 && iv.MinValue != nil && iv.MaxValue != nil && (*iv.MaxValue-*iv.MinValue)%*iv.Step != 0 {
		return false
	}

	// If all conditions pass, return true indicating Step is valid
	return true
}
