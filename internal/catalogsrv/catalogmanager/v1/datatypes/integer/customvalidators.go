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
			log.Error().
				Any("validation", iv).
				Any("panic_value", r).
				Str("field", fl.FieldName()).
				Str("struct", fl.Parent().Type().Name()).
				Msg("panic occurred during integer step validation")
			ret = false
			return
		}
	}()

	// Retrieve the parent Validation struct
	iv, ok := fl.Parent().Interface().(Validation)
	if !ok {
		log.Error().
			Str("field", fl.FieldName()).
			Str("struct", fl.Parent().Type().Name()).
			Msg("failed to cast parent to Validation struct")
		return false
	}

	// Step should be included only if minValue is present and Step is positive
	// If stepPresent is true and Step is greater than zero, minValue must be present
	if iv.Step != nil && *iv.Step > 0 && iv.MinValue == nil {
		log.Debug().
			Int("step", *iv.Step).
			Msg("positive step requires minValue to be set")
		return false
	}

	// Step should be included only if maxValue is present if Step is negative
	// If stepPresent is true and Step is less than zero, maxValue must be present
	if iv.Step != nil && *iv.Step < 0 && iv.MaxValue == nil {
		log.Debug().
			Int("step", *iv.Step).
			Msg("negative step requires maxValue to be set")
		return false
	}

	// Step should not be zero if it is present
	if iv.Step != nil && *iv.Step == 0 {
		log.Debug().Msg("step value cannot be zero")
		return false
	}

	// If Step is positive, and both minValue and maxValue are present,
	// ensure that minValue + Step does not exceed maxValue
	if iv.Step != nil && *iv.Step > 0 && iv.MaxValue != nil && iv.MinValue != nil {
		if *iv.MinValue+*iv.Step > *iv.MaxValue {
			log.Debug().
				Int("min", *iv.MinValue).
				Int("max", *iv.MaxValue).
				Int("step", *iv.Step).
				Msg("minValue + step exceeds maxValue")
			return false
		}
	}

	// If Step is negative, and both minValue and maxValue are present,
	// ensure that maxValue + Step does not go below minValue
	if iv.Step != nil && *iv.Step < 0 && iv.MinValue != nil && iv.MaxValue != nil {
		if *iv.MaxValue+*iv.Step < *iv.MinValue {
			log.Debug().
				Int("min", *iv.MinValue).
				Int("max", *iv.MaxValue).
				Int("step", *iv.Step).
				Msg("maxValue + step is less than minValue")
			return false
		}
	}

	// Ensure the range is divisible by the step
	if iv.Step != nil && iv.MinValue != nil && iv.MaxValue != nil {
		if (*iv.MaxValue-*iv.MinValue)%*iv.Step != 0 {
			log.Debug().
				Int("min", *iv.MinValue).
				Int("max", *iv.MaxValue).
				Int("step", *iv.Step).
				Msg("range is not divisible by step")
			return false
		}
	}

	return true
}
