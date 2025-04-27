package schemamanager

import "encoding/json"

type OptionsConfig struct {
	Validate             bool
	ValidateDependencies bool
	SetDefaultValues     bool
	SchemaLoaders        SchemaLoaders
	ParamValues          json.RawMessage
}

type Options func(*OptionsConfig)

func WithValidation(validate ...bool) Options {
	return func(cfg *OptionsConfig) {
		if len(validate) > 0 {
			cfg.Validate = validate[0]
		} else {
			cfg.Validate = true
		}
	}
}

func WithValidateDependencies(validate ...bool) Options {
	return func(cfg *OptionsConfig) {
		if len(validate) > 0 {
			cfg.ValidateDependencies = validate[0]
		} else {
			cfg.ValidateDependencies = true
		}
	}
}

func WithSchemaLoaders(loaders SchemaLoaders) Options {
	return func(cfg *OptionsConfig) {
		cfg.SchemaLoaders = loaders
	}
}

func WithDefaultValues(set ...bool) Options {
	return func(cfg *OptionsConfig) {
		if len(set) > 0 {
			cfg.SetDefaultValues = set[0]
		} else {
			cfg.SetDefaultValues = true
		}
	}
}

func WithParamValues(values json.RawMessage) Options {
	return func(cfg *OptionsConfig) {
		cfg.ParamValues = values
	}
}
