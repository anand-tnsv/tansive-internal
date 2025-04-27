package schemavalidator

import (
	"fmt"

	schemaerr "github.com/tansive/tansive-internal/internal/catalogsrv/catalogmanager/schema/errors"
	"github.com/xeipuuv/gojsonschema"
)

func ValidateJsonSchema(jsonSchema string, jsonDocument string) schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	schemaLoader := gojsonschema.NewStringLoader(jsonSchema)
	documentLoader := gojsonschema.NewStringLoader(jsonDocument)

	// Validate the JSON document against the schema
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	// Custom error handling for validation results
	if !result.Valid() {
		for _, desc := range result.Errors() {
			// Customizing error messages based on the type of error
			switch desc.Type() {
			case "required":
				fmt.Printf("- Missing required field: %s\n", desc.Field())
				ves = append(ves, schemaerr.ValidationError{
					Field:  desc.Field(),
					ErrStr: "missing required attribute",
				})
			case "enum":
				fmt.Printf("- Invalid value for field '%s': %v. Expected one of: %v\n", desc.Field(), desc.Value(), desc.Details()["allowed"])
				ves = append(ves, schemaerr.ValidationError{
					Field:  desc.Field(),
					ErrStr: "invalid value",
				})
			case "type":
				fmt.Printf("- Field '%s' has an invalid type. Expected type: %v\n", desc.Field(), desc.Details()["expected"])
				ves = append(ves, schemaerr.ValidationError{
					Field:  desc.Field(),
					ErrStr: "invalid type",
				})
			default:
				fmt.Printf("- %s\n", desc)
				ves = append(ves, schemaerr.ValidationError{
					Field:  desc.Field(),
					ErrStr: "validation failed for attribute",
				})
			}
		}
	}
	return ves
}
