package catalogmanager

import (
	"context"
	"net/url"
	"path"
	"strings"

	"github.com/tansive/tansive-internal/internal/catalogsrv/catcommon"
	"github.com/tansive/tansive-internal/internal/common/apperrors"
	"github.com/tansive/tansive-internal/pkg/types"
	"github.com/tidwall/gjson"
)

// PolicyRequest represents a request to validate against a view policy
type PolicyRequest struct {
	ViewDefinition *types.ViewDefinition
	Metadata       string
	ResourceName   string
	Action         types.Action
	Target         types.TargetResource
	Params         url.Values
	ResourceJSON   []byte // Moved here since it's a parameter of the original function
}

// ValidateViewPolicy checks if a given action on a target resource is allowed according to the view definition.
// It validates the action against the view's rules, considering the resource's metadata, name, and any additional parameters.
//
// Parameters:
//   - ctx: The context for the request
//   - req: The policy request containing the view definition, metadata, resource name, action, target, and parameters
//
// Returns:
//   - apperrors.Error: Returns nil if the action is allowed, or an error if the action is not permitted
func ValidateViewPolicy(ctx context.Context, req PolicyRequest) apperrors.Error {
	vd := CanonicalizeViewDefinition(req.ViewDefinition)
	if vd == nil || vd.Rules == nil {
		return ErrInvalidView.Msg("invalid view definition")
	}

	// Handle collection-specific validation
	if req.ResourceName == catcommon.ResourceNameCollections {
		return validateCollectionPolicy(ctx, req)
	}

	// Handle attribute-specific validation
	if req.ResourceName == catcommon.ResourceNameAttributes {
		return validateAttributePolicy(ctx, req)
	}

	// Handle standard resource validation
	if !vd.Rules.IsActionAllowed(req.Action, req.Target) {
		return ErrDisallowedByPolicy
	}

	return nil
}

// validateCollectionPolicy handles collection-specific policy validation
// by validating the action against the collection schema in case of instantiate action
func validateCollectionPolicy(ctx context.Context, req PolicyRequest) apperrors.Error {
	_ = ctx //future use for logging
	if req.Action == types.ActionResourceCreate {
		if len(req.ResourceJSON) == 0 {
			return ErrDisallowedByPolicy.Msg("empty collection")
		}
		schema := gjson.GetBytes(req.ResourceJSON, "spec.schema")
		if !schema.Exists() || schema.String() == "" {
			return ErrDisallowedByPolicy.Msg("invalid collection")
		}
		schemaPath := req.Metadata + "/collectionschemas/" + schema.String()
		if !req.ViewDefinition.Rules.IsActionAllowed(req.Action, types.TargetResource(schemaPath)) {
			return ErrDisallowedByPolicy
		}
	} else {
		if !req.ViewDefinition.Rules.IsActionAllowed(req.Action, req.Target) {
			return ErrDisallowedByPolicy
		}
	}
	return nil
}

// validateAttributePolicy handles attribute-specific policy validation
// by validating the action against the collection access policy
func validateAttributePolicy(ctx context.Context, req PolicyRequest) apperrors.Error {
	_ = ctx //future use for logging
	var collectionPath string
	if collection := req.Params.Get("collection"); collection == "true" {
		collectionPath = string(req.Target)
	} else {
		targetPath := strings.TrimPrefix(string(req.Target), "res://")
		targetPath = strings.TrimPrefix(targetPath, "/")
		collectionPath = "res://" + path.Dir(targetPath)
	}
	if !req.ViewDefinition.Rules.IsActionAllowed(req.Action, types.TargetResource(collectionPath)) {
		return ErrDisallowedByPolicy
	}
	return nil
}
