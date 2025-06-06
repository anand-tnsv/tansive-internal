package cli

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
)

var (
	// Update command flags
	updateCatalog   string
	updateVariant   string
	updateNamespace string
)

// updateCmd represents the update command
var updateCmd = &cobra.Command{
	Use:   "apply -f FILENAME",
	Short: "Apply a resource from a file (create if not exists, update if exists)",
	Long: `Apply a resource from a file. The resource type is determined by the 'kind' field in the YAML file.
This command follows the Kubernetes-style apply pattern - it will create the resource if it doesn't exist,
or update it if it already exists.

Supported resource types include:
  - Catalogs
  - Variants
  - Namespaces
  - Views
  - Resources
  - Skillsets

Example:
  tansive apply -f catalog.yaml
  tansive apply -f variant.yaml -c my-catalog
  tansive apply -f namespace.yaml -c my-catalog -v my-variant`,
	RunE: updateResource,
}

func updateResource(cmd *cobra.Command, args []string) error {
	filename, err := cmd.Flags().GetString("filename")
	if err != nil {
		return err
	}
	if filename == "" {
		return fmt.Errorf("filename is required")
	}

	jsonData, resource, err := LoadResourceFromFile(filename)
	if err != nil {
		return err
	}

	resourceType, err := GetResourceType(resource.Kind)
	if err != nil {
		return err
	}

	client := httpclient.NewClient(GetConfig())
	queryParams := make(map[string]string)
	if updateCatalog != "" {
		queryParams["catalog"] = updateCatalog
	}
	if updateVariant != "" {
		queryParams["variant"] = updateVariant
	}
	if updateNamespace != "" {
		queryParams["namespace"] = updateNamespace
	}

	// First try to create the resource
	_, location, err := client.CreateResource(resourceType, jsonData, queryParams)
	if err != nil {
		// If we get a conflict, try to update instead
		if httpErr, ok := err.(*httpclient.HTTPError); ok && httpErr.StatusCode == http.StatusConflict {
			objectType := ""
			if resourceType == "resources" {
				objectType = "definition"
			}
			_, err = client.UpdateResource(resourceType, jsonData, queryParams, objectType)
			if err != nil {
				return fmt.Errorf("failed to update resource: %v", err)
			}
			if jsonOutput {
				kv := map[string]any{
					"kind":    resource.Kind,
					"updated": true,
				}
				printJSON(kv)
			} else {
				fmt.Printf("Successfully updated %s\n", resource.Kind)
			}
			return nil
		}
		return fmt.Errorf("failed to create resource: %v", err)
	}

	if jsonOutput {
		kv := map[string]any{
			"kind":     resource.Kind,
			"created":  true,
			"location": location,
		}
		printJSON(kv)
	} else {
		fmt.Printf("Successfully created %s\n", resource.Kind)
		fmt.Printf("Location: %s\n", location)
	}
	return nil
}

func init() {
	// Add flags to the update command
	updateCmd.Flags().StringP("filename", "f", "", "Filename to use to update the resource")
	updateCmd.MarkFlagRequired("filename")

	// Add context flags
	updateCmd.Flags().StringVarP(&updateCatalog, "catalog", "c", "", "Catalog name")
	updateCmd.Flags().StringVarP(&updateVariant, "variant", "v", "", "Variant name")
	updateCmd.Flags().StringVarP(&updateNamespace, "namespace", "n", "", "Namespace name")

	// Add the update command to the root command
	rootCmd.AddCommand(updateCmd)
}
