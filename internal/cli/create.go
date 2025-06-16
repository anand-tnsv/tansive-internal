package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
)

var (
	// Create command flags
	createCatalog   string
	createVariant   string
	createNamespace string
)

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create -f FILENAME [flags]",
	Short: "Create a resource from a file",
	Long: `Create a resource from a file. The resource type is determined by the 'kind' field in the YAML file.
Supported resource types include:
  - Catalogs
  - Variants
  - Namespaces
  - Views
  - Resources
  - Skillsets

Examples:
  # Create a new catalog
  tansive create -f catalog.yaml

  # Create a variant in a specific catalog
  tansive create -f variant.yaml -c my-catalog

  # Create a namespace in a catalog and variant
  tansive create -f namespace.yaml -c my-catalog -v my-variant

  # Create a resource in a specific context
  tansive create -f resource.yaml -c my-catalog -v my-variant -n my-namespace`,
	RunE: createResource,
}

func createResource(cmd *cobra.Command, args []string) error {
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
	if createCatalog != "" {
		queryParams["catalog"] = createCatalog
	}
	if createVariant != "" {
		queryParams["variant"] = createVariant
	}
	if createNamespace != "" {
		queryParams["namespace"] = createNamespace
	}

	_, location, err := client.CreateResource(resourceType, jsonData, queryParams)
	if err != nil {
		return err
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
	// Add flags to the create command
	createCmd.Flags().StringP("filename", "f", "", "Filename to use to create the resource")
	createCmd.MarkFlagRequired("filename")

	// Add context flags
	createCmd.Flags().StringVarP(&createCatalog, "catalog", "c", "", "Catalog name")
	createCmd.Flags().StringVarP(&createVariant, "variant", "v", "", "Variant name")
	createCmd.Flags().StringVarP(&createNamespace, "namespace", "n", "", "Namespace name")

	// Add the create command to the root command
	rootCmd.AddCommand(createCmd)
}
