package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var (
	deleteCatalog   string
	deleteVariant   string
	deleteNamespace string
	deleteWorkspace string
)

var deleteCmd = &cobra.Command{
	Use:   "delete <resourceType>/<resourceName>",
	Short: "Delete a resource by type and name",
	Long: `Delete a resource by type and name. The format is <resourceType>/<resourceName>.
Supported resource types include:
  - catalog/<catalog-name>
  - collectionschema/<schema-name>
  - collection/path/to/collection

Example:
  tansive delete catalog/my-catalog
  tansive delete collectionschema/my-schema
  tansive delete collection/path/to/collection`,
	Args: cobra.ExactArgs(1),
	RunE: deleteResource,
}

func deleteResource(cmd *cobra.Command, args []string) error {
	parts := strings.SplitN(args[0], "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid resource format. Expected <resourceType>/<resourceName>")
	}

	resourceType := parts[0]
	resourceName := parts[1]

	// Check if resource type is not allowed for delete
	if resourceType == "attribute" || resourceType == "attr" ||
		resourceType == "attributeset" || resourceType == "attrset" {
		return fmt.Errorf("delete operation is not supported for %s resource type", resourceType)
	}

	urlResourceType, err := MapResourceTypeToURL(resourceType)
	if err != nil {
		return err
	}

	client := NewHTTPClient(GetConfig())

	queryParams := make(map[string]string)
	if deleteCatalog != "" {
		queryParams["catalog"] = deleteCatalog
	}
	if deleteVariant != "" {
		queryParams["variant"] = deleteVariant
	}
	if deleteNamespace != "" {
		queryParams["namespace"] = deleteNamespace
	}
	if deleteWorkspace != "" {
		queryParams["workspace"] = deleteWorkspace
	}

	err = client.DeleteResource(urlResourceType, resourceName, queryParams)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully deleted %s/%s\n", resourceType, resourceName)
	return nil
}

func init() {
	rootCmd.AddCommand(deleteCmd)

	deleteCmd.Flags().StringVarP(&deleteCatalog, "catalog", "c", "", "Catalog name")
	deleteCmd.Flags().StringVarP(&deleteVariant, "variant", "v", "", "Variant name")
	deleteCmd.Flags().StringVarP(&deleteNamespace, "namespace", "n", "", "Namespace name")
	deleteCmd.Flags().StringVarP(&deleteWorkspace, "workspace", "w", "", "Workspace name")
}
