package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
)

var (
	deleteCatalog   string
	deleteVariant   string
	deleteNamespace string
)

var deleteCmd = &cobra.Command{
	Use:   "delete <resourceType>/<resourceName>",
	Short: "Delete a resource by type and name",
	Long: `Delete a resource by type and name. The format is <resourceType>/<resourceName>.
Supported resource types include:
  - catalog/<catalog-name>
  - views/<view-name>
  - resources/<path/to/resource>

Example:
  tansive delete catalog/my-catalog
  tansive delete views/my-view
  tansive delete resources/path/to/resource`,
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

	urlResourceType, err := MapResourceTypeToURL(resourceType)
	if err != nil {
		return err
	}

	client := httpclient.NewClient(GetConfig())

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

	objectType := ""
	if urlResourceType == "resources" {
		objectType = "definition"
	}

	err = client.DeleteResource(urlResourceType, resourceName, queryParams, objectType)
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
}
