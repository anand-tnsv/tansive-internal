package cli

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
)

var (
	// Get command flags
	getCatalog   string
	getVariant   string
	getNamespace string
	getWorkspace string
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get <resourceType>/<resourceName>",
	Short: "Get a resource by type and name",
	Long: `Get a resource by type and name. The format is <resourceType>/<resourceName>.
Supported resource types include:
  - catalogs/<catalog-name>
  - collectionschemas/<schema-name>
  - collections/<path/to/collection>
  - attributes/<path/to/attribute>
  - attributesets/<path/to/collection> (or attrsets/<path/to/collection>)

Example:
  tansive get catalogs/my-catalog
  tansive get collectionschemas/my-schema
  tansive get collections/path/to/collection
  tansive get attributesets/path/to/collection`,
	Args: cobra.ExactArgs(1),
	RunE: getResource,
}

func getResource(cmd *cobra.Command, args []string) error {
	// Split the argument into resource type and name
	parts := strings.SplitN(args[0], "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid resource format. Expected <resourceType>/<resourceName>")
	}

	resourceType := parts[0]
	resourceName := parts[1]

	// Map the resource type to its URL format
	urlResourceType, err := MapResourceTypeToURL(resourceType)
	if err != nil {
		return err
	}

	client := NewHTTPClient(GetConfig())

	queryParams := make(map[string]string)
	if getCatalog != "" {
		queryParams["catalog"] = getCatalog
	}
	if getVariant != "" {
		queryParams["variant"] = getVariant
	}
	if getNamespace != "" {
		queryParams["namespace"] = getNamespace
	}
	if getWorkspace != "" {
		queryParams["workspace"] = getWorkspace
	}

	// Add collection=true for attributeset resource type
	if resourceType == "attributeset" || resourceType == "attrset" {
		queryParams["collection"] = "true"
	}

	response, err := client.GetResource(urlResourceType, resourceName, queryParams)
	if err != nil {
		return err
	}

	var responseData map[string]any
	if err := json.Unmarshal(response, &responseData); err != nil {
		return fmt.Errorf("failed to parse response: %v", err)
	}

	if jsonOutput {
		// Format as JSON with result and value
		output := map[string]any{
			"result": 1,
			"value":  responseData,
		}

		jsonBytes, err := json.MarshalIndent(output, "", "    ")
		if err != nil {
			return fmt.Errorf("failed to format JSON output: %v", err)
		}
		fmt.Println(string(jsonBytes))
	} else {
		// Convert to YAML
		yamlBytes, err := yaml.Marshal(responseData)
		if err != nil {
			return fmt.Errorf("failed to convert to YAML: %v", err)
		}
		fmt.Println(string(yamlBytes))
	}
	return nil
}

func init() {
	rootCmd.AddCommand(getCmd)

	// Add flags
	getCmd.Flags().StringVarP(&getCatalog, "catalog", "c", "", "Catalog name")
	getCmd.Flags().StringVarP(&getVariant, "variant", "v", "", "Variant name")
	getCmd.Flags().StringVarP(&getNamespace, "namespace", "n", "", "Namespace name")
	getCmd.Flags().StringVarP(&getWorkspace, "workspace", "w", "", "Workspace name")
}
