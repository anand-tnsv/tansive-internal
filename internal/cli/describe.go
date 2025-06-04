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
	describeCatalog   string
	describeVariant   string
	describeNamespace string
)

// describeCmd represents the describe command
var describeCmd = &cobra.Command{
	Use:   "describe <resourceType>/<resourceName>",
	Short: "Describe a resource by type and name",
	Long: `Describe a resource by type and name. The format is <resourceType>/<resourceName>.
Supported resource types include:
  - catalogs/<catalog-name>
  - views/<view-name>
  - resources/<path/to/resource>

Example:
  tansive describe catalogs/my-catalog
  tansive describe views/my-view
  tansive describe resources/path/to/resource`,
	Args: cobra.ExactArgs(1),
	RunE: describeResource,
}

func describeResource(cmd *cobra.Command, args []string) error {
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
	if describeCatalog != "" {
		queryParams["catalog"] = describeCatalog
	}
	if describeVariant != "" {
		queryParams["variant"] = describeVariant
	}
	if describeNamespace != "" {
		queryParams["namespace"] = describeNamespace
	}

	objectType := ""
	if urlResourceType == "resources" {
		objectType = "definition"
	}
	response, err := client.GetResource(urlResourceType, resourceName, queryParams, objectType)
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
	rootCmd.AddCommand(describeCmd)

	// Add flags
	describeCmd.Flags().StringVarP(&describeCatalog, "catalog", "c", "", "Catalog name")
	describeCmd.Flags().StringVarP(&describeVariant, "variant", "v", "", "Variant name")
	describeCmd.Flags().StringVarP(&describeNamespace, "namespace", "n", "", "Namespace name")
}
