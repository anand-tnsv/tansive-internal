package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
)

var (
	// Get command flags
	getCatalog   string
	getVariant   string
	getNamespace string
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get <resource-path>",
	Short: "Get a resource value by path",
	Long: `Get a resource value by path. The format is <resource-path>.
This command only works with resources and returns their current values.

Example:
  tansive get path/to/resource`,
	Args: cobra.ExactArgs(1),
	RunE: getResourceByPath,
}

func getResourceByPath(cmd *cobra.Command, args []string) error {
	resourcePath := args[0]

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

	response, err := client.GetResource("resources", resourcePath, queryParams, "")
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
}
