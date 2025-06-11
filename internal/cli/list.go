package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var (
	// List command flags
	listCatalog   string
	listVariant   string
	listNamespace string
)

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:   "list <resourceType>",
	Short: "List resources of a specific type",
	Long: `List resources of a specific type. Supported resource types include:
  - catalogs
  - variants
  - namespaces
  - views
  - resources
  - skillsets

Examples:
  tansive list catalogs
  tansive list variants -c catalog
  tansive list namespaces -c catalog -v variant
  tansive list views -c catalog -v variant
  tansive list resources -c catalog -v variant
  tansive list skillsets -c catalog -v variant`,
	Args: cobra.ExactArgs(1),
	RunE: listResources,
}

func listResources(cmd *cobra.Command, args []string) error {
	resourceType := args[0]

	// Map the resource type to its URL format
	urlResourceType, err := MapResourceTypeToURL(resourceType)
	if err != nil {
		return err
	}

	client := httpclient.NewClient(GetConfig())

	queryParams := make(map[string]string)
	if listCatalog != "" {
		queryParams["catalog"] = listCatalog
	}
	if listVariant != "" {
		queryParams["variant"] = listVariant
	}
	if listNamespace != "" {
		queryParams["namespace"] = listNamespace
	}

	response, err := client.ListResources(urlResourceType, queryParams)
	if err != nil {
		return err
	}

	if jsonOutput {
		// For JSON output, keep the existing format
		var responseData map[string]any
		if err := json.Unmarshal(response, &responseData); err != nil {
			return fmt.Errorf("failed to parse response")
		}

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
		// For non-JSON output, print in a more readable format
		var responseData map[string]any
		if err := json.Unmarshal(response, &responseData); err != nil {
			return fmt.Errorf("failed to parse response: %v", err)
		}

		// Print the resource type in plural form
		fmt.Printf("%s:\n", cases.Title(language.English).String(urlResourceType))

		if views, ok := responseData["views"].([]interface{}); ok {
			// Print each item with proper indentation
			for _, item := range views {
				if viewMap, ok := item.(map[string]interface{}); ok {
					if name, ok := viewMap["name"].(string); ok {
						fmt.Printf("- %s\n", name)
					}
				}
			}
		}
	}
	return nil
}

func init() {
	rootCmd.AddCommand(listCmd)

	// Add flags
	listCmd.Flags().StringVarP(&listCatalog, "catalog", "c", "", "Catalog name")
	listCmd.Flags().StringVarP(&listVariant, "variant", "v", "", "Variant name")
	listCmd.Flags().StringVarP(&listNamespace, "namespace", "n", "", "Namespace name")
}
