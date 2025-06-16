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
	Use:   "list RESOURCE_TYPE [flags]",
	Short: "List resources of a specific type",
	Long: `List resources of a specific type. Supported resource types include:
  - catalogs
  - variants
  - namespaces
  - views
  - resources
  - skillsets
  - sessions

Examples:
  # List all catalogs
  tansive list catalogs

  # List variants in a catalog
  tansive list variants -c my-catalog

  # List namespaces in a catalog and variant
  tansive list namespaces -c my-catalog -v my-variant

  # List views in a specific context
  tansive list views -c my-catalog -v my-variant

  # List resources in a specific context
  tansive list resources -c my-catalog -v my-variant -n my-namespace

  # List skillsets in a specific context
  tansive list skillsets -c my-catalog -v my-variant

  # List resources in JSON format
  tansive list resources -j`,
	Args: cobra.ExactArgs(1),
	RunE: listResources,
}

// listResources handles listing resources of a specific type
// It retrieves the resources and formats the output based on the resource type
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

	if urlResourceType == "views" {
		return printViews(response)
	}

	return nil
}

// init initializes the list command with its flags and adds it to the root command
func init() {
	rootCmd.AddCommand(listCmd)

	// Add flags
	listCmd.Flags().StringVarP(&listCatalog, "catalog", "c", "", "Catalog name")
	listCmd.Flags().StringVarP(&listVariant, "variant", "v", "", "Variant name")
	listCmd.Flags().StringVarP(&listNamespace, "namespace", "n", "", "Namespace name")
}

// printViews formats and prints the views in either JSON or human-readable format
func printViews(response []byte) error {
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
		fmt.Printf("%s:\n", cases.Title(language.English).String("views"))

		if views, ok := responseData["views"].([]any); ok {
			// Print each item with proper indentation
			for _, item := range views {
				if viewMap, ok := item.(map[string]any); ok {
					if name, ok := viewMap["name"].(string); ok {
						fmt.Printf("- %s\n", name)
					}
				}
			}
		}
	}
	return nil
}
