package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	// Global flags
	catalog   string
	variant   string
	namespace string
	workspace string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "tansive",
	Short: "Tansive CLI - A command line interface for managing Tansive resources",
	Long: `Tansive CLI is a command line interface for managing Tansive resources.
It allows you to create, read, update, and delete resources using YAML files.
The CLI supports various resource types including catalogs, variants, namespaces, and workspaces.`,
}

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create -f FILENAME",
	Short: "Create a resource from a file",
	Long: `Create a resource from a file. The resource type is determined by the 'kind' field in the YAML file.
Supported resource types include:
  - Catalog
  - Variant
  - Namespace
  - Workspace
  - CollectionSchema
  - ParameterSchema
  - Collection

Example:
  tansive create -f catalog.yaml
  tansive create -f variant.yaml -c my-catalog
  tansive create -f namespace.yaml -c my-catalog -v my-variant`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get the filename from the flag
		filename, err := cmd.Flags().GetString("filename")
		if err != nil {
			return fmt.Errorf("error getting filename: %w", err)
		}
		if filename == "" {
			return fmt.Errorf("filename is required")
		}

		// Load the configuration
		configPath, err := GetDefaultConfigPath()
		if err != nil {
			return fmt.Errorf("failed to get config path: %w", err)
		}
		if err := LoadConfig(configPath); err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		// Load the resource from the file
		jsonData, resource, err := LoadResourceFromFile(filename)
		if err != nil {
			return fmt.Errorf("failed to load resource: %w", err)
		}

		// Get the resource type
		resourceType, err := GetResourceType(resource.Kind)
		if err != nil {
			return fmt.Errorf("failed to get resource type: %w", err)
		}

		// Create the HTTP client
		client := NewHTTPClient(GetConfig())

		// Prepare query parameters
		queryParams := make(map[string]string)
		if catalog != "" {
			queryParams["catalog"] = catalog
		}
		if variant != "" {
			queryParams["variant"] = variant
		}
		if namespace != "" {
			queryParams["namespace"] = namespace
		}
		if workspace != "" {
			queryParams["workspace"] = workspace
		}

		// Create the resource
		_, err = client.CreateResource(resourceType, jsonData, queryParams)
		if err != nil {
			return fmt.Errorf("failed to create resource: %w", err)
		}

		fmt.Printf("Successfully created %s\n", resource.Kind)
		return nil
	},
}

func init() {
	// Add flags to the create command
	createCmd.Flags().StringP("filename", "f", "", "Filename to use to create the resource")
	createCmd.MarkFlagRequired("filename")

	// Add context flags
	createCmd.Flags().StringVarP(&catalog, "catalog", "c", "", "Catalog name")
	createCmd.Flags().StringVarP(&variant, "variant", "v", "", "Variant name")
	createCmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace name")
	createCmd.Flags().StringVarP(&workspace, "workspace", "w", "", "Workspace name")

	// Add the create command to the root command
	rootCmd.AddCommand(createCmd)
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() error {
	return rootCmd.Execute()
}

// addCommands adds all the commands to the root command
func addCommands(cmd *cobra.Command) {
	// Add your commands here
	// Example:
	cmd.AddCommand(newVersionCmd())
}

// Example command implementation
func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version number of tansive-cli",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Println("tansive-cli v0.1.0")
		},
	}
}
