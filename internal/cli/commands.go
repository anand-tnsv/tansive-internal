package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	// Global flags
	jsonOutput bool
	configFile string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "tansive",
	Short: "Tansive CLI - A command line interface for managing Tansive resources",
	Long: `Tansive CLI is a command line interface for managing Tansive resources.
It allows you to create, read, update, and delete resources using YAML files.
The CLI supports various resource types including catalogs, variants, namespaces, and workspaces.`,
	PersistentPreRun: preRunHandlePersistents,
}

func init() {
	// Set up persistent flags
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "", "", "Path to configuration file to override default")
	rootCmd.PersistentFlags().BoolVarP(&jsonOutput, "json", "j", false, "Output in JSON format")

	// Add commands
	rootCmd.AddCommand(newVersionCmd())
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	rootCmd.SilenceErrors = true // Prevent Cobra from printing the error
	rootCmd.SilenceUsage = true  // Prevent Cobra from printing usage on error

	err := rootCmd.Execute()
	if err != nil {
		if jsonOutput {
			kv := map[string]string{
				"error": err.Error(),
			}
			printJSON(kv)
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		os.Exit(1)
	}
}

func preRunHandlePersistents(cmd *cobra.Command, args []string) {
	// if a config file is provided, load config from config file
	if configFile == "" {
		var err error
		configFile, err = GetDefaultConfigPath()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}

	isConfig := false
	c := cmd
	for c != nil {
		if c.Name() == "config" {
			isConfig = true
			break
		}
		c = c.Parent()
	}

	if !isConfig {
		if err := LoadConfig(configFile); err != nil {
			if os.IsNotExist(err) {
				fmt.Println("Tansive config file not found. Configure tansive with \"tansive config create\" first.")
				os.Exit(1)
			} else {
				fmt.Printf("Unable to load config file: %s\n", err.Error())
				os.Exit(1)
			}
		}
	}
}

// Example command implementation
func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version number of tansive-cli",
		Run: func(cmd *cobra.Command, args []string) {
			if jsonOutput {
				kv := map[string]string{
					"version": "v0.2.0",
				}
				printJSON(kv)
			} else {
				cmd.Println("tansive-cli v0.2.0")
			}
		},
	}
}

// printJSON prints the given map as JSON to stdout
func printJSON(data interface{}) {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(jsonData))
}
