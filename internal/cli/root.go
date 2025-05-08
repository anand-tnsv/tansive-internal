package cli

import (
	"github.com/spf13/cobra"
)

// NewRootCmd creates a new root command for the CLI
func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tansive-cli",
		Short: "Tansive CLI is a command line interface for Tansive",
		Long: `Tansive CLI is a command line interface for interacting with Tansive services.
It provides various commands to manage and interact with your Tansive environment.`,
	}

	addCommands(cmd)
	return cmd
}
