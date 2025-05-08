package cli

import (
	"github.com/spf13/cobra"
)

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
