package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"

	"github.com/spf13/cobra"
	srvsession "github.com/tansive/tansive-internal/internal/catalogsrv/session"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
	"github.com/tansive/tansive-internal/internal/tangent/session/hashlog"
)

// auditLogCmd represents the audit command
var auditLogCmd = &cobra.Command{
	Use:   "audit-log",
	Short: "Audit related commands",
	Long:  `Commands for auditing and verifying logs.`,
}

// verifyLogCmd represents the verify-log subcommand
var verifyLogCmd = &cobra.Command{
	Use:   "verify <log-file>",
	Short: "Verify the integrity of a log file",
	Long: `Verify the integrity of a log file by checking its hash chain and HMAC.
The command will:
1. Read the specified log file
2. Verify the hash chain and HMAC for each entry
3. Report any verification failures

Example:
  tansive session audit-log verify /path/to/logfile.log`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		logFile := args[0]

		// Open the log file
		file, err := os.Open(logFile)
		if err != nil {
			return fmt.Errorf("failed to open log file: %v", err)
		}
		defer file.Close()

		// TODO: Get HMAC key from configuration or environment
		hmacKey := []byte("tansive-dev-hmac-key") // This should be properly configured

		// Verify the log
		if err := hashlog.VerifyHashedLog(file, hmacKey); err != nil {
			if jsonOutput {
				output := map[string]any{
					"result": 0,
					"error":  err.Error(),
				}
				jsonBytes, err := json.MarshalIndent(output, "", "    ")
				if err != nil {
					return fmt.Errorf("failed to format JSON output: %v", err)
				}
				fmt.Println(string(jsonBytes))
				return nil
			}
			return fmt.Errorf("log verification failed: %v", err)
		}

		if jsonOutput {
			output := map[string]any{
				"result": 1,
				"value": map[string]any{
					"status": "success",
					"file":   logFile,
				},
			}
			jsonBytes, err := json.MarshalIndent(output, "", "    ")
			if err != nil {
				return fmt.Errorf("failed to format JSON output: %v", err)
			}
			fmt.Println(string(jsonBytes))
		} else {
			fmt.Println("Log verification successful")
		}
		return nil
	},
}

// renderHtmlCmd represents the render-html subcommand
var renderHtmlCmd = &cobra.Command{
	Use:   "view <log-file>",
	Short: "Generate an HTML visualization of the log file",
	Long: `Generate an HTML visualization of the log file. The command will:
1. Read the specified log file
2. Generate an HTML file with the same name but .html extension
3. Optionally open the generated HTML file in the default browser

Example:
  tansive session audit-log view /path/to/logfile.tlog
  tansive session audit-log view /path/to/logfile.log --no-open`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		logFile := args[0]

		// Generate HTML
		if err := hashlog.RenderHashedLogToHTML(logFile); err != nil {
			if jsonOutput {
				output := map[string]any{
					"result": 0,
					"error":  err.Error(),
				}
				jsonBytes, err := json.MarshalIndent(output, "", "    ")
				if err != nil {
					return fmt.Errorf("failed to format JSON output: %v", err)
				}
				fmt.Println(string(jsonBytes))
				return nil
			}
			return fmt.Errorf("failed to generate HTML: %v", err)
		}

		htmlFile := logFile[:len(logFile)-len(".tlog")] + ".html"
		if jsonOutput {
			output := map[string]any{
				"result": 1,
				"value": map[string]any{
					"status": "success",
					"file":   htmlFile,
				},
			}
			jsonBytes, err := json.MarshalIndent(output, "", "    ")
			if err != nil {
				return fmt.Errorf("failed to format JSON output: %v", err)
			}
			fmt.Println(string(jsonBytes))
		} else {
			fmt.Printf("HTML file generated: %s\n", htmlFile)
		}

		// Open in browser if -v flag is set
		if !noOpen {
			var err error
			switch runtime.GOOS {
			case "darwin":
				err = exec.Command("open", htmlFile).Start()
			case "windows":
				err = exec.Command("cmd", "/c", "start", htmlFile).Start()
			default: // "linux", "freebsd", etc.
				err = exec.Command("xdg-open", htmlFile).Start()
			}
			if err != nil {
				return fmt.Errorf("failed to open browser: %v", err)
			}
		}

		return nil
	},
}

// getAuditLogCmd represents the get subcommand
var getAuditLogCmd = &cobra.Command{
	Use:   "get <session-id>",
	Short: "Get the audit log for a session",
	Long: `Get the audit log for a specific session in the Catalog. This will show the history of events
and changes that occurred during the session's lifecycle.

Example:
  tansive session audit-log get 123e4567-e89b-12d3-a456-426614174000`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		sessionID := args[0]
		client := httpclient.NewClient(GetConfig())

		response, err := client.GetResource("sessions", sessionID+"/auditlog", nil, "")
		if err != nil {
			return err
		}

		auditLogFile := sessionID + ".tlog"
		if outputFile != "" {
			auditLogFile = outputFile
		}
		if err := srvsession.DecodeAndUncompressAuditLogFile(string(response), auditLogFile); err != nil {
			return err
		}
		return nil
	},
}

var (
	noOpen     bool
	outputFile string
)

func init() {
	sessionCmd.AddCommand(auditLogCmd)
	auditLogCmd.AddCommand(verifyLogCmd)
	auditLogCmd.AddCommand(renderHtmlCmd)
	auditLogCmd.AddCommand(getAuditLogCmd)

	// Add view flag to render-html command
	renderHtmlCmd.Flags().BoolVarP(&noOpen, "no-open", "n", false, "Do not open the generated HTML file in the default browser")
	// Add output file flag to get command
	getAuditLogCmd.Flags().StringVarP(&outputFile, "output", "o", "", "Specify the output file path (default: <session-id>.tlog)")
}
