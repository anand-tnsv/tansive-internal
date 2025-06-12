package cli

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	srvsession "github.com/tansive/tansive-internal/internal/catalogsrv/session"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
)

// sessionCmd represents the session command
var sessionCmd = &cobra.Command{
	Use:   "session",
	Short: "Manage sessions in the Catalog",
	Long:  `Create, get, and manage sessions in the Catalog.`,
}

// createSessionCmd represents the create subcommand
var createSessionCmd = &cobra.Command{
	Use:   "create <skill-path>",
	Short: "Create a new session in the Catalog",
	Long: `Create a new session in the Catalog. This will create a session with the specified skill path and view.
The command will:
1. Create a new session with the specified skill path and view
2. Optionally set session variables and input arguments
3. Return the session ID and other details

Example:
  tansive session create /valid-skillset/test-skill --view valid-view --session-vars '{"key1":"value1"}' --input-args '{"input":"test input"}'`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		skillPath := args[0]
		client := httpclient.NewClient(GetConfig())

		var sessionVars map[string]any
		if sessionVarsStr != "" {
			if err := json.Unmarshal([]byte(sessionVarsStr), &sessionVars); err != nil {
				return fmt.Errorf("invalid session variables JSON: %v", err)
			}
		}

		var inputArgs map[string]any
		if inputArgsStr != "" {
			if err := json.Unmarshal([]byte(inputArgsStr), &inputArgs); err != nil {
				return fmt.Errorf("invalid input arguments JSON: %v", err)
			}
		}

		requestBody := map[string]any{
			"skillPath": skillPath,
			"viewName":  viewName,
		}
		if sessionVars != nil {
			requestBody["sessionVariables"] = sessionVars
		}
		if inputArgs != nil {
			requestBody["inputArgs"] = inputArgs
		}

		bodyBytes, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %v", err)
		}

		// Generate code verifier (UUID) and challenge
		codeVerifier := uuid.New().String()
		hashed := sha256.Sum256([]byte(codeVerifier))
		codeChallenge := base64.RawURLEncoding.EncodeToString(hashed[:])

		opts := httpclient.RequestOptions{
			Method: http.MethodPost,
			Path:   "sessions",
			Body:   bodyBytes,
			QueryParams: map[string]string{
				"interactive":    "true",
				"code_challenge": codeChallenge,
			},
		}

		body, _, err := client.DoRequest(opts)
		if err != nil {
			return err
		}

		var response srvsession.InteractiveSessionRsp
		if err := json.Unmarshal(body, &response); err != nil {
			return fmt.Errorf("failed to parse response: %v", err)
		}

		if jsonOutput {
			printJSON(response)
		} else {
			fmt.Printf("Interactive session created\n")
			if response.Code != "" {
				fmt.Printf("Code: %s\n", response.Code)
			}
			if response.TangentURL != "" {
				fmt.Printf("Tangent URL: %s\n", response.TangentURL)
			}
			fmt.Printf("Code Verifier: %s\n", codeVerifier)
		}

		return nil
	},
}

var (
	sessionVarsStr string
	inputArgsStr   string
	viewName       string
)

func init() {
	rootCmd.AddCommand(sessionCmd)
	sessionCmd.AddCommand(createSessionCmd)

	createSessionCmd.Flags().StringVar(&viewName, "view", "", "Name of the view to use (required)")
	createSessionCmd.MarkFlagRequired("view")
	createSessionCmd.Flags().StringVar(&sessionVarsStr, "session-vars", "", "JSON string of session variables")
	createSessionCmd.Flags().StringVar(&inputArgsStr, "input-args", "", "JSON string of input arguments")
}
