package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/tansive/tansive-internal/internal/common/httpclient"
)

// selectCatalogCmd represents the select-catalog command
var selectCatalogCmd = &cobra.Command{
	Use:   "select-catalog <catalog-name>",
	Short: "Select your default view in the Catalog",
	Long: `Select your default view in the Catalog. This view will be used for all subsequent operations until you switch views.
The command will:
1. Adopt the default view for the specified catalog
2. Store the authentication token for this view
3. Use this token for all subsequent operations until you switch views or the token expires

Example:
  tansive select-catalog my-catalog`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		catalogName := args[0]
		client := httpclient.NewClient(GetConfig())

		// Make the POST request to get the token
		opts := httpclient.RequestOptions{
			Method: http.MethodPost,
			Path:   fmt.Sprintf("auth/default-view-adoptions/%s", catalogName),
		}

		body, _, err := client.DoRequest(opts)
		if err != nil {
			return err
		}

		// Parse the response
		var response struct {
			Token     string    `json:"token"`
			ExpiresAt time.Time `json:"expires_at"`
		}
		if err := json.Unmarshal(body, &response); err != nil {
			return fmt.Errorf("failed to parse response: %v", err)
		}

		// Update the config with the new token
		cfg := GetConfig()
		cfg.CurrentToken = response.Token
		cfg.TokenExpiry = response.ExpiresAt.Format(time.RFC3339)
		cfg.CurrentCatalog = catalogName

		// Save the config
		configFile, err := GetDefaultConfigPath()
		if err != nil {
			return fmt.Errorf("failed to get config path: %v", err)
		}
		if err := cfg.WriteConfig(configFile); err != nil {
			return fmt.Errorf("failed to save config: %v", err)
		}

		if jsonOutput {
			printJSON(map[string]int{"result": 1})
		} else {
			fmt.Printf("Catalog set to %s\n", catalogName)
		}

		return nil
	},
}

// selectCatalogCmd represents the select-catalog command
var adoptViewCmd = &cobra.Command{
	Use:   "adopt-view <catalog-ref>/<view-label>",
	Short: "Adopt a view in the Catalog",
	Long: `Adopt a view in the Catalog. This view will be used for all subsequent operations until you switch views.
The command will:
1. Adopt the view for the specified catalog
2. Store the authentication token for this view
3. Use this token for all subsequent operations until you switch views or the token expires

Example:
tansive adopt-view my-catalog/my-view`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		parts := strings.SplitN(args[0], "/", 2)
		if len(parts) != 2 {
			config := GetConfig()
			if config.CurrentCatalog == "" {
				return fmt.Errorf("invalid view format. Expected <catalog-ref>/<view-label>")
			}
			parts = []string{config.CurrentCatalog, args[0]}
		}
		catalogRef := parts[0]
		viewLabel := parts[1]
		client := httpclient.NewClient(GetConfig())

		// Make the POST request to get the token
		opts := httpclient.RequestOptions{
			Method: http.MethodPost,
			Path:   fmt.Sprintf("auth/view-adoptions/%s/%s", catalogRef, viewLabel),
		}

		body, _, err := client.DoRequest(opts)
		if err != nil {
			return err
		}

		// Parse the response
		var response struct {
			Token     string    `json:"token"`
			ExpiresAt time.Time `json:"expires_at"`
		}
		if err := json.Unmarshal(body, &response); err != nil {
			return fmt.Errorf("failed to parse response: %v", err)
		}

		// Update the config with the new token
		cfg := GetConfig()
		cfg.CurrentToken = response.Token
		cfg.TokenExpiry = response.ExpiresAt.Format(time.RFC3339)
		cfg.CurrentCatalog = catalogRef

		// Save the config
		configFile, err := GetDefaultConfigPath()
		if err != nil {
			return fmt.Errorf("failed to get config path: %v", err)
		}
		if err := cfg.WriteConfig(configFile); err != nil {
			return fmt.Errorf("failed to save config: %v", err)
		}

		if jsonOutput {
			printJSON(map[string]int{"result": 1})
		} else {
			fmt.Printf("View set to %s\n", viewLabel)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(selectCatalogCmd)
	rootCmd.AddCommand(adoptViewCmd)
}
