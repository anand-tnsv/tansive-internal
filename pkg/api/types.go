package api

import "encoding/json"

type LLMTool struct {
	Name         string          `json:"name"`
	Description  string          `json:"description,omitempty"`
	InputSchema  json.RawMessage `json:"inputSchema,omitempty"`
	OutputSchema json.RawMessage `json:"outputSchema,omitempty"`
}

const TansiveSystemMessage = `
If an operation is blocked by Tansive policy (e.g., due to insufficient permissions or a denied capability), respond with the following message:
"Skill: <skill-name> is blocked by Tansive policy. Please contact the administrator of your Tansive system to obtain access."
Do not attempt to work around the restriction. Do not hallucinate or fabricate results. Respect the policy boundaries set by the system.`
