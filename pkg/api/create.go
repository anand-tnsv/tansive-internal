package api

type CreateReq struct {
	ApiVersion string `json:"api_version,omitempty"`
	Request    string `json:"request"`
}
