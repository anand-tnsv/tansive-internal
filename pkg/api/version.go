package api

import "github.com/tansive/tansive-internal/internal/common/types"

const ApiVersion_1_0 = "1.0"

type GetVersionReq struct {
	ApiVersion string `json:"api_version,omitempty"`
}

func (r GetVersionReq) RequestMethod() (string, string) {
	return "GET", "/catalogs/version"
}

func (r GetVersionReq) AuthMethod() types.AuthMethod {
	return types.AuthMethodIdToken
}

type GetVersionRsp struct {
	ServerVersion string `json:"server_version"`
	ApiVersion    string `json:"api_version"`
}
