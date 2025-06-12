package tangentcommon

type SessionCreateRequest struct {
	Interactive  bool   `json:"interactive"`
	CodeVerifier string `json:"code_verifier"`
	Code         string `json:"code"`
}
