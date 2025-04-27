package types

type AuthMethod int

const AuthMethodAPIToken AuthMethod = 1
const AuthMethodAccessToken AuthMethod = 2
const AuthMethodIdToken AuthMethod = 3
const AuthMethodInternal AuthMethod = 4
