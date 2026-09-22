// Package herdrcompat defines the native JSON API contracts verified by the mesh.
package herdrcompat

// SupportsProtocol is deliberately not a range: unverified native versions must
// not gain observation or mutation access merely by being newer.
func SupportsProtocol(version int64) bool {
	return version == 18 || version == 20 || version == 22
}
