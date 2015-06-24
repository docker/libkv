package store

import (
	"net/url"
	"strings"
)

// CreateEndpoints creates a list of endpoints given the right scheme
func CreateEndpoints(addrs []string, scheme string) (entries []string) {
	for _, addr := range addrs {
		// Note that in the event of a bad URL that's still parseable (such as
		// localhost:1234) the scheme will equal the hostname. We check against the
		// supplied scheme to ensure we at least tried to handle this
		// appropriately.
		u, err := url.Parse(addr)
		if err != nil || (u != nil && u.Scheme != scheme) {
			entries = append(entries, scheme+"://"+addr)
		} else {
			entries = append(entries, addr)
		}
	}
	return entries
}

// Normalize the key for each store to the form:
//
//     /path/to/key
//
func Normalize(key string) string {
	return "/" + join(SplitKey(key))
}

// GetDirectory gets the full directory part of
// the key to the form:
//
//     /path/to/
//
func GetDirectory(key string) string {
	parts := SplitKey(key)
	parts = parts[:len(parts)-1]
	return "/" + join(parts)
}

// SplitKey splits the key to extract path informations
func SplitKey(key string) (path []string) {
	if strings.Contains(key, "/") {
		path = strings.Split(key, "/")
	} else {
		path = []string{key}
	}
	return path
}

// join the path parts with '/'
func join(parts []string) string {
	return strings.Join(parts, "/")
}
