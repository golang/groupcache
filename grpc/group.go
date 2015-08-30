package grpc

import (
	"github.com/golang/groupcache"
)

// NewGroup creates a coordinated group-aware Getter from a grpc Getter.
func NewGroup(name string, cacheBytes int64, getter groupcache.Getter) *groupcache.Group {
	return groupcache.NewGroupWithPicker(name, cacheBytes, getter, getPeers())
}
