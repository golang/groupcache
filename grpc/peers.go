/*
Copyright 2015 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// peers.go defines how processes find and communicate with their peers.

package grpc

import (
	"github.com/golang/groupcache"
)

var (
	portPicker func() groupcache.PeerPicker
)

// RegisterPeerPicker registers the peer initialization function.
// It is called once, when the first group is created.
func RegisterPeerPicker(fn func() groupcache.PeerPicker) {
	if portPicker != nil {
		panic("RegisterPeerPicker called more than once")
	}
	portPicker = fn
}

// getPeers first check portPicker and then portPicker
func getPeers() groupcache.PeerPicker {
	if portPicker == nil {
		return groupcache.NoPeers{}
	}
	pk := portPicker()
	if pk == nil {
		pk = groupcache.NoPeers{}
	}
	return pk
}
