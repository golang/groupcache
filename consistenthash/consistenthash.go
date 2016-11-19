/*
Copyright 2013 Google Inc.

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

// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type Map struct {
	hash     Hash
	replicas int
	keys     []int // Sorted
	hashMap  map[int]string
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

// Adds some keys to the hash.
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// Gets the <amount> closest items in the hash to the provided key,
// if they're permitted by the accept function. This can be used
// to implement placement strategies. Like storing items in different
// availability zones.
//
// Two utility functions are provided that can be used as accept-callback:
// - AcceptAny: Allow any and all items to be returned by GetN()
// - AcceptUnique: Ensure only unique entries are returned.
func (m *Map) GetN(key string, amount int, accept func([]string, string) bool) []string {
	out := []string{}
	if m.IsEmpty() || amount < 1 {
		return out
	}

	if accept == nil {
		accept = AcceptAny
	}

	hash := int(m.hash([]byte(key)))
	hashKey := m.getKeyFromHash(hash)
	out = append(out, m.hashMap[hashKey])

	ringLength := len(m.hashMap)
	for i := 1; len(out) < amount && i < ringLength; i++ {
		hashKey = m.getKeyFromHash(hashKey + 1)
		res := m.hashMap[hashKey]
		if accept(out, res) {
			out = append(out, res)
		}
	}

	return out
}

// Gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hash([]byte(key)))
	return m.hashMap[m.getKeyFromHash(hash)]
}

func (m *Map) getKeyFromHash(hash int) int {
	// Binary search for appropriate replica.
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	// Means we have cycled back to the first replica.
	if idx == len(m.keys) {
		idx = 0
	}

	return m.keys[idx]
}

// Used as callback by GetN() to not filter out anything
func AcceptAny([]string, string) bool { return true }

// Used as callback by GetN() to filter out any duplicates
func AcceptUnique(stack []string, found string) bool {
	for _, v := range stack {
		if v == found {
			return false
		}
	}
	return true
}
