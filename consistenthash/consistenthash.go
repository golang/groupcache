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
	"math/bits"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

const defaultHashExpansion = 6

type Map struct {
	// Inputs

	// hash is the hash function that will be applied to both added
	// keys and fetched keys
	hash Hash

	// replicas is the number of virtual nodes that will be inserted
	// into the consistent hash ring for each key added
	replicas int

	// prefixTableExpansion is the multiple of virtual nodes that
	// will be inserted into the internal hash table for O(1) lookups.
	prefixTableExpansion int

	// Internal data

	// keys is the hash of the virtual nodes, sorted by hash value
	keys []int // Sorted

	// hashMap maps the hashed keys back to the input strings.
	// Note that all virtual nodes will map back to the same input
	// string
	hashMap map[int]string

	// prefixShift is the number of bits an input hash should
	// be right-shifted to act as a lookup in the prefixTable
	prefixShift uint32

	// prefixTable is a map of the most significant bits of
	// a hash value to output all hashes with that prefix
	// map to.  If the result is ambiguous (i.e. there is a
	// hash range split within this prefix) the value will
	// be blank and we should fall back to a binary search
	// through keys to find the exact output
	prefixTable []string
}

// New returns a blank consistent hash ring that will return
// the key whose hash comes next after the hash of the input to
// Map.Get.
// Increasing the number of replicas will improve the smoothness
// of the hash ring and reduce the data moved when adding/removing
// nodes, at the cost of more memory.
func New(replicas int, fn Hash) *Map {
	return NewConsistentHash(replicas, defaultHashExpansion, fn)
}

// NewConsistentHash returns a blank consistent hash ring that will return
// the key whose hash comes next after the hash of the input to
// Map.Get.
// Increasing the number of replicas will improve the smoothness
// of the hash ring and reduce the data moved when adding/removing
// nodes.
// Increasing the tableExpansion will allocate more entries in the
// internal hash table, reducing the frequency of lg(n) binary
// searches during calls to the Map.Get method.
func NewConsistentHash(replicas int, tableExpansion int, fn Hash) *Map {
	m := &Map{
		replicas:             replicas,
		hash:                 fn,
		hashMap:              make(map[int]string),
		prefixTableExpansion: tableExpansion,
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

// Add adds some keys to the hash.
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)

	// Find minimum number of bits to hold |keys| * prefixTableExpansion
	prefixBits := uint32(bits.Len32(uint32(len(m.keys) * m.prefixTableExpansion)))
	m.prefixShift = 32 - prefixBits

	prefixTableSize := 1 << prefixBits
	m.prefixTable = make([]string, prefixTableSize)

	previousKeyPrefix := -1 // Effectively -Inf
	currentKeyIdx := 0
	currentKeyPrefix := m.keys[currentKeyIdx] >> m.prefixShift

	for i := range m.prefixTable {
		if previousKeyPrefix < i && currentKeyPrefix > i {
			// All keys with this prefix will map to a single value
			m.prefixTable[i] = m.hashMap[m.keys[currentKeyIdx]]
		} else {
			// Several keys might have the same prefix.  Walk
			// over them until it changes
			previousKeyPrefix = currentKeyPrefix
			for currentKeyPrefix == previousKeyPrefix {
				currentKeyIdx++
				if currentKeyIdx < len(m.keys) {
					currentKeyPrefix = m.keys[currentKeyIdx] >> m.prefixShift
				} else {
					currentKeyIdx = 0
					currentKeyPrefix = prefixTableSize + 1 // Effectively +Inf
				}
			}
		}
	}
}

// Get gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hash([]byte(key)))

	// Look for the hash prefix in the prefix table
	prefixSlot := hash >> m.prefixShift
	tableResult := m.prefixTable[prefixSlot]
	if len(tableResult) > 0 {
		return tableResult
	}

	// Binary search for appropriate replica.
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	// Means we have cycled back to the first replica.
	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]]
}
