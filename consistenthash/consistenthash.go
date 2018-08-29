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

type Map struct {
	hash                 Hash
	replicas             int
	prefixTableExpansion int

	keys    []int // Sorted
	hashMap map[int]string

	prefixBits  uint32
	prefixShift uint32
	prefixTable []string
}

func New(replicas int, tableExpansion int, fn Hash) *Map {
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

	// Find minimum number of bits to hold |keys| * prefixTableExpansion
	m.prefixBits = uint32(bits.Len32(uint32(len(m.keys) * m.prefixTableExpansion)))
	m.prefixShift = 32 - m.prefixBits

	prefixTableSize := 1 << m.prefixBits
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

// Gets the closest item in the hash to the provided key.
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
