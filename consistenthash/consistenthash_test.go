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

package consistenthash

import (
	"fmt"
	"strconv"
	"testing"
)

func TestHashing(t *testing.T) {

	// Override the hash function to return easier to reason about values. Assumes
	// the keys can be converted to an integer.
	hash := New(3, func(key []byte) uint32 {
		i, err := strconv.Atoi(string(key))
		if err != nil {
			panic(err)
		}
		return uint32(i)
	})

	// Given the above hash function, this will give replicas with "hashes":
	// 2, 4, 6, 12, 14, 16, 22, 24, 26
	hash.Add("6", "4", "2")

	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// Adds 8, 18, 28
	hash.Add("8")

	// 27 should now map to 8.
	testCases["27"] = "8"

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}
}

func TestGetN(t *testing.T) {
	// Override the hash function to return easier to reason about values. Assumes
	// the keys can be converted to an integer.
	hash := New(1, func(key []byte) uint32 {
		i, err := strconv.Atoi(string(key))
		if err != nil {
			panic(err)
		}
		return uint32(i)
	})

	if len(hash.GetN("1", 1, nil)) > 0 {
		t.Errorf("Should not be able to get items from empty ring")
	}

	hash.Add("6", "1337", "1236", "1723")
	if res := hash.GetN("1", 0, nil); len(res) > 0 {
		t.Errorf("Asked for 0 items but got %d items instead", len(res))
	}

	testCases := map[string][]string{
		"1235": []string{"1236", "1337", "1723"},
		"1236": []string{"1236", "1337", "1723"},
		"1237": []string{"1337", "1723", "6"},
		"1238": []string{"1337", "1723", "6"},
		"1338": []string{"1723", "6", "1236"},
	}

	for k, v := range testCases {
		res := hash.GetN(k, 3, nil)
		if len(res) != 3 {
			t.Errorf("Asking for %s, should have yielded %s, but got: %s", k, v, res)
			continue
		}

		for i, resV := range res {
			if v[i] != resV {
				t.Errorf("Asking for %s, should have yielded %s, but got: %s", k, v, res)
			}
		}
	}

	// Verify ring size (in this case 4) is used as upper bound
	if res := hash.GetN("9999", 10, nil); len(res) > 4 {
		t.Errorf("Ring only contains 4 items but we received %d: %s", len(res), res)
	}
}

func TestGetNWithReplicas(t *testing.T) {
	hash := New(250, nil)
	hash.Add("6", "1337", "1236", "1723")

	testCases := map[string][]string{
		"1338": []string{"6", "1337", "1723"}, // [6 1337 1337 6 1723]
		"1238": []string{"1236", "6", "1337"}, // [1236 6 6 1236 1337]
	}

	for k, v := range testCases {
		res := hash.GetN(k, 3, AcceptUnique)
		if len(res) != 3 {
			t.Errorf("Asking for %s, should have yielded %s, but got: %s", k, v, res)
			continue
		}

		for i, resV := range res {
			if v[i] != resV {
				t.Errorf("Asking for %s, should have yielded %s, but got: %s", k, v, res)
			}
		}
	}
}

func TestConsistency(t *testing.T) {
	hash1 := New(1, nil)
	hash2 := New(1, nil)

	hash1.Add("Bill", "Bob", "Bonny")
	hash2.Add("Bob", "Bonny", "Bill")

	if hash1.Get("Ben") != hash2.Get("Ben") {
		t.Errorf("Fetching 'Ben' from both hashes should be the same")
	}

	hash2.Add("Becky", "Ben", "Bobby")

	if hash1.Get("Ben") != hash2.Get("Ben") ||
		hash1.Get("Bob") != hash2.Get("Bob") ||
		hash1.Get("Bonny") != hash2.Get("Bonny") {
		t.Errorf("Direct matches should always return the same entry")
	}

}

func BenchmarkGet8(b *testing.B)   { benchmarkGet(b, 8) }
func BenchmarkGet32(b *testing.B)  { benchmarkGet(b, 32) }
func BenchmarkGet128(b *testing.B) { benchmarkGet(b, 128) }
func BenchmarkGet512(b *testing.B) { benchmarkGet(b, 512) }

func benchmarkGet(b *testing.B, shards int) {

	hash := New(50, nil)

	var buckets []string
	for i := 0; i < shards; i++ {
		buckets = append(buckets, fmt.Sprintf("shard-%d", i))
	}

	hash.Add(buckets...)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hash.Get(buckets[i&(shards-1)])
	}
}
