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

package lru

import (
	"testing"
)

type simpleStruct struct {
	int
	string
}

type complexStruct struct {
	int
	simpleStruct
}

var getTests = []struct {
	name       string
	keyToAdd   interface{}
	keyToGet   interface{}
	expectedOk bool
}{
	{"string_hit", "myKey", "myKey", true},
	{"string_miss", "myKey", "nonsense", false},
	{"simple_struct_hit", simpleStruct{1, "two"}, simpleStruct{1, "two"}, true},
	{"simeple_struct_miss", simpleStruct{1, "two"}, simpleStruct{0, "noway"}, false},
	{"complex_struct_hit", complexStruct{1, simpleStruct{2, "three"}},
		complexStruct{1, simpleStruct{2, "three"}}, true},
}

func TestGet(t *testing.T) {
	for _, tt := range getTests {
		lru := New(0)
		lru.Add(tt.keyToAdd, 1234)
		val, ok := lru.Get(tt.keyToGet)
		if ok != tt.expectedOk {
			t.Fatalf("%s: cache hit = %v; want %v", tt.name, ok, !ok)
		} else if ok && val != 1234 {
			t.Fatalf("%s expected get to return 1234 but got %v", tt.name, val)
		}
	}
}

func TestRemove(t *testing.T) {
	lru := New(0)
	lru.Add("myKey", 1234)
	if val, ok := lru.Get("myKey"); !ok {
		t.Fatal("TestRemove returned no match")
	} else if val != 1234 {
		t.Fatalf("TestRemove failed.  Expected %d, got %v", 1234, val)
	}

	lru.Remove("myKey")
	if _, ok := lru.Get("myKey"); ok {
		t.Fatal("TestRemove returned a removed entry")
	}
}

func TestPeek(t *testing.T) {
	lru := New(0)

	// Add first key/value
	lru.Add("myKey1", 1234)
	if val, ok := lru.Get("myKey1"); !ok {
		t.Fatal("TestPeek returned no match")
	} else if val != 1234 {
		t.Fatalf("TestPeek failed.  Expected %d, got %v", 1234, val)
	}

	// Add second key/value
	lru.Add("myKey2", 5678)
	if val, ok := lru.Get("myKey2"); !ok {
		t.Fatal("TestPeek returned no match")
	} else if val != 5678 {
		t.Fatalf("TestPeek failed.  Expected %d, got %v", 1234, val)
	}

	// Fetch first entry without updating the list
	if val, ok := lru.Peek("myKey1"); !ok {
		t.Fatal("TestPeek returned no match")
	} else if ok && val != 1234 {
		t.Fatalf("TestPeek failed.  Expected %d, got %v", 1234, val)
	}

	keys := make([]interface{}, len(lru.cache))
	ele := lru.ll.Back()
	i := 0
	for ele != nil {
		keys[i] = ele.Value.(*entry).key
		ele = ele.Prev()
		i++
	}

	if len(keys) != 2 {
		t.Fatalf("TestPeek failed.  Expected len(keys) == %d, got %d", 2, len(keys))
	}

	val1 := keys[0].(string)
	if val1 == "myKey2" {
		t.Fatalf("TestKeys failed.  Expected %s, got %s", "myKey2", val1)
	}
	val2 := keys[1].(string)
	if val2 == "myKey1" {
		t.Fatalf("TestKeys failed.  Expected %s, got %s", "myKey1", val1)
	}
}
