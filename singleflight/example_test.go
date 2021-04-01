/*
Copyright 2018 The Go Authors.

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

package singleflight_test

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/golang/groupcache/singleflight"
)

var counter int32
var group singleflight.Group

func Example() {
	// No matter how many goroutines call Do, only one call per key will be in
	// progress at any time. Callers that share a key will get the same results
	// as an in-flight call with that key.
	n := int32(41)
	v, err := group.Do(strconv.FormatInt(int64(n), 10), func() (interface{}, error) {
		time.Sleep(time.Millisecond) // simulate time-consuming action
		return n + 1, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(v.(int32))
	// Output: 42
}
