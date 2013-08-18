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

package groupcache

import (
	"testing"
	"time"
)

var (
	timestampGroup *Group
	// cFills is the number of times timestampGroup Getter has been called.
	// Read using the cFills function.
	cFills AtomicInt
)

const (
	cSize = 1 << 20
)

type testTimeProvider struct {
	NowChannel   chan int64
	AfterChannel chan time.Time
}

func (t testTimeProvider) NowUnixSeconds() int64 {
	if len(t.NowChannel) == 0 {
		panic("empty nowChannel queue") // Keep miswritten tests from hanging.
	}
	newTime := <-t.NowChannel
	return newTime
}

func (t testTimeProvider) After(d time.Duration) <-chan time.Time {
	return t.AfterChannel
}

var nowChannel chan int64
var afterChannel chan time.Time
var contentChannel chan string

func testingSetup(groupName string) {
	nowChannel = make(chan int64, 10)
	afterChannel = make(chan time.Time, 10)
	contentChannel = make(chan string, 10)
	tp := testTimeProvider{NowChannel: nowChannel, AfterChannel: afterChannel}
	setTimeProvider(tp)

	timestampGroup = NewGroup(groupName, cSize, GetterFunc(func(_ Context, key string, dest Sink) error {
		if len(contentChannel) == 0 {
			panic("empty contentChannel queue")
		}
		content := []byte(<-contentChannel)
		cFills.Add(1)
		// 1 ms delay Should be sufficient to make functionality of select logic in
		// handleExpiration() deterministic.  The test passes without this, but we
		// should be conservative.
		time.Sleep(1 * time.Millisecond)
		return dest.SetTimestampBytes(content, GetTime())
	}))
}

func callGet(t *testing.T, key string) (content []byte, timestamp int64) {
	var packedContent []byte
	if err := timestampGroup.Get(nil, key, AllocatingByteSliceSink(&packedContent)); err != nil {
		t.Fatal(err)
	}
	content, timestamp, err := UnpackTimestamp(packedContent)
	if err != nil {
		t.Fatal(err)
	}
	return content, timestamp
}

func verifyContent(t *testing.T, want string, got []byte) {
	if string(got) != want {
		t.Errorf("Got: %s, Want:%s", string(got), want)
	}
}

func verifyTimestamp(t *testing.T, want int64, got int64) {
	if got != want {
		t.Errorf("Got: %d, Want:%d", got, want)
	}
}

// TestExpires tests expiration functionality.
func TestExpires(t *testing.T) {
	testingSetup("expires-group")
	testKey := "testKey1"
	timestampGroup.SetExpiration(time.Duration(300) * time.Second)

	startFills := cFills.Get()

	contentChannel <- "blahblah1"
	nowChannel <- 100 // Becomes data timestamp
	content, timestamp := callGet(t, testKey)
	verifyContent(t, "blahblah1", content)
	verifyTimestamp(t, 100, timestamp)

	nowChannel <- 399 // Used for expiration check.
	content, timestamp = callGet(t, testKey)
	verifyContent(t, "blahblah1", content)
	verifyTimestamp(t, 100, timestamp)

	afterChannel <- time.Time{}              // Force erroneous stale data if we select the regen during stale period branch.
	nowChannel <- 400                        // Used for expiration check.
	nowChannel <- 401                        // Used for data timestamp.
	contentChannel <- "foofoo1"              // New content.
	content, timestamp = callGet(t, testKey) // Expiration.
	verifyContent(t, "foofoo1", content)
	verifyTimestamp(t, 401, timestamp)

	nowChannel <- 700 // Used for expiration check.
	content, timestamp = callGet(t, testKey)
	verifyContent(t, "foofoo1", content)
	verifyTimestamp(t, 401, timestamp)

	afterChannel <- time.Time{}              // Force erroneous stale data if we select the regen during stale period branch.
	nowChannel <- 701                        // Used for expiration check.
	nowChannel <- 702                        // Used for data timestamp.
	contentChannel <- "barbar1"              // New content.
	content, timestamp = callGet(t, testKey) // Expiration.
	verifyContent(t, "barbar1", content)
	verifyTimestamp(t, 702, timestamp)

	totalFills := cFills.Get() - startFills
	if totalFills != 3 {
		t.Errorf("Wanted 3, got %d", totalFills)
	}
}

// TestStale tests that stale values are returned correctly.
func TestStale(t *testing.T) {
	testingSetup("stale-group")
	testKey := "testKey2"
	timestampGroup.SetExpiration(time.Duration(300) * time.Second)
	timestampGroup.SetStalePeriod(time.Duration(300) * time.Second)

	startFills := cFills.Get()

	contentChannel <- "blahblah2"
	nowChannel <- 100 // Becomes data timestamp
	content, timestamp := callGet(t, testKey)
	verifyContent(t, "blahblah2", content)
	verifyTimestamp(t, 100, timestamp)

	nowChannel <- 399 // Used for expiration check.
	content, timestamp = callGet(t, testKey)
	verifyContent(t, "blahblah2", content)
	verifyTimestamp(t, 100, timestamp)

	nowChannel <- 400                        // Used for expiration check.
	nowChannel <- 401                        // Used for data timestamp.
	contentChannel <- "foofoo2"              // New content.
	content, timestamp = callGet(t, testKey) // Expiration.
	verifyContent(t, "foofoo2", content)
	verifyTimestamp(t, 401, timestamp)

	nowChannel <- 700 // Used for expiration check.
	content, timestamp = callGet(t, testKey)
	verifyContent(t, "foofoo2", content)
	verifyTimestamp(t, 401, timestamp)

	afterChannel <- time.Time{}              // Force stale data by triggering deadline.
	nowChannel <- 701                        // Used for expiration check.
	nowChannel <- 702                        // Used for data timestamp.
	contentChannel <- "barbar2"              // New content.
	content, timestamp = callGet(t, testKey) // Expiration.
	verifyContent(t, "foofoo2", content)     // Should get stale data.
	verifyTimestamp(t, 401, timestamp)

	totalFills := cFills.Get() - startFills
	if totalFills != 2 {
		t.Errorf("Wanted 2, got %d", totalFills)
	}
}
