/*
Copyright 2012 Google Inc.

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
	"math"
	"sync"
	"time"
)

// qpsWindow is the half-life over which request rate is measured.
const qpsWindow = time.Minute

// keyStats estimates the per-minute request rate for a single key using an
// exponentially weighted moving average. Each observation decays toward zero
// with a half-life of qpsWindow, so a key that stops being requested fades out
// of "hotness" on its own.
type keyStats struct {
	mu    sync.Mutex
	rate  float64
	stamp time.Time
}

// touch records one request at time now and returns the resulting estimated
// rate in requests per qpsWindow.
func (k *keyStats) touch(now time.Time) float64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.decay(now)
	k.rate++
	k.stamp = now
	return k.rate
}

// peek returns the estimated rate at time now without recording a request.
func (k *keyStats) peek(now time.Time) float64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.decay(now)
	k.stamp = now
	return k.rate
}

func (k *keyStats) decay(now time.Time) {
	if !k.stamp.IsZero() {
		k.rate *= math.Exp2(-float64(now.Sub(k.stamp)) / float64(qpsWindow))
	}
}
