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
	"testing"
	"time"
)

func TestKeyStatsRampUp(t *testing.T) {
	var k keyStats
	now := time.Unix(0, 0)
	if got := k.touch(now); got != 1 {
		t.Fatalf("first touch = %v; want 1", got)
	}
	// Many requests within the same instant accumulate without decay.
	for i := 0; i < 9; i++ {
		k.touch(now)
	}
	if got := k.peek(now); got != 10 {
		t.Errorf("rate after 10 instantaneous touches = %v; want 10", got)
	}
}

func TestKeyStatsDecay(t *testing.T) {
	var k keyStats
	now := time.Unix(0, 0)
	k.touch(now)
	// After one half-life with no requests, the rate should halve.
	if got := k.peek(now.Add(qpsWindow)); got != 0.5 {
		t.Errorf("rate after one window = %v; want 0.5", got)
	}
	// And after another, halve again.
	if got := k.peek(now.Add(2 * qpsWindow)); got != 0.25 {
		t.Errorf("rate after two windows = %v; want 0.25", got)
	}
}

func TestCacheQPSUntracked(t *testing.T) {
	// hotCache does not track QPS, so peekQPS is always 0.
	var c cache
	c.add("k", ByteView{s: "v"})
	if got := c.peekQPS("k", time.Unix(0, 0)); got != 0 {
		t.Errorf("untracked peekQPS = %v; want 0", got)
	}
	if got := c.peekQPS("absent", time.Unix(0, 0)); got != 0 {
		t.Errorf("absent peekQPS = %v; want 0", got)
	}
}

func TestCacheQPSTracked(t *testing.T) {
	c := cache{trackQPS: true}
	c.add("k", ByteView{s: "v"})
	now := time.Unix(0, 0)
	for i := 0; i < 5; i++ {
		c.recordRequest("k", now)
	}
	if got := c.peekQPS("k", now); got != 5 {
		t.Errorf("tracked peekQPS = %v; want 5", got)
	}
}
