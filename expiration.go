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
	"time"
)

type TimeProvider interface {
	// Return the current Unix epoch time in seconds.
	NowUnixSeconds() int64

	// Return the time on a channel after delay d.
	After(d time.Duration) <-chan time.Time
}

type defaultTimeProvider struct{}

func (t defaultTimeProvider) NowUnixSeconds() int64 {
	return time.Now().Unix()
}

func (t defaultTimeProvider) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// Hook for testing.
var timeProvider TimeProvider = defaultTimeProvider{}

func setTimeProvider(tg TimeProvider) { // Should only be set by testing code.
	timeProvider = tg
}

// GetTime should be called to produce a new timestamp to supply for
// Group.SetTimeStampBytes()
func GetTime() int64 {
	return timeProvider.NowUnixSeconds()
}

// SetExpiration sets the cache expiration time.  Internally the duration is
// truncated to seconds, so it's not useful to set with greater precision. If
// unset or set to 0, will cache items forever.  To effectively disable caching,
// set to 1ns.
//
// In getter, set timestamp using SetTimestampBytes():
//
// func(gctx groupcache.Context, key string, dest groupcache.Sink) error {
//    content = MakeMyContent(key)
//    return dest.SetTimestampBytes(content, groupcache.GetTime())
//  }
//
// In call to Get() use AllocatingByteSliceSink, StringSink, or ByteViewSink,
// then unpack returned result.  Using an unsupported sink implementation will
// produce an error when calling Get().
//
// var packedContent []byte
// var timestamp int64
// if err := gcache.Get(nil, key, groupcache.AllocatingByteSliceSink(&packedContent)); err != nil {
// 	handleErr()
// }
//	content, timestamp, err := groupcache.UnpackTimestamp(packedContent)
//	if err != nil {
//	  handleErr()
//	}
//
func (g *Group) SetExpiration(d time.Duration) *Group {
	g.expiration = d
	return g
}

// SetStalePeriod sets the duration after expiration in which stale data may be served.
// See SetExpiration() for details.  Internally this is truncated to seconds,
// so it's not useful to set with greater precision.
func (g *Group) SetStalePeriod(d time.Duration) *Group {
	g.stalePeriod = d
	return g
}

// SetStaleDeadline sets the deadline during the stale period for a background
// reload after which stale data will be returned if fresh data is not ready.
func (g *Group) SetStaleDeadline(d time.Duration) *Group {
	g.staleDeadline = d
	return g
}

// SetDisableHotCache controls disabling of writes to the hotCache.  May be used
// with expiration functionality to prevent clients from getting inconsistent
// data freshness due to unsynchronized clocks in a multi-peer setup.  With the
// hotCache disabled, there is only ever one server in a system where an item
// may be cached (the authorative server for a given key).  With the hotCache
// disabled, if the authoritative server for a given key goes down, its peers
// will generate the value locally, but will not cache it (otherwise the value
// would be generated locally and cached locally).  Writes to the hotCache are
// enabled by default.
func (g *Group) SetDisableHotCache(disable bool) *Group {
	g.disableHotCache = disable
	return g
}

func (g *Group) handleExpiration(ctx Context, key string, dest Sink, value ByteView) error {
	timestamp, err := getTimestampByteView(value)
	if err != nil {
		return err
	}
	age := GetTime() - timestamp
	// <0 means okay, >=0 means expired, >=stalePeriod means must reload.
	expiredOffset := age - int64(g.expiration.Seconds())

	if expiredOffset >= int64(g.stalePeriod.Seconds()) { // Regenerate only.
		return g.loadOnMiss(ctx, key, dest, true) // Will generate with a new timestamp.
	} else if expiredOffset >= 0 { // Can serve stale data and regen in background.
		// Kick off a load in the background.
		var backgroundBytes []byte
		backgroundDest := AllocatingByteSliceSink(&backgroundBytes)
		backgroundErrResult := make(chan error)
		go func() {
			backgroundErrResult <- g.loadOnMiss(ctx, key, backgroundDest, true)
		}()

		select {
		case err := <-backgroundErrResult: // Made deadline so use regenerated value.
			if err != nil {
				return err
			}
			bdView, _ := backgroundDest.view() // Contains a new timestamp.
			return setSinkView(dest, bdView)
		case <-timeProvider.After(g.staleDeadline): // Missed deadline, so use stale value.
			break
		}
	} // else still cached and valid.

	// Fall through for still cached, or cached and stale.  Reuseing the previous timestamp.
	return setSinkView(dest, value)
}
