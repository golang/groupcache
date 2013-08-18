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
	"bytes"
	"encoding/binary"
)

// PackTimestamp returns a new []byte with the given timestamp appended.
// Group.SetTimestampBytes() is preferred.  Used when cache expiration
// functionality is needed.  See documentation for Group.SetExpiration() for more.
func packTimestamp(b []byte, timestamp int64) (result []byte, err error) {
	w := bytes.NewBuffer(b)
	if err := binary.Write(w, binary.LittleEndian, timestamp); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// UnpackTimestamp unpacks the original data and the timestamp encoded with
// Timestamp() or PackTimestamp().  Used when cache expiration functionality is
// needed.  See Group.SetExpiration() for more.
func UnpackTimestamp(b []byte) (result []byte, timestamp int64, err error) {
	if len(b) >= 8 {
		if timestamp, err = getTimestamp(b); err != nil {
			return nil, 0, err
		}
		return b[:len(b)-8], timestamp, nil
	}
	return b, 0, nil
}

func getTimestamp(b []byte) (timestamp int64, err error) {
	timestampBytes := b[len(b)-8:]
	r := bytes.NewBuffer(timestampBytes)
	if err := binary.Read(r, binary.LittleEndian, &timestamp); err != nil {
		return 0, err
	}
	return timestamp, nil
}

func getTimestampByteView(bv ByteView) (timestamp int64, err error) {
	var timestampByteView ByteView
	if bv.Len() >= 8 {
		timestampByteView = bv.SliceFrom(bv.Len() - 8)
	}
	r := bytes.NewBuffer(timestampByteView.ByteSlice())
	if err := binary.Read(r, binary.LittleEndian, &timestamp); err != nil {
		return 0, err
	}
	return timestamp, nil
}
