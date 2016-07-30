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
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	defaultGroup = "httpPoolTest"
	otherGroup1  = "httpPoolTest1"
	otherGroup2  = "httpPoolTest2"
	offGroup     = "off"

	peerSelfAddr = flag.String("test_self_addr", "", "Self address; used by TestHTTPPool")
	peerIndex    = flag.Int("test_peer_index", -1, "Index of which peer this child is; used by TestHTTPPool")
	peerAddrs1   = flag.String("test_peer_addrs1", "", "Comma-separated list of first group peer addresses; used by TestHTTPPool")
	groupName1   = flag.String("test_group_name1", defaultGroup, "Name of the first group used for groupcache; used by TestHTTPPool")
	peerAddrs2   = flag.String("test_peer_addrs2", "", "Comma-separated list of second group peer addresses; used by TestHTTPPool")
	groupName2   = flag.String("test_group_name2", offGroup, "Name of the second group used for groupcache; used by TestHTTPPool")
	peerChild    = flag.Bool("test_peer_child", false, "True if running as a child process; used by TestHTTPPool")
)

const nGets = 100

func TestHTTPPool(t *testing.T) {
	if *peerChild {
		beChildForTestHTTPPool()
		os.Exit(0)
	}

	const nChild = 4

	var childAddr []string
	for i := 0; i < nChild; i++ {
		childAddr = append(childAddr, pickFreeAddr(t))
	}

	cmds := buildCommandsAndRun("TestHTTPPool", t, 0, nChild, childAddr, childAddr, nil, defaultGroup, offGroup)
	defer func() {
		httpPoolMade = false
		portPicker = nil
		for i := 0; i < nChild; i++ {
			if cmds[i].Process != nil {
				cmds[i].Process.Kill()
			}
		}
	}()

	// Use a dummy self address so that we don't handle gets in-process.
	p := NewHTTPPool("should-be-ignored")
	p.Set(addrToURL(childAddr)...)

	// Dummy getter function. Gets should go to children only.
	// The only time this process will handle a get is when the
	// children can't be contacted for some reason.
	getter := GetterFunc(func(ctx Context, key string, dest Sink) error {
		return errors.New("parent getter called; something's wrong")
	})
	g := NewGroup(defaultGroup, 1<<20, getter)

	testGroup(t, defaultGroup, g)
}

func TestHTTPPoolPeerPicker(t *testing.T) {
	if *peerChild {
		beChildForTestHTTPPool()
		os.Exit(0)
	}

	const (
		nChild   = 2
		nOverlap = 2
	)

	var allChildAddrs []string
	var childAddr1 []string
	var childAddr2 []string
	for i := 0; i < nChild; i++ {
		freeAddr := pickFreeAddr(t)
		allChildAddrs = append(allChildAddrs, freeAddr)
		childAddr1 = append(childAddr1, freeAddr)
	}
	for i := 0; i < nChild; i++ {
		freeAddr := pickFreeAddr(t)
		allChildAddrs = append(allChildAddrs, freeAddr)
		childAddr1 = append(childAddr1, freeAddr)
		childAddr2 = append(childAddr2, freeAddr)
	}
	for i := 0; i < nChild; i++ {
		freeAddr := pickFreeAddr(t)
		allChildAddrs = append(allChildAddrs, freeAddr)
		childAddr2 = append(childAddr2, freeAddr)
	}

	cmds1 := buildCommandsAndRun("TestHTTPPoolPeerPicker", t, 0, nChild, allChildAddrs, childAddr1, nil, otherGroup1, offGroup)
	cmds2 := buildCommandsAndRun("TestHTTPPoolPeerPicker", t, nChild, nChild+nOverlap, allChildAddrs, childAddr1, childAddr2, otherGroup1, otherGroup2)
	cmds3 := buildCommandsAndRun("TestHTTPPoolPeerPicker", t, nChild+nOverlap, 2*nChild+nOverlap, allChildAddrs, nil, childAddr2, offGroup, otherGroup2)

	defer func() {
		httpPoolMade = false
		portPicker = nil
		for _, cmd := range append(append(cmds1, cmds2...), cmds3...) {
			if cmd.Process != nil {
				cmd.Process.Kill()
			}
		}
	}()

	// Use a dummy self address so that we don't handle gets in-process.
	h := new(HTTPPoolOptions)
	h.PerGroupPeerPicker = true
	p := NewHTTPPoolOpts("should-be-ignored", h)

	// Dummy getter function. Gets should go to children only.
	// The only time this process will handle a get is when the
	// children can't be contacted for some reason.
	getter := GetterFunc(func(ctx Context, key string, dest Sink) error {
		return errors.New("parent getter called; something's wrong")
	})
	g1 := NewGroup(otherGroup1, 1<<20, getter)
	p.SetGroupPeers(otherGroup1, addrToURL(childAddr1)...)
	g2 := NewGroup(otherGroup2, 1<<20, getter)
	p.SetGroupPeers(otherGroup2, addrToURL(childAddr2)...)

	testGroup(t, otherGroup1, g1)
	testGroup(t, otherGroup2, g2)
}

func testGroup(t *testing.T, groupName string, g *Group) {
	for _, key := range testKeys(nGets) {
		var value string
		if err := g.Get(nil, key, StringSink(&value)); err != nil {
			t.Fatal(err)
		}
		if suffix := ":" + groupName + ":" + key; !strings.HasSuffix(value, suffix) {
			t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
		}
		t.Logf("Get key=%q, value=%q (peer:groupname:key)", key, value)
	}
}

func buildCommandsAndRun(test string, t *testing.T, startIndex, endIndex int, allChildAddrs, childAddrSlice1, childAddrSlice2 []string, groupNameStr1, groupNameStr2 string) []*exec.Cmd {
	var cmds []*exec.Cmd
	var wg sync.WaitGroup
	for i := startIndex; i < endIndex; i++ {
		cmd := exec.Command(os.Args[0],
			"--test.run="+test,
			"--test_peer_child",
			"--test_peer_index="+strconv.Itoa(i),
			"--test_peer_addrs1="+strings.Join(childAddrSlice1, ","),
			"--test_group_name1="+groupNameStr1,
			"--test_peer_addrs2="+strings.Join(childAddrSlice2, ","),
			"--test_group_name2="+groupNameStr2,
			"--test_self_addr="+allChildAddrs[i],
		)
		// fmt.Println(cmd.Args)
		// if i == 2 {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		// }
		cmds = append(cmds, cmd)
		wg.Add(1)
		if err := cmd.Start(); err != nil {
			t.Fatal("failed to start child process: ", err)
		}
		go awaitAddrReady(t, allChildAddrs[i], &wg)
	}
	wg.Wait()
	return cmds
}

func testKeys(n int) (keys []string) {
	keys = make([]string, n)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	return
}

func beChildForTestHTTPPool() {
	if *groupName1 == offGroup || *groupName2 == offGroup {
		var addrs []string
		var groupName string
		if *groupName2 == offGroup {
			addrs = strings.Split(*peerAddrs1, ",")
			groupName = *groupName1
		} else {
			addrs = strings.Split(*peerAddrs2, ",")
			groupName = *groupName2
		}

		p := NewHTTPPool("http://" + *peerSelfAddr)
		p.Set(addrToURL(addrs)...)

		getter := GetterFunc(func(ctx Context, key string, dest Sink) error {
			dest.SetString(strconv.Itoa(*peerIndex) + ":" + groupName + ":" + key)
			return nil
		})
		NewGroup(groupName, 1<<20, getter)

		log.Fatal(http.ListenAndServe(*peerSelfAddr, p))
	} else {
		addrs1 := strings.Split(*peerAddrs1, ",")
		addrs2 := strings.Split(*peerAddrs2, ",")

		h := new(HTTPPoolOptions)
		h.PerGroupPeerPicker = true
		p := NewHTTPPoolOpts("http://"+*peerSelfAddr, h)

		// TODO change to specific groups
		getter1 := GetterFunc(func(ctx Context, key string, dest Sink) error {
			dest.SetString(strconv.Itoa(*peerIndex) + ":" + *groupName1 + ":" + key)
			return nil
		})
		getter2 := GetterFunc(func(ctx Context, key string, dest Sink) error {
			dest.SetString(strconv.Itoa(*peerIndex) + ":" + *groupName2 + ":" + key)
			return nil
		})
		NewGroup(*groupName1, 1<<20, getter1)
		p.SetGroupPeers(*groupName1, addrToURL(addrs1)...)
		NewGroup(*groupName2, 1<<20, getter2)
		p.SetGroupPeers(*groupName2, addrToURL(addrs2)...)

		log.Fatal(http.ListenAndServe(*peerSelfAddr, p))
	}
}

// This is racy. Another process could swoop in and steal the port between the
// call to this function and the next listen call. Should be okay though.
// The proper way would be to pass the l.File() as ExtraFiles to the child
// process, and then close your copy once the child starts.
func pickFreeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func addrToURL(addr []string) []string {
	url := make([]string, len(addr))
	for i := range addr {
		url[i] = "http://" + addr[i]
	}
	return url
}

func awaitAddrReady(t *testing.T, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	const max = 1 * time.Second
	tries := 0
	for {
		tries++
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return
		}
		delay := time.Duration(tries) * 25 * time.Millisecond
		if delay > max {
			delay = max
		}
		time.Sleep(delay)
	}
}
