package grpc

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/golang/groupcache"
	pb3 "github.com/golang/groupcache/grpc/groupcachepb3"
	test "github.com/golang/groupcache/test"
	"google.golang.org/grpc"
)

var (
	peerAddrs = flag.String("test_peer_addrs", "", "Comma-separated list of peer addresses; used by TestGRPCPool")
	peerIndex = flag.Int("test_peer_index", -1, "Index of which peer this child is; used by TestGRPCPool")
	peerChild = flag.Bool("test_peer_child", false, "True if running as a child process; used by TestGRPCPool")
)

func TestGPRCPool(t *testing.T) {
	if *peerChild {
		beChildForTestGRPCPool()
		os.Exit(0)
	}

	const (
		nChild = 4
		nGets  = 100
	)

	var childAddr []string
	for i := 0; i < nChild; i++ {
		childAddr = append(childAddr, pickFreeAddr(t))
	}

	var cmds []*exec.Cmd
	var wg sync.WaitGroup
	for i := 0; i < nChild; i++ {
		cmd := exec.Command(os.Args[0],
			"--test.run=TestGPRCPool",
			"--test_peer_child",
			"--test_peer_addrs="+strings.Join(childAddr, ","),
			"--test_peer_index="+strconv.Itoa(i),
		)
		cmds = append(cmds, cmd)
		wg.Add(1)
		if err := cmd.Start(); err != nil {
			t.Fatal("failed to start child process: ", err)
		}
		go awaitAddrReady(t, childAddr[i], &wg)
	}
	defer func() {
		for i := 0; i < nChild; i++ {
			if cmds[i].Process != nil {
				cmds[i].Process.Kill()
			}
		}
	}()
	wg.Wait()

	// Use a dummy self address so that we don't handle gets in-process.
	p := NewPool("should-be-ignored")
	p.Set(childAddr...)

	// Dummy getter function. Gets should go to children only.
	// The only time this process will handle a get is when the
	// children can't be contacted for some reason.
	getter := groupcache.GetterFunc(func(ctx groupcache.Context, key string, dest groupcache.Sink) error {
		return errors.New("parent getter called; something's wrong")
	})
	g := NewGroup("grpcPoolTest", 1<<20, getter)

	for _, key := range testKeys(nGets) {
		var value string
		if err := g.Get(nil, key, groupcache.StringSink(&value)); err != nil {
			t.Fatal(err)
		}
		if suffix := ":" + key; !strings.HasSuffix(value, suffix) {
			t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
		}
		t.Logf("Get key=%q, value=%q (peer:key)", key, value)
	}
	// TODO: do we need to shutdown gracefully?
}

func beChildForTestGRPCPool() {
	addrs := strings.Split(*peerAddrs, ",")
	myAddr := addrs[*peerIndex]
	_, port, err := net.SplitHostPort(myAddr)
	if err != nil {
		log.Fatal(err)
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal(err)
	}

	g := NewPool(myAddr)
	g.Set(addrs...)
	getter := groupcache.GetterFunc(func(ctx groupcache.Context, key string, dest groupcache.Sink) error {
		dest.SetString(strconv.Itoa(*peerIndex) + ":" + key)
		return nil
	})
	groupcache.NewGroup("grpcPoolTest", 1<<20, getter)

	grpcServer := grpc.NewServer()
	pb3.RegisterGroupCacheServer(grpcServer, g)
	grpcServer.Serve(lis)
}

func pickFreeAddr(t *testing.T) string {
	addr, err := test.PickFreeAddr()
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func awaitAddrReady(t *testing.T, addr string, wg *sync.WaitGroup) {
	defer wg.Done()

	test.AwaitAddrReady(addr)
}

func testKeys(n int) (keys []string) {
	keys = make([]string, n)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	return
}
