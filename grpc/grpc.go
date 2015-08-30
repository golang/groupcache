package grpc

import (
	"sync"

	"github.com/golang/groupcache"
	"github.com/golang/groupcache/consistenthash"
	pb "github.com/golang/groupcache/groupcachepb"
	pb3 "github.com/golang/groupcache/grpc/groupcachepb3"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// PoolOption configures how we set up grpc pool
type PoolOption func(*poolOptions)

// WithReplicas return a PoolOption that specifies a function
// to used for configuring #replicas
func WithReplicas(replicas int) PoolOption {
	return func(o *poolOptions) {
		o.replicas = replicas
	}
}

// poolOptions are the configurations of a grpc Pool.
type poolOptions struct {
	// replicas specifies the number of key replicas on the consistent hash.
	replicas int

	// hashFn specifies the hash function of the consistent hash.
	// If blank, it defaults to crc32.ChecksumIEEE.
	hashFn consistenthash.Hash
}

// Pool implements PeerPicker for a pool of grpc peers
type Pool struct {
	// Dial is a dial function. leave it blank if you wan to use the default one
	Dial func(ctx context.Context, address string) (*grpc.ClientConn, error)

	// this peer's base URL, e.g. "https://example.net:8000"
	self string

	mu      sync.Mutex // guards peers and getters
	peers   *consistenthash.Map
	getters map[string]*getter // keyed by e.g. http://10.0.0.2:8080
}

// NewPool initializes an pool of peers, and registers itself as a PeerPicker.
// the self only contains a host and a port.
// for example "example.net:8000"
// Caller need to register the pool into pb.RegisterGroupCacheServer
func NewPool(self string) *Pool {
	return NewPoolOpts(self, WithReplicas(50 /*default: 50*/))
}

var poolMode bool

// NewPoolOpts initializes a grpc pool of peers with the given options.
func NewPoolOpts(self string, opts ...PoolOption) *Pool {
	if poolMode {
		panic("groupcache: NewPool must be called only once")
	}
	poolMode = true

	config := &poolOptions{}
	for _, opt := range opts {
		opt(config)
	}

	p := &Pool{
		self:    self,
		peers:   consistenthash.New(config.replicas, config.hashFn),
		getters: make(map[string]*getter),
	}
	RegisterPeerPicker(func() groupcache.PeerPicker { return p })
	return p
}

// Set updates the pool's list of peers
// Each peer valud should be a valid base URL
// for example "http://example.net:8000"
func (g *Pool) Set(peers ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.peers.Add(peers...)
	g.getters = make(map[string]*getter, len(peers))
	for _, peer := range peers {
		g.getters[peer] = &getter{
			hostAndPort: peer,
			Dial:        g.Dial,
			mu:          sync.Mutex{},
		}
	}
}

// PickPeer implement the interface of PeerPicker
func (g *Pool) PickPeer(key string) (groupcache.ProtoGetter, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.peers.IsEmpty() {
		return nil, false
	}
	if peer := g.peers.Get(key); peer != g.self {
		return g.getters[peer], true
	}
	return nil, false
}

// Get implements the interface of pb3.GroupCacheServer
func (g *Pool) Get(ctx context.Context, in *pb3.GetRequest) (*pb3.GetResponse, error) {
	groupName := in.Group
	key := in.Key

	group := groupcache.GetGroup(groupName)
	if group == nil {
		return nil, grpc.Errorf(codes.NotFound, "no such group: %v", groupName)
	}
	group.Stats.ServerRequests.Add(1)
	var value []byte
	err := group.Get(ctx, key, groupcache.AllocatingByteSliceSink(&value))
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "%v", err)
	}
	return &pb3.GetResponse{Value: value}, nil
}

type getter struct {
	// Dial is used to dial a new conn
	Dial func(ctx context.Context, address string) (*grpc.ClientConn, error)

	hostAndPort string

	// mu is used to guard rawConn to prevent multiple initialization
	mu      sync.Mutex
	rawConn *grpc.ClientConn
}

// Get implements ProtoGetter interface
// Get sents the reqeust specified in in and response the result in out
// if the returned is non-nil error, it will be escalated to caller
// TODO(hsinho): we can replace c Context with context.Context
func (gg *getter) Get(c groupcache.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	ctx := context.Background() // TODO: use timeout context
	if err := gg.acquireConn(ctx); err != nil {
		return err
	}

	client := pb3.NewGroupCacheClient(gg.rawConn)

	// TODO: this is for backward compatibility
	// the auto-generated signatures between proto2 and proto3 are different
	// we can use pb3 of course, but that means we need to change the interface defined in http.go
	// making such change may be broken dependencies.
	// we can do that in the next major release
	// For now, just make a wrap on the request/response parts
	pb3In := &pb3.GetRequest{
		Group: in.GetGroup(),
		Key:   in.GetKey(),
	}

	pb3Out, err := client.Get(ctx, pb3In)
	if err == nil {
		// convert pb3out to out
		out.Value = pb3Out.Value
		out.MinuteQps = proto.Float64(pb3Out.MinuteQps)
		return nil
	}
	return err
}

func (gg *getter) acquireConn(ctx context.Context) error {
	gg.mu.Lock()
	defer gg.mu.Unlock()

	dial := gg.Dial
	if dial == nil {
		dial = defaultDial
	}
	rawConn, err := dial(ctx, gg.hostAndPort)
	if err != nil {
		return err
	}
	gg.rawConn = rawConn
	return nil
}

func defaultDial(
	ctx context.Context,
	address string) (*grpc.ClientConn, error) {
	return grpc.Dial(address, grpc.WithInsecure())
}
