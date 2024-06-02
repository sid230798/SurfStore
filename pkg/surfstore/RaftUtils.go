package surfstore

import (
	"bufio"
	context "context"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here
	conns := make([]*grpc.ClientConn, 0)
	for _, addr := range config.RaftAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}

	serverStatusMutex := sync.RWMutex{}
	raftStateMutex := sync.RWMutex{}

	server := RaftSurfstore{
		serverStatus:      ServerStatus_FOLLOWER,
		serverStatusMutex: &serverStatusMutex,
		term:              0,
		metaStore:         NewMetaStore(config.BlockAddrs),
		log:               make([]*UpdateOperation, 0),

		id:          id,
		commitIndex: -1,
		lastApplied: -1,

		nextIndex:  make([]int64, len(config.RaftAddrs)),
		matchIndex: make([]int64, len(config.RaftAddrs)),

		unreachableFrom: make(map[int64]bool),
		grpcServer:      grpc.NewServer(),
		rpcConns:        conns,

		raftStateMutex: &raftStateMutex,
		peers:          config.RaftAddrs,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	RegisterRaftSurfstoreServer(server.grpcServer, server)

	log.Println("Successfully started the RAFT server with id:", server.id)
	l, e := net.Listen("tcp", server.peers[server.id])

	if e != nil {
		return e
	}

	return server.grpcServer.Serve(l)
}

// Check status of machine here and return different errors
func (s *RaftSurfstore) checkStatus() error {
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED {
		return ErrServerCrashed
	}

	if serverStatus != ServerStatus_LEADER {
		return ErrNotLeader
	}

	return nil
}

// Upload difference between lastApplied and commitIndex to upload to metaStore
func (s *RaftSurfstore) uploadToMetaStore(ctx context.Context) {
	for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
		if s.log[i].FileMetaData != nil {
			version, _ := s.metaStore.UpdateFile(ctx, s.log[i].FileMetaData)
			// File has not been updated properly
			if version.Version == -1 {
				break
			}
		}
		s.lastApplied++
	}
}

// Make append Entries input for hearBeats
func (s *RaftSurfstore) CreateAppendEntries(ctx context.Context, serverId int, noOp bool, entries *AppendEntryInput) {
	s.raftStateMutex.RLock()
	defer s.raftStateMutex.RUnlock()

	entries.Term = s.term
	entries.LeaderId = s.id
	entries.PrevLogIndex = s.nextIndex[serverId] - 1
	if entries.PrevLogIndex >= 0 {
		entries.PrevLogTerm = s.log[entries.PrevLogIndex].Term
	}

	if !noOp {
		entries.Entries = s.log[s.nextIndex[serverId]:]
	}
	entries.LeaderCommit = s.commitIndex
}
