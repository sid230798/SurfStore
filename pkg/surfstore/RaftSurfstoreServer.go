package surfstore

import (
	context "context"
	"log"
	"math"
	"sync"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	serverStatus      ServerStatus
	serverStatusMutex *sync.RWMutex
	term              int64
	log               []*UpdateOperation
	id                int64
	metaStore         *MetaStore
	commitIndex       int64
	lastApplied       int64
	nextIndex         []int64
	matchIndex        []int64

	raftStateMutex *sync.RWMutex

	rpcConns   []*grpc.ClientConn
	grpcServer *grpc.Server
	peers      []string

	/*--------------- Chaos Monkey --------------*/
	unreachableFrom map[int64]bool
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Ensure that the majority of servers are up
	success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if !success.Flag || err != nil {
		return nil, err
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// Ensure that the majority of servers are up
	success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if !success.Flag || err != nil {
		return nil, err
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)

}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Ensure that the majority of servers are up
	success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if !success.Flag || err != nil {
		return nil, err
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)

}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// Ensure that the request gets replicated on majority of the servers.
	// Commit the entries and then apply to the state machine
	serverStatus := s.checkStatus()
	if serverStatus != nil {
		return nil, serverStatus
	}

	s.raftStateMutex.RLock()
	updateOperation := &UpdateOperation{Term: s.term, FileMetaData: filemeta}
	s.log = append(s.log, updateOperation)
	reqIndex := len(s.log) - 1
	log.Printf("Update operation has been called on server %d with reqIndex %d", s.id, reqIndex)
	s.raftStateMutex.RUnlock()

	// Keep on trying till we apply this log index on state machine
	for {
		// Send append entries call
		_, err := s.SendPersistentHeartbeats(ctx, false)
		if err != nil {
			return nil, err
		}

		s.raftStateMutex.RLock()
		// Only break and return metastore update, only if we applied this reqIndex on last applied
		if s.lastApplied >= int64(reqIndex) {
			s.raftStateMutex.RUnlock()
			break
		}
		s.raftStateMutex.RUnlock()
	}

	// Now, we know majority has voted and this request has been applied to statemachine
	// So, call getFileInfo map and return the version
	if filemeta != nil {
		fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
		return &Version{Version: fileInfoMap.FileInfoMap[filemeta.Filename].Version}, nil
	} else {
		// This represents the no-op log push
		return &Version{Version: 0}, nil
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex or whose term
// doesn't match prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	serverStatus := s.checkStatus()
	if serverStatus == ErrServerCrashed || s.unreachableFrom[input.LeaderId] {
		return &AppendEntryOutput{Term: s.term, ServerId: s.id, Success: false, MatchedIndex: -1}, ErrServerCrashedUnreachable
	}
	log.Printf("Got the append entries call for server %d with input as %s", s.id, input)
	// Case 1: term < currentTerm
	if input.Term < s.term {
		return &AppendEntryOutput{Term: s.term, ServerId: s.id, Success: false, MatchedIndex: -1}, nil
	} else {
		// Change the status of this node, to follower and change the term of node as well
		s.serverStatusMutex.RLock()
		s.serverStatus = ServerStatus_FOLLOWER
		s.serverStatusMutex.RUnlock()

		s.raftStateMutex.RLock()
		s.term = input.Term
		s.raftStateMutex.RUnlock()
	}

	// Case 2: Now check the prevLogIndex and term conditions
	s.raftStateMutex.RLock()
	if input.PrevLogIndex >= int64(len(s.log)) || (input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm) {
		return &AppendEntryOutput{Term: s.term, ServerId: s.id, Success: false, MatchedIndex: -1}, nil
	}
	defer s.raftStateMutex.RUnlock()

	// Case 3: Now check for prevLogIndex + 1, and delete entries with terms not matching
	// Can directly copy but will implement in loopy way
	// This also signifies loop invariant, if same index and same term it will have same value
	if len(input.Entries) > 0 { // Can be a no_op operation
		k := int(input.PrevLogIndex + 1)
		for ; k < len(s.log); k++ {
			if s.log[k].Term != input.Entries[k-int(input.PrevLogIndex+1)].Term {
				s.log = s.log[:k]
			}
		}

		// Case 4: Now, copy the rest of the entries
		for ; k < int(input.PrevLogIndex+1)+len(input.Entries); k++ {
			s.log = append(s.log, input.Entries[k-int(input.PrevLogIndex+1)])
		}
	}

	// Case 5: Set the commitIndex according to leader's
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}

	s.uploadToMetaStore(ctx)
	// Need to use matchedIndex of the log replicated, will be used to take the majority vote in leader
	return &AppendEntryOutput{Term: s.term, ServerId: s.id, Success: true, MatchedIndex: int64(len(s.log) - 1)}, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	serverStatus := s.checkStatus()
	if serverStatus == ErrServerCrashed {
		return &Success{Flag: false}, ErrServerCrashed
	}

	s.serverStatusMutex.RLock()
	s.serverStatus = ServerStatus_LEADER
	log.Printf("Server %d has been set as a leader", s.id)
	s.serverStatusMutex.RUnlock()

	// Increment the term for the leader
	s.raftStateMutex.RLock()
	s.term += 1
	log.Printf("Server %d has been set as a leader with term %d", s.id, s.term)

	// Set the nextIndex and matchIndex and set the intial values
	// As setLeader is being only called by test we can ignore locking
	for id := range s.peers {
		s.nextIndex[id] = int64(len(s.log))
		s.matchIndex[id] = -1
	}
	s.raftStateMutex.RUnlock()

	// Send a no-op heartbeat to every other server
	// Assuming, setLeader will be called at the time, when majority is up
	_, err := s.UpdateFile(ctx, nil) // No-op push and wait till it commits
	if err != nil {
		return &Success{Flag: false}, err
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, peerId int64, noOp bool, peerResponses chan<- bool) {
	client := NewRaftSurfstoreClient(s.rpcConns[peerId])
	entries := &AppendEntryInput{}
	s.CreateAppendEntries(ctx, int(peerId), noOp, entries)

	// Send to AppendEntries
	reply, err := client.AppendEntries(ctx, entries)
	if err != nil {
		// This specifies server has crashed.
		// We can retry in these but for now send false
		peerResponses <- false
	} else if reply.Success {
		// This represents that append entries was successful
		s.raftStateMutex.RLock()
		s.nextIndex[reply.ServerId] = reply.MatchedIndex + 1
		s.matchIndex[reply.ServerId] = reply.MatchedIndex
		s.raftStateMutex.RUnlock()

		peerResponses <- true
	} else if reply.Term > s.term {
		// Leader has changed, make this a follower
		s.serverStatusMutex.RLock()
		s.serverStatus = ServerStatus_FOLLOWER
		s.serverStatusMutex.RUnlock()
		peerResponses <- false
	} else {
		// This will be the case where we need to keep reducing the index and resend the append entries
		s.raftStateMutex.RLock()
		s.nextIndex[reply.ServerId] -= 1
		s.raftStateMutex.RUnlock()
		peerResponses <- false
	}
}

// Send Persistent Heartbeats
func (s *RaftSurfstore) SendPersistentHeartbeats(ctx context.Context, noOp bool) (*Success, error) {
	serverStatus := s.checkStatus()
	// Proceed only if leader
	if serverStatus != nil {
		return &Success{Flag: false}, serverStatus
	}

	numServers := len(s.peers)
	peerResponses := make(chan bool, numServers-1)
	// Iterate through peers and send parallel requests
	for idx := range s.peers {
		if idx == int(s.id) {
			continue
		}
		go s.sendToFollower(ctx, int64(idx), noOp, peerResponses)
	}

	// We wait till we get responses back from servers
	// Use numAliveServers to send Success or not
	totalResponses := 1
	numAliveServers := 1
	for totalResponses < numServers {
		response := <-peerResponses
		totalResponses += 1
		if response {
			numAliveServers += 1
		}
	}

	if serverStatus := s.checkStatus(); serverStatus != nil {
		return &Success{Flag: false}, serverStatus
	}

	s.raftStateMutex.RLock()
	// set commitIndex, if logs match the current term then,
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	for N := len(s.log) - 1; N > int(s.commitIndex); N-- {
		if s.log[N].Term != s.term {
			continue
		}
		cnt := 1
		for i := range s.matchIndex {
			if s.matchIndex[i] >= int64(N) {
				cnt++
			}
		}
		if cnt > numServers/2 {
			s.commitIndex = int64(N)
			break
		}
	}
	s.uploadToMetaStore(ctx)
	s.raftStateMutex.RUnlock()

	// Check if majority servers are alive then
	if numAliveServers > numServers/2 {
		return &Success{Flag: true}, nil
	} else {
		return &Success{Flag: false}, nil
	}
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	serverStatus := s.checkStatus()
	// Proceed only if leader
	if serverStatus != nil {
		return &Success{Flag: false}, serverStatus
	}

	// Keep on sending heartbeats till we get majority
	for {
		success, err := s.SendPersistentHeartbeats(ctx, false)
		if success.Flag {
			break
		} else if err != nil {
			return success, err
		}
	}

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) NoOpHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	serverStatus := s.checkStatus()
	// Proceed only if leader
	if serverStatus != nil {
		return &Success{Flag: false}, serverStatus
	}

	// Keep on sending heartbeats till we get majority
	for {
		success, err := s.SendPersistentHeartbeats(ctx, true)
		if success.Flag {
			break
		} else if err != nil {
			return success, err
		}
	}

	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) MakeServerUnreachableFrom(ctx context.Context, servers *UnreachableFromServers) (*Success, error) {
	s.raftStateMutex.Lock()
	for _, serverId := range servers.ServerIds {
		s.unreachableFrom[serverId] = true
	}
	log.Printf("Server %d is unreachable from", s.unreachableFrom)
	s.raftStateMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_CRASHED
	log.Printf("Server %d is crashed", s.id)
	s.serverStatusMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_FOLLOWER
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.unreachableFrom = make(map[int64]bool)
	s.raftStateMutex.Unlock()

	log.Printf("Server %d is restored to follower and reachable from all servers", s.id)

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.serverStatusMutex.RLock()
	s.raftStateMutex.RLock()
	state := &RaftInternalState{
		Status:      s.serverStatus,
		Term:        s.term,
		CommitIndex: s.commitIndex,
		Log:         s.log,
		MetaMap:     fileInfoMap,
	}
	s.raftStateMutex.RUnlock()
	s.serverStatusMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
