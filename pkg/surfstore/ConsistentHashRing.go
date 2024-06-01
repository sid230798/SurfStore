package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

/* Need to return successor for the blockId */
func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {

	/* Get the sorted hash list of server keys */
	serverHashKeyList := []string{}
	for hashKey := range c.ServerMap {
		serverHashKeyList = append(serverHashKeyList, hashKey)
	}
	sort.Strings(serverHashKeyList)

	/* Now search for the index just greater than blockHash */
	n := len(serverHashKeyList)
	index := sort.Search(n, func(i int) bool {
		return serverHashKeyList[i] >= blockId
	})

	// Cycle back to 0th index as it's consistent ring hash.
	// sort.Search return n if func is not satisfied
	if index == n {
		index = 0
	}

	return c.ServerMap[serverHashKeyList[index]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

/* Just the server address prefixed with "blockstore" */
func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	addrPrefix := "blockstore"
	consistentHashRing := ConsistentHashRing{ServerMap: make(map[string]string)}
	for _, serverAddr := range serverAddrs {
		serverHash := consistentHashRing.Hash(fmt.Sprintf("%s%s", addrPrefix, serverAddr))
		consistentHashRing.ServerMap[serverHash] = serverAddr
	}
	return &consistentHashRing
}
