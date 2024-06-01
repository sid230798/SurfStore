package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fileName := fileMetaData.Filename
	// File may be present initially
	if prevMeta, ok := m.FileMetaMap[fileName]; ok {
		// Check if new version is exactly 1 greater than cloud version, then update the map
		if fileMetaData.Version-prevMeta.Version == 1 {
			m.FileMetaMap[fileName] = fileMetaData
			return &Version{Version: fileMetaData.Version}, nil
		} else {
			// Inconsistent, version is too old. Return -1
			return &Version{Version: -1}, nil
		}
	}

	// If file name is correct and its a new file add it to metamap
	m.FileMetaMap[fileName] = fileMetaData
	return &Version{Version: fileMetaData.Version}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := &BlockStoreMap{BlockStoreMap: make(map[string]*BlockHashes)}

	/*Iterate through the hashes and get the responsible block store address*/
	for _, blockHash := range blockHashesIn.Hashes {
		serverAddr := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		if _, ok := blockStoreMap.BlockStoreMap[serverAddr]; !ok {
			blockStoreMap.BlockStoreMap[serverAddr] = &BlockHashes{Hashes: []string{}}
		}
		blockStoreMap.BlockStoreMap[serverAddr].Hashes = append(blockStoreMap.BlockStoreMap[serverAddr].Hashes, blockHash)
	}

	return blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
