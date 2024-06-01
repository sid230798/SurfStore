package surfstore

import (
	context "context"
	"fmt"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

/*
* GetBlock returns corresponding block to provided hash. Returns an error if hash is not present
 */
func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	block, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		// Need to verify the cases if asked for hash value present or not
		// But this case should never happen
		log.Printf("hash not found in blockstore...")
		return nil, fmt.Errorf("hash not found in the blockstore")
	}
	return block, nil
}

/*
* PutBlock will add the new block in the block store by generating it's hash key
 */
func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// Generate the hash of the block and add it to block store
	// Hopefully, hash collision will not take place
	bs.BlockMap[GetBlockHashString(block.BlockData)] = block
	return &Success{Flag: true}, nil
}

/*
* Given a list of hashes “in”, returns a list containing the hashes that are not stored in the key-value store.
 */

func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var hashesOut []string
	for _, blockHash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[blockHash]; !ok {
			hashesOut = append(hashesOut, blockHash)
		}
	}
	return &BlockHashes{Hashes: hashesOut}, nil
}

/*
* Return a list containing all blockHashes on this block server
 */
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	var blockHashes []string
	for blockHash := range bs.BlockMap {
		blockHashes = append(blockHashes, blockHash)
	}
	return &BlockHashes{Hashes: blockHashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
