package surfstore

import (
	context "context"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	Success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}

	*succ = Success.Flag
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) MissingBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	BlockHashesOut, err := c.MissingBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}

	*blockHashesOut = BlockHashesOut.Hashes
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	BlockHashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = BlockHashes.Hashes
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		FileInfoData, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})

		// Now, we can recieve either server is not leader or server is crash or context timeouts
		// So, just verify when err is nil
		if err == nil {
			*serverFileInfoMap = FileInfoData.FileInfoMap
			// close the connection
			return conn.Close()
		}
		conn.Close()
	}
	return fmt.Errorf("all servers crashed or unreachable or leader not set")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		Version, err := c.UpdateFile(ctx, fileMetaData)
		if err == nil {
			*latestVersion = Version.Version
			// close the connection
			return conn.Close()
		}
		conn.Close()
	}
	return fmt.Errorf("all servers crashed or unreachable or leader not set")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		BlockStoreMap, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err == nil {
			for serverAddr, blockHashes := range BlockStoreMap.BlockStoreMap {
				(*blockStoreMap)[serverAddr] = blockHashes.Hashes
			}
			// close the connection
			return conn.Close()
		}
		conn.Close()
	}
	return fmt.Errorf("all servers crashed or unreachable or leader not set")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		BlockStoreAddrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err == nil {
			*blockStoreAddrs = BlockStoreAddrs.BlockStoreAddrs
			// close the connection
			return conn.Close()
		}
		conn.Close()
	}
	return fmt.Errorf("all servers crashed or unreachable or leader not set")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
