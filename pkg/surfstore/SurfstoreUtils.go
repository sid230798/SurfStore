package surfstore

import (
	"log"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

func getNumberOfBlocks(fileSize float64, blockSize float64) int {
	return int(math.Ceil(fileSize / blockSize))
}

func GetBlockStoreAddr(client RPCClient, blockHashIn string) string {
	blockStoreMap := make(map[string][]string)
	err := client.GetBlockStoreMap([]string{blockHashIn}, &blockStoreMap)
	if err != nil {
		log.Fatal("Error connecting to metastore : ", err)
	}
	/* BlockStore map recieved will have only 1 element. So extract that key and return that blockStoreaddress */
	for blockStoreAddr, blockHashes := range blockStoreMap {
		if len(blockHashes) == 1 && blockHashes[0] == blockHashIn {
			return blockStoreAddr
		}
	}
	log.Fatal("Not a possible scenario in our usecase....")
	return ""
}

// Implementing the uploading local to remote directory fileMeta and blocks
func UploadToRemote(client RPCClient, fileMeta *FileMetaData) int32 {
	filePath, _ := filepath.Abs(ConcatPath(client.BaseDir, fileMeta.Filename))
	var latestVersion int32
	readFile, err := os.Open(filePath)
	if err != nil {
		// This shows, file is deleted in base directory. So, upload the meta
		err = client.UpdateFile(fileMeta, &latestVersion)
		if err != nil {
			log.Fatal("Error updating file: ", err)
		}
		return latestVersion
	}
	defer readFile.Close()

	fileInfo, err := readFile.Stat()
	if err != nil {
		log.Println("Error getting file info: ", err)
	}

	numBlocks := getNumberOfBlocks(float64(fileInfo.Size()), float64(client.BlockSize))

	/* Read the file in chunks and create hash of it and store in indexing map */
	for i := 0; i < numBlocks; i++ {
		block := make([]byte, client.BlockSize)
		n, err := readFile.Read(block)
		if err != nil {
			log.Println("Error reading file: ", err)
		}
		block = block[:n]

		/* Put this block in BlockStore */
		var succ bool
		blockStoreAddr := GetBlockStoreAddr(client, GetBlockHashString(block))
		err = client.PutBlock(&Block{BlockData: block, BlockSize: int32(n)}, blockStoreAddr, &succ)
		if err != nil {
			log.Println("Put block failed: ", err)
		}
	}

	// No need to upload to block store if file is empty only meta data needs to be uploaded
	// // Handling empty file differently
	// if numBlocks == 0 {
	// 	/* Put this block in BlockStore */
	// 	var succ bool
	// 	blockStoreAddr := GetBlockStoreAddr(client, GetBlockHashString(nil))
	// 	err = client.PutBlock(&Block{BlockData: nil, BlockSize: int32(0)}, blockStoreAddr, &succ)
	// 	if err != nil {
	// 		log.Println("Put block failed: ", err)
	// 	}
	// }

	/* Now upload it to metaStore */
	err = client.UpdateFile(fileMeta, &latestVersion)
	if err != nil {
		log.Panic("Connection paused or broken")
	}

	return latestVersion
}

// Implementing the downloading function from remote to local server here
func DownloadToLocal(client RPCClient, remoteFileMeta *FileMetaData) {
	filePath, _ := filepath.Abs(ConcatPath(client.BaseDir, remoteFileMeta.Filename))
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal("Error creating file: ", err)
	}
	defer file.Close()

	// This sees if file was deleted in remote server
	if remoteFileMeta.BlockHashList[0] == TOMBSTONE_HASHVALUE {
		err := os.Remove(filePath)
		if err != nil {
			log.Fatal("Error removing file: ", err)
		}
	} else if remoteFileMeta.BlockHashList[0] == EMPTYFILE_HASHVALUE {
		// No need to do anything. Just return
		return
	} else {
		data := ""
		for _, blockHash := range remoteFileMeta.BlockHashList {
			blockStoreAddr := GetBlockStoreAddr(client, blockHash)

			var block Block
			err := client.GetBlock(blockHash, blockStoreAddr, &block)
			if err != nil {
				log.Fatal("Error getting block: ", err)
			}
			data += string(block.BlockData)
		}
		file.WriteString(data)
	}

}

// Get the remoteMeta for a particular fileName
func HandleUpdateFail(client RPCClient, fileName string) *FileMetaData {
	/*
	* 1. Now load remote index map and get the particular fileMeta
	 */
	remoteIndexMap := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndexMap); err != nil {
		log.Fatal("error recieving remote file info ", err)
	}

	/* Now check for fileMetaData */
	if remoteMetaData, ok := remoteIndexMap[fileName]; ok {
		DownloadToLocal(client, remoteMetaData)
		return remoteMetaData
	} else {
		log.Fatalf("Not a possible scenario unless server is shut down!!")
		return nil
	}
}

// Validation routine to check client and server are in complete sync
func ValidateClientServer(client RPCClient) {
	/* Step 1: Read from index.db and save it in clientIndexMap */
	clientIndexMap, _ := LoadMetaFromMetaFile(client.BaseDir)

	/* Step 2: Read server meta map */
	serverIndexMap := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&serverIndexMap); err != nil {
		log.Fatal("error recieving remote file info ", err)
	}

	/*Check for equality*/
	if reflect.DeepEqual(serverIndexMap, clientIndexMap) {
		log.Printf("Client Server are in perfect Sync.......")
	} else {
		log.Printf("Something went wrong......Verify maps printing below")
		log.Println("Client Index Map :")
		PrintMetaMap(clientIndexMap)
		log.Println("Server Index Map :")
		PrintMetaMap(serverIndexMap)

	}
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	/* Step 1: First check if index file is present in base directory */
	indexFilePath, _ := filepath.Abs(ConcatPath(client.BaseDir, DEFAULT_META_FILENAME))
	_, err := os.Stat(indexFilePath)

	if os.IsNotExist(err) {
		indexFile, err := os.Create(indexFilePath)
		if err != nil {
			log.Fatal(err)
		}
		defer indexFile.Close()
	}
	/* Step 2: Read from index.db and save it in localIndexMap */
	localIndexMap, _ := LoadMetaFromMetaFile(client.BaseDir)

	/* Step 3: Read all the entries in the directory and create dirIndexMap */
	dirIndexMap := make(map[string][]string)
	entries, err := os.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal("Error reading BaseDir: ", err)
	}

	for _, entry := range entries {

		// Ignore index.db file
		// Ignore files having forward slash or , in the name
		if entry.Name() == DEFAULT_META_FILENAME || strings.ContainsAny(entry.Name(), ",/") {
			continue
		}

		fileInfo, _ := entry.Info()
		numBlocks := getNumberOfBlocks(float64(fileInfo.Size()), float64(client.BlockSize))

		filePath, _ := filepath.Abs(ConcatPath(client.BaseDir, entry.Name()))
		readFile, err := os.Open(filePath)
		if err != nil {
			log.Fatal("Error opening file: ", err)
		}
		defer readFile.Close()

		/* Read the file in chunks and create hash of it and store in indexing map */
		for i := 0; i < numBlocks; i++ {
			block := make([]byte, client.BlockSize)
			n, err := readFile.Read(block)
			if err != nil {
				log.Println("Error reading file: ", err)
			}
			block = block[:n]
			hash := GetBlockHashString(block)
			dirIndexMap[entry.Name()] = append(dirIndexMap[entry.Name()], hash)
		}

		// Adding empty file as well
		if numBlocks == 0 {
			// This reflects as empty file
			dirIndexMap[entry.Name()] = []string{EMPTYFILE_HASHVALUE}
		}

		/*
		* Now this can be a new file or modified from before.
		* 1. If modified, check for complete hashList and add 1 in new version
		* 2. IF new, add a new entry in localIndexMap with version 1
		 */
		if localFileHash, ok := localIndexMap[entry.Name()]; ok {
			if !reflect.DeepEqual(localFileHash.BlockHashList, dirIndexMap[entry.Name()]) { // If file modified
				localIndexMap[entry.Name()].BlockHashList = dirIndexMap[entry.Name()]
				localIndexMap[entry.Name()].Version++ // Increase the version for update
			}
		} else {
			// Added new file with version 1
			localIndexMap[entry.Name()] = &FileMetaData{Filename: entry.Name(), Version: int32(VERSION_INDEX), BlockHashList: dirIndexMap[entry.Name()]}
		}
	}

	/*
	* There can be a scenario, where file will be deleted.
	* So, check for such files and set hash to 0, and increment the version
	 */
	for fileName, fileMeta := range localIndexMap {
		if _, ok := dirIndexMap[fileName]; !ok {
			// This check will ensure, this entry is not already deleted
			if fileMeta.BlockHashList[0] != TOMBSTONE_HASHVALUE {
				localIndexMap[fileName].BlockHashList = []string{TOMBSTONE_HASHVALUE}
				localIndexMap[fileName].Version++
			}
		}
	}

	/*
	* 1. Now load remote index map and verify the changes with localIndexMap
	 */
	remoteIndexMap := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndexMap); err != nil {
		log.Fatal("error recieving remote file info ", err)
	}

	/*
		* 1. Step 4: Now check for new file additions in remote directory :
			a. It can either be new file
			b. It can be a modified file
			Download this data to base directory
	*/
	for fileName, remoteFileMeta := range remoteIndexMap {
		if localFileMeta, ok := localIndexMap[fileName]; ok {
			if remoteFileMeta.Version >= localFileMeta.Version {
				// This case, where file has modified on remote directory
				// download content on base directory
				// Download only in case if update is different or just update the version
				if !reflect.DeepEqual(remoteFileMeta.BlockHashList, localFileMeta.BlockHashList) {
					DownloadToLocal(client, remoteFileMeta)
				}
				localIndexMap[fileName] = remoteFileMeta
			}
		} else {
			DownloadToLocal(client, remoteFileMeta)
			localIndexMap[fileName] = remoteFileMeta
		}
	}

	/*
		* 1. Step 5: Now check for new file additions in base directory :
			a. It can either be new file
			b. It can be a modified file
			Upload this data to remote server

		* 2. Now, if multiple clients uploading and upload fails, we return version as -1.
			Now load meta for this particular file and download the data
	*/
	for fileName, localFileMeta := range localIndexMap {
		if remoteFileMeta, ok := remoteIndexMap[fileName]; ok {
			if localFileMeta.Version > remoteFileMeta.Version {
				// This means file has been modified on local
				// Define upload function to upload this blocks to remote map
				latestVersion := UploadToRemote(client, localFileMeta)
				if latestVersion == -1 {
					remoteFileMetaNew := HandleUpdateFail(client, fileName)
					localIndexMap[fileName] = remoteFileMetaNew
				} else {
					localIndexMap[fileName].Version = latestVersion
				}
			}
		} else {
			// This case constitutes new file in local base directory
			// Upload new file blocks and update meta map
			latestVersion := UploadToRemote(client, localFileMeta)
			if latestVersion == -1 {
				remoteFileMetaNew := HandleUpdateFail(client, fileName)
				localIndexMap[fileName] = remoteFileMetaNew
			} else {
				localIndexMap[fileName].Version = latestVersion
			}
		}
	}

	// Finally, write content to index.db
	WriteMetaFile(localIndexMap, client.BaseDir)

	// Validation Client and Server must be in complete Sync
	ValidateClientServer(client)
}
