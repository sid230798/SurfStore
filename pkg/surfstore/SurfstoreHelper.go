package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
	fileName TEXT, 
	version INT,
	hashIndex INT,
	hashValue TEXT
);`

const insertTuple string = `INSERT INTO indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	statement, err = db.Prepare(insertTuple)
	if err != nil {
		log.Fatal("Error Preparing the insert statement")
	}
	for _, fileMeta := range fileMetas {
		for i, blockHash := range fileMeta.BlockHashList {
			_, err := statement.Exec(fileMeta.Filename, fileMeta.Version, i, blockHash)
			if err != nil {
				log.Fatal("Error During Meta Write Back")
			}
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `SELECT DISTINCT(fileName) from indexes;`

const getTuplesByFileName string = `SELECT version, hashValue from indexes WHERE fileName = ? ORDER BY hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	/* Execute create if not exists comamnd */
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	/* Get all the rows from data and create FileMetaData */
	fileNames, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Fatal("Error When Querying the data")
	}
	defer fileNames.Close()
	for fileNames.Next() {
		var fileName string
		err = fileNames.Scan(&fileName)
		if err != nil {
			log.Fatal("Error When Retrieving Query")
		}

		/* Initialise the fileMetaMap for fileName here */
		fileMetaMap[fileName] = &FileMetaData{Filename: fileName}

		/* Get rows for the fileName and populate fileMetaMap */
		tupleRows, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			log.Fatal("Error During executing query tuple statement")
		}

		for tupleRows.Next() {
			var Version int32
			var BlockHashValue string
			err = tupleRows.Scan(&Version, &BlockHashValue)
			if err != nil {
				log.Fatal("Error When Retrieving Query")
			}
			fileMetaMap[fileName].Version = Version
			fileMetaMap[fileName].BlockHashList = append(fileMetaMap[fileName].BlockHashList, BlockHashValue)
		}
		tupleRows.Close()
	}

	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
