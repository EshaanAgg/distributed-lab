package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"strings"
)

// Manages one reduce task:
// Reads the intermediate key/value pairs (produced by the map phase), sorts them
// by key, calls the user-defined reduce function `reduceF` for each key, and writes the output to disk.
func doReduce(
	jobName string,
	reduceTaskNumber int,
	outFile string,
	nMap int,
	reduceF func(key string, values []string) string,
) {
	allKeyValues := make(map[string][]string)

	for mapTask := 0; mapTask < nMap; mapTask++ {
		// Read the content of the apporpriate intermediate file and convert it to a slice of KeyValue
		inFile := reduceName(jobName, mapTask, reduceTaskNumber)
		contents, err := os.ReadFile(inFile)
		if err != nil {
			log.Fatalf("doReduce can't read the file %v: %v", inFile, err)
		}

		decoder := json.NewDecoder(strings.NewReader(string(contents)))
		kv := make([]KeyValue, 0)
		err = decoder.Decode(&kv)
		if err != nil {
			log.Fatalf("doReduce can't decode the contents of the file %v: %v", inFile, err)
		}

		// For each key-value pair, add all the value to the appropiate global state
		for _, pair := range kv {
			_, exists := allKeyValues[pair.Key]
			if !exists {
				allKeyValues[pair.Key] = make([]string, 0)
			}
			allKeyValues[pair.Key] = append(allKeyValues[pair.Key], pair.Value)
		}
	}

	// Get the reduced values for all the keys
	reducedKeyValues := make([]KeyValue, 0)
	for key := range allKeyValues {
		reducedKeyValues = append(
			reducedKeyValues,
			KeyValue{
				Key:   key,
				Value: reduceF(key, allKeyValues[key]),
			},
		)
	}

	// Write the key-value pairs to temporary file
	writeFilePath := mergeName(jobName, reduceTaskNumber)
	writeFile, err := os.OpenFile(
		writeFilePath,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0666,
	)

	if err != nil {
		log.Fatalf("doReduce can't open result file: %v", err)
	}
	defer writeFile.Close()

	enc := json.NewEncoder(writeFile)
	enc.Encode(reducedKeyValues)
}
