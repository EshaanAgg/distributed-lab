package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
)

// Manages one map task:
// It reads one input file `inFile`, calls the user-defined map function `mapF` for
// the file's contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string,
	mapTaskNumber int,
	inFile string,
	nReduce int,
	mapF func(file string, contents string) []KeyValue,
) {
	// Read the file inFile from disk
	contents, err := os.ReadFile(inFile)
	if err != nil {
		log.Fatalf("doMap can't read the provided input file %v: %v", inFile, err)
	}

	kv := mapF(inFile, string(contents))

	reduceTaskMaps := make(map[int][]KeyValue, 0)

	for _, kv := range kv {
		reduceTask := ihash(kv.Key) % nReduce
		reduceTaskMaps[reduceTask] = append(reduceTaskMaps[reduceTask], kv)
	}

	makeMapResultDirectory()
	for reduceTask, kv := range reduceTaskMaps {
		writeMapResultToDisk(jobName, mapTaskNumber, reduceTask, kv)
	}
}

// Produces the hash value of a string `s`
func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

// Writes the intermediate result to disk
func writeMapResultToDisk(jobName string, mapTaskNumber int, reduceTask int, kv []KeyValue) {
	intermediateFileName := reduceName(jobName, mapTaskNumber, reduceTask)
	intermediateFile, err := os.OpenFile(
		intermediateFileName,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0666,
	)

	if err != nil {
		log.Fatalf("doMap can't open intermediate result file: %v", err)
	}
	defer intermediateFile.Close()

	enc := json.NewEncoder(intermediateFile)
	err = enc.Encode(&kv)
	if err != nil {
		log.Fatalf("doMap can't encode the resulting key-value pairs to JSON while writing to file: %v", err)
	}
}
