package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

// Combines the results of reduce jobs into a single output file
func (mr *Master) merge() {
	debug("Merge phase started\n")

	allKeyValues := make([]KeyValue, 0)

	for i := 0; i < mr.nReduce; i++ {
		p := mergeName(mr.jobName, i)
		debug("Merge: reading file %s\n", p)

		file, err := os.Open(p)
		if err != nil {
			log.Fatalf("Merge: failed to open the output file %s: %v", p, err)
		}

		kvs := make([]KeyValue, 0)

		dec := json.NewDecoder(file)
		err = dec.Decode(&kvs)
		if err != nil {
			log.Fatalf("Merge: failed to JSON decode the output file %s: %v", p, err)
		}

		allKeyValues = append(allKeyValues, kvs...)

		file.Close()
	}

	sort.Slice(allKeyValues, func(i, j int) bool {
		return allKeyValues[i].Key < allKeyValues[j].Key
	})

	file, err := os.Create(getFilePath("mrtmp." + mr.jobName))
	if err != nil {
		log.Fatalf("Merge: failed to create the final result file: %v", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range allKeyValues {
		fmt.Fprintf(w, "%s: %s\n", k.Key, k.Value)
	}
	w.Flush()
	file.Close()
}

// A simple wrapper around `os.Remove()` that logs errors
func removeFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatalf("Cleanup Files failed: %v", err)
	}
}

// Removes all intermediate files produced by running Map-Reduce
func (mr *Master) CleanupFiles() {
	for i := range mr.files {
		for j := 0; j < mr.nReduce; j++ {
			removeFile(reduceName(mr.jobName, i, j))
		}
	}

	for i := 0; i < mr.nReduce; i++ {
		removeFile(mergeName(mr.jobName, i))
	}

	removeFile(getFilePath("mrtmp." + mr.jobName))
}
