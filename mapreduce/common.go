package mapreduce

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

// Set if the debugging is enabled or not
const debugEnabled = false

const resultsDirectory = "tmp"

// Prints the result only if `debugEnabled` is true
func debug(format string, a ...any) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// Indicates whether a task is scheduled as a map or reduce task
type jobPhase string

const (
	mapPhase    = "Map"
	reducePhase = "Reduce"
)

type KeyValue struct {
	Key   string
	Value string
}

// Constructs the name of the intermediate file which map task `mapTask` produces for reduce task `reduceTask`
func reduceName(jobName string, mapTask int, reduceTask int) string {
	fileName := "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
	return getFilePath(fileName)
}

// Constructs the name of the output file of reduce task `reduceTask`
func mergeName(jobName string, reduceTask int) string {
	fileName := "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
	return getFilePath(fileName)

}

// Checks if the intermediate result directory exists and creates the same if it doesn't
func makeMapResultDirectory() {
	if _, err := os.Stat(resultsDirectory); os.IsNotExist(err) {
		err := os.Mkdir(resultsDirectory, 0777)
		if err != nil {
			log.Fatalf("doMap can't create intermediate result directory `%s` which is needed to store the map results: %v", resultsDirectory, err)
		}
	}
}

func getFilePath(name string) string {
	return filepath.Join(resultsDirectory, name)
}
