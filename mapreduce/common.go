package mapreduce

import (
	"fmt"
	"path/filepath"
	"strconv"
)

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

func getFilePath(name string) string {
	return filepath.Join(resultsDirectory, name)
}
