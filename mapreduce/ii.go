package mapreduce

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"unicode"

	"github.com/eshaanagg/distributed/utils"
)

// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func iiMapF(filePath string, content string) (res []KeyValue) {
	keyValueMap := make(map[string]string)

	f := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}
	words := strings.FieldsFunc(content, f)

	for _, word := range words {
		// Only add the filename after the /, not the entire path
		document := filePath[strings.LastIndex(filePath, "/")+1:]
		keyValueMap[word] = document
	}

	kvs := make([]KeyValue, 0)
	for k, v := range keyValueMap {
		kvs = append(kvs, KeyValue{k, v})
	}

	return kvs
}

func iiReduceF(key string, values []string) string {
	sort.Strings(values)
	return fmt.Sprintf("%d %s", len(values), strings.Join(values, ","))
}

func IIMain(args []string) {
	if len(args) < 3 {
		fmt.Printf("Not enough arguments passed from the command line. Expected at least 3, got %d\n", len(args))
	} else if args[0] == "master" {
		var mr *Master
		filePaths := args[2:]

		err := utils.EmptyDirectory(resultsDirectory)
		if err != nil {
			log.Fatalf("Failed as the results directory couldn't be cleaned: %v", err)
		}

		if args[1] == "sequential" {
			mr = Sequential("iiwc", filePaths, 3, iiMapF, iiReduceF)
		} else {
			mr = Distributed("iiwc", filePaths, 3, args[1])
		}
		mr.Wait()
	} else {
		RunWorker(args[2], args[3], iiMapF, iiReduceF, 100)
	}
}
