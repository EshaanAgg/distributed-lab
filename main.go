package main

import (
	"log"
	"os"

	"github.com/eshaanagg/distributed/mapreduce"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Please specify the program suite you want to run.")
	}

	if os.Args[1] == "mapreduce" {
		mapreduce.Main(os.Args[2:])
	} else {
		log.Fatalf("Unknown program suite: %s\n", os.Args[1])
	}
}
