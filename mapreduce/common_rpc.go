package mapreduce

import (
	"fmt"
	"net/rpc"
)

// Holds the arguments that are passed to a worker when a job is scheduled on it
type DoTaskArgs struct {
	JobName    string
	File       string   // Stored only for the Map phase; Represents the input file
	Phase      jobPhase // Shows whether the task is a Map or Reduce task
	TaskNumber int      // The index of the task in the current phase

	// It is the total number of tasks in other phase
	// mappers use this to compute the number of output bins
	// reducers use this to know how many input files to collect
	NumOtherPhase int
}

// It is the response to a WorkerShutdown.
// It holds the number of tasks this worker has processed since it was started.
type ShutdownReply struct {
	NTasks int
}

// Argument passed when a worker registers with the master
type RegisterArgs struct {
	Worker string // Worker's UNIX-domain socket name, i.e. its RPC address
}

// Sends an RPC to the `rpcname` handler on server `srv` with arguments `args`,
// waits for the reply, and returns the reply in the `reply` struct.
// The `reply` argument should be the address of a reply structure.
//
// It returns `true` if the server responded, and `false“ if it wasn't able to contact the server.
// In particular, reply's contents are valid if and only if `call()` returned true.
//
// Please use it to send all RPCs. You can assume that the function would timeout after a while if the server doesn't respond.
func call(srv string, rpcname string, args any, reply any) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("Error in calling the RPC: %v", err)
	return false
}
